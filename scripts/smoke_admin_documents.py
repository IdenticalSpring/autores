"""Smoke test event `admin_documents` qua Kafka (chạy trong container AI).

Mục đích: xác minh SAU DEPLOY rằng AI **luôn** publish response và response
**echo đủ identifier** (event_type, user_id, document_id, session_id) — tương ứng
sự cố 2026-10-23 (3 request admin_documents không có response).

Cách chạy (trong container, nơi có env Kafka/MinIO):

    python scripts/smoke_admin_documents.py                    # topic *_local
    python scripts/smoke_admin_documents.py --prod             # topic thật
    python scripts/smoke_admin_documents.py --path autoresearching/sample/sample.pdf
    python scripts/smoke_admin_documents.py --timeout 600

Exit code: 0 = nhận response hợp lệ; 1 = timeout hoặc response thiếu identifier.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import uuid

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: E402

from document_setup.src.configs.app import settings  # noqa: E402
from event_type import DocumentSetupEvent  # noqa: E402
import topics  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ECHO_REQUIRED = ("event_type", "user_id", "document_id", "session_id")


def build_payload(pdf_path: str, model_id: str, user_id: str, document_id: str, session_id: str) -> dict:
    return {
        "event_type": DocumentSetupEvent.ADMIN_DOCUMENTS.value,
        "model_id": model_id,
        "user_id": user_id,
        "document_id": document_id,
        "session_id": session_id,
        "language": "vi",
        "documents": [
            {
                "title": "Smoke admin documents",
                "url": pdf_path,
                "source": "ADMIN",
                "ref_id": f"smoke-{uuid.uuid4().hex[:8]}",
            }
        ],
    }


def matches(message: dict, document_id: str, session_id: str) -> bool:
    """Chỉ nhận response của lần smoke này (tránh message của job khác)."""
    return message.get("document_id") == document_id or message.get("session_id") == session_id


async def run(args) -> int:
    request_topic = topics.DOCUMENT_SETUP_REQUEST if args.prod else topics.DOCUMENT_SETUP_REQUEST_LOCAL
    response_topic = topics.DOCUMENT_SETUP_RESPONSE if args.prod else topics.DOCUMENT_SETUP_RESPONSE_LOCAL
    suffix = uuid.uuid4().hex[:8]
    session_id = str(uuid.uuid4())
    document_id = f"smoke-admin-{suffix}"
    payload = build_payload(args.path, args.model, "smoke-user", document_id, session_id)

    logger.info("=" * 70)
    logger.info("SMOKE admin_documents")
    logger.info(f"  broker        : {settings.KAFKA}")
    logger.info(f"  request topic : {request_topic}")
    logger.info(f"  response topic: {response_topic}")
    logger.info(f"  document_id   : {document_id}")
    logger.info(f"  session_id    : {session_id}")
    logger.info(f"  file          : {args.path}")
    logger.info(f"  model_id      : {args.model}")
    logger.info("=" * 70)

    consumer = AIOKafkaConsumer(
        response_topic,
        bootstrap_servers=settings.KAFKA,
        group_id=f"smoke-admin-docs-{suffix}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA)
    await consumer.start()
    await producer.start()
    try:
        await consumer.getmany(timeout_ms=500)  # dọn message cũ
        await producer.send_and_wait(request_topic, json.dumps(payload).encode())
        logger.info("Đã publish request, chờ response...")

        deadline = asyncio.get_event_loop().time() + args.timeout
        while asyncio.get_event_loop().time() < deadline:
            batches = await consumer.getmany(timeout_ms=1000)
            for records in batches.values():
                for record in records:
                    try:
                        message = json.loads(record.value)
                    except json.JSONDecodeError:
                        continue
                    if not matches(message, document_id, session_id):
                        continue
                    logger.info("RESPONSE: %s", json.dumps(message, ensure_ascii=False, indent=2))
                    missing = [key for key in ECHO_REQUIRED if not message.get(key)]
                    if missing:
                        logger.error("FAIL: response thiếu identifier %s", missing)
                        return 1
                    if message.get("error_code"):
                        logger.warning("Response là lỗi (error_code=%s): %s",
                                       message["error_code"], message.get("error"))
                        if int(message["error_code"]) == 401:
                            logger.warning(
                                "Gợi ý 401: kiểm tra Redis AI_KEY:%s và env LOCALLLM_BASE_URL "
                                "(model 'localhost') — 401 KHÔNG nhất thiết là bug code AI.",
                                args.model,
                            )
                    else:
                        logger.info("OK: processed_docs=%d", len(message.get("processed_docs") or []))
                    return 0

        logger.error("FAIL: không nhận response trong %ss (BE sẽ coi là timeout)", args.timeout)
        return 1
    finally:
        await producer.stop()
        await consumer.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test admin_documents qua Kafka")
    parser.add_argument("--prod", action="store_true", help="Dùng topic thật thay vì *_local")
    parser.add_argument("--path", default="autoresearching/sample/sample.pdf",
                        help="MinIO path dạng bucket/key của file mẫu")
    parser.add_argument("--timeout", type=int, default=300, help="Giây chờ response")
    parser.add_argument("--model", default=os.getenv("SMOKE_MODEL_ID", "localhost"),
                        help="model_id gửi trong payload (dùng đúng model BE đang dùng, "
                             "vd gemini-3.5-flash-lite). Có thể set env SMOKE_MODEL_ID.")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
