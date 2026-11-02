"""Seed hạ tầng cho 6 event file: user_documents → embed_user_documents → delete_documents,
kèm payload mẫu cho admin_documents / user_guide_documents / search_user_documents.

Vì sao cần: 16 event đang SKIP trong smoke vì thiếu dữ liệu thật trong MinIO/Qdrant/Mongo.
Script này:
  1. Kiểm tra file fixture tồn tại trên MinIO (định dạng `bucket/key`).
  2. Chạy chuỗi `user_documents` → `embed_user_documents` qua Kafka (dùng đúng AI server).
  3. Truy vấn Qdrant collection (= document_id) để lấy `uuids` cho `delete_documents`.
  4. Xuất `reports/infra-fixtures.json` để BE nạp cho `npm run e2e:seed-infra`.

Chạy trong container:
    python scripts/seed_infra_fixtures.py --model deepseek-flash \
        --out reports/infra-fixtures.json
"""

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: E402
from qdrant_client import QdrantClient  # noqa: E402

from document_setup.src.configs.app import settings  # noqa: E402
from utils import get_minio_client  # noqa: E402
import topics  # noqa: E402

REQUEST_TOPIC = topics.DOCUMENT_SETUP_REQUEST
RESPONSE_TOPIC = topics.DOCUMENT_SETUP_RESPONSE
ROOT = Path(__file__).resolve().parent.parent


def check_minio_object(bucket: str, key: str) -> dict:
    """Trả thông tin object; raise nếu không tồn tại để tránh seed rỗng."""
    client = get_minio_client()
    if not client.bucket_exists(bucket):
        raise SystemExit(f"Bucket '{bucket}' không tồn tại trên MinIO")
    try:
        stat = client.stat_object(bucket, key)
        return {"bucket": bucket, "key": key, "url": f"{bucket}/{key}", "size": stat.size}
    except Exception:
        sample = [
            obj.object_name
            for obj in client.list_objects(bucket, recursive=True)
        ][:10]
        raise SystemExit(
            f"Không thấy '{bucket}/{key}' trên MinIO. Object hiện có (mẫu): {sample}"
        )


def split_url(url: str) -> tuple[str, str]:
    bucket, _, key = url.partition("/")
    if not bucket or not key:
        raise SystemExit(f"url phải dạng 'bucket/key', nhận: {url!r}")
    return bucket, key


async def wait_for_assignment(consumer: AIOKafkaConsumer) -> None:
    for _ in range(100):
        await consumer.getmany(timeout_ms=200)
        if consumer.assignment():
            return


async def publish_and_wait(
    producer: AIOKafkaProducer,
    consumer: AIOKafkaConsumer,
    payload: dict,
    id_field: str,
    timeout: float,
) -> dict:
    identifier = payload[id_field]
    started = time.time()
    await producer.send_and_wait(
        REQUEST_TOPIC, json.dumps(payload).encode(), key=str(identifier).encode()
    )
    while time.time() - started < timeout:
        batches = await consumer.getmany(timeout_ms=2000)
        for records in batches.values():
            for record in records:
                try:
                    message = json.loads(record.value)
                except json.JSONDecodeError:
                    continue
                if message.get(id_field) != identifier:
                    continue
                if message.get("event_type") != payload["event_type"]:
                    continue
                return message
    raise SystemExit(
        f"Timeout {timeout}s khi chờ {payload['event_type']} ({id_field}={identifier})"
    )


def collect_uuids(document_id: str, limit: int = 1000) -> list[str]:
    """Lấy point id trong Qdrant collection của document (collection = document_id)."""
    client = QdrantClient(url=settings.QDRANT_URL)
    try:
        if not client.collection_exists(document_id):
            return []
        points, _ = client.scroll(
            collection_name=document_id, limit=limit, with_payload=False
        )
        return [str(point.id) for point in points]
    finally:
        client.close()


def build_payloads(args, fixture: dict) -> dict:
    """Payload chuẩn cho 6 case file (BE copy nguyên vào infra-fixtures)."""
    common = {
        "model_id": args.model,
        "user_id": args.user_id,
        "language": "Vietnamese",
    }
    doc_item = {
        "title": "AI fixture sample",
        "url": fixture["url"],
        "source": "USER",
        "authors": "AI fixture",
        "year": "2024",
        "journal": "AI fixture journal",
        "ref_id": "ai-fixture-ref-1",
    }
    return {
        "admin_documents": {
            **common,
            "event_type": "admin_documents",
            "documents": [
                {"url": fixture["url"], "title": "AI fixture sample", "source": "ADMIN"}
            ],
        },
        "user_guide_documents": {
            **common,
            "event_type": "user_guide_documents",
            "data_id": args.data_id,  # BẮT BUỘC (listener đọc trực tiếp)
            "documents_urls": [fixture["url"]],
        },
        "user_documents": {
            **common,
            "event_type": "user_documents",
            "document_id": args.document_id,
            "session_id": args.session_id,
            "data_id": args.data_id,
            "documents": [doc_item],
        },
        "search_user_documents": {
            **common,
            "event_type": "search_user_documents",
            "document_id": args.document_id,
            "session_id": args.session_id,
            "documents": [{"url": fixture["url"], "title": doc_item["title"], "source": "USER"}],
        },
        "embed_user_documents": {
            **common,
            "event_type": "embed_user_documents",
            "document_id": args.document_id,
            "session_id": args.session_id,
            "data_id": args.data_id,
            "processed_docs": [],  # điền từ response user_documents
        },
    }


async def run(args) -> int:
    run_id = uuid.uuid4().hex[:6]
    bucket, key = split_url(args.url)
    fixture = check_minio_object(bucket, key)
    print(f"[seed] MinIO OK: {fixture['url']} ({fixture['size']} bytes)")

    payloads = build_payloads(args, fixture)
    payloads["user_documents"]["document_id"] = f"{args.document_id}-{run_id}"
    payloads["user_documents"]["session_id"] = f"{args.session_id}-{run_id}"
    for event in ("embed_user_documents", "search_user_documents"):
        payloads[event]["document_id"] = payloads["user_documents"]["document_id"]
        payloads[event]["session_id"] = payloads["user_documents"]["session_id"]

    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA)
    consumer = AIOKafkaConsumer(
        RESPONSE_TOPIC,
        bootstrap_servers=settings.KAFKA,
        group_id=f"seed_infra_{run_id}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await producer.start()
    await consumer.start()
    try:
        await wait_for_assignment(consumer)

        print("[seed] user_documents ...", flush=True)
        ud_result = await publish_and_wait(
            producer, consumer, payloads["user_documents"], "document_id", args.timeout
        )
        if ud_result.get("error_code"):
            raise SystemExit(
                f"user_documents lỗi: {ud_result.get('error_code')} {ud_result.get('error')}"
            )
        processed_docs = ud_result.get("processed_docs") or []
        print(f"    OK: {len(processed_docs)} processed_docs")

        payloads["embed_user_documents"]["processed_docs"] = processed_docs
        print("[seed] embed_user_documents ...", flush=True)
        embed_result = await publish_and_wait(
            producer, consumer, payloads["embed_user_documents"], "document_id", args.timeout
        )
        if embed_result.get("error_code"):
            raise SystemExit(
                f"embed_user_documents lỗi: {embed_result.get('error_code')} {embed_result.get('error')}"
            )
        print(f"    OK: embed_tokens={embed_result.get('embed_tokens')}")

        document_id = payloads["user_documents"]["document_id"]
        uuids = collect_uuids(document_id)
        print(f"[seed] Qdrant collection '{document_id}': {len(uuids)} uuids")

        payloads["delete_documents"] = {
            "model_id": args.model,
            "user_id": args.user_id,
            "event_type": "delete_documents",
            "document_id": document_id,
            "session_id": payloads["user_documents"]["session_id"],
            "data_id": args.data_id,  # BẮT BUỘC
            "collection": document_id,
            "documents": [{"uuids": uuids}],
        }

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "model_id": args.model,
            "minio": fixture,
            "document_id": document_id,
            "session_id": payloads["user_documents"]["session_id"],
            "data_id": args.data_id,
            "uuids": uuids,
            "processed_docs_count": len(processed_docs),
            "payloads": payloads,
        }
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[seed] Đã ghi {out_path}")
        print(f"[seed] document_id={document_id} data_id={args.data_id} uuids={len(uuids)}")
        return 0
    finally:
        await consumer.stop()
        await producer.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="Seed fixture hạ tầng cho 6 event file")
    parser.add_argument("--model", default=os.getenv("AI_DEFAULT_MODEL", "deepseek-flash"))
    parser.add_argument("--url", default="autoresearching/sample/sample.pdf", help="bucket/key trên MinIO")
    parser.add_argument("--document-id", default="e2e-live-user-documents")
    parser.add_argument("--session-id", default="e2e-session-user-documents")
    parser.add_argument("--data-id", default="e2e-data-001")
    parser.add_argument("--user-id", default="e2e-user")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--out", default="reports/infra-fixtures.json")
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(asyncio.run(run(parse_args())))
