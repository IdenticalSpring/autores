"""Smoke toàn bộ event Kafka của AI server (kiểm chứng echo + độ phủ).

Chạy trong container:
    python scripts/smoke_all_events.py --model deepseek-flash \
        --out reports/smoke-$(date +%F).json

Đặc điểm:
- Lặp qua **mọi** event trong `event_type.py`, dùng fixture `scripts/payloads/<event>.json`.
- Mỗi event: gắn identifier riêng, publish, chờ response trên đúng response topic.
- **Assert contract F10**: response phải có `event_type` + identifier khớp request.
- Event cần Mongo/MinIO/Qdrant fixture thật được đánh dấu SKIP (trừ khi `--include-infra`).
- Exit code: 0 = không có FAIL, 1 = có FAIL.
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

from document_setup.src.configs.app import settings  # noqa: E402
import topics  # noqa: E402
from error_type import TIME_OUT_DICT  # noqa: E402
from event_type import AIEvent, DocumentSetupEvent, EnhancementEvent, WriteEvent  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
PAYLOAD_DIR = ROOT / "scripts" / "payloads"

GROUP_ROUTES = {
    "document_setup": (topics.DOCUMENT_SETUP_REQUEST, topics.DOCUMENT_SETUP_RESPONSE),
    "chatbot": (topics.CHATBOT_REQUEST, topics.CHATBOT_RESPONSE),
    "enhancement": (topics.ENHANCEMENT_REQUEST, topics.ENHANCEMENT_RESPONSE),
    "write": (topics.WRITE_SECTION_REQUEST, topics.WRITE_SECTION_RESPONSE),
}

EVENT_TO_GROUP: dict[str, str] = {}
for _enum, _group in (
    (DocumentSetupEvent, "document_setup"),
    (AIEvent, "chatbot"),
    (EnhancementEvent, "enhancement"),
    (WriteEvent, "write"),
):
    for _event in _enum:
        EVENT_TO_GROUP[_event.value] = _group

# Event cần dữ liệu thật trong Mongo/MinIO/Qdrant/Postgres ⇒ chỉ chạy khi có seed.
NEEDS_INFRA = {
    "admin_documents",
    "user_guide_documents",
    "user_documents",
    "embed_user_documents",
    "search_user_documents",
    "update_user_refs",
    "delete_documents",
    "chuyen_de_1",
    "chuyen_de_2",
    "chuyen_de_3",
    "translate_report",
    "parsed_value",
    "propose_method",
    "comment_data",
    "check_logic_data",
    "writer",
}

ID_FIELDS = ("document_id", "conv_id", "session_id", "section_id")
DEFAULT_TIMEOUT = 180


def build_events(only: list[str] | None) -> list[str]:
    events = sorted(EVENT_TO_GROUP)
    if only:
        wanted = set(only)
        unknown = wanted - set(events)
        if unknown:
            raise SystemExit(f"Event không tồn tại: {sorted(unknown)}")
        return [e for e in events if e in wanted]
    return events


def load_payload(event: str, model: str, run_id: str) -> dict | None:
    path = PAYLOAD_DIR / f"{event}.json"
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["event_type"] = event
    if model:
        payload["model_id"] = model
    # Mỗi lần chạy phải có identifier riêng, nếu không consumer (auto_offset_reset=earliest)
    # sẽ đọc lại response cũ của lần chạy trước và báo PASS/FAIL sai (elapsed ~0.0s).
    # `delete_report` dùng document_id/thread_id dạng list[str] → xử lý riêng.
    for field_name in ("document_id", "conv_id", "session_id"):
        value = payload.get(field_name)
        if isinstance(value, list):
            payload[field_name] = [
                f"{item}-{run_id}" if isinstance(item, str) else item for item in value
            ]
        elif value:
            payload[field_name] = f"{value}-{run_id}"
        else:
            payload[field_name] = f"smoke-{event}-{run_id}"
    if isinstance(payload.get("thread_id"), list):
        payload["thread_id"] = [
            f"{item}-{run_id}" if isinstance(item, str) else item
            for item in payload["thread_id"]
        ]
    if not payload.get("section_id"):
        payload["section_id"] = f"section-{run_id}"
    return payload


async def wait_for_assignment(consumer: AIOKafkaConsumer) -> None:
    for _ in range(100):
        await consumer.getmany(timeout_ms=200)
        if consumer.assignment():
            return



async def run_event(
    consumer: AIOKafkaConsumer,
    producer: AIOKafkaProducer,
    event: str,
    payload: dict,
    timeout: float,
) -> dict:
    group = EVENT_TO_GROUP[event]
    request_topic, _ = GROUP_ROUTES[group]
    ids = {str(payload[f]) for f in ID_FIELDS if payload.get(f)}
    started = time.time()
    await producer.send_and_wait(
        request_topic, json.dumps(payload).encode(), key=str(next(iter(ids))).encode()
    )
    result = {
        "event": event,
        "group": group,
        "status": "TIMEOUT",
        "elapsed_s": None,
        "error_code": None,
        "error": None,
        "echo_ok": False,
        "missing_fields": [],
    }
    while time.time() - started < timeout:
        batches = await consumer.getmany(timeout_ms=2000)
        for records in batches.values():
            for record in records:
                try:
                    message = json.loads(record.value)
                except json.JSONDecodeError:
                    continue
                if not ids & {str(message.get(f)) for f in ID_FIELDS if message.get(f)}:
                    continue
                missing = []
                if not message.get("event_type"):
                    missing.append("event_type")
                # Chỉ yêu cầu echo những identifier thuộc contract response
                # (`section_id` không nằm trong contract: BE dùng để route, AI không echo).
                for field_name in ("document_id", "conv_id", "session_id", "data_id"):
                    if payload.get(field_name) and not message.get(field_name):
                        missing.append(field_name)
                result.update(
                    {
                        "status": "FAIL" if message.get("error_code") else "PASS",
                        "elapsed_s": round(time.time() - started, 1),
                        "error_code": message.get("error_code"),
                        "error": message.get("error"),
                        "echo_ok": not missing,
                        "missing_fields": missing,
                    }
                )
                return result
    result["elapsed_s"] = round(time.time() - started, 1)
    return result



async def main(args) -> int:
    run_id = uuid.uuid4().hex[:6]
    events = build_events(args.only)
    results: list[dict] = []
    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA)
    await producer.start()
    consumers: dict[str, AIOKafkaConsumer] = {}
    try:
        for group, (_, response_topic) in GROUP_ROUTES.items():
            consumer = AIOKafkaConsumer(
                response_topic,
                bootstrap_servers=settings.KAFKA,
                group_id=f"smoke_all_{group}_{run_id}",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
            )
            await consumer.start()
            await wait_for_assignment(consumer)
            consumers[group] = consumer

        for index, event in enumerate(events, start=1):
            print(f"[{index}/{len(events)}] {event} ...", flush=True)
            if event in NEEDS_INFRA and not args.include_infra:
                results.append(
                    {
                        "event": event,
                        "group": EVENT_TO_GROUP[event],
                        "status": "SKIP",
                        "reason": "needs Mongo/MinIO/Qdrant fixture",
                        "echo_ok": False,
                    }
                )
                print("    SKIP (cần fixture hạ tầng)", flush=True)
                continue
            payload = load_payload(event, args.model, run_id)
            if payload is None:
                results.append(
                    {
                        "event": event,
                        "group": EVENT_TO_GROUP[event],
                        "status": "SKIP",
                        "reason": "missing payload fixture",
                        "echo_ok": False,
                    }
                )
                print("    SKIP (thiếu fixture)", flush=True)
                continue
            # `TIME_OUT_DICT = 0` là marker "listener tự xử lý timeout riêng"
            # (document_setup dùng 3600s cho 4 event file) ⇒ runner không được dùng 0.
            configured_timeout = TIME_OUT_DICT.get(event) or 0
            timeout = args.timeout if configured_timeout <= 0 else min(configured_timeout, args.timeout)
            result = await run_event(
                consumers[EVENT_TO_GROUP[event]], producer, event, payload, timeout
            )
            results.append(result)
            print(
                f"    {result['status']} {result.get('elapsed_s')}s "
                f"echo_ok={result['echo_ok']} missing={result['missing_fields']}",
                flush=True,
            )
    finally:
        for consumer in consumers.values():
            await consumer.stop()
        await producer.stop()

    totals = {"PASS": 0, "FAIL": 0, "SKIP": 0, "TIMEOUT": 0}
    for result in results:
        totals[result["status"]] = totals.get(result["status"], 0) + 1
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model_id": args.model or os.getenv("AI_DEFAULT_MODEL", ""),
        "totals": totals,
        "echo_violations": [
            r["event"] for r in results if r.get("echo_ok") is False and r["status"] != "SKIP"
        ],
        "results": results,
    }
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n[smoke_all] báo cáo: {out_path}")
    print(f"[smoke_all] totals={totals}")
    print(f"[smoke_all] echo_violations={report['echo_violations']}")
    return 1 if totals.get("FAIL") else 0


def parse_args():
    parser = argparse.ArgumentParser(description="Smoke toàn bộ event AI server")
    parser.add_argument("--only", nargs="*", default=None, help="Chỉ chạy các event này")
    parser.add_argument("--model", default=os.getenv("SMOKE_MODEL_ID", ""), help="Ghi đè model_id")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="Timeout mỗi event (giây)")
    parser.add_argument("--out", default="", help="Đường dẫn file JSON báo cáo")
    parser.add_argument("--include-infra", action="store_true", help="Chạy cả event cần fixture hạ tầng")
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(asyncio.run(main(parse_args())))
