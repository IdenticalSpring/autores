"""Smoke test tổng quát cho 1 event Kafka bất kỳ.

Ví dụ:
    python scripts/smoke_event.py --event get_suggestions \
        --payload scripts/payloads/get_suggestions.json --timeout 180

Tự sinh identifier nếu payload thiếu; ghi đè `model_id` bằng env `SMOKE_MODEL_ID`.
Exit code: 0 = nhận response không có error_code, 1 = response lỗi, 2 = timeout.
"""

import argparse
import asyncio
import json
import os
import sys
import time
import uuid

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: E402

from document_setup.src.configs.app import settings  # noqa: E402
import topics  # noqa: E402

from event_type import AIEvent, DocumentSetupEvent, EnhancementEvent, WriteEvent

# event_type -> (request topic, response topic)
ROUTES = {
    "document_setup": (topics.DOCUMENT_SETUP_REQUEST, topics.DOCUMENT_SETUP_RESPONSE),
    "chatbot": (topics.CHATBOT_REQUEST, topics.CHATBOT_RESPONSE),
    "enhancement": (topics.ENHANCEMENT_REQUEST, topics.ENHANCEMENT_RESPONSE),
    "write": (topics.WRITE_SECTION_REQUEST, topics.WRITE_SECTION_RESPONSE),
}

EVENT_TO_GROUP = {}
for _e in DocumentSetupEvent:
    EVENT_TO_GROUP[_e.value] = "document_setup"
for _e in AIEvent:
    EVENT_TO_GROUP[_e.value] = "chatbot"
for _e in EnhancementEvent:
    EVENT_TO_GROUP[_e.value] = "enhancement"
for _e in WriteEvent:
    EVENT_TO_GROUP[_e.value] = "write"

ID_FIELDS = ("document_id", "conv_id", "session_id", "section_id")


def _prepare_payload(args) -> dict:
    with open(args.payload, encoding="utf-8") as f:
        payload = json.load(f)

    payload["event_type"] = args.event
    identifier = f"smoke-{uuid.uuid4().hex[:8]}"
    for field_name in ID_FIELDS:
        if field_name in payload and not payload[field_name]:
            payload[field_name] = identifier
    if not any(payload.get(f) for f in ID_FIELDS):
        payload["document_id"] = identifier  # trường mặc định cho mọi service

    model_override = os.getenv("SMOKE_MODEL_ID")
    if model_override:
        payload["model_id"] = model_override
    return payload


async def run(args) -> int:
    payload = _prepare_payload(args)
    group = args.group
    if not group or group == "auto":
        group = EVENT_TO_GROUP.get(args.event, "document_setup")
    req_topic, res_topic = ROUTES[group]
    ids = {str(payload[f]) for f in ID_FIELDS if payload.get(f)}

    consumer = AIOKafkaConsumer(
        res_topic,
        bootstrap_servers=settings.KAFKA,
        group_id=f"smoke_event_{uuid.uuid4().hex[:6]}",
        # group luôn mới ⇒ đọc từ đầu để không lỡ response publish quá nhanh (race assignment)
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA)
    await consumer.start()
    await producer.start()

    # Ép consumer join group + nhận partition TRƯỚC khi publish.
    # (assignment chỉ có sau khi poll ít nhất 1 lần — nếu chỉ ngủ thì sẽ miss response)
    for _ in range(100):
        await consumer.getmany(timeout_ms=200)
        if consumer.assignment():
            break

    start = time.time()
    print(f"[smoke] event={args.event} req={req_topic} res={res_topic} ids={sorted(ids)}")
    try:
        await producer.send_and_wait(req_topic, json.dumps(payload).encode(), key=str(next(iter(ids))).encode())
        print("[smoke] đã publish, chờ response...")

        while time.time() - start < args.timeout:
            batches = await consumer.getmany(timeout_ms=2000)
            for records in batches.values():
                for record in records:
                    try:
                        message = json.loads(record.value)
                    except json.JSONDecodeError:
                        continue
                    if not ids & {str(message.get(f)) for f in ID_FIELDS if message.get(f)}:
                        continue
                    elapsed = time.time() - start
                    err_code = message.get("error_code")
                    summary = {
                        "event_type": message.get("event_type"),
                        "document_id": message.get("document_id"),
                        "error_code": err_code,
                        "error": message.get("error"),
                        "input_tokens": message.get("input_tokens"),
                        "output_tokens": message.get("output_tokens"),
                        "elapsed_s": round(elapsed, 1),
                        "keys": sorted(message.keys()),
                    }
                    print("[smoke] RESPONSE:", json.dumps(summary, ensure_ascii=False))
                    if args.show:
                        preview = json.dumps(message, ensure_ascii=False)
                        print(f"[smoke] PAYLOAD_PREVIEW({len(preview)} chars):", preview[: args.show])
                    return 0 if not err_code else 1
        print(f"[smoke] TIMEOUT sau {args.timeout}s")
        return 2
    finally:
        await producer.stop()
        await consumer.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test 1 event Kafka")
    parser.add_argument("--group", default="auto",
                        choices=["auto"] + sorted(ROUTES), help="Nhóm service nhận event (mặc định auto)")
    parser.add_argument("--event", required=True, help="event_type, ví dụ get_suggestions")
    parser.add_argument("--payload", required=True, help="File JSON payload")
    parser.add_argument("--timeout", type=int, default=180, help="Giây chờ response")
    parser.add_argument("--show", type=int, default=0,
                        help="In N ký tự đầu của message response để kiểm chứng artifact")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
