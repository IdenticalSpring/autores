"""Kiểm chứng `delete_report` bằng ARTIFACT thật (không chỉ response).

Quy trình:
1. Seed dữ liệu giả: Mongo `user_documents.reports_refs`, `BotGraph.BotCheckpoint`,
   `BotGraph.BotWrite`, Redis key `*:<thread_id>:*`, Qdrant `<doc_id>` và `<doc_id>_user`.
2. Xác nhận tiền trạng thái (tất cả phải TỒN TẠI).
3. Publish event `delete_report` thật lên Kafka write topic.
4. Chờ response và đối chiếu.
5. Xác nhận hậu trạng thái (tất cả phải BIẾN MẤT).

Exit code: 0 = mọi assert PASS, 1 = có assert FAIL, 2 = timeout.

Chạy trong container:
    docker exec autoresearching-ai-dev python scripts/verify_delete_report_artifacts.py
"""

import asyncio
import json
import os
import sys
import time
import uuid

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: E402

import topics  # noqa: E402
from document_setup.src.configs.app import settings  # noqa: E402
from utils import get_mongodb_client, get_redis_client  # noqa: E402

TIMEOUT = int(os.getenv("VERIFY_TIMEOUT", "120"))


def _failures(results: list[tuple[str, bool, str]]) -> int:
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name} — {detail}")
    return 0 if all(ok for _, ok, _ in results) else 1


async def _qdrant_status(name: str) -> int:
    url = f"{settings.QDRANT_URL}/collections/{name}"
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(url)
        return response.status_code


async def _qdrant_create(name: str) -> None:
    url = f"{settings.QDRANT_URL}/collections/{name}"
    body = {"vectors": {"size": 4, "distance": "Dot"}}
    async with httpx.AsyncClient(timeout=15.0) as client:
        await client.put(url, json=body)


async def _seed(mongo, redis, doc_id: str, thread_id: str) -> None:
    await mongo["user_documents"]["reports_refs"].replace_one(
        {"_id": doc_id}, {"_id": doc_id, "refs": ["seed"], "seeded_at": int(time.time())}, upsert=True
    )
    await mongo["BotGraph"]["BotCheckpoint"].replace_one(
        {"thread_id": thread_id}, {"thread_id": thread_id, "seed": True}, upsert=True
    )
    await mongo["BotGraph"]["BotWrite"].replace_one(
        {"thread_id": thread_id}, {"thread_id": thread_id, "seed": True}, upsert=True
    )
    await redis.set(f"checkpoint:{thread_id}:state", "seeded", ex=600)
    if await _qdrant_status(doc_id) != 200:
        await _qdrant_create(doc_id)
    if await _qdrant_status(f"{doc_id}_user") != 200:
        await _qdrant_create(f"{doc_id}_user")


async def _snapshot(mongo, redis, doc_id: str, thread_id: str) -> dict:
    reports_refs = await mongo["user_documents"]["reports_refs"].count_documents({"_id": doc_id})
    checkpoint = await mongo["BotGraph"]["BotCheckpoint"].count_documents({"thread_id": thread_id})
    writes = await mongo["BotGraph"]["BotWrite"].count_documents({"thread_id": thread_id})
    redis_keys = [k async for k in redis.scan_iter(match=f"*:{thread_id}:*")]
    return {
        "mongo_reports_refs": reports_refs,
        "mongo_checkpoint": checkpoint,
        "mongo_write": writes,
        "redis_keys": len(redis_keys),
        "qdrant_doc": await _qdrant_status(doc_id),
        "qdrant_doc_user": await _qdrant_status(f"{doc_id}_user"),
    }


async def _publish_and_wait(payload: dict, ids: set[str]) -> dict | None:
    from document_setup.src.configs.app import settings as app_settings

    consumer = AIOKafkaConsumer(
        topics.WRITE_SECTION_RESPONSE,
        bootstrap_servers=app_settings.KAFKA,
        group_id=f"verify_delete_{uuid.uuid4().hex[:6]}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(bootstrap_servers=app_settings.KAFKA)
    await consumer.start()
    await producer.start()
    try:
        for _ in range(100):
            await consumer.getmany(timeout_ms=200)
            if consumer.assignment():
                break
        start = time.time()
        await producer.send_and_wait(
            topics.WRITE_SECTION_REQUEST, json.dumps(payload).encode(), key=str(next(iter(ids))).encode()
        )
        while time.time() - start < TIMEOUT:
            batches = await consumer.getmany(timeout_ms=2000)
            for records in batches.values():
                for record in records:
                    try:
                        message = json.loads(record.value)
                    except json.JSONDecodeError:
                        continue
                    if not ids & {str(message.get(f)) for f in ("document_id", "session_id") if message.get(f)}:
                        continue
                    message["_elapsed_s"] = round(time.time() - start, 1)
                    return message
        return None
    finally:
        await producer.stop()
        await consumer.stop()


async def run() -> int:
    doc_id = f"verify-del-doc-{uuid.uuid4().hex[:6]}"
    thread_id = f"verify-del-thread-{uuid.uuid4().hex[:6]}"
    mongo = get_mongodb_client()
    redis = get_redis_client()
    results: list[tuple[str, bool, str]] = []

    await _seed(mongo, redis, doc_id, thread_id)
    before = await _snapshot(mongo, redis, doc_id, thread_id)
    print("[verify] BEFORE:", json.dumps(before, ensure_ascii=False))
    seeded_ok = all(
        [
            before["mongo_reports_refs"] == 1,
            before["mongo_checkpoint"] == 1,
            before["mongo_write"] == 1,
            before["redis_keys"] >= 1,
            before["qdrant_doc"] == 200,
            before["qdrant_doc_user"] == 200,
        ]
    )
    results.append(("seed dữ liệu giả", seeded_ok, json.dumps(before, ensure_ascii=False)))
    if not seeded_ok:
        return _failures(results)

    payload = {
        "event_type": "delete_report",
        "user_id": "smoke-user",
        "session_id": f"verify-session-{uuid.uuid4().hex[:6]}",
        "document_id": [doc_id],
        "thread_id": [thread_id],
    }
    message = await _publish_and_wait(payload, {doc_id, payload["session_id"]})
    if message is None:
        results.append(("nhận response delete_report", False, f"timeout {TIMEOUT}s"))
        return _failures(results)

    results.append(
        (
            "response delete_report",
            not message.get("error_code"),
            f"error_code={message.get('error_code')} elapsed={message['_elapsed_s']}s",
        )
    )

    after = await _snapshot(mongo, redis, doc_id, thread_id)
    print("[verify] AFTER:", json.dumps(after, ensure_ascii=False))
    results.append(("Mongo reports_refs đã xoá", after["mongo_reports_refs"] == 0, f"count={after['mongo_reports_refs']}"))
    results.append(("Mongo BotCheckpoint đã xoá", after["mongo_checkpoint"] == 0, f"count={after['mongo_checkpoint']}"))
    results.append(("Mongo BotWrite đã xoá", after["mongo_write"] == 0, f"count={after['mongo_write']}"))
    results.append(("Redis checkpoint đã xoá", after["redis_keys"] == 0, f"keys={after['redis_keys']}"))
    results.append(("Qdrant collection chính đã xoá", after["qdrant_doc"] == 404, f"http={after['qdrant_doc']}"))
    results.append(("Qdrant collection _user đã xoá", after["qdrant_doc_user"] == 404, f"http={after['qdrant_doc_user']}"))

    return _failures(results)


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
