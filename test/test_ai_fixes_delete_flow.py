"""Runtime end-to-end cho `delete_report` (B1 best-effort + B1b cleanup).

Chạy TRONG image AI:

    docker run --rm -v "$PWD":/app -w /app autoresearching-ai:latest \
        python test/test_ai_fixes_delete_flow.py

Handler thật `_handle_write_section_event` được gọi với payload `delete_report`;
chỉ Mongo / Redis / Kafka producer / webhook được thay bằng fake.
"""

import asyncio
import json
import pathlib
import sys
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import listener as listener_module  # noqa: E402
import write_listener as write_listener_module  # noqa: E402
from write_listener import WriteSectionEventListener  # noqa: E402

DOCUMENT_IDS = ["docA", "docB"]
THREAD_IDS = ["t1", "t2"]


class FakeCursor:
    def __init__(self, keys):
        self._keys = list(keys)
        self._index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self._keys):
            raise StopAsyncIteration
        key = self._keys[self._index]
        self._index += 1
        return key


class FakeCleanupRedis:
    def __init__(self):
        self.scanned = []
        self.deleted = []

    def scan_iter(self, match=None):
        self.scanned.append(match)
        return FakeCursor([b"k1", b"k2"])

    async def delete(self, *keys):
        self.deleted.extend(keys)


class FakeCollection:
    def __init__(self, failed_ids=()):
        self.failed_ids = set(failed_ids)
        self.deleted = []
        self.checkpoint_queries = []

    async def find_one_and_delete(self, query):
        doc_id = query["_id"]
        if doc_id in self.failed_ids:
            raise RuntimeError(f"mongo failure for {doc_id}")
        self.deleted.append(doc_id)
        return {"_id": doc_id}

    async def delete_many(self, query):
        self.checkpoint_queries.append(query)
        return None


class FakeMongo:
    def __init__(self, failed_ids=()):
        self.reports_refs = FakeCollection(failed_ids)
        self.checkpoint = FakeCollection()
        self.writes = FakeCollection()

    def __getitem__(self, name):
        if name == "user_documents":
            return {"reports_refs": self.reports_refs}
        if name == "BotGraph":
            return {"BotCheckpoint": self.checkpoint, "BotWrite": self.writes}
        if name == "admin":
            return {"outlines": FakeCollection()}
        raise KeyError(name)


class _FakeHttpResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def run_delete_report(document_ids=DOCUMENT_IDS, thread_ids=THREAD_IDS, failed_ids=()):
    fake_mongo = FakeMongo(failed_ids)
    fake_redis = FakeCleanupRedis()
    captured = {"messages": []}

    listener = object.__new__(WriteSectionEventListener)
    listener.mongo_client = fake_mongo
    listener.redis_client = fake_redis

    async def fake_send_message(producer, topic, message, key=None):
        captured["messages"].append(dict(message))

    async def fake_task_timer(*args, **kwargs):
        return None

    async def fake_get_producer():
        return object()

    async def fake_send_health_check(*args, **kwargs):
        return None

    listener.send_message = fake_send_message
    listener.task_timer = fake_task_timer

    original_producer = write_listener_module.get_producer
    original_health_check = listener_module.send_health_check
    original_urlopen = urllib.request.urlopen

    write_listener_module.get_producer = fake_get_producer
    listener_module.send_health_check = fake_send_health_check
    urllib.request.urlopen = lambda request, timeout=None: _FakeHttpResponse()

    payload = json.dumps(
        {
            "event_type": "delete_report",
            "document_id": document_ids,
            "thread_id": thread_ids,
            "user_id": "u1",
            "session_id": "s1",
        }
    )
    try:
        asyncio.run(listener._handle_write_section_event("write_section_request", payload))
    finally:
        write_listener_module.get_producer = original_producer
        listener_module.send_health_check = original_health_check
        urllib.request.urlopen = original_urlopen

    return captured["messages"][-1], fake_mongo, fake_redis


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_b1_delete_report_all_success_runtime():
    message, fake_mongo, fake_redis = run_delete_report()
    assert sorted(fake_mongo.reports_refs.deleted) == ["docA", "docB"]
    assert "error_code" not in message
    assert "failed_ids" not in message
    # B1b: cleanup phải thực sự chạy cho cả 2 thread (mỗi thread xoá 2 key)
    assert fake_redis.scanned == ["*:t1:*", "*:t2:*"]
    assert fake_redis.deleted == [b"k1", b"k2", b"k1", b"k2"]


def test_b1_delete_report_partial_failure_runtime():
    message, fake_mongo, fake_redis = run_delete_report(failed_ids={"docB"})
    # best-effort: docA vẫn bị xoá, không dừng giữa đường
    assert fake_mongo.reports_refs.deleted == ["docA"]
    assert message["error_code"] == 640
    assert message["failed_ids"] == ["docB"]
    assert "docB" in message["error"]
    assert fake_redis.scanned == ["*:t1:*", "*:t2:*"]


def test_no_missing_await_clean_up_repo_wide():
    """Guard: mọi lời gọi `clean_up(...)` phải được `await` (B1b + chatbot_listener)."""
    offenders = []
    for path in sorted(ROOT.rglob("*.py")):
        if "__pycache__" in str(path) or "site-packages" in str(path):
            continue
        if "test" in path.parts:  # bỏ qua chính các file test (chứa chuỗi mẫu)
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line_no, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if "clean_up(" not in stripped:
                continue
            if stripped.startswith(("#", "def ", "async def ", '"""', "'''")):
                continue
            if "await self.clean_up(" in stripped:
                continue
            # generator được truyền vào asyncio.gather(...) cũng là await hợp lệ
            if stripped.startswith("*(self.clean_up("):
                continue
            offenders.append(f"{path.relative_to(ROOT)}:{line_no}: {stripped[:70]}")
    assert offenders == [], "clean_up() phải được await — offenders: " + " | ".join(offenders)


if __name__ == "__main__":
    failures = 0
    checks = [(name, func) for name, func in sorted(globals().items()) if name.startswith("test_")]
    for name, func in checks:
        try:
            func()
            print(f"PASS  {name}")
        except AssertionError as error:
            failures += 1
            print(f"FAIL  {name}: {error}")
        except Exception as error:  # noqa: BLE001
            failures += 1
            print(f"ERROR {name}: {type(error).__name__}: {error}")

    print(f"\n{len(checks) - failures}/{len(checks)} delete-flow checks passed")
    raise SystemExit(1 if failures else 0)
