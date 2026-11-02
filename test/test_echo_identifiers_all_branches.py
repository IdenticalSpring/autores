"""F10/QA-3 — MỌI response Kafka phải echo `event_type` + identifier.

Incident 2026-11-01: response success của `ai_in_doc` thiếu `session_id`, nhánh
lỗi sớm của `enhance` thiếu cả `event_type` ⇒ harness BE chỉ thấy "timeout" dù
AI đã xử lý xong. Fix gốc: echo ở tầng `send_message` (contextvars) để không
nhánh nào có thể quên.
"""

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import listener as listener_module  # noqa: E402
from listener import BaseAsyncListener, IDENTIFIER_FIELDS  # noqa: E402


class _FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, key, value):
        self.sent.append({"topic": topic, "key": key, "value": value})


class _FakeRedis:
    def __init__(self):
        self.sadd_calls = []

    async def sadd(self, key, value):
        self.sadd_calls.append((key, value))
        return 1

    async def get(self, key):
        return None


class _FakeRecord:
    def __init__(self, value: dict):
        self.value = json.dumps(value).encode()


@pytest.fixture(autouse=True)
def _reset_context():
    BaseAsyncListener.clear_request_context()
    yield
    BaseAsyncListener.clear_request_context()


# --------------------------------------------------------------------------- #
# Tầng publish: send_message luôn echo
# --------------------------------------------------------------------------- #
def test_identifier_fields_cover_five_keys():
    assert IDENTIFIER_FIELDS == ("user_id", "document_id", "session_id", "conv_id", "data_id")


def test_send_message_echoes_all_identifiers_from_context():
    BaseAsyncListener.set_request_context(
        {
            "event_type": "ai_in_doc",
            "user_id": "u1",
            "document_id": "d1",
            "session_id": "s1",
            "conv_id": "c1",
            "data_id": "x1",
        }
    )
    producer = _FakeProducer()
    message = {"response": "ok"}

    import asyncio

    asyncio.run(BaseAsyncListener.send_message(BaseAsyncListener, producer, "topic", message))

    assert message["event_type"] == "ai_in_doc"
    for key in IDENTIFIER_FIELDS:
        assert message[key], f"thiếu {key} trong response"
    assert producer.sent[0]["value"] is message


def test_send_message_never_overrides_existing_values():
    BaseAsyncListener.set_request_context({"session_id": "from-request", "event_type": "write_content"})
    producer = _FakeProducer()
    message = {"session_id": "from-handler", "event_type": "chuyen_de_1"}

    import asyncio

    asyncio.run(BaseAsyncListener.send_message(BaseAsyncListener, producer, "topic", message))

    assert message["session_id"] == "from-handler"
    assert message["event_type"] == "chuyen_de_1"


def test_send_message_without_context_is_noop():
    producer = _FakeProducer()
    message = {"error_code": 603}

    import asyncio

    asyncio.run(BaseAsyncListener.send_message(BaseAsyncListener, producer, "topic", message))

    assert message == {"error_code": 603}


def test_set_request_context_from_record_parses_payload():
    BaseAsyncListener._set_request_context_from_record(
        _FakeRecord({"event_type": "gen_slides", "document_id": "d9", "session_id": "s9"})
    )
    ctx = BaseAsyncListener.current_request_context()
    assert ctx["event_type"] == "gen_slides"
    assert ctx["document_id"] == "d9"


def test_set_request_context_from_bad_json_clears_context():
    BaseAsyncListener.set_request_context({"document_id": "old"})

    class _BadRecord:
        value = b"not-json"

    BaseAsyncListener._set_request_context_from_record(_BadRecord())
    assert BaseAsyncListener.current_request_context() == {}


# --------------------------------------------------------------------------- #
# Tầng listener: nhánh lỗi sớm vẫn echo (enhance & _finalize_task)
# --------------------------------------------------------------------------- #
def _make_listener(cls):
    """Listener tối thiểu: redis giả (để send_message ghi DOCUMENT_REPORT) — KHÔNG
    thay `send_message` để test đúng đường echo thật."""
    obj = object.__new__(cls)
    obj.redis = _FakeRedis()
    obj.mongo_client = object()
    obj.redis_client = object()

    async def fake_task_timer(*args, **kwargs):
        return None

    obj.task_timer = fake_task_timer
    return obj


def test_finalize_task_echoes_identifiers(monkeypatch):
    from enhance_listener import EnhancementEventListener

    monkeypatch.setattr(listener_module, "send_health_check", _noop_health_check, raising=False)

    obj = _make_listener(EnhancementEventListener)
    BaseAsyncListener.set_request_context(
        {"event_type": "ai_in_doc", "user_id": "u1", "document_id": "d1", "session_id": "s1"}
    )
    producer = _FakeProducer()

    import asyncio

    asyncio.run(
        obj._finalize_task(None, False, "d1", producer, "topic", {"error_code": 670}, "ai_in_doc")
    )

    message = producer.sent[0]["value"]
    assert message["event_type"] == "ai_in_doc"
    assert message["session_id"] == "s1"
    assert message["document_id"] == "d1"


async def _noop_health_check(*args, **kwargs):
    return None


def test_enhance_missing_field_response_keeps_event_type_and_ids(monkeypatch):
    import enhance_listener as mod
    from enhance_listener import EnhancementEventListener

    producer = _FakeProducer()

    async def fake_get_producer():
        return producer

    monkeypatch.setattr(mod, "get_producer", fake_get_producer)
    monkeypatch.setattr(listener_module, "send_health_check", _noop_health_check, raising=False)

    obj = _make_listener(EnhancementEventListener)

    import asyncio

    payload = json.dumps(
        {"event_type": "ai_in_doc", "document_id": "d1", "user_id": "u1", "session_id": "s1"}
    )
    asyncio.run(obj._handle_enhancement_event("topic", payload))

    message = producer.sent[0]["value"]
    assert message["error_code"] == 603
    assert message["event_type"] == "ai_in_doc"
    assert message["session_id"] == "s1"
