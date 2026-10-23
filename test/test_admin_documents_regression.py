"""Regression tests cho sự cố `admin_documents` không trả response (2026-10-23).

Chạy TRONG image AI (cần ý langgraph/minio/pandas...):

    docker run --rm -v "$PWD":/app -w /app autoresearching-ai:latest \
        python test/test_admin_documents_regression.py
    # hoặc: python -m pytest -q test/test_admin_documents_regression.py

Bảo vệ 4 hành vi:
1. `documents[]` nhận `source` dạng path MinIO (không chỉ `url`).
2. Không còn "thành công rỗng": không có document hợp lệ ⇒ AIERROR 618.
3. MỌI response lỗi (603/604/618...) đều echo user_id/document_id/session_id/event_type.
4. Exception thoát khỏi handler vẫn publish response (không im lặng).
"""

import asyncio
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import document_setup_listener as listener_module  # noqa: E402
from document_setup.src.modules.ingest_docs import VectorDBProcessor  # noqa: E402
from document_setup_listener import DocumentSetupEventListener  # noqa: E402
from exception_type import AIERROR  # noqa: E402

BE_PAYLOAD_DOC = {
    "title": "Tài liệu mẫu Task 19",
    "source": "autoresearching/sample/sample.pdf",
    "ref_id": "sample-postman-test-002",
}
BE_PAYLOAD = {
    "event_type": "admin_documents",
    "model_id": "localhost",
    "user_id": "7eb53376-0000-0000-0000-000000000000",
    "document_id": "admin-doc",
    "session_id": "a7a7ac28-462b-4505-bbb9-689e7a297e83",
    "language": "vi",
    "documents": [BE_PAYLOAD_DOC],
}


# --------------------------------------------------------------------------- #
# 1. Normalize schema documents[]
# --------------------------------------------------------------------------- #
def test_normalize_admin_doc_uses_source_as_url():
    normalized = VectorDBProcessor._normalize_admin_doc(BE_PAYLOAD_DOC)
    assert normalized["url"] == "autoresearching/sample/sample.pdf"
    assert normalized["source"] == "ADMIN"
    assert normalized["ref_id"] == "sample-postman-test-002"


def test_normalize_admin_doc_keeps_label_source_and_url():
    normalized = VectorDBProcessor._normalize_admin_doc(
        {"title": "x", "url": "autoresearching/a/x.pdf", "source": "USER", "ref_id": "r1"}
    )
    assert normalized["url"] == "autoresearching/a/x.pdf"
    assert normalized["source"] == "USER"


# --------------------------------------------------------------------------- #
# 2. Không có document hợp lệ ⇒ 618 (trước đây trả processed_docs: [] im lặng)
# --------------------------------------------------------------------------- #
def test_all_vector_processor_calls_pass_model_id():
    """Không được bỏ model_id: thiếu ⇒ processor dùng default gpt-4.1-mini ⇒ lỗi 401."""
    source = (ROOT / "document_setup_listener.py").read_text(encoding="utf-8")
    missing = [
        line.strip()
        for line in source.splitlines()
        if "VectorDBProcessor(" in line and "model_id" not in line
    ]
    assert missing == [], "Thiếu model_id khi tạo VectorDBProcessor: " + str(missing)


def test_ingest_admin_docs_raises_618_when_no_valid_docs():
    processor = object.__new__(VectorDBProcessor)
    processor.input_tokens = 0
    processor.output_tokens = 0
    try:
        asyncio.run(processor.ingest_admin_docs([{"title": "no-path", "ref_id": "r1"}], None))
    except AIERROR as error:
        assert error.status_code == 618, error.status_code
        assert "No valid documents" in error.message
    else:
        raise AssertionError("ingest_admin_docs must raise AIERROR 618 when no item has a path")


# --------------------------------------------------------------------------- #
# Helpers cho listener
# --------------------------------------------------------------------------- #
class FakeProducer:
    async def send_and_wait(self, *args, **kwargs):
        return None


def build_listener(captured):
    listener = object.__new__(DocumentSetupEventListener)

    async def fake_finalize_task(timer_task, success, _id, producer, topic, message, event_type, is_suspended=False):
        captured.append(dict(message))

    async def fake_task_timer(*args, **kwargs):
        return None

    async def fake_get_key(*args, **kwargs):
        return "", "", ""

    listener._finalize_task = fake_finalize_task
    listener.task_timer = fake_task_timer
    listener.get_key = fake_get_key
    return listener


def run_handler(payload, capture_exception=False):
    captured = []
    listener = build_listener(captured)

    async def fake_get_producer():
        return FakeProducer()

    original = listener_module.get_producer
    listener_module.get_producer = fake_get_producer
    try:
        if capture_exception:
            async def boom(*args, **kwargs):
                raise RuntimeError("boom inside handler")

            listener._handle_document_setup_event = boom
            record = type("Record", (), {"topic": "document_setup_request_local",
                                         "value": json.dumps(payload).encode()})()
            asyncio.run(listener._handle_record(record))
        else:
            asyncio.run(
                listener._handle_document_setup_event("document_setup_response_local", json.dumps(payload))
            )
    finally:
        listener_module.get_producer = original
    return captured


# --------------------------------------------------------------------------- #
# 3. Echo identifier trong response lỗi (603 khi documents rỗng)
# --------------------------------------------------------------------------- #
def test_admin_documents_validation_error_echoes_identifiers():
    payload = dict(BE_PAYLOAD)
    payload["documents"] = []
    captured = run_handler(payload)
    assert captured, "phải có response dù payload sai"
    message = captured[-1]
    assert message["error_code"] == 603
    for identifier in ("event_type", "user_id", "document_id", "session_id", "model_id"):
        assert message.get(identifier) == payload[identifier], f"thiếu echo {identifier}"


def test_missing_documents_field_echoes_identifiers():
    payload = {k: v for k, v in BE_PAYLOAD.items() if k != "documents"}
    captured = run_handler(payload)
    message = captured[-1]
    assert message["error_code"] == 603
    assert message["document_id"] == payload["document_id"]
    assert message["session_id"] == payload["session_id"]


# --------------------------------------------------------------------------- #
# 4. Exception thoát khỏi handler ⇒ vẫn publish response
# --------------------------------------------------------------------------- #
def test_unhandled_exception_still_publishes_response():
    captured = run_handler(BE_PAYLOAD, capture_exception=True)
    assert captured, "exception không được phép làm mất response"
    message = captured[-1]
    assert message["event_type"] == "admin_documents"
    assert message["error_code"] == 618
    assert "boom inside handler" in message["error"]
    assert message["user_id"] == BE_PAYLOAD["user_id"]
    assert message["document_id"] == BE_PAYLOAD["document_id"]
    assert message["session_id"] == BE_PAYLOAD["session_id"]


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

    print(f"\n{len(checks) - failures}/{len(checks)} admin_documents checks passed")
    raise SystemExit(1 if failures else 0)
