"""Khoá hành vi cho 2 lỗi DeepSeek còn lại (incident 2026-11-01).

1. Response contract: mọi response phải có `event_type` + identifier
   (response success của `ai_in_doc` từng thiếu `session_id` → BE báo timeout).
2. Structured output: DeepSeek KHÔNG được bind `with_structured_output`
   (`json_schema` → 400 response_format; `function_calling` → 400 tool_choice),
   và thinking mode phải tắt mặc định.
"""

import asyncio
import sys
from pathlib import Path

from pydantic import BaseModel

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import get_llm_response as glr  # noqa: E402
from listener import BaseAsyncListener  # noqa: E402


class _Q(BaseModel):
    queries: list[str]


# --------------------------------------------------------------------------- #
# 1. Echo identifier
# --------------------------------------------------------------------------- #
def test_attach_identifiers_adds_session_and_event():
    msg = {"response": "ok"}
    BaseAsyncListener.attach_identifiers(
        msg,
        {"document_id": "d1", "session_id": "s1", "event_type": "ai_in_doc", "user_id": "u1"},
    )

    assert msg["session_id"] == "s1"
    assert msg["event_type"] == "ai_in_doc"
    assert msg["document_id"] == "d1"
    assert msg["user_id"] == "u1"


def test_attach_identifiers_never_overrides_existing_values():
    msg = {"session_id": "keep-me"}
    BaseAsyncListener.attach_identifiers(msg, {"session_id": "new-value"})

    assert msg["session_id"] == "keep-me"


def test_attach_identifiers_tolerates_bad_input():
    msg: dict = {}
    BaseAsyncListener.attach_identifiers(msg, None)      # type: ignore[arg-type]
    BaseAsyncListener.attach_identifiers(msg, {})        # ok, không thêm gì

    assert msg == {}


def test_all_three_listeners_call_attach_identifiers():
    for filename in ("enhance_listener.py", "write_listener.py", "document_setup_listener.py"):
        src = (ROOT / filename).read_text(encoding="utf-8")
        assert "self.attach_identifiers(message, value)" in src, (
            f"{filename} chưa echo identifier trong response"
        )


# --------------------------------------------------------------------------- #
# 2. DeepSeek: thinking disabled + không bind structured output
# --------------------------------------------------------------------------- #
def test_deepseek_disables_thinking_by_default():
    llm = glr.get_llm("deepseek-flash", "sk-test")

    extra = getattr(llm, "extra_body", None)
    if extra is None:
        extra = (getattr(llm, "model_kwargs", None) or {}).get("extra_body")
    assert extra == {"thinking": {"type": "disabled"}}, f"thinking phải tắt mặc định, nhận {extra}"


def test_deepseek_is_marked_structured_unsupported():
    llm = glr.get_llm("deepseek-flash", "sk-test")

    assert glr._is_structured_unsupported(llm) is True
    assert glr._is_structured_unsupported(glr.get_llm("deepseek-v4-pro", "sk-test")) is True


def test_deepseek_skips_structured_binding_and_uses_plain_json(monkeypatch):
    class _FakeLLM:
        model_name = "deepseek-flash"

        def with_structured_output(self, *args, **kwargs):
            raise AssertionError("Không được bind structured output cho deepseek")

    calls = {"n": 0}

    async def fake_plain(_llm, _messages, structure):
        calls["n"] += 1
        return structure(queries=["q1"]), 5, 2, '{"queries": ["q1"]}'

    monkeypatch.setattr(glr, "_plain_json_fallback", fake_plain)

    error, success, result, in_tok, out_tok = asyncio.run(
        glr.get_answer_with_schema("t-skip", _FakeLLM(), "sys", "content", _Q)
    )

    assert success is True and error is None
    assert result.queries == ["q1"]
    assert calls["n"] == 1, "phải đi plain-JSON đúng 1 lần, không retry structured"
    assert (in_tok, out_tok) == (5, 2)
