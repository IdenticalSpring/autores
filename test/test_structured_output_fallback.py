"""Test fallback parse khi `with_structured_output` trả `parsed = None`.

Bối cảnh: smoke `admin_documents` với `gemini-3.5-flash-lite` trả
`500 Structured output was empty for schema KeyPoints` dù model có trả nội dung.
`get_llm_response.get_answer_with_schema` nay parse lại raw content (JSON, kể cả
bọc ```json ... ```) trước khi bỏ cuộc.

Chạy trong image AI:
    python test/test_structured_output_fallback.py
    python -m pytest -q test/test_structured_output_fallback.py
"""

import pathlib
import sys
import types

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pydantic import BaseModel  # noqa: E402

import get_llm_response as llm_module  # noqa: E402


class DummySchema(BaseModel):
    main_topic: str
    importance: str


def response_with(content, parsed=None):
    return {"raw": types.SimpleNamespace(content=content), "parsed": parsed}


def test_fenced_json_is_parsed():
    content = '```json\n{"main_topic": "AI in research", "importance": "faster"}\n```'
    result = llm_module._parse_structured_fallback(response_with(content), DummySchema)
    assert isinstance(result, DummySchema)
    assert result.main_topic == "AI in research"


def test_json_inside_prose_is_parsed():
    content = 'Here is the JSON:\n{"main_topic": "X", "importance": "Y"}\nDone.'
    result = llm_module._parse_structured_fallback(response_with(content), DummySchema)
    assert result is not None and result.importance == "Y"


def test_content_list_parts_are_joined():
    content = [{"type": "text", "text": '{"main_topic": "A", '}, {"type": "text", "text": '"importance": "B"}'}]
    result = llm_module._parse_structured_fallback(response_with(content), DummySchema)
    assert result is not None and result.main_topic == "A"


def test_invalid_or_empty_content_returns_none():
    assert llm_module._parse_structured_fallback(response_with("not json at all"), DummySchema) is None
    assert llm_module._parse_structured_fallback(response_with(""), DummySchema) is None
    assert llm_module._parse_structured_fallback({"parsed": None}, DummySchema) is None


def test_missing_required_field_returns_none():
    content = '{"main_topic": "only one field"}'
    assert llm_module._parse_structured_fallback(response_with(content), DummySchema) is None


def test_raw_text_helper_handles_str_and_list():
    assert llm_module._raw_message_text(response_with("abc")) == "abc"
    listed = [{"text": "x"}, "y"]
    assert llm_module._raw_message_text(response_with(listed)) == "xy"
    assert llm_module._raw_message_text({"raw": None}) == ""


def test_parse_structured_text_direct():
    assert (
        llm_module._parse_structured_text('{"main_topic": "T", "importance": "I"}', DummySchema).main_topic
        == "T"
    )
    assert llm_module._parse_structured_text("", DummySchema) is None
    assert llm_module._parse_structured_text("garbage", DummySchema) is None


def test_finish_reason_extraction():
    response = response_with("x")
    response["raw"].response_metadata = {"finish_reason": "length"}
    assert llm_module._finish_reason(response) == "length"
    assert llm_module._finish_reason({"raw": None}) == ""


class FakeLlm:
    def __init__(self, content):
        self.content = content
        self.calls = 0

    async def ainvoke(self, messages):
        self.calls += 1
        return types.SimpleNamespace(
            content=self.content,
            usage_metadata={"input_tokens": 11, "output_tokens": 7},
        )


def test_plain_json_fallback_success():
    import asyncio

    fake = FakeLlm('{"main_topic": "fallback", "importance": "ok"}')
    result, in_tok, out_tok, text = asyncio.run(llm_module._plain_json_fallback(fake, [], DummySchema))
    assert result is not None and result.main_topic == "fallback"
    assert (in_tok, out_tok) == (11, 7)
    assert fake.calls == 1 and "fallback" in text


def test_plain_json_fallback_empty_content():
    import asyncio

    fake = FakeLlm("")
    result, _, _, text = asyncio.run(llm_module._plain_json_fallback(fake, [], DummySchema))
    assert result is None and text == ""


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

    print(f"\n{len(checks) - failures}/{len(checks)} fallback checks passed")
    raise SystemExit(1 if failures else 0)
