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


def test_missing_required_field_is_filled_with_empty_string():
    """Model trả JSON thiếu field ⇒ bù rỗng (không chặn ingest, không bịa nội dung)."""
    content = '{"main_topic": "only one field"}'
    result = llm_module._parse_structured_fallback(response_with(content), DummySchema)
    assert isinstance(result, DummySchema)
    assert result.main_topic == "only one field"
    assert result.importance == ""


def test_truncated_json_is_repaired():
    """JSON bị cắt cụt (hết max_tokens) vẫn cứu được các field hoàn chỉnh."""
    content = '{"main_topic": "AI in research", "importance": "faster", "backgr'
    result = llm_module._parse_structured_fallback(response_with(content), DummySchema)
    assert isinstance(result, DummySchema)
    assert result.main_topic == "AI in research"
    assert result.importance == "faster"


def test_fill_missing_fields_types():
    class RichSchema(BaseModel):
        text_field: str
        list_field: list[str]
        int_field: int
        bool_field: bool
        optional_field: str = "default"

    data = {}
    missing = llm_module._fill_missing_fields(data, RichSchema)
    assert data == {"text_field": "", "list_field": [], "int_field": 0, "bool_field": False}
    assert "optional_field" not in data  # field có default: để Pydantic tự xử lý
    assert sorted(missing) == ["bool_field", "int_field", "list_field", "text_field"]


def test_coerce_scalar_fields():
    class CoerceSchema(BaseModel):
        year: str
        tags: list[str]
        others: str

    data = {"year": 2024, "tags": "single", "others": "ok"}
    coerced = llm_module._coerce_scalar_fields(data, CoerceSchema)
    assert data == {"year": "2024", "tags": ["single"], "others": "ok"}
    assert sorted(coerced) == ["tags", "year"]


def test_wrong_scalar_type_is_coerced_then_validated():
    class YearSchema(BaseModel):
        year: str
        note: str

    result = llm_module._parse_structured_text('{"year": 2024, "note": "n"}', YearSchema)
    assert isinstance(result, YearSchema) and result.year == "2024"


def test_partial_key_points_still_validates():
    """Mô phỏng thực tế: KeyPoints có 15 field bắt buộc nhưng model chỉ trả vài field."""
    from document_setup.src.schemas.search_papers import KeyPoints

    content = '{"main_topic": "Digital transformation", "methodology": "PLS-SEM"}'
    result = llm_module._parse_structured_text(content, KeyPoints)
    assert isinstance(result, KeyPoints)
    assert result.main_topic == "Digital transformation"
    assert result.methodology == "PLS-SEM"
    assert result.research_gap == ""


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


def test_structured_unsupported_cache_flow():
    llm = types.SimpleNamespace(model_name="test-model-unsupported")
    assert llm_module._is_structured_unsupported(llm) is False
    assert llm_module._mark_structured_unsupported(llm) is True   # lần đầu: đánh dấu
    assert llm_module._is_structured_unsupported(llm) is True
    assert llm_module._mark_structured_unsupported(llm) is False  # đã có: không đánh dấu lại


def test_model_name_extraction():
    assert llm_module._model_name(types.SimpleNamespace(model_name="m1")) == "m1"
    assert llm_module._model_name(types.SimpleNamespace(model="m2")) == "m2"
    assert llm_module._model_name(types.SimpleNamespace()) == ""


def test_literal_newlines_in_json_are_parsed():
    """Gemini hay trả literal newline trong chuỗi khi prompt bảo bullet points."""
    content = '{\n  "main_topic": "Topic Line 1\nTopic Line 2",\n  "importance": "Imp"\n}'
    result = llm_module._parse_structured_text(content, DummySchema)
    assert isinstance(result, DummySchema)
    assert "Line 2" in result.main_topic


def test_trailing_commas_in_json_are_parsed():
    content = '{"main_topic": "T", "importance": "I",}'
    result = llm_module._parse_structured_text(content, DummySchema)
    assert isinstance(result, DummySchema)
    assert result.main_topic == "T"


def test_json_array_is_parsed():
    content = '[{"main_topic": "Arr", "importance": "Imp"}]'
    result = llm_module._parse_structured_text(content, DummySchema)
    assert isinstance(result, DummySchema)
    assert result.main_topic == "Arr"


def test_wrapped_root_dict_is_unwrapped():
    content = '{"KeyPoints": {"main_topic": "Wrapped", "importance": "Imp"}}'
    result = llm_module._parse_structured_text(content, DummySchema)
    assert isinstance(result, DummySchema)
    assert result.main_topic == "Wrapped"


def test_key_points_with_complex_types_coerced_successfully():
    """KeyPoints với variables list[dict], methodology dict, importance list[str]."""
    from document_setup.src.schemas.search_papers import KeyPoints

    content = """{
      "main_topic": "Green SCM",
      "background": "2024",
      "importance": ["Advantage 1", "Advantage 2"],
      "methodology": {"approach": "quantitative", "model": "SEM"},
      "variables": [
        {"name": "Supply Chain", "role": "independent", "description": "desc"}
      ]
    }"""
    result = llm_module._parse_structured_text(content, KeyPoints)
    assert isinstance(result, KeyPoints)
    assert result.main_topic == "Green SCM"
    assert "Advantage 1" in result.importance and "Advantage 2" in result.importance
    assert "quantitative" in result.methodology
    assert isinstance(result.variables, list)
    assert len(result.variables) == 1
    assert "Supply Chain" in result.variables[0]



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
