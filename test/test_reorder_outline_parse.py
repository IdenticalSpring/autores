"""F9 regression — `ReorderOutline` trả rỗng với deepseek-flash (incident 2026-11-01).

Nguyên nhân gốc:
1. Prompt yêu cầu **mảng JSON trần** `[1,4,2,3]` nhưng schema là object
   `{"reorder_outline": [...]}` ⇒ `_load_json_lenient` cũ chỉ nhận dict ⇒ parse None.
2. `reorder_outline: list[int] = Field(None)` ⇒ không required ⇒ model trả `{}`
   vẫn "success" với `None` ⇒ caller gọi `len(None)` → TypeError.
3. Vòng `while ... trial < 5` không tăng `trial` ở một số nhánh lỗi ⇒ lặp vô hạn.

Test khoá cả 3 tầng: parse → schema → caller.
"""

import asyncio
import sys
from pathlib import Path

import pytest
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import get_llm_response as glr  # noqa: E402
from document_setup.src.schemas.outline import ReorderOutline  # noqa: E402
from exception_type import AIERROR  # noqa: E402

PARSE = glr._parse_structured_text


# --------------------------------------------------------------------------- #
# Tầng 1 — parse: chấp nhận mảng trần, số dạng string, object chuẩn
# --------------------------------------------------------------------------- #
def test_parse_bare_json_list_is_wrapped():
    result = PARSE("[1, 4, 2, 3]", ReorderOutline)
    assert result is not None
    assert result.reorder_outline == [1, 4, 2, 3]


def test_parse_object_form_still_works():
    result = PARSE('{"reorder_outline": [3, 1, 4, 2]}', ReorderOutline)
    assert result.reorder_outline == [3, 1, 4, 2]


def test_parse_numeric_strings_are_coerced():
    """DeepSeek hay trả `["1", "4"]` — trước đây self-heal xoá sạch list."""
    result = PARSE('{"reorder_outline": ["1", "4", 2.0]}', ReorderOutline)
    assert result.reorder_outline == [1, 4, 2]


def test_parse_bare_list_with_string_numbers():
    result = PARSE('["2", "1"]', ReorderOutline)
    assert result.reorder_outline == [2, 1]


def test_parse_empty_object_yields_none_not_crash():
    """`{}` ⇒ field optional ⇒ None (caller phải retry, KHÔNG được len(None))."""
    result = PARSE("{}", ReorderOutline)
    assert result is not None
    assert result.reorder_outline is None


def test_parse_keeps_only_valid_elements_instead_of_wiping_list():
    result = PARSE('{"reorder_outline": [2, "rác", 1]}', ReorderOutline)
    assert result.reorder_outline == [2, 1]


def test_parse_list_fenced_with_text():
    raw = "Đây là kết quả:\n```json\n[2, 3, 1]\n```\nHết."
    assert PARSE(raw, ReorderOutline).reorder_outline == [2, 3, 1]


def test_wrap_skipped_when_schema_has_many_list_fields():
    class TwoLists(BaseModel):
        a: list[int] | None = None
        b: list[int] | None = None

    # Không thể biết mảng thuộc field nào ⇒ không tự bọc, trả None thay vì đoán sai.
    assert PARSE("[1, 2]", TwoLists) is None


# --------------------------------------------------------------------------- #
# Tầng 2 — caller: sort_subheading không treo, không nuốt lỗi quota
# --------------------------------------------------------------------------- #
def _make_generator():
    """Bỏ qua __init__ nặng (Qdrant/LLM); chỉ cần attribute mà sort_subheading dùng."""
    from document_setup.src.modules.outline_generation import OutlineGeneration

    obj = object.__new__(OutlineGeneration)
    obj.document_id = "test-f9"
    obj.llm = None
    obj.input_tokens = 0
    obj.output_tokens = 0
    return obj


def _patch_answer(monkeypatch, handler):
    from document_setup.src.modules import outline_generation as mod

    calls = {"n": 0}

    async def fake(*args, **kwargs):
        calls["n"] += 1
        return await handler(calls["n"])

    monkeypatch.setattr(mod, "get_answer_with_schema", fake)
    return calls


SUBS = [{"subheading": "Mở đầu"}, {"subheading": "Kết quả"}, {"subheading": "Kết luận"}]


def test_sort_subheading_applies_returned_order(monkeypatch):
    async def handler(_n):
        return None, True, ReorderOutline(reorder_outline=[2, 3, 1]), 1, 1

    calls = _patch_answer(monkeypatch, handler)
    ordered = asyncio.run(_make_generator().sort_subheading(SUBS, "Chương 1"))
    assert [s["subheading"] for s in ordered] == ["Mở đầu", "Kết quả", "Kết luận"]
    assert calls["n"] == 1


def test_sort_subheading_retries_when_empty_then_degrades(monkeypatch):
    async def handler(_n):
        return None, True, ReorderOutline(reorder_outline=None), 0, 0

    calls = _patch_answer(monkeypatch, handler)
    ordered = asyncio.run(_make_generator().sort_subheading(SUBS, "Chương 1"))
    assert [s["subheading"] for s in ordered] == ["Mở đầu", "Kết quả", "Kết luận"]
    assert calls["n"] == 5, "phải thử đúng 5 lần rồi fallback, không lặp vô hạn"


def test_sort_subheading_increments_trial_on_unexpected_error(monkeypatch):
    async def handler(_n):
        return AIERROR(status_code=500, message="parse rỗng"), False, None, 0, 0

    calls = _patch_answer(monkeypatch, handler)
    ordered = asyncio.run(_make_generator().sort_subheading(SUBS, "Chương 1"))
    assert ordered == SUBS
    assert calls["n"] == 5


def test_sort_subheading_raises_on_quota_error(monkeypatch):
    async def handler(_n):
        return AIERROR(status_code=429, message="quota"), False, None, 0, 0

    _patch_answer(monkeypatch, handler)
    with pytest.raises(AIERROR):
        asyncio.run(_make_generator().sort_subheading(SUBS, "Chương 1"))


def test_sort_subheading_rejects_wrong_length(monkeypatch):
    async def handler(n):
        if n == 1:
            return None, True, ReorderOutline(reorder_outline=[1, 2]), 0, 0
        return None, True, ReorderOutline(reorder_outline=[3, 1, 2]), 0, 0

    calls = _patch_answer(monkeypatch, handler)
    ordered = asyncio.run(_make_generator().sort_subheading(SUBS, "Chương 1"))
    # Lần 1 sai số lượng ⇒ retry; lần 2 hợp lệ. Logic chuẩn hoá min/max buộc
    # "subheading đầu = ưu tiên thấp nhất" nên thứ tự đầu/cuối được giữ nguyên.
    assert calls["n"] == 2
    assert [s["subheading"] for s in ordered] == ["Mở đầu", "Kết quả", "Kết luận"]


# --------------------------------------------------------------------------- #
# Tầng 3 — prompt phải khớp schema (object, không phải mảng trần)
# --------------------------------------------------------------------------- #
def test_reorder_prompts_request_object_shape():
    from document_setup.src.modules.outline_prompt_bank import (
        REORDER_OUTLINE,
        REORDER_SUBHEADINGS_PRIORITIES_V2,
    )

    for prompt in (REORDER_OUTLINE, REORDER_SUBHEADINGS_PRIORITIES_V2):
        assert "reorder_outline" in prompt
        assert '{"reorder_outline"' in prompt, "prompt phải nêu rõ shape object để khớp schema"
