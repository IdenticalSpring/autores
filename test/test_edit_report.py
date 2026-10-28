"""Test event `edit_report` (trước đây rơi nhánh else ⇒ 606)."""

import asyncio
from pathlib import Path

import pytest

import error_type
from exception_type import AIERROR
from write_reports.src.modules import edit_report as mod

ROOT = Path(__file__).resolve().parents[1]
LISTENER_SRC = (ROOT / "write_listener.py").read_text(encoding="utf-8")


def run(coro):
    return asyncio.run(coro)


def test_validate_rejects_missing_or_empty_fields():
    with pytest.raises(ValueError, match="content cannot be empty"):
        mod.validate_edit_report_input("  ", "rút gọn", "", "")
    with pytest.raises(ValueError, match="instruction must be a string"):
        mod.validate_edit_report_input("nội dung", None, "", "")
    with pytest.raises(ValueError, match="section_name must be a string"):
        mod.validate_edit_report_input("nội dung", "rút gọn", 1, "")


def test_validate_accepts_minimal_payload():
    mod.validate_edit_report_input("nội dung", "rút gọn", "", "")


def test_build_user_prompt_contains_all_parts():
    prompt = mod.build_user_prompt("ABC", "làm ngắn", "Mở đầu", "ngữ cảnh X")
    for part in ("Section: Mở đầu", "ngữ cảnh X", "làm ngắn", "ABC"):
        assert part in prompt


def _patch_llm(monkeypatch, result):
    monkeypatch.setattr(mod, "get_llm", lambda *a, **k: object())

    async def fake(*args, **kwargs):
        return result

    monkeypatch.setattr(mod, "get_answer_with_schema", fake)


def test_edit_report_returns_content_and_tokens(monkeypatch):
    _patch_llm(monkeypatch, (None, True, mod.EditedContent(content="ngắn gọn"), 10, 5))
    content, tin, tout = run(mod.edit_report("id", "m", "k", 100, "Vietnamese", "dài dòng", "rút gọn"))
    assert (content, tin, tout) == ("ngắn gọn", 10, 5)


def test_edit_report_propagates_llm_error(monkeypatch):
    _patch_llm(monkeypatch, (AIERROR(429, "quota"), False, "", 0, 0))
    with pytest.raises(AIERROR) as exc:
        run(mod.edit_report("id", "m", "k", 100, "Vietnamese", "a", "b"))
    assert exc.value.status_code == 429


def test_edit_report_rejects_empty_output(monkeypatch):
    _patch_llm(monkeypatch, (None, True, mod.EditedContent(content="  "), 1, 1))
    with pytest.raises(AIERROR):
        run(mod.edit_report("id", "m", "k", 100, "Vietnamese", "a", "b"))


def test_error_maps_have_edit_report():
    assert error_type.ERROR_DICT["edit_report"] == error_type.WriteSectionError.EDIT_REPORT == 643
    assert "edit_report" in error_type.TIME_OUT_DICT
    assert "edit_report" in error_type.MAX_TOKENS_DICT


def test_listener_dispatches_before_606_else():
    branch = LISTENER_SRC.index("event_type == WriteEvent.EDIT_REPORT")
    wrong = LISTENER_SRC.index("Wrong event_type - event")
    assert branch < wrong


def test_unknown_event_still_606():
    idx = LISTENER_SRC.index("Wrong event_type - event")
    assert "Error.EVENT_NOT_FOUND" in LISTENER_SRC[idx : idx + 200]
