"""Test event `parsed_value` (trước đây rơi nhánh else ⇒ 606)."""

import asyncio
from pathlib import Path

import pytest

import error_type
from data_analysis.src.modules import parsed_value as mod
from exception_type import AIERROR

ROOT = Path(__file__).resolve().parents[1]
LISTENER_SRC = (ROOT / "write_listener.py").read_text(encoding="utf-8")

SURVEY = [
    {
        "variable_name": "Sự hài lòng",
        "variable_code": "HL",
        "variable_type": "Phụ thuộc",
        "measurement_type": "Tiềm ẩn",
        "questions": "Bạn hài lòng với dịch vụ?",
        "answer_content": "1-5",
    },
    {
        "variable_name": "Sự hài lòng",
        "variable_code": "HL",
        "variable_type": "Phụ thuộc",
        "measurement_type": "Tiềm ẩn",
        "questions": "Bạn sẽ giới thiệu dịch vụ?",
        "answer_content": "1-5",
    },
]


def run(coro):
    return asyncio.run(coro)


def test_validate_accepts_valid_payload():
    mod.validate_parsed_value_input(SURVEY, [])


@pytest.mark.parametrize(
    "survey, variables, message",
    [
        ([], [], "non-empty list"),
        ("x", [], "non-empty list"),
        ([1], [], r"survey_questions\[0\] must be a dictionary"),
        ([{"questions": "q"}], [], "variable_code or variable_name"),
        ([{"variable_code": "A"}], [], "requires questions"),
        (SURVEY, "x", "variables must be a list"),
        (SURVEY, [1], "variables must be a list"),
    ],
)
def test_validate_rejects_bad_payload(survey, variables, message):
    with pytest.raises(ValueError, match=message):
        mod.validate_parsed_value_input(survey, variables)


def test_pre_parse_builds_latent_parent_and_children():
    parsed = mod.manually_parse_survey_to_variables([], SURVEY)
    assert [v["code"] for v in parsed] == ["HL", "HL1", "HL2"]
    assert parsed[0]["parent_code"] is None and parsed[1]["parent_code"] == "HL"
    assert parsed[1]["statement"] == "Bạn hài lòng với dịch vụ?"


def _patch(monkeypatch, result):
    monkeypatch.setattr(mod, "get_llm", lambda *a, **k: object())
    seen = {}

    async def fake(**kwargs):
        seen.update(kwargs)
        return result

    monkeypatch.setattr(mod, "parse_raw_variables", fake)
    return seen


def test_parse_values_returns_variables_and_passes_pre_parsed(monkeypatch):
    seen = _patch(monkeypatch, (None, True, [{"code": "HL"}], 7, 3))
    out = run(mod.parse_values("id", "m", "k", 100, "Vietnamese", [], SURVEY))
    assert out == ([{"code": "HL"}], 7, 3)
    assert len(seen["pre_parsed_variables"]) == 3


def test_parse_values_raises_ai_error_on_failure(monkeypatch):
    _patch(monkeypatch, (AIERROR(500, "bad"), False, [], 0, 0))
    with pytest.raises(AIERROR) as exc:
        run(mod.parse_values("id", "m", "k", 100, "Vietnamese", [], SURVEY))
    assert exc.value.status_code == 500


def test_parse_values_wraps_non_ai_error(monkeypatch):
    _patch(monkeypatch, (ValueError("x"), False, [], 0, 0))
    with pytest.raises(AIERROR):
        run(mod.parse_values("id", "m", "k", 100, "Vietnamese", [], SURVEY))


def test_error_maps_have_parsed_value():
    assert error_type.ERROR_DICT["parsed_value"] == 639
    assert "parsed_value" in error_type.MAX_TOKENS_DICT


def test_listener_dispatches_before_606_else():
    assert LISTENER_SRC.index("event_type == WriteEvent.PARSED_VALUE") < LISTENER_SRC.index(
        "Wrong event_type - event"
    )
