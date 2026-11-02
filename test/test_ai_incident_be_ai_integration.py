"""Regression tests cho incident tích hợp BE ↔ AI server (2026-11-01).

Phủ 2 lỗi P0 từng lọt qua bộ test cũ:
1. UnboundLocalError `current_report` khi section KHÔNG có subsection
   (mã lỗi 633 / chuyen_de_2) — seminar_graph_1.py + seminar_graph_2.py.
2. Provider từ chối `response_format` json_schema (DeepSeek) phải chuyển
   ngay sang plain-JSON thay vì retry 3 vòng rồi trả 400.
"""

import asyncio
import sys
from pathlib import Path

from pydantic import BaseModel

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import get_llm_response as glr  # noqa: E402
from write_reports.src.modules import seminar_graph_1 as g1  # noqa: E402
from write_reports.src.modules import seminar_graph_2 as g2  # noqa: E402
from write_reports.src.schemas.section import SubSectionDescription  # noqa: E402


def _base_state() -> dict:
    return {
        "outline": {"outline": []},
        "current_section": object(),
        "proposal": {"title": "Nghien cuu"},
        "language": "Vietnamese",
        "field": "Kinh te",
        "domain": "Quan tri kinh doanh",
        "retry": 0,
        "model_id": "deepseek-flash",
        "llm_key": "sk-test",
        "use_web_search": False,
        "search_key": "",
        "document_id": "00000000-0000-0000-0000-000000000002",
        "propose_method": 99,
    }


def test_assign_workers_without_subsections_no_unbound_local():
    """Section không có subsection → nhánh else PHẢI có current_report (lỗi 633)."""
    state = _base_state()
    state["current_section_batch"] = []          # chuyen_de_2: không có subheadings
    state["other_sections"] = [{"content": "Noi dung chuong mot"}]

    for module in (g1, g2):
        msg = module.assign_workers_subsection(dict(state))
        report = msg.arg["current_report"]
        assert "Noi dung chuong mot" in report, (
            f"{module.__name__}: current_report rỗng/None — vẫn còn UnboundLocalError"
        )


def test_assign_workers_with_subsections_still_works():
    """Nhánh có subsection vẫn trả list Send như trước (không hồi quy)."""
    state = _base_state()
    state["current_section_batch"] = [
        [SubSectionDescription(detail_description="d", subheading="s",
                               subheading_word_count="100-200")]
    ]
    state["current_count_batch"] = 0
    state["other_sections"] = [{"content": "Chuong mot"}]

    msgs = g2.assign_workers_subsection(dict(state))
    assert isinstance(msgs, list) and len(msgs) == 1
    assert "Chuong mot" in msgs[0].arg["current_report"]


class _Queries(BaseModel):
    queries: list[str]


class _FakeStructured:
    def __init__(self, error: Exception) -> None:
        self._error = error

    async def ainvoke(self, *args, **kwargs):
        raise self._error


class _FakeLLMForResponseFormat:
    """Model KHÔNG nằm trong unsupported set nhưng provider từ chối response_format."""

    def __init__(self, model_name: str = "gemini-3.6-flash") -> None:
        self.model_name = model_name
        self.bound = False

    def with_structured_output(self, _structure, **kwargs):
        self.bound = True
        return _FakeStructured(
            RuntimeError("400 This response_format type is unavailable now")
        )


def test_response_format_error_falls_back_to_plain_json(monkeypatch):
    llm = _FakeLLMForResponseFormat()
    calls = {"n": 0}

    async def fake_plain(_llm, _messages, structure):
        calls["n"] += 1
        return structure(queries=["q1", "q2"]), 7, 3, '{"queries": ["q1", "q2"]}'

    monkeypatch.setattr(glr, "_plain_json_fallback", fake_plain)

    error, success, result, in_tok, out_tok = asyncio.run(
        glr.get_answer_with_schema("t-rf", llm, "Return queries", "AI in SMEs", _Queries)
    )

    assert llm.bound is True, "model không-unsupported vẫn phải thử structured trước"
    assert success is True and error is None, f"phải fallback thành công, nhận {error}"
    assert result.queries == ["q1", "q2"]
    assert calls["n"] == 1, "chỉ fallback 1 lần, không retry mù 3 vòng"
    assert (in_tok, out_tok) == (7, 3)
