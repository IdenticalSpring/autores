"""Regression tests cho lỗi `analyzer` mã 631 `KeyError: 'final_report'` (2026-11-01).

Nguyên nhân gốc: `_parse_structured_text` gặp lỗi ở MỘT phần tử của list
(vd `("steps", 0, "parameters")`) nhưng lại gán `data["steps"] = []` ⇒ plan còn
0 bước ⇒ reporting subgraph không chạy ⇒ thiếu `final_report` ⇒ KeyError.
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Optional

import pytest
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from get_llm_response import _parse_structured_text  # noqa: E402


class _TightStep(BaseModel):
    tool: str
    parameters: Optional[dict[str, int]] = None     # chặt: chỉ dict[str, int]


class _TightPlan(BaseModel):
    steps: list[_TightStep]


def test_self_heal_keeps_list_elements():
    """Lỗi ở 1 phần tử KHÔNG được xoá cả list (bug gốc)."""
    raw = {
        "steps": [
            {"tool": "run_anova_ttest_analysis", "parameters": {"alpha": 1}},
            {"tool": "run_regression_analysis", "parameters": {"alpha": "bad"}},
        ]
    }

    result = _parse_structured_text(json.dumps(raw), _TightPlan)

    assert result is not None, "phải self-heal được, không trả None"
    assert len(result.steps) == 2, "KHÔNG được xoá list khi chỉ 1 phần tử lỗi"
    assert result.steps[0].parameters == {"alpha": 1}
    assert result.steps[1].parameters is None, "chỉ phần tử lỗi bị set None"


def test_simplified_plan_accepts_free_dict_parameters():
    """Schema phải nhận mọi kiểu parameters LLM sinh (bool/list/[...])."""
    from data_analysis.src.schemas.analyzer_states import SimplifiedPlan

    plan = SimplifiedPlan.model_validate({
        "analysis_description": "anova + regression",
        "steps": [
            {
                "tool": "run_anova_ttest_analysis",
                "parameters": {"group": "gioi_tinh", "target": ["chi_tieu"], "flag": True},
                "model_extra_field": "được phép",
            },
            {"tool": "run_regression_analysis", "parameters": {"dv": "chi_tieu"}},
        ],
    })

    assert len(plan.steps) == 2
    assert plan.steps[0].parameters["flag"] is True


def test_analyzer_does_not_read_final_report_by_indexing():
    """analyzer.py không được `message["final_report"]` (gây KeyError)."""
    src = (ROOT / "data_analysis" / "src" / "modules" / "analyzer.py").read_text(encoding="utf-8")

    assert 'original_markdown = message["final_report"]' not in src
    assert 'message.get("final_report")' in src


def test_collector_node_sets_final_report_when_no_sections():
    """Reporting node với report_sections rỗng vẫn phải trả final_report."""
    graph = pytest.importorskip("data_analysis.src.modules.analyzer_graph")

    state = {
        "document_id": "00000000-0000-0000-0000-000000000002",
        "report_sections": [],
        "detailed_report_sections": [],
        "generated_files": {},
        "get_detailed_report": False,
        "input_tokens": 5,
        "output_tokens": 7,
    }

    result = asyncio.run(graph.collector_node(state))

    assert "final_report" in result, "thiếu final_report ⇒ analyzer sẽ KeyError (mã 631)"
    assert result["final_report"].strip()
    assert result["input_tokens"] == 5 and result["output_tokens"] == 7
