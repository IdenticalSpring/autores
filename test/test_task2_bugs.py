"""Regression tests for bugs listed in docs/task2.md."""
import asyncio
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _src(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_search_papers_year_fields_optional():
    src = _src("document_setup_listener.py")
    assert 'value["cut_off_year_low"]' not in src
    assert 'value["cut_off_year_high"]' not in src
    assert 'value.get("cut_off_year_low")' in src
    assert 'value.get("cut_off_year_high")' in src
    assert 'value.get("oa_filter", False)' in src
    assert 'value.get("downloaded_papers", [])' in src


def test_papers_search_may_be_empty():
    src = _src("write_listener.py")
    assert 'value["papers_search"]' not in src
    assert not re.search(r'"papers_search":\s*papers_search\s*\n\s*\}', src)


def test_heading_fallback_in_load_current_section():
    src = _src("write_reports/src/modules/utils/utils_func.py")
    assert 'heading.get("title")' in src
    assert 'f"chapter_{idx + 1}"' in src


def test_safe_similarity_search_returns_empty():
    from document_setup.src.modules.outline_generation import (
        safe_qdrant_store,
        safe_similarity_search,
    )

    assert asyncio.run(safe_similarity_search(None, query="x")) == []

    class Boom:
        async def asimilarity_search(self, **_):
            raise RuntimeError("404 Not Found: Collection doesn't exist")

    assert asyncio.run(safe_similarity_search(Boom(), query="x")) == []
    assert callable(safe_qdrant_store)


def test_outline_index_guards_present():
    src = _src("document_setup/src/modules/outline_generation.py")
    assert "if len(final_subs) <= 1:" in src
    assert "if len(original_subs) > 1:" in src
    # F9: không index trực tiếp `ordered_outline.reorder_outline[i]` nữa; thay bằng
    # danh sách priority đã validate độ dài trước khi sort (tránh IndexError/TypeError).
    assert "ordered_priorities" in src
    assert "i < len(ordered_outline.reorder_outline)" not in src


def test_ai_bug_10_ai_in_doc_actions():
    src = _src("ai_chatbot/src/modules/ai_in_doc.py")
    assert '"fix_grammar": "fix_grammar"' in src
    assert '"make_academic": "make_academic"' in src
    assert '"rewrite": "better"' in src
    assert '"expand": "longer"' in src
    assert '"shorten": "shorter"' in src
    assert "internal_action in [" in src
    assert "Invalid action:" in src


def test_ai_bug_11_format_pages_and_year_safety():
    from document_setup.src.modules.search_papers import format_pages
    assert format_pages({"first_page": "1", "last_page": "10"}) == "1-10"
    assert format_pages({"first_page": "10", "last_page": "1"}) == "1-10"
    assert format_pages({"first_page": "e13908"}) == "e13908"
    assert format_pages({"first_page": "e13908", "last_page": "e13910"}) == "e13908-e13910"
    assert format_pages({}) == ""
    assert format_pages(None) == ""

    src = _src("document_setup/src/modules/search_papers.py")
    assert 'int(biblio["first_page"])' not in src
    assert "format_pages(biblio)" in src


def test_ai_bug_12_subsections_none_safe():
    for f in ["seminar_graph_1.py", "seminar_graph_2.py", "seminar_graph_3.py"]:
        src = _src(f"write_reports/src/modules/{f}")
        assert 'getattr(state["current_section"], "subsections", None)' in src
        assert 'content = f"Here is the section data:\\n{state[\'current_section\']}\\nGenerate from 0 to {len(subsections) - 1}"' in src or \
               'Generate from 0 to {len(subsections) - 1}' in src

    from write_reports.src.schemas.section import SectionDescription
    from write_reports.src.modules.seminar_graph_2 import continue_to_write_subsection, create_subsection_batch

    dummy_section = SectionDescription(
        heading="Introduction",
        overview="Overview",
        word_count="1000",
        subsections=None,
    )
    dummy_state = {
        "document_id": "test_doc_12345",
        "current_section": dummy_section,
        "current_count_batch": 0,
        "current_section_batch": [],
        "other_sections": [{"name": "intro"}],
        "outline": {"outline": [{"heading": "Introduction"}]},
        "input_tokens": 0,
        "output_tokens": 0,
    }

    # Should not raise TypeError: object of type 'NoneType' has no len()
    batch_res = asyncio.run(create_subsection_batch(dummy_state))
    assert batch_res["current_section_batch"] == []

    decision = continue_to_write_subsection(dummy_state)
    assert decision in ["write_section", "add_ref"]

    from write_reports.src.modules.seminar_graph_3 import (
        assign_workers_subsection as assign_workers_3,
        create_subsection_batch as create_batch_3,
    )
    dummy_state_3 = {
        **dummy_state,
        "need_data": False,
        "sections_need_data": [],
        "used_files": [],
        "domain": "AI",
        "field": "CS",
        "language": "en",
        "model_id": "test",
        "proposal": {},
        "tab_count": 0,
        "fig_count": 0,
        "retry": 0,
        "llm_key": "test",
        "use_web_search": False,
        "search_key": "test",
        "user_content": "",
        "other_sections": [{"content": "Section 1 content"}],
    }
    batch_res_3 = asyncio.run(create_batch_3(dummy_state_3))
    assert batch_res_3["current_section_batch"] == []

    worker_res_3 = assign_workers_3(dummy_state_3)
    assert worker_res_3.node == "write_subsection"


if __name__ == "__main__":
    test_search_papers_year_fields_optional()
    print("PASS test_search_papers_year_fields_optional")
    test_papers_search_may_be_empty()
    print("PASS test_papers_search_may_be_empty")
    test_heading_fallback_in_load_current_section()
    print("PASS test_heading_fallback_in_load_current_section")
    test_safe_similarity_search_returns_empty()
    print("PASS test_safe_similarity_search_returns_empty")
    test_outline_index_guards_present()
    print("PASS test_outline_index_guards_present")
    test_ai_bug_10_ai_in_doc_actions()
    print("PASS test_ai_bug_10_ai_in_doc_actions")
    test_ai_bug_11_format_pages_and_year_safety()
    print("PASS test_ai_bug_11_format_pages_and_year_safety")
    test_ai_bug_12_subsections_none_safe()
    print("PASS test_ai_bug_12_subsections_none_safe")
    print("\nALL 8 REGRESSION TESTS PASSED 100%!")
