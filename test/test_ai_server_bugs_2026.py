"""Unit tests verifying fixes for AI-BUG-01 to AI-BUG-04 and timeout upgrades.

Runs standalone with python3 (no external dependencies required):
    python3 test/test_ai_server_bugs_2026.py
"""

import ast
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import error_type  # noqa: E402


def test_ai_bug_03_write_section_error_enums():
    """Verify WriteSectionError has backward-compatible aliases and correct codes."""
    assert hasattr(error_type.WriteSectionError, "INVALID_PARAMS")
    assert error_type.WriteSectionError.INVALID_PARAMS == 603
    assert error_type.WriteSectionError.INVALID_PARAMS == error_type.Error.INVALID_PARAMS

    assert hasattr(error_type.WriteSectionError, "NOT_IMPLEMENT_MODEL")
    assert error_type.WriteSectionError.NOT_IMPLEMENT_MODEL == 605
    assert error_type.WriteSectionError.NOT_IMPLEMENT_MODEL == error_type.Error.NOT_IMPLEMENT_MODEL


def test_ai_bug_03_delete_report_code_in_listener():
    """Verify write_listener.py uses Error.INVALID_PARAMS and Error.NOT_IMPLEMENT_MODEL."""
    write_listener_path = ROOT / "write_listener.py"
    source = write_listener_path.read_text(encoding="utf-8")

    assert "message[\"error_code\"] = Error.INVALID_PARAMS" in source
    # Check that in DELETE_REPORT block, Error.INVALID_PARAMS is used
    delete_report_idx = source.find("elif event_type == WriteEvent.DELETE_REPORT:")
    gen_slides_idx = source.find("elif event_type == WriteEvent.GEN_SLIDES:")
    assert delete_report_idx != -1 and gen_slides_idx != -1
    delete_block = source[delete_report_idx:gen_slides_idx]

    assert "WriteSectionError.INVALID_PARAMS" not in delete_block
    assert "message[\"error_code\"] = Error.INVALID_PARAMS" in delete_block
    assert "message[\"error_code\"] = Error.NOT_IMPLEMENT_MODEL" in delete_block


def test_ai_bug_04_search_papers_exception_and_caching():
    """Verify search_papers.py catches httpx exceptions, has Redis cache and semaphore."""
    search_papers_path = ROOT / "document_setup" / "src" / "modules" / "search_papers.py"
    source = search_papers_path.read_text(encoding="utf-8")

    # Exception handling
    assert "requests.exceptions.Timeout" not in source
    assert "httpx.TimeoutException" in source
    assert "httpx.RequestError" in source

    # Redis cache
    assert "cache_key = f\"openalex:search:" in source
    assert "await self.redis_client.get(cache_key)" in source
    assert "await self.redis_client.set(cache_key, json.dumps(res), ex=86400)" in source

    # Title translation concurrency control
    assert "semaphore = asyncio.Semaphore(10)" in source
    assert "_translate_with_semaphore" in source


def test_timeouts_upgraded():
    """Verify all timeouts in TIME_OUT_DICT were appropriately increased."""
    timeouts = error_type.TIME_OUT_DICT

    # Document setup
    assert timeouts["search_papers"] >= 300
    assert timeouts["generate_domains"] >= 120
    assert timeouts["generate_subdomains"] >= 120
    assert timeouts["generate_keywords"] >= 120
    assert timeouts["generate_titles"] >= 420
    assert timeouts["generate_outline"] >= 1500

    # Write section
    assert timeouts["write_content"] >= 1800
    assert timeouts["chuyen_de_1"] >= 1800
    assert timeouts["analyzer"] >= 420
    assert timeouts["gen_slides"] >= 420

    # Chatbot & Enhancement
    assert timeouts["outer_chatbot"] >= 240
    assert timeouts["inner_chatbot"] >= 240
    assert timeouts["writer_chatbot"] >= 240
    assert timeouts["ai_in_doc"] >= 240
    assert timeouts["get_suggestions"] >= 240
    assert timeouts["enhance"] >= 240
    assert timeouts["enhance_all"] >= 240


def test_ai_bug_01_02_consumer_groups_separated():
    """Verify chatbot and enhancement have separated consumer groups to prevent rebalance storm."""
    main_path = ROOT / "main.py"
    main_src = main_path.read_text(encoding="utf-8")
    assert "\"enhancement_09_06_2025\"" in main_src
    assert "\"chatbot_09_06_2025\"" in main_src

    main_enhance_path = ROOT / "main_enhance.py"
    main_enhance_src = main_enhance_path.read_text(encoding="utf-8")
    assert "\"enhancement_09_06_2025\"" in main_enhance_src

    main_chat_path = ROOT / "main_chat.py"
    main_chat_src = main_chat_path.read_text(encoding="utf-8")
    assert "\"chatbot_09_06_2025\"" in main_chat_src

    health_alert_path = ROOT / "scripts" / "health_alert.py"
    health_alert_src = health_alert_path.read_text(encoding="utf-8")
    assert "\"enhancement_09_06_2025\": [topics.ENHANCEMENT_REQUEST]" in health_alert_src
    assert "\"chatbot_09_06_2025\": [topics.CHATBOT_REQUEST]" in health_alert_src


def main():
    tests = [
        test_ai_bug_03_write_section_error_enums,
        test_ai_bug_03_delete_report_code_in_listener,
        test_ai_bug_04_search_papers_exception_and_caching,
        test_timeouts_upgraded,
        test_ai_bug_01_02_consumer_groups_separated,
    ]
    passed = 0
    for test in tests:
        try:
            test()
            print(f"PASS  {test.__name__}")
            passed += 1
        except Exception as e:
            print(f"FAIL  {test.__name__}: {e}")
            raise
    print(f"\n{passed}/{len(tests)} checks passed.")


if __name__ == "__main__":
    main()
