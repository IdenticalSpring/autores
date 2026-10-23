"""Verification for the 8 AI fixes (B1, B1b, B2, B3, B4, B5, B6, R1).

The AI worker image has heavy dependencies (aiokafka, langgraph, pandas...).
This file therefore runs with the standard library only:

1. Runtime test of the REAL `_build_analyze_log` source (extracted from
   data_analysis/src/modules/analyzer.py via `ast`, so the tested code is the
   code in the repo, not a copy).
2. Static assertions for the fixes that cannot run without the full stack
   (delete_report, error-code mapping, health-check webhook de-duplication).

Run directly:      python3 test/test_ai_fixes.py
Run with pytest:   pytest test/test_ai_fixes.py -q
"""

import ast
import json
import pathlib
import types

ROOT = pathlib.Path(__file__).resolve().parents[1]
ANALYZER = ROOT / "data_analysis" / "src" / "modules" / "analyzer.py"
WRITE_LISTENER = ROOT / "write_listener.py"
UTILS = ROOT / "utils.py"


def load_build_analyze_log():
    """Compile the real `_build_analyze_log` method as a standalone function."""
    source = ANALYZER.read_text(encoding="utf-8")
    tree = ast.parse(source)

    target = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_build_analyze_log":
            target = node
            break
    assert target is not None, "_build_analyze_log not found in analyzer.py"

    # Drop `self` so the method can be called directly.
    target.args.args = target.args.args[1:]
    target.decorator_list = []

    module = ast.Module(body=[target], type_ignores=[])
    ast.fix_missing_locations(module)

    namespace = {
        "json": json,
        "logger": types.SimpleNamespace(warning=lambda *a, **k: None, info=lambda *a, **k: None),
    }
    exec(compile(module, str(ANALYZER), "exec"), namespace)
    return namespace["_build_analyze_log"]


build_analyze_log = load_build_analyze_log()


# --------------------------------------------------------------------------- #
# B3 - analyze_log is real text, never the "Done" placeholder
# --------------------------------------------------------------------------- #
def test_analyze_log_uses_real_logs():
    result = build_analyze_log(
        message={"logs": ["Cronbach alpha = 0.87 for all constructs", "EFA KMO = 0.912"]},
        data_dict={"X1": [1, 2, 3], "X2": [4, 5, 6]},
        variables_data=[{"code": "X1"}, {"code": "X2"}],
        generated_files={"chart_1.png": "https://minio/chart_1.png"},
        result_html="https://minio/report.html",
    )
    assert result != "Done"
    assert "Cronbach alpha = 0.87" in result
    assert "EFA KMO = 0.912" in result
    assert "3 rows x 2 columns" in result
    assert "X1, X2" in result
    assert "chart_1.png" in result
    assert "uploaded successfully" in result


def test_analyze_log_without_logs_is_honest():
    result = build_analyze_log(
        message={},
        data_dict={},
        variables_data=[],
        generated_files={},
        result_html="",
    )
    assert "did not return log entries" in result
    assert "upload failed" in result
    assert "not reported by the graph" in result


def test_analyze_log_surfaces_warnings():
    result = build_analyze_log(
        message={"logs": ["WARNING: KMO = 0.51 below threshold", "Regression R2 = 0.44"]},
        data_dict={"Y": [1, 2]},
        variables_data=[{"code": "Y"}],
        generated_files={},
        result_html="https://minio/report.html",
    )
    assert "Warnings:" in result
    assert "KMO = 0.51" in result
    assert "Warnings: none reported" not in result


def test_analyze_log_never_invents_hypothesis_conclusion():
    """Không có dữ liệu giả thuyết => không được tự sinh kết luận H1..Hn."""
    result = build_analyze_log(
        message={"logs": ["Descriptive statistics computed"]},
        data_dict={"Y": [1, 2, 3]},
        variables_data=[{"code": "Y"}],
        generated_files={},
        result_html="https://minio/report.html",
    )
    lowered = result.lower()
    for fabricated in ("accepted", "rejected", "chấp nhận", "bác bỏ"):
        assert fabricated not in lowered


# --------------------------------------------------------------------------- #
# B2 / B6 - response payload of analyzer
# --------------------------------------------------------------------------- #
def test_b6_upload_failed_flag_present():
    source = ANALYZER.read_text(encoding="utf-8")
    assert '"upload_failed": not bool(public_url)' in source
    # upload_failed is additive: must NOT be paired with an error_code
    assert '"upload_failed": not bool(public_url),\n                    "analyze_log"' in source


def test_b2_current_variables_uses_graph_state():
    source = ANALYZER.read_text(encoding="utf-8")
    assert '"current_variables": [\n' in source
    assert 'for variable in (message.get("current_variables") or [])' in source
    assert '"current_variables": [],' not in source
    assert '"analyze_log": "Done"' not in source


# --------------------------------------------------------------------------- #
# B1 / B1b - delete_report
# --------------------------------------------------------------------------- #
def delete_report_block() -> str:
    source = WRITE_LISTENER.read_text(encoding="utf-8")
    start = source.index("elif event_type == WriteEvent.DELETE_REPORT:")
    end = source.index("elif event_type == WriteEvent.GEN_SLIDES:")
    return source[start:end]


def test_b1_delete_uses_element_not_list():
    block = delete_report_block()
    source = WRITE_LISTENER.read_text(encoding="utf-8")
    assert 'find_one_and_delete({"_id": doc_id})' in block
    assert 'find_one_and_delete({"_id": document_id})' not in source
    # URL Qdrant chỉ được dựng trong helper, từ collection_name (không phải list)
    assert 'collections/{document_id}' not in source
    assert 'collections/{collection_name}' in source
    assert 'f"{doc_id}"' in block and 'f"{doc_id}_user"' in block
    # Lỗi xoá Mongo không còn bị nuốt
    assert "except Exception:\n" not in block
    assert "Failed to delete reports_refs" in block


def test_b1_delete_is_best_effort_with_failed_ids():
    block = delete_report_block()
    assert "failed_ids: list[str] = []" in block
    assert "failed_ids.append(doc_id)" in block
    assert 'message["failed_ids"] = failed_ids' in block
    assert "WriteSectionError.DELETE_REPORT" in block
    # best-effort: không có early-exit trong vòng lặp
    loop = block[block.index("for doc_id in document_id:"):]
    loop = loop[: loop.index("cleanup_results")]
    assert "break" not in loop
    assert "continue" not in loop


def test_b1b_cleanup_is_awaited():
    block = delete_report_block()
    assert "[self.clean_up(id) for id in thread_id]" not in block
    assert "await asyncio.gather(" in block
    assert "return_exceptions=True" in block


def test_b1_qdrant_helper_is_async_and_non_blocking():
    source = WRITE_LISTENER.read_text(encoding="utf-8")
    assert "async def _delete_qdrant_collection(" in source
    assert "loop.run_in_executor(None, _delete)" in source
    assert "timeout=30" in source
    assert source.count("await self._delete_qdrant_collection(") == 2


# --------------------------------------------------------------------------- #
# B4 - error code per phase of chuyen_de_3
# --------------------------------------------------------------------------- #
def test_b4_write_phase_maps_to_635():
    source = WRITE_LISTENER.read_text(encoding="utf-8")
    assert "write_phase = False" in source
    assert "write_phase = True" in source
    assert "WriteSectionError.CHUYEN_DE_3 if write_phase else WriteSectionError.ANALYZER" in source
    assert "CHUYEN_DE_3 if has_finished else" not in source


# --------------------------------------------------------------------------- #
# B5 / R1 - send_health_check
# --------------------------------------------------------------------------- #
def test_b5_webhook_only_on_status_change():
    source = UTILS.read_text(encoding="utf-8")
    assert "previous_status = None" in source
    assert "previous_status = json.loads(status_bytes).get(\"processStatus\")" in source
    assert "if previous_status != status:" in source
    assert 'status["processStatus"] != status' not in source


def test_r1_redis_client_not_awaited():
    source = UTILS.read_text(encoding="utf-8")
    assert "await get_redis_client()" not in source
    assert "redis_client = get_redis_client()" in source
    assert "await redis_client.set(" in source


if __name__ == "__main__":
    failures = 0
    for name, func in sorted(globals().items()):
        if not name.startswith("test_") or not callable(func):
            continue
        try:
            func()
            print(f"PASS  {name}")
        except AssertionError as error:
            failures += 1
            print(f"FAIL  {name}: {error}")
        except Exception as error:  # noqa: BLE001
            failures += 1
            print(f"ERROR {name}: {type(error).__name__}: {error}")

    total = sum(1 for name in globals() if name.startswith("test_"))
    print(f"\n{total - failures}/{total} checks passed")
    raise SystemExit(1 if failures else 0)
