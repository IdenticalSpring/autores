"""Runtime tests (R1, B5, B3, B1-helper) — cần full dependency stack.

Chạy TRONG image AI (aiokafka/langgraph/pandas/redis/httpx...):

    docker run --rm -v "$PWD":/app -w /app autoresearching-ai:latest \
        python test/test_ai_fixes_runtime.py

Phần test end-to-end `delete_report` (B1 + B1b) nằm ở
`test/test_ai_fixes_delete_flow.py`.
"""

import asyncio
import pathlib
import sys
import types
import urllib.error

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import utils  # noqa: E402  (cần dependency nặng -> chỉ trong docker)
from data_analysis.src.modules.analyzer import Analyzer  # noqa: E402
from write_listener import WriteSectionEventListener  # noqa: E402


# --------------------------------------------------------------------------- #
# Fakes cho send_health_check
# --------------------------------------------------------------------------- #
class FakeRedis:
    def __init__(self, stored=None):
        self.stored = stored or {}
        self.set_calls = []

    async def get(self, key):
        return self.stored.get(key)

    async def set(self, key, value, ex=None):
        self.set_calls.append((key, value, ex))


class FakeResponse:
    def raise_for_status(self):
        return None


class FakeHttpClient:
    put_calls = []

    def __init__(self, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def put(self, url, headers=None, json=None):
        FakeHttpClient.put_calls.append({"url": url, "json": json})
        return FakeResponse()


def run_send_health_check(fake_redis, status):
    FakeHttpClient.put_calls.clear()
    utils.get_redis_client = lambda: fake_redis
    utils.httpx.AsyncClient = FakeHttpClient
    asyncio.run(
        utils.send_health_check(
            id="doc123",
            status=status,
            error=None,
            error_message=None,
            ttl=60,
            event_type="analyzer",
            topic="write_section_response",
        )
    )
    return list(FakeHttpClient.put_calls)


# --------------------------------------------------------------------------- #
# R1 + B5 — send_health_check
# --------------------------------------------------------------------------- #
def test_r1_health_check_runs_without_redis_init_and_sets_status():
    """Không còn `await` lên factory sync; vẫn SET AI_STATUS."""
    fake_redis = FakeRedis()
    puts = run_send_health_check(fake_redis, status=1)
    assert len(puts) == 1, "PENDING status must call webhook once"
    assert fake_redis.set_calls and fake_redis.set_calls[0][0] == "AI_STATUS:doc123"
    assert fake_redis.set_calls[0][2] == 60


def test_b5_health_check_skips_webhook_when_status_unchanged():
    import json

    same_status = json.dumps({"eventType": "analyzer", "processStatus": 2}).encode()
    fake_redis = FakeRedis({"AI_STATUS:doc123": same_status})
    assert run_send_health_check(fake_redis, status=2) == []


def test_b5_health_check_sends_webhook_when_status_changes():
    import json

    old_status = json.dumps({"eventType": "analyzer", "processStatus": 2}).encode()
    fake_redis = FakeRedis({"AI_STATUS:doc123": old_status})
    puts = run_send_health_check(fake_redis, status=3)
    assert len(puts) == 1
    assert puts[0]["json"]["processStatus"] == 3


# --------------------------------------------------------------------------- #
# B3 — analyze_log từ module thật
# --------------------------------------------------------------------------- #
def test_b3_analyze_log_from_real_module():
    pseudo_self = types.SimpleNamespace(model_id="localhost")
    analyze_log = Analyzer._build_analyze_log(
        pseudo_self,
        message={
            "logs": ["Cronbach alpha = 0.81", "WARNING: KMO = 0.55 below 0.6"],
            "current_variables": [
                types.SimpleNamespace(code="X1", model_dump=lambda: {"code": "X1"})
            ],
        },
        data_dict={"X1": [1, 2, 3, 4], "Y": [2, 3, 4, 5]},
        variables_data=[{"code": "X1"}],
        generated_files={"hist.png": "https://minio/hist.png"},
        result_html="https://minio/report.html",
    )
    assert analyze_log != "Done"
    assert "4 rows x 2 columns" in analyze_log
    assert "Cronbach alpha = 0.81" in analyze_log
    assert "Warnings:" in analyze_log
    assert "hist.png" in analyze_log


# --------------------------------------------------------------------------- #
# B1 — Qdrant helper (sync part chạy qua run_in_executor)
# --------------------------------------------------------------------------- #
class _FakeHttpResponse:
    def __init__(self, status_code):
        self.status = status_code

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def call_qdrant_helper(exception=None, status=200):
    listener = object.__new__(WriteSectionEventListener)
    import urllib.request as urllib_request

    original = urllib_request.urlopen

    def fake_urlopen(request, timeout=None):
        if exception is not None:
            raise exception
        return _FakeHttpResponse(status)

    urllib_request.urlopen = fake_urlopen
    try:
        return asyncio.run(listener._delete_qdrant_collection("doc123", "doc"))
    finally:
        urllib_request.urlopen = original


def test_b1_qdrant_200_is_success():
    assert call_qdrant_helper(status=200) is True


def test_b1_qdrant_404_is_success():
    assert call_qdrant_helper(urllib.error.HTTPError("u", 404, "not found", {}, None)) is True


def test_b1_qdrant_500_is_failure():
    assert call_qdrant_helper(urllib.error.HTTPError("u", 500, "boom", {}, None)) is False


def test_b1_qdrant_connection_error_is_failure():
    assert call_qdrant_helper(urllib.error.URLError("connection refused")) is False


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

    print(f"\n{len(checks) - failures}/{len(checks)} runtime checks passed")
    raise SystemExit(1 if failures else 0)
