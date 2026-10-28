"""Regression: lỗi 429 (rate limit) phải được RETRY, không được abort ngay.

Bối cảnh (error_audit E1): trước đây `get_answer` và `get_answer_with_schema`
coi 429 là "unrecoverable" nên job `search_papers` fail ngay sau 107 lần log
`Unrecoverable error (Status 429)` dù Google trả kèm `Please retry in Xs`.
"""

import asyncio
import types

from google.api_core import exceptions as google_exceptions

from get_llm_response import _parse_retry_delay, get_answer


class FakeLLM:
    """LLM giả: n lần đầu raise 429, sau đó trả kết quả bình thường."""

    def __init__(self, failures: int, exc=None):
        self.failures = failures
        self.calls = 0
        self.exc = exc or google_exceptions.ResourceExhausted(
            "Quota exceeded ... Please retry in 0.01s."
        )

    async def ainvoke(self, _inp):
        self.calls += 1
        if self.calls <= self.failures:
            raise self.exc
        return types.SimpleNamespace(
            content="OK",
            usage_metadata={"input_tokens": 1, "output_tokens": 2},
        )


def test_parse_retry_delay_reads_google_message():
    assert _parse_retry_delay("... Please retry in 12.3s.") == 13
    assert _parse_retry_delay("no hint here", default=7) == 7


def test_429_is_retried_and_eventually_succeeds():
    llm = FakeLLM(failures=2)
    error, success, result, in_tokens, out_tokens = asyncio.run(get_answer("t429", llm, "hi"))

    assert success is True, f"429 phải được retry, nhận error={error}"
    assert result == "OK"
    assert llm.calls == 3, "phải gọi lại sau mỗi lần 429 (không abort)"


def test_401_still_aborts_immediately():
    llm = FakeLLM(failures=5, exc=google_exceptions.Unauthenticated("bad api key"))
    error, success, result, _, _ = asyncio.run(get_answer("t401", llm, "hi"))

    assert success is False
    assert error.status_code == 401
    assert llm.calls == 1, "401 vẫn phải abort ngay, không retry"
