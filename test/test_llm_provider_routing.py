"""Routing test: get_llm() phải chọn đúng provider cho từng model_id.

Không gọi mạng — chỉ kiểm tra class và tham số được sinh ra, để đảm bảo
việc bổ sung DeepSeek không làm hỏng Gemini / LiteLLM (alias local-*).
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from langchain_google_genai import ChatGoogleGenerativeAI  # noqa: E402
from langchain_openai import ChatOpenAI  # noqa: E402

from get_llm_response import (  # noqa: E402
    DEEPSEEK_MAX_OUTPUT_TOKENS,
    get_llm,
)


def test_deepseek_routes_to_openai_compatible_client():
    llm = get_llm("deepseek-chat", "sk-test", max_output_tokens=20000)

    assert isinstance(llm, ChatOpenAI)
    base = str(getattr(llm, "openai_api_base", "") or "")
    assert "deepseek" in base, f"base_url phải trỏ DeepSeek, nhận: {base}"
    # 20000 < trần 65536 ⇒ giữ nguyên giá trị caller yêu cầu
    assert llm.max_tokens == 20000


def test_deepseek_output_above_cap_is_clamped():
    llm = get_llm("deepseek-flash", "sk-test", max_output_tokens=400000)

    assert isinstance(llm, ChatOpenAI)
    assert llm.max_tokens == DEEPSEEK_MAX_OUTPUT_TOKENS, "phải clamp về trần 64K"


def test_deepseek_reasoner_drops_temperature():
    llm = get_llm("deepseek-reasoner", "sk-test", temperature=0.6)

    assert isinstance(llm, ChatOpenAI)
    # deepseek-reasoner từ chối temperature/top_p ở tầng API
    assert "temperature" not in (getattr(llm, "_default_params", {}) or {})


def test_deepseek_chat_keeps_temperature():
    llm = get_llm("deepseek-chat", "sk-test", temperature=0.6)

    assert (getattr(llm, "_default_params", {}) or {}).get("temperature") == 0.6


def test_gemini_still_routes_to_google_client():
    llm = get_llm("gemini-2.5-flash", "fake-key")

    assert isinstance(llm, ChatGoogleGenerativeAI)


def test_local_alias_still_routes_to_litellm():
    """Alias local-* dùng cho LiteLLM (không cần sửa code khi thêm provider)."""
    llm = get_llm("local-deepseek", "None")

    assert isinstance(llm, ChatOpenAI)
    base = str(getattr(llm, "openai_api_base", "") or "")
    assert "8000" in base or "localhost" in base, f"phải trỏ LiteLLM, nhận: {base}"


def test_unknown_model_raises_not_implemented():
    with pytest.raises(NotImplementedError):
        get_llm("some-unknown-model-xyz", "k")
