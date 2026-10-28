"""Regression tĩnh: KHÔNG được để exception thoát khỏi `_handle_record`.

Bug #17: `chatbot_listener._handle_chatbot_event` lấy `value["conv_id"]` **ngoài** khối try
⇒ payload thiếu `conv_id` ném KeyError ra worker ⇒ không publish response ⇒ BE timeout 300s
dù AI chưa làm gì. Tương tự, chatbot/enhance/write thiếu catch-all nên mọi lỗi bất ngờ
đều rơi vào im lặng.

Test này đọc source để đảm bảo cả 4 listener đều:
  1. Có `except Exception` (catch-all) trong `_handle_record`.
  2. Không truy cập `value["conv_id"]` một cách cứng nhắc ở nơi dễ ném.
  3. Echo identifier trong response lỗi.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LISTENERS = {
    "chatbot": ROOT / "chatbot_listener.py",
    "enhance": ROOT / "enhance_listener.py",
    "write": ROOT / "write_listener.py",
    "document_setup": ROOT / "document_setup_listener.py",
}


def handle_record_block(listener_file: Path) -> str:
    source = listener_file.read_text(encoding="utf-8")
    start = source.index("async def _handle_record(")
    end = source.index("    async def ", start + 10)
    return source[start:end]


def test_all_listeners_have_catch_all_in_handle_record():
    for name, path in LISTENERS.items():
        block = handle_record_block(path)
        assert "except Exception as e:" in block, f"{name} thiếu catch-all trong _handle_record"
        assert "publish" in block.lower(), f"{name} không cố publish response lỗi"


def test_chatbot_does_not_read_conv_id_outside_try():
    """`_id = value["conv_id"]` trước try chính là nguyên nhân bug #17."""
    source = LISTENERS["chatbot"].read_text(encoding="utf-8")
    start = source.index("async def _handle_chatbot_event(")
    try_line = source.index("        try:", start)
    # Bỏ comment (comment mô tả bug có nhắc tới cú pháp cũ)
    code_lines = [
        line for line in source[start:try_line].splitlines() if not line.strip().startswith("#")
    ]
    preamble = "\n".join(code_lines)
    assert '_id = value["conv_id"]' not in preamble, "không được đọc value['conv_id'] ngoài khối try"
    assert 'value.get("conv_id")' in preamble, "phải dùng .get('conv_id') để không ném KeyError"


def test_chatbot_invalid_params_echoes_identifiers():
    source = LISTENERS["chatbot"].read_text(encoding="utf-8")
    start = source.index("except KeyError as e:")
    block = source[start : start + 900]
    assert "Error.INVALID_PARAMS" in block
    for identifier in ("document_id", "conv_id", "user_id"):
        assert identifier in block, f"response INVALID_PARAMS phải echo {identifier}"


def test_wrong_event_type_echoes_identifiers():
    """Response 606 trước đây không có identifier ⇒ BE không ghép được request."""
    for name in ("chatbot", "enhance"):
        source = LISTENERS[name].read_text(encoding="utf-8")
        idx = source.index("Wrong event_type")
        window = source[idx : idx + 700]
        assert 'for identifier in ("user_id", "document_id", "session_id", "conv_id")' in window, (
            f"{name}: nhánh Wrong event_type phải echo identifier"
        )
