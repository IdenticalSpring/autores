"""Helper chọn topic cho các smoke test Kafka.

Kiến trúc chuẩn: 1 container chạy `main.py` (4 listener) — consume topic **production**
(`document_setup_request`, `write_section_request`, ...).

Truyền `--local` (hoặc đặt env `SMOKE_USE_LOCAL=1`) để test với topic `*_local`
(dùng khi chạy các entrypoint `main_*_local.py`).
"""

import os
import sys

USE_LOCAL = ("--local" in sys.argv) or os.getenv("SMOKE_USE_LOCAL", "0") == "1"


def topic(local_value: str, prod_value: str) -> str:
    """Trả về topic `*_local` nếu bật chế độ local, ngược lại trả về topic production."""
    return local_value if USE_LOCAL else prod_value


def model_id() -> str:
    """model_id gửi trong payload smoke test (mặc định 'localhost' = LiteLLM proxy)."""
    return os.getenv("SMOKE_MODEL_ID", "localhost")
