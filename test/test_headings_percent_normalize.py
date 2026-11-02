"""Guard `headings_percent` thang 0..1 (contract AI quyết 2026-11-01).

BE từng gửi `15/25/60` (thang 0..100) ⇒ `word_count` bị nhân 15 lần
(`outline_generation.py:372`) và `update_outline_percent` luôn fail vì `sum != 1`.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from listener import BaseAsyncListener  # noqa: E402

NORMALIZE = BaseAsyncListener.normalize_percent_list


def test_fraction_is_kept():
    values = [0.15, 0.25, 0.6]
    assert NORMALIZE(values) == values


def test_percent_0_100_is_converted():
    assert NORMALIZE([15, 25, 60]) == [0.15, 0.25, 0.6]
    assert NORMALIZE([100]) == [1.0]
    assert sum(NORMALIZE([15, 25, 60])) == 1.0


def test_float_percent_is_converted():
    assert NORMALIZE([12.5, 87.5]) == [0.125, 0.875]


def test_empty_or_invalid_is_untouched():
    assert NORMALIZE([]) == []
    assert NORMALIZE(None) is None
    assert NORMALIZE(["15", "25"]) == ["15", "25"]
    assert NORMALIZE([True, False]) == [True, False]


def test_listener_normalizes_before_use():
    """3 điểm nhận `headings_percent`/`new_headings_percent` phải đi qua guard."""
    source = (ROOT / "document_setup_listener.py").read_text(encoding="utf-8")
    assert source.count("self.normalize_percent_list(value[\"headings_percent\"])") == 2
    assert (
        'self.normalize_percent_list(value["new_headings_percent"], "new_headings_percent")'
        in source
    )
