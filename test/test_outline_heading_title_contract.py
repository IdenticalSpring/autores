"""Contract `heading` vs `title` (F1/F3/F8, incident 2026-11-01).

BE có thể gửi section với `title` (contract cũ) hoặc `heading` (contract mới).
Trước đây `write_graph`/`seminar_graph_1`/`write_chat_graph` đọc thẳng
`section["heading"]` ⇒ KeyError khi chỉ có `title`.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from write_reports.src.modules.utils.utils_func import (  # noqa: E402
    normalize_outline,
    section_heading,
    strip_heading_number,
)


def test_section_heading_prefers_heading_then_title():
    assert section_heading({"heading": "Chương 1", "title": "Khác"}) == "Chương 1"
    assert section_heading({"title": "Chương 2"}) == "Chương 2"
    assert section_heading({"section_name": "Chương 3"}) == "Chương 3"
    assert section_heading({}) == ""
    assert section_heading(None) == ""


def test_normalize_outline_fills_heading_from_title():
    outline = {
        "title": "Đề tài",
        "outline": [
            {"title": "1. Giới thiệu", "overview": "mở đầu"},
            {"title": "2. Phương pháp"},
        ],
    }
    result = normalize_outline(outline)

    assert [s["heading"] for s in result["outline"]] == ["1. Giới thiệu", "2. Phương pháp"]
    assert result["outline"][1]["overview"] == ""
    assert result["outline"][1]["word_count"] == ""


def test_normalize_outline_keeps_heading_when_both_present():
    outline = {"outline": [{"heading": "Heading mới", "title": "Title cũ"}]}
    assert normalize_outline(outline)["outline"][0]["heading"] == "Heading mới"


def test_normalize_outline_falls_back_to_generated_chapter_name():
    outline = {"outline": [{}, {"title": "  "}]}
    result = normalize_outline(outline)
    assert result["outline"][0]["heading"] == "chapter_1"
    assert result["outline"][1]["heading"] == "chapter_2"


def test_normalize_outline_handles_subheadings_as_dicts_and_strings():
    outline = {
        "outline": [
            {
                "title": "Chương 1",
                "subheadings": [
                    {"title": "1.1. Bối cảnh"},
                    "1.2. Mục tiêu",
                ],
            }
        ]
    }
    subs = normalize_outline(outline)["outline"][0]["subheadings"]

    assert subs[0]["subheading"] == "1.1. Bối cảnh"
    assert subs[0]["subheading_word_count"] == ""
    assert subs[1]["subheading"] == "1.2. Mục tiêu"
    assert subs[1]["detail_description"] == ""


def test_normalize_outline_preserves_none_subheadings():
    """`subheadings=None` = viết ở cấp heading ⇒ KHÔNG được đổi thành []."""
    outline = {"outline": [{"heading": "Chương 1", "subheadings": None}]}
    assert normalize_outline(outline)["outline"][0]["subheadings"] is None


def test_normalize_outline_tolerates_bad_input():
    assert normalize_outline(None) is None
    assert normalize_outline({"outline": "not-a-list"}) == {"outline": "not-a-list"}


def test_strip_heading_number():
    assert strip_heading_number("1. Giới thiệu") == "Giới thiệu"
    assert strip_heading_number("2.1. Phân tích") == "Phân tích"
    assert strip_heading_number("Giới thiệu") == "Giới thiệu"
    assert strip_heading_number("") == ""


def test_write_listener_normalizes_outline_before_graphs():
    """Chốt: mọi entrypoint write/chuyen_de_1/2/3 phải đi qua normalize_outline."""
    source = (ROOT / "write_listener.py").read_text(encoding="utf-8")
    assert source.count("normalize_outline(value[\"outline\"])") == 3
    assert "from write_reports.src.modules.utils.utils_func import normalize_outline" in source
