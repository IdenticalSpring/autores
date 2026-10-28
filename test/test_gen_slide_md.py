"""Unit tests for gen_slide_md module in write_reports."""

import asyncio
import pytest

from write_reports.src.modules.gen_slide_md import (
    preprocess_markdown,
    parse_markdown_with_titles,
    get_clean_bullet_text,
    parse_agenda_content,
    detect_heading_structure,
    build_slide_contents_from_markdown,
    create_presentation_from_markdown,
    _doc_tag,
    fetch_html_content,
    extract_outlines_and_chunk_content,
    slide_layouts,
)
from exception_type import AIERROR


def test_doc_tag():
    assert _doc_tag("650000000000000000000001") == "65000000"
    assert _doc_tag("") == "unknown"
    assert _doc_tag(None) == "unknown"


def test_preprocess_markdown():
    # Fewer than 2 sections -> unchanged
    md_short = "# Title\nSome content"
    assert preprocess_markdown(md_short) == md_short

    # Duplicate thank-you slides in last two sections
    md_dup = """
# Slide 1
---
# Thank you
Cảm ơn quý vị đã lắng nghe!
---
# Thank you
Thank you for your attention!
"""
    cleaned = preprocess_markdown(md_dup)
    sections = [s.strip() for s in cleaned.split("---") if s.strip()]
    assert len(sections) == 2
    assert "Thank you for your attention!" in sections[-1]

    # No duplicate thank you
    md_normal = """
# Slide 1
---
## Nội dung
---
# Thank you
"""
    assert len([s.strip() for s in preprocess_markdown(md_normal).split("---") if s.strip()]) == 3


def test_parse_markdown_with_titles():
    content = """# Heading 1
## Heading 2
### Heading 3
> Quote line
```python
x = 10
y = 20
```
- Bullet 1
* Bullet 2
+ Bullet 3
1. Numbered item
Normal text paragraph"""

    elements = parse_markdown_with_titles(content)
    types = [el["type"] for el in elements]

    assert "h1" in types
    assert "h2" in types
    assert "h3" in types
    assert "quote" in types
    assert "code" in types
    assert "bullet" in types
    assert "text" in types

    # Check code element
    code_el = next(el for el in elements if el["type"] == "code")
    assert "x = 10" in code_el["text"]


def test_get_clean_bullet_text():
    assert get_clean_bullet_text("  - Item A") == "Item A"
    assert get_clean_bullet_text("* Item B") == "Item B"
    assert get_clean_bullet_text("+ Item C") == "Item C"
    assert get_clean_bullet_text("1. Item D") == "Item D"
    assert get_clean_bullet_text("Regular text") == "Regular text"


def test_parse_agenda_content():
    agenda_str = "  Mục 1\n  Mục 2\n\n  Mục 3  "
    items = parse_agenda_content(agenda_str)
    assert items == ["Mục 1", "Mục 2", "Mục 3"]


def test_detect_heading_structure():
    assert detect_heading_structure(["## Section A\n### Sub A"]) == "h2_structure"
    assert detect_heading_structure(["# Section A\n## Sub A"]) == "h1_structure"
    assert detect_heading_structure(["No heading here"]) == "h2_structure"


def test_build_slide_contents_from_markdown():
    # Standard presentation with title, agenda, sections, and thank you
    markdown = """
# Bài Báo Cáo Nghiên Cứu
---
## Chương Trình
- Phần 1: Giới thiệu
- Phần 2: Phương pháp
---
## 1. Giới thiệu
### Bối cảnh nghiên cứu
- Tầm quan trọng của livestream bán hàng
- Xu hướng tại Việt Nam
### Mục tiêu
- Phân tích hành vi người mua
---
# Cảm Ơn
Xin cảm ơn hội đồng đã lắng nghe!
"""
    slides = build_slide_contents_from_markdown(markdown, slide_layouts, "doc12345")
    assert len(slides) >= 4

    layout_keys = [s[0] for s in slides]
    assert "opening_title" in layout_keys
    assert "agenda" in layout_keys
    assert "section_title" in layout_keys
    assert "subsection" in layout_keys
    assert "thank_you" in layout_keys


def test_build_slide_contents_h1_structure():
    # Case with h1 for sections and h2 for subsections
    markdown = """
# Bài Báo Cáo
---
## Agenda
- Mục A
---
# 1. Phần Mở Đầu
## 1.1 Khái Niệm
- Nội dung mở đầu
```python
code_sample = 1
```
---
# Cảm Ơn
"""
    slides = build_slide_contents_from_markdown(markdown, slide_layouts, "doc12345")
    assert len(slides) >= 3
    layout_keys = [s[0] for s in slides]
    assert "opening_title" in layout_keys
    assert "section_title" in layout_keys
    assert "subsection" in layout_keys


def test_create_presentation_from_markdown():
    template_path = "write_reports/src/template/template.pptx"
    markdown = """
# Bài Báo Cáo Thử Nghiệm
---
## Agenda
- Phần 1
- Phần 2
---
## 1. Nội dung chính
### Điểm nhấn 1
- Chi tiết điểm 1
---
# Thank you
"""
    pptx_bytes = create_presentation_from_markdown(
        template_path=template_path,
        markdown=markdown,
        slide_layouts=slide_layouts,
        output_path=None,
        document_id="doc12345",
    )
    assert pptx_bytes is not None
    assert len(pptx_bytes) > 1000


def test_fetch_html_content_direct_html():
    async def _run():
        html_input = "<html><body><h1>Report Title</h1><p>Content</p></body></html>"
        res = await fetch_html_content({"document_id": "doc123", "raw_report": html_input})
        assert res["raw_html"] == html_input
        assert res["total_input_tokens"] == 0

    asyncio.run(_run())


def test_fetch_html_content_invalid_format():
    async def _run():
        with pytest.raises(AIERROR) as exc:
            await fetch_html_content({"document_id": "doc123", "raw_report": "ftp://invalid-url.com"})
        assert exc.value.status_code == 634

    asyncio.run(_run())


def test_extract_outlines_and_chunk_content():
    async def _run():
        # Sections with > 100 chars each to verify section chunking
        p1 = "Nội dung khảo sát chi tiết về các hành vi tiêu dùng trực tuyến trong thương mại điện tử hiện đại. " * 3
        p2 = "Nội dung đánh giá mô hình hồi quy đa biến và các giả thuyết nghiên cứu được chấp nhận đầy đủ. " * 3
        html = f"""
        <html>
            <head><title>Test Presentation</title></head>
            <body>
                <h1>Tiêu đề chính</h1>
                <h2>Phần 1: Khảo sát</h2>
                <p>{p1}</p>
                <h2>Phần 2: Đánh giá</h2>
                <p>{p2}</p>
            </body>
        </html>
        """
        state = {
            "document_id": "doc12345",
            "raw_html": html,
            "manual_mode": True,
            "max_chunk_size": 5000,
        }
        res = await extract_outlines_and_chunk_content(state)
        assert res["extracted_title"] == "Tiêu đề chính"
        assert "Phần 1: Khảo sát" in res["outlines"]
        assert len(res["chunks"]) >= 2

    asyncio.run(_run())
