"""Regression: `get_key_points` phải chịu được cache-miss (smoke 2026-11-01).

Trước fix: `cached_data = await collection.find_one(...)` → `None` khi chưa có
tài liệu ⇒ `cached_data["documents"]` ⇒ `TypeError: 'NoneType' object is not subscriptable`
⇒ `generate_outline_with_refs` trả lỗi `626`.
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from document_setup.src.modules.outline_generation import OutlineGeneration  # noqa: E402


class _FakeCollection:
    def __init__(self, doc):
        self.doc = doc

    async def find_one(self, *_args, **_kwargs):
        return self.doc


class _FakeMongo:
    def __init__(self, doc):
        self.doc = doc

    def __getitem__(self, _name):
        return {"reports_refs": _FakeCollection(self.doc)}


def _make_generator(doc):
    obj = object.__new__(OutlineGeneration)
    obj.document_id = "doc-cache-test"
    obj.mongo_client = _FakeMongo(doc)
    obj.input_tokens = 0
    obj.output_tokens = 0
    obj.embed_tokens = 0
    return obj


PAPERS = [{"title": "Paper A"}, {"title": "Paper B"}]


def test_cache_miss_returns_papers_with_empty_key_points():
    gen = _make_generator(None)
    result = asyncio.run(gen.get_key_points([dict(p) for p in PAPERS]))

    assert [p["title"] for p in result] == ["Paper A", "Paper B"]
    assert all(p["key_points"] == [] and p["source"] == "" for p in result)


def test_document_without_documents_key_is_safe():
    gen = _make_generator({"_id": "doc-cache-test"})
    result = asyncio.run(gen.get_key_points([dict(p) for p in PAPERS]))
    assert all(p["key_points"] == [] for p in result)


def test_format_key_points_accepts_dict_and_list():
    from document_setup.src.modules.outline_generation import (
        format_key_points,
        format_paper_prompt,
    )

    assert format_key_points({"ý 1": "nội dung"}) == "1. ý 1: nội dung"
    assert format_key_points(["a", "b"]) == "1. a\n---\n2. b"
    assert format_key_points(None) == ""
    assert format_key_points({"x": 1}) == "1. x: 1"

    block = format_paper_prompt(0, {"title": "Paper A", "key_points": ["kp"]})
    assert "Paper's index: 1" in block
    assert "Paper's title: Paper A" in block
    assert "1. kp" in block
    assert format_paper_prompt(1, {}).startswith("Paper's index: 2")


def test_existing_documents_are_mapped_by_title():
    gen = _make_generator(
        {"documents": [{"title": "Paper A", "key_points": ["kp1"], "source": "ADMIN"}]}
    )
    result = asyncio.run(gen.get_key_points([dict(p) for p in PAPERS]))
    assert result[0]["key_points"] == ["kp1"]
    assert result[0]["source"] == "ADMIN"
    # Paper B không có trong cache ⇒ default, không crash
    assert result[1]["key_points"] == []


def test_outline_generation_never_calls_items_on_key_points():
    """Chốt: không còn chỗ nào gọi `.items()` trực tiếp trên key_points."""
    source = (
        ROOT / "document_setup" / "src" / "modules" / "outline_generation.py"
    ).read_text(encoding="utf-8")
    assert 'paper["key_points"].items()' not in source
    assert 'ref_paper["key_points"].items()' not in source
    assert "format_key_points(" in source

