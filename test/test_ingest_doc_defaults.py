"""Default `source`/`ref_id` trong `_process_single_paper` (smoke 2026-11-01).

Trước fix: `admin_documents` → `KeyError: 'ref_id'` (618);
`embed_user_documents` (processed_docs thiếu `source`) → `KeyError: 'source'` (621).
"""

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from document_setup.src.modules.ingest_docs import VectorDBProcessor  # noqa: E402


class _Collection:
    def __init__(self):
        self.inserted = []

    async def find_one(self, *_args, **_kwargs):
        return None

    async def insert_one(self, doc):
        self.inserted.append(doc)


class _Mongo:
    def __init__(self, collection):
        self.collection = collection

    def __getitem__(self, name):
        return {"processed_docs": self.collection}


def _make_processor():
    obj = object.__new__(VectorDBProcessor)
    obj.document_id = "doc-defaults-test"
    obj.input_tokens = 0
    obj.output_tokens = 0

    async def fake_full_text(_title, _url):
        return "nội dung giả lập", "file-1"

    async def fake_key_points(_text):
        return {"ý": "nội dung"}, 0, 0

    async def fake_publication_info(_text):
        return SimpleNamespace(
            authors=["A"], year="2024", journal="J", publisher="P",
            language="vi", type="article", status="published",
        ), 0, 0

    async def fake_abstract(_text):
        return SimpleNamespace(abstract="Tóm tắt"), 0, 0

    obj.get_full_text_async = fake_full_text
    obj.get_key_points = fake_key_points
    obj.publication_info = fake_publication_info
    obj.get_abstract = fake_abstract
    return obj


def test_user_doc_without_source_or_ref_id_is_accepted():
    processor = _make_processor()
    result = asyncio.run(
        processor._process_single_paper({"url": "bucket/key.pdf", "title": "Paper"}, None)
    )

    assert result[0]["source"] == "USER"
    assert result[0]["title"] == "Paper"
    assert result[0]["authors"] == "Author"
    assert result[0]["year"] == "Unknown"


def test_admin_doc_without_ref_id_or_metadata_is_accepted():
    processor = _make_processor()
    collection = _Collection()
    result = asyncio.run(
        processor._process_single_paper(
            {"url": "bucket/key.pdf", "source": "ADMIN"}, _Mongo(collection)
        )
    )

    assert result[0]["source"] == "ADMIN"
    # Thiếu title ⇒ lấy tên file từ url thay vì KeyError
    assert result[0]["title"] == "key.pdf"
    assert result[0]["url"] == "bucket/key.pdf"
    assert collection.inserted, "admin doc phải được lưu processed_docs"
    assert collection.inserted[0]["documentId"] == "bucket/key.pdf"
