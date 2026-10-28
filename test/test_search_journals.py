"""Unit test cho `document_setup/src/modules/search_journals.py`.

Nguồn dữ liệu: OpenAlex (thay thế Scimago do bị Cloudflare chặn HTTP 403).
Q1-Q4 là ước lượng theo phân vị trích dẫn (OpenAlex citations).
"""

import asyncio
import json

import pytest

from document_setup.src.configs.app import settings
from document_setup.src.modules import search_journals as sj
from exception_type import AIERROR

VALID_OPENALEX_DATA = {
    "meta": {"count": 2},
    "results": [
        {
            "id": "https://openalex.org/S1",
            "display_name": "Journal One",
            "issn_l": "1234-5678",
            "issn": ["1234-5678"],
            "counts_by_year": [{"year": 2025, "cited_by_count": 1000}],
            "summary_stats": {"2yr_mean_citedness": 10.0},
        },
        {
            "id": "https://openalex.org/S2",
            "display_name": "Journal Two",
            "issn_l": None,
            "issn": ["8765-4321"],
            "counts_by_year": [{"year": 2025, "cited_by_count": 50}],
            "summary_stats": {"2yr_mean_citedness": 1.0},
        },
    ],
}
VALID_OPENALEX_JSON = json.dumps(VALID_OPENALEX_DATA).encode("utf-8")


class FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value):
        self.store[key] = value
        return True


class FakeResponse:
    def __init__(self, status_code: int, content: bytes):
        self.status_code = status_code
        self.content = content


class FakeAsyncClient:
    """Thay `httpx.AsyncClient`: trả nội dung cố định và lưu danh sách gọi kèm params."""

    calls: list[str] = []
    params: list[dict | None] = []
    response: FakeResponse = FakeResponse(200, VALID_OPENALEX_JSON)

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def get(self, url, params=None):
        FakeAsyncClient.calls.append(url)
        FakeAsyncClient.params.append(params)
        return FakeAsyncClient.response


@pytest.fixture(autouse=True)
def patch_httpx(monkeypatch):
    FakeAsyncClient.calls = []
    FakeAsyncClient.params = []
    FakeAsyncClient.response = FakeResponse(200, VALID_OPENALEX_JSON)
    monkeypatch.setattr(sj.httpx, "AsyncClient", FakeAsyncClient)
    yield


def test_setup_noop_when_last_updated_reaches_current_year():
    """Không có năm nào cần crawl ⇒ trả 0 và KHÔNG gọi HTTP."""
    redis = FakeRedis()
    written = asyncio.run(sj.SearchJournal(redis).setup(settings.CURRENT_YEAR))
    assert written == 0
    assert FakeAsyncClient.calls == []
    assert redis.store == {}


def test_setup_writes_issn_and_ranking_keys():
    redis = FakeRedis()
    written = asyncio.run(sj.SearchJournal(redis).setup(settings.CURRENT_YEAR - 1))
    assert len(FakeAsyncClient.calls) == 1
    assert written == 4  # 2 journal_issn + 2 journal_ranking
    assert sum(1 for k in redis.store if k.startswith("journal_issn:")) == 2
    assert sum(1 for k in redis.store if k.startswith("journal_ranking:")) == 2

    # Top journal phải là Q1
    ranking_values = [v for k, v in redis.store.items() if k.startswith("journal_ranking:")]
    assert any('"Q1"' in v for v in ranking_values)


def test_setup_is_idempotent_on_second_run():
    redis = FakeRedis()
    journal = sj.SearchJournal(redis)
    first = asyncio.run(journal.setup(settings.CURRENT_YEAR - 1))
    second = asyncio.run(journal.setup(settings.CURRENT_YEAR - 1))
    assert first == 4
    assert second == 0  # đã có key ⇒ không ghi lại


def test_setup_raises_on_html_response():
    """Khi bị chặn hoặc trả về HTML bất thường ⇒ raise AIERROR(900)."""
    FakeAsyncClient.response = FakeResponse(200, b"<!DOCTYPE html><html><title>Just a moment...</title>")
    with pytest.raises(AIERROR) as exc:
        asyncio.run(sj.SearchJournal(FakeRedis()).setup(settings.CURRENT_YEAR - 1))
    assert exc.value.status_code == 900
    assert "HTML" in exc.value.message


def test_setup_raises_on_http_403():
    FakeAsyncClient.response = FakeResponse(403, b"forbidden")
    with pytest.raises(AIERROR) as exc:
        asyncio.run(sj.SearchJournal(FakeRedis()).setup(settings.CURRENT_YEAR - 1))
    assert exc.value.status_code == 900
    assert "403" in exc.value.message


def test_parse_openalex_response_rejects_missing_results():
    with pytest.raises(AIERROR) as exc:
        sj._parse_openalex_response(b'{"meta": {"count": 0}}')
    assert exc.value.status_code == 900
    assert "results" in exc.value.message


def test_estimate_quartiles_distribution():
    sources = [
        {"display_name": "J1", "issn_l": "1111", "counts_by_year": [{"year": 2025, "cited_by_count": 1000}]},
        {"display_name": "J2", "issn_l": "2222", "counts_by_year": [{"year": 2025, "cited_by_count": 500}]},
        {"display_name": "J3", "issn_l": "3333", "counts_by_year": [{"year": 2025, "cited_by_count": 200}]},
        {"display_name": "J4", "issn_l": "4444", "counts_by_year": [{"year": 2025, "cited_by_count": 10}]},
    ]
    df = sj._estimate_quartiles(sources, 2025)
    assert len(df) == 4
    q_map = dict(zip(df["Title"], df["SJR Best Quartile"]))
    assert q_map["J1"] == "Q1"
    assert q_map["J2"] == "Q2"
    assert q_map["J3"] == "Q3"
    assert q_map["J4"] == "Q4"


def test_get_q_issn_returns_unknown_when_empty():
    q, issn = asyncio.run(sj.SearchJournal(FakeRedis()).get_q_issn("journal_one", "2025"))
    assert (q, issn) == ("Unknown", "Unknown")


def test_get_q_issn_returns_cached_values():
    redis = FakeRedis()
    name = "journal_one"
    hashed = sj.hash_blake3(name)
    redis.store[f"journal_ranking:{hashed}:2025"] = json.dumps({"q": "Q1"})
    redis.store[f"journal_issn:{hashed}"] = json.dumps("1234-5678")

    q, issn = asyncio.run(sj.SearchJournal(redis).get_q_issn(name, "2025"))
    assert q == "Q1"
    assert issn == "1234-5678"


def test_normalize_title():
    assert sj._normalize_title("Journal One") == "journal_one"
    assert sj._normalize_title("journal_one") == "journal_one"
    assert sj._normalize_title("Journal: One") == "journal_one"
    assert sj._normalize_title("  Nature   Communications  ") == "nature_communications"
    assert sj._normalize_title("") == ""


def test_openalex_params_sent():
    redis = FakeRedis()
    asyncio.run(sj.SearchJournal(redis).setup(settings.CURRENT_YEAR - 1))
    assert len(FakeAsyncClient.params) >= 1
    params = FakeAsyncClient.params[0]
    assert params["filter"] == "type:journal"
    assert params["sort"] == "cited_by_count:desc"
    assert params["per_page"] == 200
    assert params["cursor"] == "*"
    assert params["select"] == "id,display_name,issn_l,issn,cited_by_count,summary_stats,counts_by_year"


def test_round_trip_setup_and_get_q_issn():
    redis = FakeRedis()
    journal = sj.SearchJournal(redis)
    target_year = settings.CURRENT_YEAR - 1
    written = asyncio.run(journal.setup(target_year))
    assert written == 4

    # 1. Tra cứu bằng tên hiển thị gốc ("Journal One")
    q, issn = asyncio.run(journal.get_q_issn("Journal One", target_year))
    assert q == "Q1"
    assert issn == "1234-5678"

    # 2. Tra cứu bằng slug đã chuẩn hoá ("journal_one")
    q, issn = asyncio.run(journal.get_q_issn("journal_one", str(target_year)))
    assert q == "Q1"
    assert issn == "1234-5678"

    # 3. Tra cứu có dấu hai chấm ("Journal: One")
    q, issn = asyncio.run(journal.get_q_issn("Journal: One", target_year))
    assert q == "Q1"
    assert issn == "1234-5678"

    # 4. Tra cứu Journal Two (Q3 khi N=2 vì ratio=0.5, issn lấy phần tử đầu tiên của list)
    q, issn = asyncio.run(journal.get_q_issn("Journal Two", target_year))
    assert q == "Q3"
    assert issn == "8765-4321"

    # 5. Fallback khi năm bài báo cũ hơn (vd 2020) ⇒ fallback lấy ranking năm gần nhất
    q, issn = asyncio.run(journal.get_q_issn("Journal One", 2020))
    assert q == "Q1"
    assert issn == "1234-5678"

    # 6. Tạp chí không có trong catalog
    q, issn = asyncio.run(journal.get_q_issn("Unknown Journal", target_year))
    assert q == "Unknown"
    assert issn == "Unknown"


