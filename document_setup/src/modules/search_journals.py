import asyncio
import json
import logging
from typing import Any

import httpx
import pandas as pd
from redis.asyncio import Redis

from document_setup.src.configs.app import settings
from exception_type import AIERROR
from utils import hash_blake3

logger = logging.getLogger(__name__)

JOURNAL_RANKING_KEY = "journal_ranking:{journal_name}:{year}"
JOURNAL_ISSN_KEY = "journal_issn:{journal_name}"

OPENALEX_SOURCES_URL = "https://api.openalex.org/sources"


def _normalize_title(title: str) -> str:
    """Normalize journal title for consistent Redis key hashing across setup and query."""
    if not title:
        return ""
    cleaned = str(title).strip().lower().replace(" ", "_").replace(":", "_")
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_")


def _estimate_quartiles(sources: list[dict[str, Any]], year: int) -> pd.DataFrame:
    """Estimate journal quartile (Q1-Q4) based on OpenAlex citation distribution.

    Note on catalog:
    ----------------
    OpenAlex provides citations, works count, and 2yr_mean_citedness.
    This quartile metric is an ESTIMATE based on citation ranking within the fetched
    catalog (top N journals sorted by citation count), NOT the official Scimago SJR.
    """
    if not sources:
        return pd.DataFrame(columns=["Title", "Issn", "SJR Best Quartile", "year"])

    records = []
    for s in sources:
        title = s.get("display_name") or ""
        if not title:
            continue
        issn_l = s.get("issn_l")
        issn_list = s.get("issn") or []
        issn = issn_l or (issn_list[0] if issn_list else "Unknown")

        # Citation count for the specific target year
        c_year = None
        for cy in s.get("counts_by_year", []):
            if cy.get("year") == year:
                c_year = cy.get("cited_by_count", 0)
                break
        if c_year is None:
            stats = s.get("summary_stats") or {}
            c_year = stats.get("2yr_mean_citedness", 0) or s.get("cited_by_count", 0)

        records.append({
            "Title": title,
            "Issn": issn,
            "citations": float(c_year or 0),
        })

    if not records:
        return pd.DataFrame(columns=["Title", "Issn", "SJR Best Quartile", "year"])

    # Rank journals in descending order of citation metric
    records.sort(key=lambda x: x["citations"], reverse=True)
    n = len(records)
    for i, r in enumerate(records):
        ratio = i / n
        if ratio < 0.25:
            q = "Q1"
        elif ratio < 0.50:
            q = "Q2"
        elif ratio < 0.75:
            q = "Q3"
        else:
            q = "Q4"
        r["SJR Best Quartile"] = q
        r["year"] = year

    df = pd.DataFrame(records)
    return df[["Title", "Issn", "SJR Best Quartile", "year"]]


def _parse_openalex_payload(content: bytes) -> tuple[list[dict[str, Any]], str | None]:
    """Parse JSON payload from OpenAlex, returning (results, next_cursor)."""
    head = content[:200].lstrip().lower()
    if head.startswith(b"<"):
        preview = content[:120].decode("utf-8", errors="replace")
        raise AIERROR(900, f"OpenAlex trả về HTML thay vì JSON (bị chặn). Preview: {preview!r}")

    try:
        data = json.loads(content.decode("utf-8"))
    except Exception as e:
        raise AIERROR(900, f"OpenAlex trả JSON không hợp lệ: {e}") from e

    if not isinstance(data, dict) or "results" not in data:
        raise AIERROR(900, "OpenAlex response thiếu trường 'results'")

    next_cursor = data.get("meta", {}).get("next_cursor") if isinstance(data.get("meta"), dict) else None
    return data["results"], next_cursor


def _parse_openalex_response(content: bytes) -> list[dict[str, Any]]:
    """Parse JSON payload from OpenAlex; returns results list for backward compatibility."""
    results, _ = _parse_openalex_payload(content)
    return results


class SearchJournal:
    """Handles fetching, processing, and storing journal ranking data from OpenAlex.

    Catalog limitation note:
    ------------------------
    Fetches up to `max_sources` (default 200) top journals. Journals outside this
    catalog will return 'Unknown' upon get_q_issn() lookup.
    """

    def __init__(self, redis_client: Redis):
        self.redis_client = redis_client

    def _get_headers(self) -> dict[str, str]:
        email = getattr(settings, "OPEN_ALEX_EMAIL", "") or "dev@autoresearching.ai"
        headers = {
            "User-Agent": f"autoresearching-ai/1.0 (mailto:{email})",
            "Accept": "application/json",
        }
        api_key = getattr(settings, "OPEN_ALEX_KEY", "")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    async def _fetch_sources(
        self,
        client: httpx.AsyncClient,
        max_sources: int = 200,
        per_page: int = 200,
    ) -> list[dict[str, Any]]:
        """Fetch journals from OpenAlex API using cursor pagination up to max_sources."""
        all_sources: list[dict[str, Any]] = []
        cursor = "*"
        limit = max(1, min(per_page, 200))

        while len(all_sources) < max_sources:
            params = {
                "filter": "type:journal",
                "sort": "cited_by_count:desc",
                "per_page": limit,
                "cursor": cursor,
                "select": "id,display_name,issn_l,issn,cited_by_count,summary_stats,counts_by_year",
            }
            response = await client.get(OPENALEX_SOURCES_URL, params=params)
            if response.status_code != 200:
                raise AIERROR(
                    900,
                    f"OpenAlex trả HTTP {response.status_code} ({len(response.content)} bytes)",
                )
            results, next_cursor = await asyncio.to_thread(_parse_openalex_payload, response.content)
            if not results:
                break
            all_sources.extend(results)
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor

        return all_sources[:max_sources]

    async def setup(self, last_updated: int = 1999, max_sources: int = 200) -> int:
        """Fetches journal ranking data from OpenAlex for years [last_updated, CURRENT_YEAR)."""
        years_j = list(range(last_updated, settings.CURRENT_YEAR))
        if not years_j:
            return 0

        async with httpx.AsyncClient(headers=self._get_headers(), timeout=60.0, follow_redirects=True) as client:
            sources = await self._fetch_sources(client, max_sources=max_sources)

        if not sources:
            return 0

        frames = [_estimate_quartiles(sources, year) for year in years_j]
        df_jr = pd.concat(frames, ignore_index=True)
        if df_jr.empty:
            return 0

        issn_dict = df_jr[["Title", "Issn"]].drop_duplicates().set_index("Title").to_dict()["Issn"]

        written = 0
        for title, issn_value in issn_dict.items():
            norm_title = _normalize_title(title)
            key = JOURNAL_ISSN_KEY.format(journal_name=hash_blake3(norm_title))
            if await self.redis_client.get(key) is None:
                issn = issn_value if issn_value != "-" else "Unknown"
                await self.redis_client.set(key, json.dumps(issn))
                written += 1

        for row in range(len(df_jr)):
            norm_title = _normalize_title(str(df_jr["Title"][row]))
            key = JOURNAL_RANKING_KEY.format(
                journal_name=hash_blake3(norm_title),
                year=df_jr["year"][row],
            )
            if await self.redis_client.get(key) is None:
                quartile = df_jr["SJR Best Quartile"][row]
                value = {"q": quartile if quartile != "-" else "Unknown"}
                await self.redis_client.set(key, json.dumps(value))
                written += 1

        return written

    async def get_q_issn(self, journal_name: str, year: str | int) -> tuple[str, str]:
        """Retrieves Best Quartile (q) and ISSN for a given journal name and year from Redis."""
        norm_title = _normalize_title(journal_name)
        hashed = hash_blake3(norm_title)

        ranking_key = JOURNAL_RANKING_KEY.format(journal_name=hashed, year=str(year))
        q_bytes = await self.redis_client.get(ranking_key)

        # Fallback về năm gần nhất nếu năm yêu cầu không có trong cache
        if q_bytes is None and year:
            fallback_year = str(settings.CURRENT_YEAR - 1)
            if str(year) != fallback_year:
                fallback_key = JOURNAL_RANKING_KEY.format(journal_name=hashed, year=fallback_year)
                q_bytes = await self.redis_client.get(fallback_key)

        q = "Unknown"
        if q_bytes is not None:
            try:
                q = json.loads(q_bytes).get("q", "Unknown")
            except Exception:
                q = "Unknown"

        issn_key = JOURNAL_ISSN_KEY.format(journal_name=hashed)
        issn_bytes = await self.redis_client.get(issn_key)
        issn = "Unknown"
        if issn_bytes is not None:
            try:
                issn = json.loads(issn_bytes)
            except Exception:
                issn = "Unknown"
        return q, issn
