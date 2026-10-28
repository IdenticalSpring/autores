"""Unit tests for idea_generation module in document_setup."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from document_setup.src.modules.idea_generation import (
    _criteria_schema,
    _criteria_text,
    _lang_key,
    IdeaGeneration,
    DEFAULT_LEVEL,
)
from document_setup.src.schemas.proposal import (
    Domains,
    SubDomains,
    Phrases,
    KeywordType,
    ResearchType,
    Ideas,
    Idea,
    IdeaV2,
)


def test_criteria_helpers():
    # Valid level
    text = _criteria_text("LUAN_VAN_THAC_SI")
    assert text != ""

    # Invalid level triggers fallback to DEFAULT_LEVEL
    text_fallback = _criteria_text("UNKNOWN_LEVEL_XYZ")
    assert text_fallback == _criteria_text(DEFAULT_LEVEL)

    # Schema helper
    schema = _criteria_schema("LUAN_VAN_THAC_SI")
    assert schema is not None

    schema_fallback = _criteria_schema("UNKNOWN_LEVEL_XYZ")
    assert schema_fallback == _criteria_schema(DEFAULT_LEVEL)


def test_lang_key():
    assert _lang_key("vi") == "tiếng việt"
    assert _lang_key("Vietnamese") == "tiếng việt"
    assert _lang_key("en") == "english"
    assert _lang_key("English") == "english"
    assert _lang_key("unknown_lang") == "tiếng việt"


@pytest.fixture
def mock_idea_gen():
    with patch("document_setup.src.modules.idea_generation.get_llm") as mock_get_llm, \
         patch("document_setup.src.modules.idea_generation.get_minio_client") as mock_minio:
        mock_get_llm.return_value = MagicMock()
        mock_minio.return_value = MagicMock()
        igen = IdeaGeneration(
            document_id="650000000000000000000001",
            model_id="gemini-flash",
            language="Vietnamese",
            llm_key="fake-key",
            max_tokens=2048,
        )
        return igen


def test_idea_generation_id_and_str(mock_idea_gen):
    assert mock_idea_gen.id == "65000000"
    assert str(mock_idea_gen) == "65000000"


def test_generate_domains(mock_idea_gen):
    async def _run():
        dummy_domains = Domains(domains=["Kinh tế số", "Quản trị kinh doanh", "Thương mại điện tử", "Marketing"])
        with patch(
            "document_setup.src.modules.idea_generation.get_answer_with_schema",
            new_callable=AsyncMock,
            return_value=(None, True, dummy_domains, 100, 50),
        ):
            res, in_tok, out_tok = await mock_idea_gen.generate_domains("Kinh tế", 4)
            assert "domains" in res
            assert len(res["domains"]) == 4
            assert in_tok == 100
            assert out_tok == 50

    asyncio.run(_run())


def test_generate_subdomains(mock_idea_gen):
    async def _run():
        dummy_sub = SubDomains(subdomains=["Hành vi người tiêu dùng", "Livestream bán hàng", "Thương mại di động"])
        with patch(
            "document_setup.src.modules.idea_generation.get_answer_with_schema",
            new_callable=AsyncMock,
            return_value=(None, True, dummy_sub, 80, 40),
        ):
            res, in_tok, out_tok = await mock_idea_gen.generate_subdomains("Kinh tế", "Thương mại", 3)
            assert "subdomains" in res
            assert len(res["subdomains"]) == 3
            assert in_tok == 80
            assert out_tok == 40

    asyncio.run(_run())


def test_generate_keywords_v3(mock_idea_gen):
    async def _run():
        dummy_phrases = Phrases(phrases=["livestream bán hàng", "hành vi mua sắm"])
        mock_idea_gen.match_phrase = AsyncMock(side_effect=[
            ("main_keywords", "livestream bán hàng", 10, 5),
            ("supplementary_keywords", "hành vi mua sắm", 10, 5),
        ])
        with patch(
            "document_setup.src.modules.idea_generation.get_answer_with_schema",
            new_callable=AsyncMock,
            return_value=(None, True, dummy_phrases, 50, 20),
        ):
            res, in_tok, out_tok = await mock_idea_gen.generate_keywords_v3({"title": "Test Title"})
            assert "main_keywords" in res
            assert "livestream bán hàng" in res["main_keywords"]
            assert "supplementary_keywords" in res
            assert "hành vi mua sắm" in res["supplementary_keywords"]

    asyncio.run(_run())


def test_match_phrase(mock_idea_gen):
    async def _run():
        dummy_kw = KeywordType(keyword_type="main_keywords")
        with patch(
            "document_setup.src.modules.idea_generation.get_answer_with_schema",
            new_callable=AsyncMock,
            return_value=(None, True, dummy_kw, 30, 15),
        ):
            k_type, phrase, in_t, out_t = await mock_idea_gen.match_phrase("livestream", "Livestream và bán hàng")
            assert k_type == "main_keywords"
            assert phrase == "livestream"
            assert in_t == 30

    asyncio.run(_run())


def test_get_research_type(mock_idea_gen):
    async def _run():
        dummy_rt = ResearchType(type=1, reason="Nghiên cứu định lượng")
        with patch(
            "document_setup.src.modules.idea_generation.get_answer_with_schema",
            new_callable=AsyncMock,
            return_value=(None, True, dummy_rt, 40, 20),
        ):
            res, in_t, out_t = await mock_idea_gen.get_research_type({"title": "ABC"}, "Kinh tế", "TMĐT")
            assert res["type"] == 1
            assert res["reason"] == "Nghiên cứu định lượng"

    asyncio.run(_run())


def test_mix_titles_v2(mock_idea_gen):
    async def _run():
        dummy_ideas = Ideas(ideas=[
            IdeaV2(idea="Ý tưởng 1", web_search_result="kết quả 1"),
            IdeaV2(idea="Ý tưởng 2", web_search_result="kết quả 2"),
        ])
        mock_idea_gen._generate_proposal_v2 = AsyncMock(side_effect=[
            ({"title": "Đề tài 1", "content": "Nội dung 1"}, 20, 10),
            ({"title": "Đề tài 2", "content": "Nội dung 2"}, 20, 10),
        ])
        with patch(
            "document_setup.src.modules.idea_generation.get_answer_with_schema",
            new_callable=AsyncMock,
            return_value=(None, True, dummy_ideas, 60, 30),
        ):
            proposals, in_t, out_t = await mock_idea_gen.mix_titles_v2(
                field="Kinh tế",
                domain="Thương mại",
                level="LUAN_VAN_THAC_SI",
                used_proposals=[{"title": "Đề tài cũ"}],
                num_ideas=2,
            )
            assert len(proposals) == 2
            assert proposals[0]["title"] == "Đề tài 1"
            assert proposals[1]["title"] == "Đề tài 2"

    asyncio.run(_run())


def test_update_title(mock_idea_gen):
    async def _run():
        mock_idea_gen._generate_proposal_v2 = AsyncMock(
            return_value=({"title": "Old Title", "desc": "Desc"}, 15, 10)
        )
        res = await mock_idea_gen.update_title(
            field="Kinh tế",
            domain="Marketing",
            knowledge_base=[],
            title="New Title",
            level="LUAN_VAN_THAC_SI",
        )
        assert res["title"] == "New Title"

    asyncio.run(_run())


def test_generate_proposal_v2(mock_idea_gen):
    async def _run():
        dummy_ideas = Ideas(ideas=[
            IdeaV2(idea="Ý tưởng A", web_search_result="web A"),
        ])
        mock_idea_gen._generate_proposal_v2 = AsyncMock(
            return_value=({"title": "Đề xuất A", "desc": "Chi tiết A"}, 25, 12)
        )
        with patch(
            "document_setup.src.modules.idea_generation.get_answer_with_schema",
            new_callable=AsyncMock,
            return_value=(None, True, dummy_ideas, 50, 20),
        ):
            proposals = await mock_idea_gen.generate_proposal_v2(
                field="Kinh tế",
                domain="Marketing",
                knowledge_base=[],
                level="LUAN_VAN_THAC_SI",
                num_ideas=1,
            )
            assert len(proposals) == 1
            assert proposals[0]["title"] == "Đề xuất A"

    asyncio.run(_run())


def test_generate_titles_v2_empty_subdomains(mock_idea_gen):
    async def _run():
        mock_idea_gen.generate_proposal_v2 = AsyncMock(
            return_value=[{"title": "Đề xuất 1"}]
        )
        proposals, kb = await mock_idea_gen.generate_titles_v2(
            field="Kinh tế",
            domain="Tài chính",
            subdomains=[],
            level="LUAN_VAN_THAC_SI",
        )
        assert len(proposals) == 1
        assert kb == []

    asyncio.run(_run())

