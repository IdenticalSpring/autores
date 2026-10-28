import asyncio
from bson import ObjectId
from collections import defaultdict
import copy
from langchain_qdrant import QdrantVectorStore
from utils import get_embeddings
import logging
from pymongo import AsyncMongoClient, ASCENDING
from qdrant_client import models
import string
import random
import rapidfuzz
import re
import tiktoken

from document_setup.src.modules.outline_prompt_bank import (
    SUBHEADINGS_DESCRIPTION_PROMPT_V2,
    HEADINGS_DESCRIPTION_PROMPT,
    CHOOSE_REFS,
    UPDATE_OUTLINE,
    UPDATE_OUTLINE_PERCENT,
    CHOOSE_USER_REFS,
    CHOOSE_USER_REFS_V2,
    GENRATE_SUBHEADINGS,
    CHOOSE_REFS_SECTION,
    REORDER_OUTLINE,
    UPDATE_OUTLINE_CHAT,
    REORDER_SUBHEADINGS_PRIORITIES_V2,
    CHOOSE_SUBSECTION,
)
from document_setup.src.schemas.outline import (
    Outline, 
    SectionRefs,
    Heading, 
    OutlineChat,
    OutlinePercent,
    UserPaper,
    SubHeadingInfo, 
    HeadingDescription,
    UpdatedOutline,
    UpdatedSubheading,
    SubsectionRefs,
    SubHeading,
    ReorderOutline,
    SearchQuery,
    BatchReferenceUsagePlan,
    IntegrationParagraph,
    UserPaperV2,
    SubsectionMapping,
)
from document_setup.src.configs.app import settings

from get_llm_response import get_llm, get_answer_with_schema
from translate import LANGUAGE_KIT, get_language_dict
from utils import markdownify_keep_images, hash_blake3, _clean_text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


class OutlineGeneration():

    def __init__(self, document_id: str, mongo_client: AsyncMongoClient, model_id: str, language: str, llm_key: str, max_tokens: int):
        self.document_id = document_id
        self.model_id = model_id
        self.llm = get_llm(self.model_id, llm_key, max_tokens, 1.0)
        self.model_id = model_id
        self.language = language
        self.input_tokens = 0
        self.output_tokens = 0
        self.embed_tokens = 0
        self.mongo_client = mongo_client
        self.get_refs = False

    @property
    def id(self):
        return self.document_id[:8]

    def __str__(self):
        return self.id

    def generate_random_string(self) -> str:
        characters = string.ascii_letters + string.digits
        return "".join(random.choices(characters, k=15))

    async def get_key_points(self, papers: list[dict]) -> dict:
        db = self.mongo_client["user_documents"]
        collection = db["reports_refs"]
        cached_data = await collection.find_one({"_id": self.document_id})
        documents = cached_data["documents"]
        doc_map = {d["title"]: d for d in documents if "title" in d}
        for paper in papers:
            document = doc_map[paper["title"]]
            paper["key_points"] = document["key_points"]
            paper["source"] = document["source"]
        return papers

    async def get_subheading_detail(
        self, 
        default_outline: list[dict] | str,
        final_proposal: dict, 
        section: dict, 
        section_over_view: str, 
        subheading: str | dict,
    ) -> tuple[dict, int, int]:
        language_dict = get_language_dict(self.language)
        subheading_title = subheading if isinstance(subheading, str) else subheading.get("subheading", "")
        if isinstance(subheading, str) or (isinstance(subheading, dict) and not (subheading.get("detail_description") and subheading.get("subheading_word_count"))):
            logger.info(f"[{self}] Heading {section['heading']} - Subheading {subheading_title}")
            content = f"""
            Example outline:
            {default_outline}
            Final proposal:
            {final_proposal}
            Section detail:
            {section}
            Section overview:
            {section_over_view}
            Target subheading:
            {subheading}
            User's language:
            {self.language}
            """
            error, success, subheading_description, input_tokens, output_tokens = await get_answer_with_schema(
                self.id, 
                self.llm, 
                SUBHEADINGS_DESCRIPTION_PROMPT_V2, 
                content, 
                SubHeadingInfo
            )
            if not success:
                if error.status_code in [401, 403, 429, 500]:
                    raise error
                logger.info(f"[{self}] Fail to generate subheading description")
                word_count = section.get("word_count", "0-0").split("-")
                lower_bound = int(word_count[0] / len(section["subheadings"]))
                upper_bound = int(word_count[1] / len(section["subheadings"]))
                subheading_description = SubHeadingInfo(
                    detail_description=subheading_title,
                    subheading_word_count=f"{lower_bound}-{upper_bound}"
                )
            subheading_tunned = SubHeading(
                subheading=subheading_title,
                detail_description=subheading_description.detail_description.replace(
                    "- what to write for subsection ", language_dict["- what to write for subsection "]
                ).replace(
                    "- subsection ", language_dict["- subsection "]
                ).replace("\n", "</br>"),
                subheading_word_count=subheading_description.subheading_word_count,
            )
            return subheading_tunned, input_tokens, output_tokens
        else:
            subheading_tunned = SubHeading(
                subheading=subheading["subheading"],
                detail_description=subheading["detail_description"],
                subheading_word_count=subheading["subheading_word_count"],
            )
            return subheading_tunned, 0 , 0
    
    async def get_heading_description(
        self, 
        default_outline: list[dict] | str, 
        final_proposal: dict, 
        section: dict
    ) -> tuple[Heading, int, int]:
        logger.info(f"[{self}] Heading {section["heading"]}")
        if ("overview" in section and not section["overview"]) or not section.get("overview"):
            content = f"""
            Example outline:
            {default_outline}
            Final proposal:
            {final_proposal}
            Section detail:
            {section}
            """
            error, success, description, des_input_tokens, des_output_tokens = await get_answer_with_schema(
                self.id, 
                self.llm, 
                HEADINGS_DESCRIPTION_PROMPT, 
                content, 
                HeadingDescription
            )
            if not success:
                if error.status_code in [401, 403, 429, 500]:
                    raise error
                logger.info(f"[{self}] Fail to generate heading description")
                description = HeadingDescription(overview=section["heading"])
                des_input_tokens = 0
                des_output_tokens = 0
        else:
            description = HeadingDescription(overview=section["overview"])
            des_input_tokens = 0
            des_output_tokens = 0
        # Check if this section should have subheadings
        # If subheadings key is explicitly None, keep it as None (heading-only section)
        # If subheadings is missing or empty list, auto-generate them
        has_subheadings = section.get("subheadings") is not None

        if has_subheadings and (not section["subheadings"] or (isinstance(section["subheadings"], list) and len(section["subheadings"]) == 0)):
            # Auto-generate subheadings if they should exist but are empty
            content = f"""
            Example outline:
            {default_outline}
            Final proposal:
            {final_proposal}
            Section name: {section}
            Section description: {description.overview}
            """
            error, success, list_subheadings, sub_input_tokens, sub_output_tokens = await get_answer_with_schema(
                self.id, 
                self.llm, 
                GENRATE_SUBHEADINGS, 
                content, 
                UpdatedSubheading
            )
            if not success:
                if error.status_code in [401, 403, 429, 500]:
                    raise error
                logger.info(f"[{self}] Fail to generate subheading")
                section["subheadings"] = section["heading"] + ":"
                sub_input_tokens = 0
                sub_output_tokens = 0
            else:
                section["subheadings"] = list_subheadings.subheadings
        else:
            sub_input_tokens = 0
            sub_output_tokens = 0

        # Process subheadings if they exist
        if has_subheadings and section.get("subheadings"):
            tasks = [
                self.get_subheading_detail(default_outline, final_proposal, section, description.overview, subsection)
                for subsection in section["subheadings"] if isinstance(subsection, str) or (isinstance(subsection, dict) and subsection["subheading"])
            ]
            results = await asyncio.gather(*tasks)
            subheadings = [result[0] for result in results]
            input_tokens = [result[1] for result in results]
            output_tokens = [result[2] for result in results]
        else:
            # No subheadings for this section
            subheadings = None
            input_tokens = []
            output_tokens = []

        return Heading(
            heading=section.get("heading", ""),
            word_count=section.get("word_count", ""),
            overview=description.overview,
            subheadings=subheadings
        ), sum(input_tokens) + des_input_tokens + sub_input_tokens, sum(output_tokens) + des_output_tokens + sub_output_tokens
    
    async def generate_outline_description_v2(
        self,
        field: str,
        domain: str,
        research_type: str,
        word_count_str: str, 
        outline: list[dict], 
        outline_code: list[str],
        headings_percent: list[float], 
        final_proposal: dict,
    ) -> tuple[dict, int, int]:
        word_count = [
            int(num) for num in word_count_str.split("-")
        ]
        headings_word_count = [
            f"{int(word_count[0] * heading_percent)}-{int(word_count[1] * heading_percent)}" 
            for heading_percent in headings_percent
        ]
        for section_num, heading_word_count in zip(range(len(outline)), headings_word_count):
            outline[section_num]["word_count"] = heading_word_count
        logger.info(f"[{self}] Generating AI outline")
        new_outline = await self.update_outline(field, domain, research_type, final_proposal, outline, outline_code)
        new_outline_code: list[str] = []
        for section in range(len(new_outline)):
            new_outline_code.append(new_outline[section]["code"])
            del new_outline[section]["code"]
        logger.info(f"[{self}] Generated AI outline")
        tasks = [self.get_heading_description(outline, final_proposal, section) for section in new_outline]
        results = await asyncio.gather(*tasks)
        headings = [result[0] for result in results]
        input_tokens = [result[1] for result in results]
        output_tokens = [result[2] for result in results]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        final_outline = Outline(
            title=final_proposal["title"],
            outline=headings,
        )
        for section_count, section in enumerate(final_outline.outline):
            section.heading = f"{section_count + 1}. " + section.heading
            # Only number subheadings if they exist
            if section.has_subheadings():
                for subsection_count, subsection in enumerate(section.subheadings):
                    subsection.subheading = f"{section_count + 1}.{subsection_count + 1}. " + subsection.subheading
        result = final_outline.model_dump()
        for section in range(len(result["outline"])):
            result["outline"][section]["code"] = new_outline_code[section]
        return result, self.input_tokens, self.output_tokens
    
    async def get_user_refs_usage(
        self,
        final_proposal: dict,
        final_outline: list[dict],
        target_paper: dict,
    ) -> tuple[bool, UserPaper, int, int]:
        logger.info(f"[{self}] Choosing sections for papers {target_paper["title"]}")
        content = f"""
            Report proposal:
            {final_proposal},
            Report sections:
            {[f"Section's heading:\n\n{section["heading"]}\n\nSection's description:\n\n{section["overview"]}\n\n" for section in final_outline]}
            **Provided research paper**:
            Paper's title: 
            {target_paper["title"]}
            Paper key points: 
            {target_paper["key_points"]}
            Generate in user's language
            User's language:
            {self.language}
        """
        error, success, user_paper, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm, 
            CHOOSE_USER_REFS, 
            content, 
            UserPaper
        )
        if not success:
            if error.status_code in [401, 403, 429, 500]:
                raise error
            logger.info(f"[{self}] Fail to choosing sections for paper {target_paper["title"]}")
            return False, UserPaper(title="", usage=[]), 0, 0
        else:
            if len(user_paper.usage):
                user_paper.title = target_paper["title"]
                headings = [section["heading"] for section in final_outline]
                user_paper_headings = [section for section in user_paper.usage]
                correct_headings = self.title_matching(user_paper_headings, headings)
                for i, heading in enumerate(correct_headings):
                    user_paper.usage[i] = heading
                user_paper.usage = list(set([usage for usage in user_paper.usage if usage]))
                return True, user_paper, input_tokens, output_tokens
            else:
                return False, UserPaper(title="", usage=[]), input_tokens, output_tokens
        
    def title_matching(self, queries: list[str], titles: list[str], cut_off_score=85) -> list[str]:
        normalized: list[str] = []
        titles_set = set(titles)
        for query in queries:
            if query in titles_set:
                normalized.append(query)
            else:
                best_match = rapidfuzz.process.extractOne(query, titles, scorer=rapidfuzz.fuzz.WRatio)
                if best_match and best_match[1] >= cut_off_score:
                    normalized.append(best_match[0])
                else:
                    normalized.append("")
        return normalized
            
    def find_best_match(self, query: str, candidates: list, key_fn, cutoff=90):
        """Return (object, index) of the best fuzzy match above cutoff."""
        if not candidates:
            return None, None
        names = [key_fn(c) for c in candidates]
        best = rapidfuzz.process.extractOne(query, names, scorer=rapidfuzz.fuzz.WRatio)
        if best and best[1] >= cutoff:
            idx = names.index(best[0])
            return candidates[idx], idx
        return None, None

    async def sort_subheading(self, final_subs: list[dict], section_heading: str) -> list[dict]:
        len_sub = 0
        trial = 0
        content = {
            "section_heading": section_heading,
            "subheadings": final_subs
        }
        while len_sub != len(final_subs) and trial < 5:    
            error, success, ordered_section, input_tokens, output_tokens = await get_answer_with_schema(
                self.id, 
                self.llm, 
                REORDER_SUBHEADINGS_PRIORITIES_V2,
                str(content),
                ReorderOutline,
            )
            self.input_tokens += input_tokens
            self.output_tokens += output_tokens
            if not success:
                if error.status_code in [401, 403, 429, 500]:
                    raise error
            else:
                priorities = ordered_section.reorder_outline
                if priorities[0] != min(priorities):
                    current_min = min(priorities)
                    delta = priorities[0] - current_min
                    priorities = [p + delta for p in priorities]
                    priorities[0] = current_min

                if priorities[-1] != max(priorities):
                    current_max = max(priorities)
                    delta = current_max - priorities[-1]
                    priorities = [p - delta for p in priorities]
                    priorities[-1] = current_max
                len_sub = len(priorities)
                trial += 1
        if len_sub != len(final_subs):
            logger.warning("Mismatch order. Returning unordered subheadings.")
            return final_subs
        else:
            sortable_items = []
            for i, sub_item in enumerate(final_subs):
                sortable_items.append({
                    "item": sub_item,
                    "priority": priorities[i],
                    "original_index": i
                })

            sorted_list = sorted(sortable_items, key=lambda x: (x['priority'], x['original_index']))
            return [item['item'] for item in sorted_list]
        
    async def update_outline(
        self, 
        field: str, 
        domain: str, 
        research_type: str, 
        proposal: dict, 
        default_outline: list[dict],
        outline_code: list[str],
    ) -> list[dict]:
        for heading in range(len(default_outline)):
            new_subheadings = [item["subheading"] for item in default_outline[heading]["subheadings"]]
            default_outline[heading]["subheadings"] = new_subheadings
        content = f"""
        Default outline:
        {default_outline}
        User's field:
        {field}
        User's domain:
        {domain}
        User's report proposal:
        {proposal}
        User's research type:
        {research_type}
        User's language:
        {self.language}
        """
        for heading in range(len(default_outline)):
            default_outline[heading]["code"] = outline_code[heading]
        error, success, updated_outline, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm, 
            UPDATE_OUTLINE, 
            content, 
            UpdatedOutline,
        )
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        if not success:
            if error.status_code in [401, 403, 429, 500]:
                raise error
            logger.info(f"[{self}] Fail to updated outline. Revert back to the default outline")
            return default_outline
        unmatched_ai_sections = list(updated_outline.updated_outline)
        final_outline = []
        seen_codes = set(outline_code)
        for original_section in default_outline:
            orig_heading = original_section["heading"]

            best_ai_match, match_idx = self.find_best_match(
                orig_heading, unmatched_ai_sections, key_fn=lambda x: x.heading
            )

            if best_ai_match:
                new_sub = False
                updated_section = copy.deepcopy(original_section)
                updated_section["heading"] = best_ai_match.heading
                updated_section["word_count"] = best_ai_match.word_count

                original_subs = list(updated_section.get("subheadings", [])) 
                ai_subs = list(best_ai_match.subheadings)
                final_subs: list[str] = []
                unmatched_originals = original_subs.copy()
                for ai_sub in ai_subs:
                    best_orig_match, sub_match_idx = self.find_best_match(
                        ai_sub, unmatched_originals, key_fn=lambda x: x
                    )
                    if best_orig_match:
                        final_subs.append(ai_sub)
                        unmatched_originals.pop(sub_match_idx)
                    else:
                        new_sub = True
                        final_subs.append(ai_sub)
                if original_subs:
                    if original_subs[0] in final_subs:
                        final_subs.pop(final_subs.index(original_subs[0]))
                    final_subs.insert(0, original_subs[0])
                    if original_subs[-1] in final_subs:
                        final_subs.pop(final_subs.index(original_subs[-1]))
                    final_subs.append(original_subs[-1])
                if new_sub:
                    updated_section["subheadings"] = await self.sort_subheading(final_subs, updated_section["heading"])
                else:
                    updated_section["subheadings"] = final_subs
                final_outline.append(updated_section)
                unmatched_ai_sections.pop(match_idx)

            else:
                logger.info(f"[{self}] No match for section: {orig_heading}")

        for new_section in unmatched_ai_sections:
            new_entry = {
                "heading": new_section.heading,
                "word_count": new_section.word_count,
                "subheadings": new_section.subheadings,
            }
            new_code = self.generate_random_string()
            while new_code in seen_codes:
                new_code = self.generate_random_string()
            new_entry["code"] = new_code
            new_entry["subheadings"] = await self.sort_subheading(new_entry["subheadings"], new_entry["heading"])
            seen_codes.add(new_code)
            final_outline.append(new_entry)
        new_heading = len(unmatched_ai_sections)
        if not new_heading:
            return final_outline
        else:
            len_order_outline = 0
            trial = 0
            content = f"""
            User's field:
            {field}
            User's domain:
            {domain}
            User's report proposal:
            {proposal}
            User's research type:
            {research_type}
            List of report's headings:
            {[heading["heading"] for heading in final_outline]}
            List of default headings:
            {[heading["heading"] for heading in default_outline]}
            """
            while len_order_outline != len(final_outline) and trial < 5:
                error, success, ordered_outline, input_tokens, output_tokens = await get_answer_with_schema(
                    self.id, 
                    self.llm, 
                    REORDER_OUTLINE, 
                    content, 
                    ReorderOutline,
                )
                self.input_tokens += input_tokens
                self.output_tokens += output_tokens
                if not success:
                    if error.status_code in [401, 403, 429, 500]:
                        raise error
                else:
                    len_order_outline = len(ordered_outline.reorder_outline)
                    trial += 1
            if len_order_outline != len(final_outline):
                logger.warning("Mismatch order. Returning unordered outline.")
                return final_outline
            sortable_items = []
            for i, section in enumerate(final_outline):
                sortable_items.append({
                    "item": section,
                    "priority": ordered_outline.reorder_outline[i],
                    "original_index": i
                })
            sorted_list = sorted(sortable_items, key=lambda x: (x['priority'], x['original_index']))
            reordered_outline = [item['item'] for item in sorted_list]
            logger.info(f"[{self}] Successfully reordered outline.")
            return reordered_outline

    async def update_outline_percent(
        self, 
        field: str,
        domain: str,
        research_type: str, 
        proposal: dict,
        outline: list[str], 
        headings_percent: list[float],
        new_outline: list[str], 
        new_headings_percent: list[float]
    ) -> list[float]:
        if sum(new_headings_percent) != 1 or 0 in new_headings_percent:
            logger.info(f"[{self}] Updating heading percentage")
            content = f"""
            User's field:
            {field}
            User's domain:
            {domain}
            User's report proposal:
            {proposal}
            User's research type:
            {research_type}
            Current outline and percentage:
            {[f"- {heading}: {100 * percent}\n" for heading, percent in zip(outline, headings_percent)]}
            New outline and percentage:
            {[f"- {heading}: {100 * percent if percent else "To be confirmed"}\n" for heading, percent in zip(new_outline, new_headings_percent)]}
            Generate a list of float from 1 to 99, the list sum up to 100, with a length of {len(new_outline)}
            """
            output_len = 0
            output_sum = 0
            total_input_tokens = 0
            total_output_tokens = 0
            trial = 0
            has_zero = 0 in new_headings_percent
            while (output_len != len(new_outline) or has_zero or output_sum != 100) and trial < 5:
                logger.info(f"[{self}] Updating heading percentage trial: {trial + 1}/5")
                error, success, updated_outline, input_tokens, output_tokens = await get_answer_with_schema(
                    self.id, 
                    self.llm, 
                    UPDATE_OUTLINE_PERCENT, 
                    content, 
                    OutlinePercent,
                )
                if not success:
                    raise error
                else:
                    output_len = len(updated_outline.headings_percent)
                    total_input_tokens += input_tokens
                    total_output_tokens += output_tokens
                    trial += 1
                    has_zero = 0 in updated_outline.headings_percent
                    output_sum = sum(updated_outline.headings_percent)
            return [percent / 100 for percent in updated_outline.headings_percent], total_input_tokens, total_output_tokens
        else:
            return new_headings_percent, 0, 0

    async def get_refs_subsection(
        self,
        section: dict,
        research_papers: list[dict], 
        use_research_papers: set,
        final_proposal: dict,
        subsection: dict,
    ) -> tuple[list[dict], int, int, set]:
        logger.info(f"[{self}] Choosing refs for section {section["heading"]} - Subsection {subsection["subheading"]}")
        content = f"""
        Report proposal:
        {final_proposal},
        Report sections:
        Section's Heading: 
        {section["heading"]}
        Section's Description: 
        {section["overview"]}
        Subsection:
        {subsection["subheading"]}
        Subsection's detail description:
        {subsection["detail_description"]}
        **Provided research papers**:
        {[f"Paper's title: {paper["title"]}\nPaper key points: {paper["key_points"]}" for paper in research_papers if paper["title"] in use_research_papers]}
        Generate in user's language
        User's language:
        {self.language}
        """
        error, success, subsection_refs, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm, 
            CHOOSE_REFS, 
            content, 
            SubsectionRefs
        )
        seen_titles: set = set()
        if not success:
            if error.status_code in [401, 403, 429, 500]:
                raise error
            logger.info(f"[{self}] Fail to choosing refs for section {section["heading"]}")
            return [], 0, 0, seen_titles
        else:
            titles = [paper["title"] for paper in research_papers]
            section_refs_titles = [ref_usage.title for ref_usage in subsection_refs.subsection_refs]
            correct_titles = self.title_matching(section_refs_titles, titles)
            final_ref_usage: list[dict] = []
            for ref_usage, correct_title in zip(subsection_refs.subsection_refs, correct_titles):
                if correct_title:
                    final_ref_usage.append(
                        {
                            "title": correct_title,
                            "usage": ref_usage.usage
                        }                           
                    )
                    seen_titles.add(correct_title)
            return final_ref_usage, input_tokens, output_tokens, seen_titles

    async def get_refs_section(
        self,
        section: dict,
        research_papers: list[dict], 
        final_proposal: dict,
    ) -> tuple[list[dict], int, int, set]:
        logger.info(f"[{self}] Choosing refs for section {section["heading"]}")
        content = f"""
        Report proposal:
        {final_proposal},
        Report sections:
        Section's Heading: 
        {section["heading"]}
        Section's Description: 
        {section["overview"]}
        **Provided research papers**:
        {[f"Paper's title: {paper["title"]}\nPaper key points: {paper["key_points"]}" for paper in research_papers]}
        Generate in user's language
        User's language:
        {self.language}
        """
        error, success, section_refs, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm, 
            CHOOSE_REFS_SECTION, 
            content, 
            SectionRefs
        )
        seen_titles: set = set()
        if not success:
            if error.status_code in [401, 403, 429, 500]:
                raise error
            logger.info(f"[{self}] Fail to choosing refs for section {section["heading"]}")
            return [], 0, 0, seen_titles
        else:
            titles = [paper["title"] for paper in research_papers]
            section_refs_titles = [ref_usage for ref_usage in section_refs.section_refs]
            correct_titles = self.title_matching(section_refs_titles, titles)
            section_ref_usage: set = set()
            for correct_title in correct_titles:
                if correct_title:
                    section_ref_usage.add(correct_title)
            tasks = [
                self.get_refs_subsection(
                    section, 
                    research_papers, 
                    section_ref_usage, 
                    final_proposal, 
                    subsection
                ) for subsection in section["subheadings"]
            ]
            results = await asyncio.gather(*tasks)
            for subsection, (refs, in_tokens, out_tokens, seen_in_subsection) in zip(section["subheadings"], results):
                subsection["refs"] = refs
                input_tokens += in_tokens
                output_tokens += out_tokens
                seen_titles.update(seen_in_subsection)
            return section, input_tokens, output_tokens, seen_titles
        
    def get_papers_chunks(self, research_papers: list, chunk_size=10):
        research_papers_chunks = [research_papers[i:i + chunk_size] for i in range(0, len(research_papers), chunk_size)]
        if len(research_papers_chunks) > 1 and len(research_papers_chunks[-1]) < chunk_size // 2:
            prev = research_papers_chunks[-2]
            half = len(prev) // 2
            # Move half of the previous chunk to the last
            research_papers_chunks[-1] = prev[half:] + research_papers_chunks[-1]
            research_papers_chunks[-2] = prev[:half]
        return research_papers_chunks

    async def get_outline_with_refs_v2(
        self,
        outline: dict, 
        final_proposal: dict,
        research_papers: list[dict],
        db_key: str, 
    ) -> tuple[list[dict], int, int]:
        self.get_refs = True
        outline_code: list[str] = []
        admin_db = self.mongo_client["admin"]
        outlines_collection = admin_db["outlines"]
        collection_chunk = admin_db["reports_refs_chunks"]
        embeddings = get_embeddings(db_key)
        store = QdrantVectorStore.from_existing_collection(
            url=settings.QDRANT_URL,
            collection_name=self.document_id,
            embedding=embeddings
        )
        try:
            encoding = tiktoken.encoding_for_model(settings.EMBEDDING_MODEL)
        except Exception:
            encoding = tiktoken.get_encoding("cl100k_base")
        for section_num in range(len(outline["outline"])):
            if "code" in outline["outline"][section_num]:
                outline_code.append(outline["outline"][section_num]["code"])
                del outline["outline"][section_num]["code"]
            else:
                outline_code.append(None)
            if outline["outline"][section_num].get("overview"):
                outline["outline"][section_num]["overview"] = markdownify_keep_images(outline["outline"][section_num]["overview"])
            for subsection_num in range(len(outline["outline"][section_num]["subheadings"])):
                if outline["outline"][section_num]["subheadings"][subsection_num].get("detail_description"):
                    outline["outline"][section_num]["subheadings"][subsection_num]["detail_description"] = markdownify_keep_images(
                        outline["outline"][section_num]["subheadings"][subsection_num]["detail_description"]
                    )

        tasks = [self.get_heading_description(outline["outline"], final_proposal, section) for section in outline["outline"]]
        results = await asyncio.gather(*tasks)
        headings = [result[0] for result in results]
        input_tokens = [result[1] for result in results]
        output_tokens = [result[2] for result in results]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        headings_dict = [heading.model_dump() for heading in headings]
        logger.info(f"[{self}] Starting reference integration process...")
        research_papers = await self.get_key_points(research_papers)
        all_papers_map = {p["title"]: p for p in research_papers}

        user_paper_titles = {p["title"] for p in research_papers if p.get("source") == "USER"}
        all_paper_titles = set(all_papers_map.keys())

        logger.info(f"[{self}] Pass 1: Mapping all {len(all_paper_titles)} papers with fail-fast strategy.")
        pass1_map, successfully_mapped_titles = await self._map_papers_to_sections(
            final_proposal, headings_dict, list(all_papers_map.values()), max_attempts=1
        )

        failed_user_titles = user_paper_titles - successfully_mapped_titles
        final_heading_map = pass1_map

        if failed_user_titles:
            logger.info(f"[{self}] Pass 2: Re-mapping {len(failed_user_titles)} failed user papers with resilient strategy.")
            failed_user_papers = [all_papers_map[title] for title in failed_user_titles]
            
            pass2_map, _ = await self._map_papers_to_sections(
                final_proposal, headings_dict, failed_user_papers, max_attempts=3
            )

            for heading, papers in pass2_map.items():
                final_heading_map[heading].extend(papers)
        distribution_limit = asyncio.Semaphore(20)
        concurrency_limit = asyncio.Semaphore(50)
        section_tasks = [
            self._process_section_planning(
                section,
                final_heading_map,
                all_papers_map,
                user_paper_titles,
                final_proposal,
                distribution_limit,
                concurrency_limit,
            ) for section in headings_dict
        ]

        section_results = await asyncio.gather(*section_tasks)

        for in_tok, out_tok in section_results:
            self.input_tokens += in_tok
            self.output_tokens += out_tok
        
        for heading in range(len(headings_dict)):
            if heading < len(outline_code) and outline_code[heading] is not None:
                headings_dict[heading]["code"] = outline_code[heading]
        final_outline = {
            "title": final_proposal["title"],
            "outline": headings_dict,
        }
        logger.info(f"[{self}] Reference integration process complete. Starting writing phase...")

        async def _bounded_write_task(*args):
            async with concurrency_limit:
                return await self._process_single_reference_task(*args)
        for section_count, section in enumerate(final_outline["outline"]):
            section_tasks = []
            for subheading_count, subheading in enumerate(section["subheadings"]):
                
                unique_refs: list[dict] = []
                seen_titles = set()
                
                for ref in subheading["refs"]:
                    if ref["title"] not in seen_titles:
                        unique_refs.append(ref)
                        seen_titles.add(ref["title"])
                        
                        section_tasks.append(_bounded_write_task(
                            ref,
                            subheading,
                            research_papers,
                            store,
                            collection_chunk,
                            encoding
                        ))
                
                final_outline["outline"][section_count]["subheadings"][subheading_count]["refs"] = unique_refs
            if not section_tasks:
                continue

            logger.info(f"[{self}] Writing {len(section_tasks)} paragraphs for Section {section_count + 1}...")
            section_results = await asyncio.gather(*section_tasks)

            ref_usage_list: list[dict] = []

            for res in section_results:
                in_tok, out_tok, emb_tok, usage_row = res
                self.embed_tokens += emb_tok
                self.input_tokens += in_tok
                self.output_tokens += out_tok
                ref_usage_list.append(usage_row)

            if ref_usage_list:
                db_index = section_count + 1
                await outlines_collection.update_one(
                    {"documentId": self.document_id, "index": db_index},
                    {"$push": {"refTable": {"$each": ref_usage_list}}},
                    upsert=True
                )

        return final_outline, self.input_tokens, self.output_tokens, self.embed_tokens
    
    async def _process_section_planning(
        self,
        section: dict,
        final_heading_map: dict,
        all_papers_map: dict,
        user_paper_titles: set,
        final_proposal: dict,
        distribution_limit: asyncio.Semaphore,
        concurrency_limit: asyncio.Semaphore,
    ) -> tuple[int, int]:
        """
        Handles the Reference Distribution and Usage Planning for a SINGLE section.
        """
        input_tokens = 0
        output_tokens = 0

        # Check if section has subheadings
        has_subheadings = section.get("subheadings") is not None and len(section.get("subheadings", [])) > 0

        # If no subheadings, assign references directly to section level
        if not has_subheadings:
            section_heading = section.get("heading")
            papers_for_section = list(set(final_heading_map.get(section_heading, [])))

            if not papers_for_section:
                section["section_refs"] = []
                return 0, 0

            # Generate proper usage descriptions for heading-level references
            # Create a pseudo-subsection that represents the entire section
            pseudo_subsection = {
                "subheading": section["heading"],
                "detail_description": section["overview"]
            }

            # Use existing usage plan generation logic
            async def _bounded_plan_task(*args):
                async with concurrency_limit:
                    return await self._generate_plans_for_subsection(*args)

            # Generate usage plans for section-level references
            section_refs, in_tok, out_tok = await _bounded_plan_task(
                section, all_papers_map, papers_for_section, final_proposal, pseudo_subsection, 3
            )

            section["section_refs"] = section_refs
            logger.info(f"[{self}] Assigned {len(section_refs)} papers to heading-only section '{section_heading}'")
            return in_tok, out_tok

        # Existing logic for sections with subheadings
        async def _bounded_distribute_task(proposal, section_data, section_papers, max_attempts):
            async with distribution_limit:
                return await self._distribute_papers_to_subsections(proposal, section_data, section_papers, max_attempts)
            
        async def _bounded_plan_task(*args):
            async with concurrency_limit:
                return await self._generate_plans_for_subsection(*args)

        section_heading = section.get("heading")
        papers_for_section = list(set(final_heading_map.get(section_heading, [])))

        if not papers_for_section:
            for subsection in section["subheadings"]:
                subsection["refs"] = []
            return 0, 0

        logger.info(f"[{self}] Distributing {len(papers_for_section)} papers across {len(section['subheadings'])} subsections for '{section_heading}'")
        
        subheading_user_papers = [title for title in papers_for_section if title in user_paper_titles]
        subheading_other_papers = [title for title in papers_for_section if title not in user_paper_titles]
        
        distribution_tasks = []
        
        if subheading_user_papers:
            distribution_tasks.append(_bounded_distribute_task(
                final_proposal, 
                section, 
                [all_papers_map[t] for t in subheading_user_papers], 
                3
            ))
        if subheading_other_papers:
            distribution_tasks.append(_bounded_distribute_task(
                final_proposal, 
                section, 
                [all_papers_map[t] for t in subheading_other_papers], 
                1
            ))
            
        distribution_results = await asyncio.gather(*distribution_tasks)
        
        subsection_paper_map = defaultdict(list)
        for result in distribution_results:
            dist_map, in_tok, out_tok = result
            input_tokens += in_tok
            output_tokens += out_tok
            
            for subheading, titles in dist_map.items():
                subsection_paper_map[subheading].extend(titles)

        pending_plans = []

        for subsection in section["subheadings"]:
            relevant_titles = subsection_paper_map.get(subsection["subheading"], [])
            
            resilient_plan_titles = [title for title in relevant_titles if title in user_paper_titles]
            fail_fast_plan_titles = [title for title in relevant_titles if title not in user_paper_titles]

            if not resilient_plan_titles and not fail_fast_plan_titles:
                subsection["refs"] = []
                continue

            if fail_fast_plan_titles:
                task = _bounded_plan_task(
                    section, all_papers_map, fail_fast_plan_titles, final_proposal, subsection, 1
                )
                pending_plans.append((subsection, task))

            if resilient_plan_titles:
                task = _bounded_plan_task(
                    section, all_papers_map, resilient_plan_titles, final_proposal, subsection, 3
                )
                pending_plans.append((subsection, task))

        if not pending_plans:
            return input_tokens, output_tokens

        results = await asyncio.gather(*[p[1] for p in pending_plans])

        temp_refs_aggregator = defaultdict(list)

        for i, (refs, in_tok, out_tok) in enumerate(results):
            target_subsection = pending_plans[i][0]
            input_tokens += in_tok
            output_tokens += out_tok
            
            temp_refs_aggregator[id(target_subsection)].extend(refs)

        for subsection in section["subheadings"]:
            if "refs" not in subsection:
                subsection["refs"] = []
            if id(subsection) in temp_refs_aggregator:
                subsection["refs"].extend(temp_refs_aggregator[id(subsection)])

        return input_tokens, output_tokens

    async def _map_papers_to_sections(
        self,
        final_proposal: dict,
        final_outline: list[dict],
        target_papers: list[dict],
        max_attempts: int = 1
    ) -> tuple[defaultdict, set[str]]:
        """Maps a list of papers to sections with a configurable number of retries."""
        heading_map = defaultdict(list)
        successfully_mapped_titles = set()
        
        papers_to_map = list(target_papers)
        attempts = 0
        mapping_concurrency = asyncio.Semaphore(10)

        async def _bounded_map_task(final_proposal, final_outline, paper):
            async with mapping_concurrency:
                return await self._map_single_paper_to_sections(final_proposal, final_outline, paper)
        while papers_to_map and attempts < max_attempts:
            tasks = [_bounded_map_task(final_proposal, final_outline, paper) for paper in papers_to_map]
            results = await asyncio.gather(*tasks)
            
            newly_mapped_papers = []
            for paper, (success, usage_map, in_tokens, out_tokens) in zip(papers_to_map, results):
                self.input_tokens += in_tokens
                self.output_tokens += out_tokens
                if success and usage_map.usage:
                    for heading in usage_map.usage:
                        heading_map[final_outline[heading]["heading"]].append(paper["title"])
                    successfully_mapped_titles.add(paper["title"])
                    newly_mapped_papers.append(paper)
            
            if not newly_mapped_papers:
                if attempts < max_attempts - 1:
                    logger.warning(f"Mapping attempt {attempts + 1} failed to map any new papers. Retrying...")
            
            papers_to_map = [p for p in papers_to_map if p not in newly_mapped_papers]
            attempts += 1
        return heading_map, successfully_mapped_titles
    
    async def _map_single_paper_to_sections(
        self,
        final_proposal: dict,
        final_outline: list[dict],
        target_paper: dict
    ) -> tuple[bool, UserPaperV2, int, int]:
        """Performs the actual LLM call to map one paper to relevant sections."""
        logger.info(f"[{self}] Choosing sections for papers {target_paper["title"]}")
        content = f"""
            Report proposal:
            {final_proposal},
            Report sections:
            {[f"Section {i + 1}'s heading:\n\n{section["heading"]}\n\nSection's description:\n\n{section["overview"]}\n\n" for i, section in enumerate(final_outline)]}
            **Provided research paper**:
            Paper's title: 
            {target_paper["title"]}
            Paper key points: 
            {target_paper["key_points"]}
            User's language:
            {self.language}
        """
        error, success, user_paper, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm, 
            CHOOSE_USER_REFS_V2, 
            content, 
            UserPaperV2
        )
        if not success:
            if error.status_code in [401, 403, 429, 500]:
                raise error
            logger.info(f"[{self}] Fail to choosing sections for paper {target_paper["title"]}")
            return False, UserPaperV2(usage=[]), 0, 0
        else:
            if len(user_paper.usage):
                user_paper.usage = list(set([usage - 1 for usage in user_paper.usage if 1 <= usage <= len(final_outline)]))
                if len(user_paper.usage):
                    return True, user_paper, input_tokens, output_tokens
                else:
                    return False, UserPaperV2(usage=[]), input_tokens, output_tokens
            else:
                return False, UserPaperV2(usage=[]), input_tokens, output_tokens

    async def _distribute_papers_to_subsections(
        self, 
        proposal: dict,
        section: dict, 
        available_papers: list[dict],
        max_attempts: int,
    ) -> tuple[dict[str, list[str]], int, int]:
        """
        Maps the pool of papers available for a Section to its specific Subheadings.
        Returns: { "Subheading Title": ["Paper Title A", "Paper Title B"] }
        """
        if not available_papers:
            return {}, 0, 0
        papers_title = [paper["title"] for paper in available_papers]
        all_papers_map = {p["title"]: p for p in available_papers}
        attempts = 0
        chunk_concurrency = asyncio.Semaphore(10)

        async def _process_chunk(chunk: list[str], section: dict) -> tuple[dict[str, list[str]], int, int, set]:
            async with chunk_concurrency:
                papers_for_prompt = [all_papers_map[title] for title in chunk]
                
                content = f"""
                The report's proposal:
                {proposal}

                The report section:
                {section['heading']} - {section["overview"]}
                
                The subsections of the section:
                {[f"{i + 1}. {sub['subheading']} - {sub['detail_description']}" for i, sub in enumerate(section["subheadings"])]}
                
                Here are the available research papers for this section:
                {[f"{i + 1}. Title: {p['title']}\nKey Points: {p['key_points']}" for i, p in enumerate(papers_for_prompt)]}
                
                Task: 
                Assign the most relevant papers to each subsection. 
                - A paper can be used in multiple subsections if relevant.
                - Use the numeric indices provided above.
                
                Return a JSON object where keys are the Subsection Index (integer) and values are lists of Paper Indices (integers).
                Example: {{ "1": [1, 2], "2": [2, 3] }}
                """        

                error, success, result, in_tok, out_tok = await get_answer_with_schema(
                    self.id, 
                    self.llm,
                    CHOOSE_SUBSECTION,
                    content,
                    SubsectionMapping
                )
                
                if not success:
                    if error and error.status_code in [401, 403, 429, 500]:
                        raise error
                    logger.warning(f"Failed to distribute papers chunk for section {section['heading']}")
                    return {}, 0, 0, set()

                chunk_map = defaultdict(list)
                chunk_processed = set()

                for assignment in result.assignments:
                    try:
                        sub_idx = assignment.subsection_index
                        if not (1 <= sub_idx <= len(section["subheadings"])):
                            continue
                        
                        target_subheading = section["subheadings"][sub_idx - 1]["subheading"]
                        
                        for p_idx in assignment.paper_indices:
                            if not (1 <= p_idx <= len(chunk)):
                                continue
                            
                            paper_title = chunk[p_idx - 1]
                            chunk_map[target_subheading].append(paper_title)
                            chunk_processed.add(paper_title)
                            
                    except ValueError:
                        continue
                
                return chunk_map, in_tok, out_tok, chunk_processed
        total_input_tokens, total_output_tokens = 0, 0
        final_map = defaultdict(list)
        while len(papers_title) and attempts < max_attempts:
            paper_chunks = self.get_papers_chunks(papers_title)
            
            tasks = [_process_chunk(chunk, section) for chunk in paper_chunks]
            results = await asyncio.gather(*tasks)
            
            processed_in_this_attempt = set()

            for chunk_map, in_tok, out_tok, processed_titles in results:
                total_input_tokens += in_tok
                total_output_tokens += out_tok
                
                for subheading, titles in chunk_map.items():
                    final_map[subheading].extend(titles)
                
                processed_in_this_attempt.update(processed_titles)

            papers_title = [title for title in papers_title if title not in processed_in_this_attempt]
            attempts += 1

        final_dict = {k: list(set(v)) for k, v in final_map.items()}
        logger.info(f"[{self}] Removed papers: {papers_title}")
        return final_dict, total_input_tokens, total_output_tokens
    
    async def _generate_plans_for_subsection(
        self,
        section: dict,
        all_papers_map: dict[str, dict],
        use_research_papers: list[str],
        final_proposal: dict,
        subsection: dict,
        max_attempts: int = 1,
    ) -> tuple[list[dict], int, int]:
        output_json = """{
        "usage_plans": [
            {
            "paper_index": 1,
            "usage_type": "METHODOLOGY_JUSTIFICATION",
            "usage_description": "This paper will be used to justify the selection of the modified Jones model for measuring earnings management, as it demonstrates its successful application within the Vietnamese market context.",
            "key_points_used": [8, 10, 11, 16]
            },
            {
            "paper_index": 2,
            "usage_type": "BACKGROUND_CONTEXT",
            "usage_description": "This source will provide foundational definitions and background on Psychological Capital (PsyCap) and Management Control Systems (MCS), setting the stage for the main argument.",
            "key_points_used": [1, 2, 3]
            }
        ]
        }
        """
        final_ref_usage: list[dict] = []
        total_input_tokens, total_output_tokens = 0, 0
        attempts = 0
        chunk_concurrency = asyncio.Semaphore(10)

        async def _process_chunk(final_proposal, all_papers_map, chunk, section, subsection) -> tuple[list[dict], int, int, set]:
            async with chunk_concurrency:
                papers_for_prompt = [all_papers_map[title] for title in chunk]
                prompt = f"""You are a strategic AI assistant specializing in research planning. Your task is to determine exactly how a reference paper should be used to support a specific subsection of a research report.

                **CONTEXT:**
                1. **Report proposal:**
                    ```
                    {final_proposal}
                    ```

                2. **Section Heading and Description:**
                    {section["heading"]} - {section["overview"]}

                3.  **Specific Subsection Heading and Description:**
                    {subsection["subheading"]} - {subsection["detail_description"]}

                4.  **Reference Paper:**
                    {[f"Paper's index: {paper_count + 1}\nPaper's title: {paper["title"]}\nPaper key points:\n{[f"{i + 1}. {k}: {v}\n---\n" for i, (k, v) in enumerate(paper["key_points"].items())]}" for paper_count, paper in enumerate(papers_for_prompt)]}

                5.  **Usage Type Categories:** When creating a plan, you must choose a `usage_type` from this list that best fits the paper's role:
                    - `BACKGROUND_CONTEXT`: Provides background context and establishes the current situational awareness.
                    - `LITERATURE_GAP`: Identifies research gaps or critiques previous scholarly works.
                    - `MODEL_INHERITANCE`: Adopts, adapts, or extends a theoretical framework or research model.
                    - `HYPOTHESIS_INHERITANCE`: Supports, adopts, or further develops specific research hypotheses.
                    - `METHODOLOGY_JUSTIFICATION`: Justifies the selection of specific research methods or approaches.
                    - `DATA_SOURCE`: Serves as a primary data source or provides illustrative dataset examples.
                    - `RESULT_COMPARISON`: Compares, contrasts, or corroborates the study's findings with existing evidence.
                    - `PRACTICAL_EXAMPLE`: Illustrates concepts using real-world examples or practical case studies.

                **YOUR TASK:**
                For each paper listed above, create a corresponding usage plan object. Analyze each paper independently based on the shared context.

                **OUTPUT FORMAT:**
                Your output must be a single JSON object. This object should contain one key, `usage_plans`, which is a list of the plan objects you created. Ensure there is one plan object for each paper provided.

                **Example JSON Output:**
                ```json
                    {output_json}
                Generate in user's language (except for usage_type)
                User's language:
                {self.language}
                """             
                error, success, batch_plan_obj, in_tokens, out_tokens = await get_answer_with_schema(
                    self.id, self.llm, "GET_BATCH_USAGE_PLANS", prompt, BatchReferenceUsagePlan
                )
                if not success:
                    if error.status_code in [401, 403, 429, 500]:
                        raise error
                    logger.info(f"[{self}] Fail to choosing refs for section {section['heading']}")
                    return [], 0, 0, []
                chunk_results: list[dict] = []
                chunk_processed_titles = set()
                                
                for plan_obj in batch_plan_obj.usage_plans:
                    if 1 <= plan_obj.paper_index <= len(chunk):
                        correct_title = chunk[plan_obj.paper_index - 1]
                        chunk_results.append(
                            {
                                "title": correct_title,
                                "usage_type": plan_obj.usage_type,
                                "usage_description": plan_obj.usage_description,
                                "key_points_used": self._validate_key_point_indices(plan_obj.key_points_used),
                                "inherited": any(item in [15, 16, 17, 18] for item in self._validate_key_point_indices(plan_obj.key_points_used))
                            }                           
                        )
                        chunk_processed_titles.add(correct_title)
                        
                return chunk_results, in_tokens, out_tokens, chunk_processed_titles
        while len(use_research_papers) and attempts < max_attempts:
            paper_chunks = self.get_papers_chunks(use_research_papers)
            tasks = [_process_chunk(final_proposal, all_papers_map, chunk, section, subsection) for chunk in paper_chunks]
            results = await asyncio.gather(*tasks)
            processed_in_attempt = set()
            for chunk_res, in_tok, out_tok, processed_titles in results:
                total_input_tokens += in_tok
                total_output_tokens += out_tok
                final_ref_usage.extend(chunk_res)
                processed_in_attempt.update(processed_titles)
            use_research_papers = [item for item in use_research_papers if item not in processed_in_attempt]
            attempts += 1
        return final_ref_usage, total_input_tokens, total_output_tokens
    
    def _validate_key_point_indices(self, indices: list) -> list[int]:
        """Validates key point indices, ensuring they are integers from 1-18."""
        if not isinstance(indices, list) or not indices:
            return list(range(1, 19))
        if any(not isinstance(num, int) or not (1 <= num <= 18) for num in indices):
            logger.warning(f"Invalid key point indices found: {indices}. Falling back to default.")
            return list(range(1, 19))
        return sorted(list(set(indices)))
    
    async def _generate_integration_paragraph(
        self,
        subsection: dict,
        paper: dict,
        paper_summary: str,
        paper_usage_type: str,
        paper_usage_description: str,
        retrieved_chunk: str
    ) -> tuple[str, int, int]:
        """
        WRITING STAGE: Creates the final paragraph using the plan and retrieved evidence.
        """
        example_output = """{
            "integration_paragraph": "The study adopts the modified Jones model (1991) [Reference Source Title] utilized in their work, which has proven effective in similar emerging market contexts. This approach allows for a quantitative assessment of discretionary accruals, using financial leverage and firm size as key independent variables, thereby providing a structured method to test this study's central hypotheses."
        }
        """
        logger.info(f"[{self}] Writing integration paragraph for {paper['title']} for subsection {subsection["subheading"]}")
        
        prompt = f"""
        You are an academic writing AI assistant. Your task is to write a fluent, scholarly paragraph that integrates a reference paper into a research report based on a predefined plan and retrieved evidence.

        **CONTEXT:**
        1.  **Subsection Goal:** You are writing for a subsection with this objective:
            ```
            {subsection["detail_description"]}
            ```
        2.  **Reference Source Title:** {paper['title']}

        3. **Usage Type Definition**:
            - `BACKGROUND_CONTEXT`: Provides background context and establishes the current situational awareness.
            - `LITERATURE_GAP`: Identifies research gaps or critiques previous scholarly works.
            - `MODEL_INHERITANCE`: Adopts, adapts, or extends a theoretical framework or research model.
            - `HYPOTHESIS_INHERITANCE`: Supports, adopts, or further develops specific research hypotheses.
            - `METHODOLOGY_JUSTIFICATION`: Justifies the selection of specific research methods or approaches.
            - `DATA_SOURCE`: Serves as a primary data source or provides illustrative dataset examples.
            - `RESULT_COMPARISON`: Compares, contrasts, or corroborates the study's findings with existing evidence.
            - `PRACTICAL_EXAMPLE`: Illustrates concepts using real-world examples or practical case studies.

        **EVIDENCE & INSTRUCTIONS:**
        1.  **MANDATORY PLAN:** You must strictly follow this plan:
            *   **Usage Type:** {paper_usage_type}
            *   **Specific Goal:** {paper_usage_description}

        2.  **KEY POINTS SUMMARY:** Use these points for high-level context:
            ```
            {paper_summary}
            ```

        3.  **RETRIEVED TEXT FROM SOURCE:** Ground your writing in this specific text. You can paraphrase its core findings.
            ```
            {retrieved_chunk}
            ```

        **YOUR TASK:**
        - Write a single, coherent academic paragraph (200-250 words). The paragraph must seamlessly synthesize the **MANDATORY PLAN** with the **KEY POINTS** and, if possible, be grounded in the **RETRIEVED TEXT**. Do not simply list the information; weave it into a compelling argument that serves the subsection's goal.
        - Do not include any citation from outside, just use the given **Reference Source Title** with the following format: "[" + Paper's title + "]"

        **OUTPUT:**
        OUTPUT FORMAT:
        Provide your response as a single JSON object with no additional text.
        Example JSON Output:
        {example_output}
        Generate in user's language
        User's language:
        {self.language}
        """
        
        # This LLM call should return a simple string
        error, success, response_str, input_tokens, output_tokens = await get_answer_with_schema(
            self.id, self.llm, "WRITE_INTEGRATION_PARAGRAPH", prompt, IntegrationParagraph
        )
        
        if not success:
            raise error
        refs = re.findall(r'\[(.*?)\]', response_str.integration_paragraph)
        if refs:
            if len(refs) == 1:
                response_str.integration_paragraph = response_str.integration_paragraph.replace(refs[0], paper["title"])
                return response_str.integration_paragraph, input_tokens, output_tokens
            else:
                best_ai_match, sub_match_idx = self.find_best_match(
                    paper["title"], refs, key_fn=lambda x: x
                )
                if best_ai_match:
                    seen_refs: list[str] = []
                    for ref_count in range(len(refs)):
                        if refs[ref_count] not in seen_refs:
                            if ref_count == sub_match_idx:
                                response_str.integration_paragraph = response_str.integration_paragraph.replace(refs[ref_count], paper["title"])
                            else:
                                response_str.integration_paragraph = response_str.integration_paragraph.replace(f"[{refs[ref_count]}]", "")
                            seen_refs.append(refs[ref_count])
                    clean_text = re.sub(r'\s+', ' ', response_str.integration_paragraph).strip()
                    clean_text = _clean_text(clean_text)
                    return clean_text, input_tokens, output_tokens
        clean_text = re.sub(r'\[.*?\]', '', response_str.integration_paragraph)
        clean_text = _clean_text(clean_text)
        clean_text = clean_text[:-1] + f" [{paper['title']}]."
        return clean_text, input_tokens, output_tokens

    async def _process_single_reference_task(
        self,
        ref: dict,
        subsection: dict,
        research_papers: list,
        store,
        collection_chunk,
        encoding
    ) -> tuple[int, int, int, dict]:
        """
        Process a single reference: Search Vector DB, Write Paragraph, Save Chunk.
        Returns: (section_index, subheading_index, input_tokens, output_tokens, embed_tokens, usage_row)
        """
        ref_paper = next((s for s in research_papers if s.get("title") == ref["title"]))
        selected_points = [
            f"- {k}: {v}" 
            for i, (k, v) in enumerate(ref_paper["key_points"].items()) 
            if i + 1 in ref["key_points_used"]
        ]
        
        ref_summary = f"""Paper title\n: {ref["title"]}
        Paper's keypoints:
        {selected_points}
        """

        search_query = await self.get_query_vector_db(
            subsection["detail_description"], 
            ref["usage_description"], 
            ref_summary,
        )

        results = await store.asimilarity_search(
            query=search_query, 
            k=1,
            filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="metadata.source",
                        match=models.MatchValue(value=ref["title"])
                    )
                ]
            )
        )
        
        retrieved_content = results[0].page_content if results else ""

        chunk_content, ref_input_tokens, ref_output_tokens = await self._generate_integration_paragraph(
            subsection,
            ref_paper,
            ref_summary,
            ref["usage_type"],
            ref["usage_description"],
            retrieved_content
        )

        chunk_id = hash_blake3(chunk_content)
        summary_ref_id = hash_blake3(str(ref_paper["key_points"]))

        try:
            await collection_chunk.insert_one({"_id": chunk_id, "content": chunk_content})
        except Exception:
            pass

        try:
            await collection_chunk.insert_one({"_id": summary_ref_id, "content": ref_paper["key_points"]})
        except Exception:
            pass
        current_embed_tokens = len(encoding.encode(search_query))

        ref["ref_chunk"] = chunk_id

        usage_row = {
            "paperTitle": ref["title"],
            "subheading": subsection["subheading"],
            "summary": summary_ref_id,
            "paperUsage": ref["usage_description"],
            "paperChunk": chunk_id,
            "inherited": ref["inherited"],
        }

        return ref_input_tokens, ref_output_tokens, current_embed_tokens, usage_row

    async def get_user_refs_subsection(
        self,
        section: dict,
        research_papers: list[dict], 
        use_research_papers: list[str],
        final_proposal: dict,
        subsection: dict,
    ) -> tuple[list[dict], int, int]:
        logger.info(f"[{self}] User - Choosing refs for section {section["heading"]} - Subsection {subsection["subheading"]}")
        count = 0
        final_ref_usage: list[dict] = []
        input_tokens, output_tokens = 0, 0
        while len(use_research_papers) and count < 3:
            content = f"""
            Report proposal:
            {final_proposal},
            Report sections:
            Section's Heading: 
            {section["heading"]}
            Section's Description: 
            {section["overview"]}
            Subsection:
            {subsection["subheading"]}
            Subsection's detail description:
            {subsection["detail_description"]}
            **Provided research papers**:
            {
                [
                    f"Paper's title: {paper["title"]}\nPaper key points: {paper["key_points"]}" 
                    for paper in research_papers if paper["title"] in use_research_papers
                ]
            }
            Generate in user's language
            User's language:
            {self.language}
            """
            error, success, subsection_refs, in_tokens, out_tokens = await get_answer_with_schema(
                self.id, 
                self.llm, 
                CHOOSE_REFS, 
                content, 
                SubsectionRefs
            )
            seen_titles: set = set()
            if not success:
                if error.status_code in [401, 403, 429, 500]:
                    raise error
                logger.info(f"[{self}] Fail to choosing refs for section {section["heading"]}")
            else:
                input_tokens += in_tokens
                output_tokens += out_tokens
                titles = [paper["title"] for paper in research_papers]
                section_refs_titles = [ref_usage.title for ref_usage in subsection_refs.subsection_refs]
                correct_titles = self.title_matching(section_refs_titles, titles)
                
                for ref_usage, correct_title in zip(subsection_refs.subsection_refs, correct_titles):
                    if correct_title:
                        final_ref_usage.append(
                            {
                                "title": correct_title,
                                "usage": ref_usage.usage
                            }                           
                        )
                        seen_titles.add(correct_title)
            use_research_papers = [item for item in use_research_papers if item not in seen_titles]
            count += 1
        return final_ref_usage, input_tokens, output_tokens

    async def get_query_vector_db(self, subheading_description: str, ref_usage: str, ref_summary: str):
        search_queries_prompt = f"""
        Based on the user's paragraph description, reference paper summary and how to use the reference paper in this paragraph.
        generate exactly 1 concise and distinct search queries (each ideally 3-7 keywords/phrases).
        Query should aim to find relevant information from the reference paper.
        User's Paragraph description:
        {subheading_description}
        Reference paper summary:
        {ref_summary}
        Reference paper usage:
        {ref_usage}
        Generate 1 distinct search queries
        """
        system_prompt = "You are an AI assistant that generates a set of distinct search queries for a database of academic reference papers."
        error, success, search_query, input_tokens, output_tokens = await get_answer_with_schema(
            self.id, 
            self.llm,
            system_prompt,
            search_queries_prompt,
            SearchQuery
        )
        if not success:
            if error.status_code in [401, 403, 429, 500]:
                raise error
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        return search_query.search_query if success else ref_usage
    
    def _find_first_paper_usage(self, paper_title: str, outline: list[dict]) -> tuple[dict, dict, bool] | tuple[None, None, bool]:
        """Scans the outline and returns the first section and subheading where the paper is cited."""
        return_section = None
        return_subheading = None
        return_inherited: bool = False
        for section in outline:
            for subheading in section.get("subheadings", []):
                for ref in subheading.get("refs", []):
                    if ref.get("title") == paper_title:
                        if ref["inherited"]:
                            return_inherited = True
                        if return_section:
                            continue
                        else:
                            return_section = section
                            return_subheading = subheading
        return return_section, return_subheading, return_inherited
    
    async def update_user_refs(self, research_papers: list[dict], db_key: str):
        admin_db = self.mongo_client["admin"]
        collection_chunk = admin_db["reports_refs_chunks"]
        embeddings = get_embeddings(db_key)
        store = QdrantVectorStore.from_existing_collection(
            url=settings.QDRANT_URL,
            collection_name=self.document_id,
            embedding=embeddings
        )
        try:
            encoding = tiktoken.encoding_for_model(settings.EMBEDDING_MODEL)
        except Exception:
            encoding = tiktoken.get_encoding("cl100k_base")
        doc_configs_collection = admin_db["document_configurations"]
        doc_config = await doc_configs_collection.find_one({"documentId": self.document_id})
        final_outline = doc_config["processedOutline"]
        proposal_id = doc_config["finalProposal"]
        proposals_collection = admin_db["proposal_titles"]
        final_proposal = await proposals_collection.find_one({"_id": ObjectId(proposal_id)})
        del final_proposal["_id"]
        del final_proposal["isDeleted"]
        del final_proposal["createdBy"]
        del final_proposal["updatedBy"]
        del final_proposal["createdAt"]
        del final_proposal["updatedAt"]
        outlines_collection = admin_db["outlines"]
        records = outlines_collection.find({"documentId": self.document_id}).sort("index", ASCENDING)
        current_section = 0
        async for record in records:
            if record["content"] or record["contentArr"]:
                final_outline["outline"] = final_outline["outline"][1:]
                current_section += 1
            else:
                break
        count = 0
        seen_titles: set = set()
        user_papers: list[UserPaper] = []
        research_papers = await self.get_key_points(research_papers)
        section_papers = copy.deepcopy(research_papers)
        while len(section_papers) and count < 3:
            tasks = [self.get_user_refs_usage(final_proposal, final_outline["outline"], target_paper) for target_paper in section_papers]
            user_usage_results = await asyncio.gather(*tasks)
            for status, user_paper, in_tokens, out_tokens in user_usage_results:
                self.input_tokens += in_tokens
                self.output_tokens += out_tokens
                if status:
                    user_papers.append(user_paper)
                    seen_titles.add(user_paper.title)
            section_papers = [paper for paper in section_papers if paper["title"] not in seen_titles]
            count += 1

        heading_map = defaultdict(list)
        for item in user_papers:
            paper = item.title
            for heading in item.usage:
                heading_map[heading].append(paper)
        user_papers_sections: list[dict] = []
        for heading, papers in heading_map.items():
            chunks = self.get_papers_chunks(papers)
            for chunk in chunks:
                user_papers_sections.append({"heading": heading, "papers": chunk})
        for user_papers_section in user_papers_sections:
            section = next((s for s in final_outline["outline"] if s.get("heading") == user_papers_section["heading"]))
            tasks = [
                self.get_user_refs_subsection(
                    section, 
                    research_papers, 
                    user_papers_section["papers"], 
                    final_proposal, 
                    subsection
                ) for subsection in section["subheadings"]
            ]
            results = await asyncio.gather(*tasks)
            for subsection, (refs, in_tokens, out_tokens) in zip(section["subheadings"], results):
                subsection["refs"] = refs
                self.input_tokens += in_tokens
                self.output_tokens += out_tokens
        for section_count in range(len(final_outline["outline"])):
            ref_usage_table: list[dict] = []
            seen_titles_section: set = set()
            for subheading_count in range(len(final_outline["outline"][section_count]["subheadings"])):
                new_refs: list[dict] = []
                seen_titles_sub: set = set()
                for ref_count in range(len(final_outline["outline"][section_count]["subheadings"][subheading_count]["refs"])):
                    ref = final_outline["outline"][section_count]["subheadings"][subheading_count]["refs"][ref_count]
                    if ref["title"] not in seen_titles_sub:
                        ref_paper = next((s for s in research_papers if s.get("title") == ref["title"]), None)
                        if ref_paper:
                            search_query = await self.get_query_vector_db(
                                final_outline["outline"][section_count]["subheadings"][subheading_count]["detail_description"], 
                                ref["usage"], 
                                ref_paper["key_points"],
                            )
                            results = await store.asimilarity_search(
                                query=search_query,  
                                k=1,
                                filter=models.Filter(
                                    must=[
                                        models.FieldCondition(
                                            key="metadata.source",
                                            match=models.MatchValue(value=ref["title"])
                                        )
                                    ]
                                )
                            )
                            chunk = f"...{results[0].page_content}..."
                            chunk_id = hash_blake3(chunk)
                            try:
                                await collection_chunk.insert_one({"_id": chunk_id, "content": chunk})
                            except Exception:
                                pass
                            summary_ref_id = hash_blake3(str(ref_paper["key_points"]))
                            try:
                                await collection_chunk.insert_one({"_id": summary_ref_id, "content": ref_paper["key_points"]})
                            except Exception:
                                pass

                            self.embed_tokens += len(encoding.encode(ref["usage"]))
                            ref["ref_chunk"] = chunk_id
                            new_refs.append(ref)
                            seen_titles_sub.add(ref["title"])
                            usage_row = {
                                "paperTitle": ref["title"],
                                "subheading": final_outline["outline"][section_count]["subheadings"][subheading_count]["subheading"],
                                "summary": summary_ref_id,
                                "paperUsage": ref["usage"],
                                "paperChunk": chunk_id,
                            }
                            ref_usage_table.append(usage_row)
                seen_titles_section.update(seen_titles_sub)
            if section_count == 0:
                unused_but_relevant_papers = seen_titles.difference(seen_titles_section)
                tmp_ref: list[dict] = []
                for paper_title in unused_but_relevant_papers:
                    first_usage_section, first_usage_subheading, inherited = self._find_first_paper_usage(
                        paper_title, final_outline["outline"]
                    )            
                    ref_paper = next((p for p in research_papers if p.get("title") == paper_title))
                    summary_ref_id = hash_blake3(str(ref_paper["key_points"]))
                    try:
                        await collection_chunk.insert_one({"_id": summary_ref_id, "content": ref_paper["key_points"]})
                    except Exception:
                        pass
                    if first_usage_section and first_usage_subheading:
                        placeholder_row = {
                            "paperTitle": paper_title,
                            "subheading": "",
                            "summary": summary_ref_id,
                            "paperUsage": f"{first_usage_section['heading']} - {first_usage_subheading['subheading']}",
                            "paperChunk": "",
                            "notUsed": True,
                            "inherited": inherited,
                        }
                        ref_usage_table.append(placeholder_row)
                    else:
                        placeholder_row = {
                            "paperTitle": paper_title,
                            "subheading": "",
                            "summary": summary_ref_id,
                            "paperUsage": get_language_dict(self.language)["unused_paper"],
                            "paperChunk": "",
                            "notUsed": True,
                            "inherited": False,
                        }
                        tmp_ref.append(placeholder_row)
                if len(ref_usage_table):
                    ref_usage_table.extend(tmp_ref)
            await outlines_collection.update_one(
                {
                    "documentId": self.document_id,
                    "index": section_count + 1 + current_section
                },
                {
                    "$push": {"refTable": {"$each": ref_usage_table}}
                }, upsert=True
            )

    async def update_user_refs_v2(self, research_papers: list[dict], db_key: str):
        admin_db = self.mongo_client["admin"]
        collection_chunk = admin_db["reports_refs_chunks"]
        outlines_collection = admin_db["outlines"]
        proposals_collection = admin_db["proposal_titles"]
        doc_configs_collection = admin_db["document_configurations"]

        embeddings = get_embeddings(db_key)
        store = QdrantVectorStore.from_existing_collection(
            url=settings.QDRANT_URL,
            collection_name=self.document_id,
            embedding=embeddings
        )
        try:
            encoding = tiktoken.encoding_for_model(settings.EMBEDDING_MODEL)
        except Exception:
            encoding = tiktoken.get_encoding("cl100k_base")

        doc_config = await doc_configs_collection.find_one({"documentId": self.document_id})
        final_outline = doc_config["processedOutline"]
        proposal_id = doc_config["finalProposal"]
        
        final_proposal = await proposals_collection.find_one({"_id": ObjectId(proposal_id)})
        for k in ["_id", "isDeleted", "createdBy", "updatedBy", "createdAt", "updatedAt"]:
            final_proposal.pop(k, None)

        records = outlines_collection.find({"documentId": self.document_id}).sort("index", ASCENDING)
        current_section_offset = 0
        async for record in records:
            if record.get("content") or record.get("contentArr"):
                if final_outline["outline"]:
                    final_outline["outline"] = final_outline["outline"][1:]
                    current_section_offset += 1
            else:
                break

        if not final_outline["outline"]:
            logger.info(f"[{self}] All sections are already written. No updates performed.")
            return

        research_papers = await self.get_key_points(research_papers)
        all_papers_map = {p["title"]: p for p in research_papers}
        extra_papers_title = [p["title"] for p in research_papers]
        logger.info(f"[{self}] Mapping user papers to relevant sections...")
        heading_map, _ = await self._map_papers_to_sections(
            final_proposal, final_outline["outline"], research_papers, 3
        )
        distribution_limit = asyncio.Semaphore(20)
        concurrency_limit = asyncio.Semaphore(50)
        section_tasks = [
            self._process_section_planning(
                section,
                heading_map,
                all_papers_map,
                extra_papers_title,
                final_proposal,
                distribution_limit,
                concurrency_limit,
            ) for section in final_outline["outline"]
        ]

        section_results = await asyncio.gather(*section_tasks)

        for in_tok, out_tok in section_results:
            self.input_tokens += in_tok
            self.output_tokens += out_tok

        logger.info(f"[{self}] Starting batched writing process...")

        async def _bounded_write_task(*args):
            async with concurrency_limit:
                return await self._process_single_reference_task(*args)

        for section_count, section in enumerate(final_outline["outline"]):
            section_tasks = []
            
            for subheading_count, subheading in enumerate(section["subheadings"]):
                if not subheading.get("refs"):
                    subheading["refs"] = []

                unique_refs: list[dict] = []
                seen_titles_sub = set()
                for ref in subheading["refs"]:
                    if ref["title"] not in seen_titles_sub and ref["title"] in extra_papers_title:
                        unique_refs.append(ref)
                        seen_titles_sub.add(ref["title"])
                        
                        section_tasks.append(_bounded_write_task(
                            ref, subheading, research_papers, store, collection_chunk, encoding
                        ))
                
                final_outline["outline"][section_count]["subheadings"][subheading_count]["refs"] = unique_refs
            ref_usage_list: list[dict] = []
            if section_tasks:
                logger.info(f"[{self}] Writing {len(section_tasks)} paragraphs for Section {section_count + 1 + current_section_offset}...")
                section_results = await asyncio.gather(*section_tasks)
                for res in section_results:
                    in_tok, out_tok, emb_tok, usage_row = res
                    self.embed_tokens += emb_tok
                    self.input_tokens += in_tok
                    self.output_tokens += out_tok
                    ref_usage_list.append(usage_row)

                if ref_usage_list:
                    db_index = section_count + 1 + current_section_offset
                    await outlines_collection.update_one(
                        {"documentId": self.document_id, "index": db_index},
                        {"$push": {"refTable": {"$each": ref_usage_list}}},
                        upsert=True
                    )
            if section_count == 0:
                if ref_usage_list:
                    used_papers = set([row["paperTitle"] for row in ref_usage_list])
                    unused_papers = set(extra_papers_title) - used_papers
                else:
                    unused_papers = set(extra_papers_title)

                if unused_papers:
                    logger.info(f"[{self}] Found {len(unused_papers)} mapped but unused papers. Adding placeholders.")
                    placeholder_rows = []
                    
                    for paper_title in unused_papers:
                        first_usage_section, first_usage_subheading, inherited = self._find_first_paper_usage(
                            paper_title, final_outline["outline"]
                        )
                        
                        ref_paper = all_papers_map.get(paper_title)
                        if not ref_paper:
                            continue

                        summary_ref_id = hash_blake3(str(ref_paper["key_points"]))
                        try:
                            await collection_chunk.insert_one({"_id": summary_ref_id, "content": ref_paper["key_points"]})
                        except Exception:
                            pass

                        if first_usage_section and first_usage_subheading:
                            usage_desc = f"{first_usage_section['heading']} - {first_usage_subheading['subheading']}"
                        else:
                            usage_desc = get_language_dict(self.language)["unused_paper"]

                        placeholder_rows.append({
                            "paperTitle": paper_title,
                            "subheading": "",
                            "summary": summary_ref_id,
                            "paperUsage": usage_desc,
                            "paperChunk": "",
                            "notUsed": True,
                            "inherited": inherited,
                        })
                    
                    if placeholder_rows:
                        first_db_index = 1 + current_section_offset
                        await outlines_collection.update_one(
                            {"documentId": self.document_id, "index": first_db_index},
                            {"$push": {"refTable": {"$each": placeholder_rows}}},
                            upsert=True
                        )

    async def format_headings(self, outline: str, word_count_str: str, report_note: dict, research_note: str) -> dict:
        research_note_str = f"Research note:\n{research_note}\n"
        content = f"""
        Outline's note:
        {outline}
        Total word count:
        {word_count_str}
        Report note:
        {report_note}
        {research_note_str}
        User's language:
        {self.language}
        """
        error, success, formatted_outline, input_tokens, output_tokens = await get_answer_with_schema(
            self.id, 
            self.llm, 
            UPDATE_OUTLINE_CHAT, 
            content, 
            OutlineChat,
        )
        if not success:
            raise error
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        return formatted_outline.model_dump()["outline"]

    async def generate_outline_description_chatbot(
        self,
        summary: dict,
        outline: str, 
        docs: list[dict],
        research_note: str,
        db_key,
    ) -> tuple[dict, dict, int, int, int]:
        if outline:
            word_count_str = f"{summary["word_count"]}-{summary["word_count"]}"
            formatted_outline = await self.format_headings(outline, word_count_str, summary, research_note)
        else:
            formatted_outline = [{
                "heading": summary["user_request"],
                "word_count": f"{summary["word_count"]}-{summary["word_count"]}"
            }]
        del summary["word_count"]
        tasks = [self.get_heading_description(outline, research_note, section) for section in formatted_outline]
        results = await asyncio.gather(*tasks)
        headings = [result[0] for result in results]
        input_tokens = [result[1] for result in results]
        output_tokens = [result[2] for result in results]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        headings_dict = [heading.model_dump() for heading in headings]

        # Keep subheadings as None if they don't exist (section-only content)
        # Don't convert to empty array - None means "write section without subsections"
        final_outline = {
            "title": summary["research_topic"],
            "outline": headings_dict,
        }
        if docs:
            all_papers_map = {p["title"]: p for p in docs}
            extra_papers_title = [p["title"] for p in docs]
            admin_db = self.mongo_client["admin"]
            collection_chunk = admin_db["reports_refs_chunks"]
            embeddings = get_embeddings(db_key)
            try:
                encoding = tiktoken.encoding_for_model(settings.EMBEDDING_MODEL)
            except Exception:
                encoding = tiktoken.get_encoding("cl100k_base")
            store = QdrantVectorStore.from_existing_collection(
                url=settings.QDRANT_URL,
                collection_name=self.document_id,
                embedding=embeddings
            )
            logger.info(f"[{self}] Mapping user papers to relevant sections...")
            heading_map, _ = await self._map_papers_to_sections(
                summary, final_outline["outline"], docs, 3
            )
            concurrency_limit = asyncio.Semaphore(50)

            async def _bounded_plan_task(*args):
                async with concurrency_limit:
                    return await self._generate_plans_for_subsection(*args)
                    
            logger.info(f"[{self}] Generating usage plans...")
            for section in final_outline["outline"]:
                # Skip sections without subheadings (section-only content)
                if section.get("subheadings") is None:
                    continue

                section_heading = section.get("heading")
                papers_for_section = list(set(heading_map.get(section_heading, [])))
                if not papers_for_section:
                    for subsection in section["subheadings"]:
                        subsection["refs"] = []
                    continue

                plan_tasks = []
                for subsection in section["subheadings"]:
                    plan_tasks.append(_bounded_plan_task(
                        section, all_papers_map, papers_for_section, summary, subsection, 3
                    ))

                plan_results = await asyncio.gather(*plan_tasks)

                for subsection, (refs, in_tokens, out_tokens) in zip(section["subheadings"], plan_results):
                    if not subsection.get("refs"):
                        subsection["refs"] = []
                    subsection["refs"].extend(refs)
                    self.input_tokens += in_tokens
                    self.output_tokens += out_tokens

            logger.info(f"[{self}] Starting batched writing process...")
                        
            async def _bounded_write_task(*args):
                async with concurrency_limit:
                    return await self._process_single_reference_task(*args)

            for section_count, section in enumerate(final_outline["outline"]):
                # Skip sections without subheadings (section-only content)
                if section.get("subheadings") is None:
                    continue

                section_tasks = []

                for subheading_count, subheading in enumerate(section["subheadings"]):
                    if not subheading.get("refs"):
                        subheading["refs"] = []

                    unique_refs: list[dict] = []
                    seen_titles_sub = set()
                    for ref in subheading["refs"]:
                        if ref["title"] not in seen_titles_sub and ref["title"] in extra_papers_title:
                            unique_refs.append(ref)
                            seen_titles_sub.add(ref["title"])

                            section_tasks.append(_bounded_write_task(
                                ref, subheading, docs, store, collection_chunk, encoding
                            ))

                    final_outline["outline"][section_count]["subheadings"][subheading_count]["refs"] = unique_refs

                if not section_tasks:
                    continue

                logger.info(f"[{self}] Writing {len(section_tasks)} paragraphs for Section {section_count + 1}...")
                section_results = await asyncio.gather(*section_tasks)

                for res in section_results:
                    in_tok, out_tok, emb_tok, _ = res
                    self.embed_tokens += emb_tok
                    self.input_tokens += in_tok
                    self.output_tokens += out_tok
        else:
            for section_count, section in enumerate(final_outline["outline"]):
                # Skip sections without subheadings (section-only content)
                if section.get("subheadings") is None:
                    continue
                for subheading_count, subheading in enumerate(section["subheadings"]):
                    subheading["refs"] = []

        return final_outline, summary, self.input_tokens, self.output_tokens, self.embed_tokens
