import asyncio
import json
from io import StringIO
from bson import ObjectId
from collections import defaultdict
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI
import logging
import operator
from pymongo.asynchronous.collection import AsyncCollection
from pymongo import ASCENDING, DESCENDING
import rapidfuzz
import re
import traceback
import time
from typing import Literal
import pandas as pd
import math
from html import escape
from markdownify import markdownify

from write_reports.src.configs.app import settings
from write_reports.src.modules.references import edit_refs, shorten_authors_name
from write_reports.src.modules.log_cleaner import LogCleaner, LogCleaningConfig
from write_reports.src.schemas.section import (
    AnalysisOutput,
    DataSection,
    SectionContent,
    SectionDescription,
    SectionContentWithTokenCount,
    SubSectionDescription,
    LitSection,
    MethodSection,
    MethodSubSection,
    ResultSection,
    ChosenLogs,
    ChosenBlocks,
    RefsUsage,
    PlacementPlan,
    LogContentBlock,
    SubsectionLogAssignment,
)
from write_reports.src.modules.write_prompt_bank import (
    identify_literature_review,
    identify_proposed_method,
    select_methodology_logs,
    identify_results,
    select_detail_logs,
    file_matching_instructions,
    assign_logs_to_subsection,
    select_content_blocks,
    section_writer_without_seminar_instructions,
)
from write_reports.src.modules.utils.prompt_utils import (
    build_block_selection_content,
    build_block_writing_content,
    build_file_matching_content,
    build_literature_review_section_prompt,
    build_subsection_log_assignment_content,
    build_method_log_selection_content,
    build_method_write_content,
    build_proposed_method_section_prompt,
    build_ref_integration_prompt,
    build_result_section_prompt,
    build_search_strategy_content,
    build_websearch_content,
    build_websearch_research_context,
    build_write_section_content,
    build_write_subsection_content,
    get_search_context,
)
from pydantic import BaseModel, Field
from enum import Enum
from get_llm_response import get_llm, get_answer_with_schema, get_answer_with_websearch
from translate import LANGUAGE_KIT
from utils import get_mongodb_client, markdownify_keep_images, _clean_text, _clean_text_enhanced

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)
DOCUMENT_EMBED_KEY = "document_embed:{file_id}"


def remove_markdown_subsections(content: str, delete_subsections: bool = False) -> str:
    """
    Post-process generated content to remove markdown subsections (##, ###, ####, etc.)
    and replace them with bullet points or bold text, or delete them entirely.

    - Keeps only # headings (for section titles - # is used for numbered sections like "# 1. Introduction")
    - If delete_subsections=False: Converts ## to **bold text**, ### and deeper to bullet points
    - If delete_subsections=True: Deletes all subsection lines (##, ###, ####, etc.)
    - Strips number tags like 1.1, 1.1.2, 1.1.2.3, etc. from heading text

    Args:
        content: The markdown content to process
        delete_subsections: If True, delete subsection lines instead of reformatting them

    Returns:
        Processed content with subsections replaced or removed
    """
    lines = content.split('\n')
    processed_lines = []

    # Pattern to match number tags like "1.1", "1.1.2", "1.1.2.3", etc.
    number_tag_pattern = re.compile(r'^\d+(\.\d+)+\.?\s*')

    for line in lines:
        # Match markdown headings (##, ###, ####, etc. but NOT #)
        heading_match = re.match(r'^(#{2,})\s+(.+)$', line)

        if heading_match:
            if delete_subsections:
                # Skip this line (delete it)
                continue

            level = len(heading_match.group(1))  # Number of # symbols
            heading_text = heading_match.group(2).strip()

            # Remove number tags like 1.1, 1.1.2, 1.1.2.3, etc.
            heading_text = number_tag_pattern.sub('', heading_text).strip()

            if level == 2:
                # Convert ## to bold text
                processed_lines.append(f'\n**{heading_text}**\n')
            elif level == 3:
                # Convert ### to bullet point
                processed_lines.append(f'• {heading_text}')
            elif level == 4:
                # Convert #### to indented bullet point
                processed_lines.append(f'  - {heading_text}')
            else:
                # Convert ##### and deeper to more indented bullet points
                indent = '    ' * (level - 4)
                processed_lines.append(f'{indent}- {heading_text}')
        else:
            # Keep the line as is (including # headings)
            processed_lines.append(line)

    return '\n'.join(processed_lines)

def format_table(df: pd.DataFrame) -> str:
    # Convert to a simple HTML table
    html = df.to_html(index=False, border=1, escape=False)

    # Replace the <table>, <th>, <td> tags with minimal inline CSS
    html = html.replace(
        '<table border="1" class="dataframe">',
        (
            "<table style=\"width:100%; border-collapse:collapse; "
            "border:1px solid #000; font-family:'Times New Roman', serif; "
            "font-size:13pt;\">"
        )
    )

    html = html.replace(
        '<th>',
        '<th style="border:1px solid #000; padding:4px; text-align:center; vertical-align:middle;">'
    )

    html = html.replace(
        '<td>',
        '<td style="border:1px solid #000; padding:4px; text-align:justify; vertical-align:top;">'
    )

    return html


def title_matching(queries: list[str], titles: list[str], cut_off_score=85) -> list[str]:
    normalized: list[str] = []
    titles_set = set(titles)
    for query in queries:
        if query in titles_set:
            normalized.append(query)
        else:
            best_match = rapidfuzz.process.extractOne(query, titles, scorer=rapidfuzz.fuzz.WRatio)
            if best_match and best_match[1] >= cut_off_score:
                normalized.append(best_match[0])
    return normalized


def split_markdown_by_paragraph(markdown_text: str) -> list[str]:
    """
    Splits Markdown into blocks: Code, Headers, Lists, and Paragraphs.
    Ensures Headers (#) are isolated as their own blocks.
    """
    split_by_code_blocks = re.split(r'(```.*?```)', markdown_text, flags=re.DOTALL)
    final_blocks: list[str] = []

    list_pattern = re.compile(r'^\s*([-*]|\d+\.)\s')
    header_pattern = re.compile(r'(^\s*#{1,6}\s.*$)', flags=re.MULTILINE)

    for i, part in enumerate(split_by_code_blocks):
        if not part.strip():
            continue
        if i % 2 == 1:
            final_blocks.append(part.strip())
        else:
            split_by_headers = header_pattern.split(part)
            for h_part in split_by_headers:
                if not h_part.strip():
                    continue

                if header_pattern.match(h_part.strip()):
                    final_blocks.append(h_part.strip())
                    continue
                paragraphs = re.split(r'\n\s*\n', h_part.strip())
                current_list_group: list[str] = []
                for p in paragraphs:
                    if not p.strip():
                        continue
                    first_line = p.strip().split('\n')[0]
                    is_list_item = list_pattern.match(first_line)
                    if is_list_item:
                        current_list_group.append(p.strip())
                    else:
                        if current_list_group:
                            final_blocks.append("\n".join(current_list_group))
                            current_list_group = []
                        final_blocks.append(p.strip())
                if current_list_group:
                    final_blocks.append("\n".join(current_list_group))

    return final_blocks


def merge_text(main_block: str, ref_text: str) -> str:
    lines = main_block.rstrip().split('\n')
    if not lines:
        return ref_text

    last_line = lines[-1].strip()
    is_complex_end = re.match(r'^\s*([-*]|\d+\.)\s', last_line) or last_line.startswith("```") or last_line.startswith("#")
    if is_complex_end:
        return f"{main_block}\n\n{ref_text}"
    else:
        return f"{main_block} {ref_text}"


async def integrate_refs_into_subsection(
    _id: str,
    subsection_content: str,
    integration_paragraphs: list[str],
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
) -> tuple[str, int, int]:
    """
    Merges a list of 'Integration Paragraphs' into the 'Main Subsection Content'
    at the most logical positions.
    """
    if not integration_paragraphs:
        return subsection_content, 0, 0
    heading, _, body = subsection_content.partition("\n")
    if not body.strip():
        return heading + "\n\n" + "\n\n".join(integration_paragraphs), 0, 0
    main_blocks = split_markdown_by_paragraph(body)

    if not main_blocks:
        return heading + "\n\n" + "\n\n".join(integration_paragraphs), 0, 0

    blocks_str = "\n".join([f"BLOCK_ID [{i}]:\n{block[:200]}..." for i, block in enumerate(main_blocks)])

    refs_str = "\n".join([f"INTEGRATION_ID [{i}]:\n{para}" for i, para in enumerate(integration_paragraphs)])

    prompt = build_ref_integration_prompt(blocks_str, refs_str, len(main_blocks) - 1)
    _, success, plan, input_tokens, output_tokens = await get_answer_with_schema(
        _id,
        llm,
        "ORGANIZE_PARAGRAPHS",
        prompt,
        PlacementPlan
    )

    if not success:
        return subsection_content + "\n\n" + "\n\n".join(integration_paragraphs)
    insertions_map = defaultdict(list)

    for placement in plan.placements:
        idx = max(-1, min(placement.insert_after_block_idx, len(main_blocks) - 1))
        if 0 <= placement.integration_idx < len(integration_paragraphs):
            ref_text = integration_paragraphs[placement.integration_idx]
            insertions_map[idx].append(ref_text)

    final_output: list[str] = []

    if -1 in insertions_map:
        final_output.append("\n\n".join(insertions_map[-1]))

    for i, block in enumerate(main_blocks):
        current_text = block
        if i in insertions_map:
            refs_to_add = insertions_map[i]
            for ref in refs_to_add:
                current_text = merge_text(current_text, ref)

        final_output.append(current_text)

    return heading + "\n\n" + "\n\n".join(final_output), input_tokens, output_tokens


async def tidy_up(
    section: SectionContent,
    state: dict,
    total_input_tokens: int,
    total_output_tokens: int,
    llm: ChatGoogleGenerativeAI | ChatOpenAI = None,
) -> tuple[str, list[str], int, int]:
    if "###" in section.content[:3]:
        section.content = section.content.replace("###", "##", 1)

    if f"## {state['section'].heading}" not in section.content:
        section.content = f"## {state['section'].heading}\n\n" + section.content
    # Keep Markdown image alt text; only strip non-numeric bracket annotations.
    section.content = re.sub(r'(?<!!)\[(?!\d+\])[^]]*\]', '', section.content)
    matches = re.findall(r'(?<!!)\[(\d+)\]', section.content)
    seen_ref: list[str] = []
    if len(matches) != 0:
        unique_matches_int = sorted(list(set(map(int, matches))))
        for match in unique_matches_int:
            if 0 < match <= len(section.ref):
                ref_title = section.ref[match - 1]
                section.content = re.sub(
                    rf'(?<!!)\[{match}\]', f'[{ref_title}]', section.content
                )

                if ref_title not in seen_ref:
                    seen_ref.append(ref_title)
            else:
                section.content = re.sub(rf'(?<!!)\[{match}\]', '', section.content)
    integration_paragraphs = [ref.ref_chunk for ref in state["subsection"].refs]
    titles = [ref.title for ref in state["subsection"].refs]
    merged_text, input_tokens, output_tokens = await integrate_refs_into_subsection(state["document_id"][:8], section.content, integration_paragraphs, llm)
    final_matches = re.findall(r'(?<!!)\[(.*?)\]', merged_text)
    reorder_ref: list[str] = []
    for match in final_matches:
        if match not in reorder_ref:
            if match in titles:
                reorder_ref.append(match)
            else:
                merged_text = re.sub(
                    rf'(?<!!)\[{re.escape(match)}\]', '', merged_text
                )
    return merged_text, reorder_ref, total_input_tokens + input_tokens, total_output_tokens + output_tokens


def clean_heading_numbers(text: str) -> str:
    def replacer(match):
        full = match.group(0)
        hash_part = re.match(r"#+", full).group(0)
        cleaned = re.sub(rf"{re.escape(hash_part)}[^\w]*[\d\.\s\-:]*", hash_part + " ", full, 1)
        return cleaned

    return re.sub(r"(#+[^\n]*\n)", replacer, text)


def number_headings_relative(hashes, skip_top_level, section_num, sub_section_num):
    min_level = len(hashes[0])
    levels = [0] * 5
    result = []

    for h in hashes:
        level = len(h) - min_level  # Relative level
        if skip_top_level and level == 0:
            result.append(str(section_num + 1) + '.' + str(sub_section_num + 1) + '.')  # No numbering for top level
            continue
        levels[level] += 1
        # Reset deeper levels
        for i in range(level + 1, len(levels)):
            levels[i] = 0
        # Construct number
        num = '.'.join(str(levels[i]) for i in range(level + 1) if levels[i] > 0)
        result.append(str(section_num + 1) + '.' + str(sub_section_num + 1) + '.' + num + '.')
    return result


async def _section_synthesizer(state: dict, section_count: int, num_subsection: int = 1) -> tuple[dict, list[str]]:
    current_section = state["current_section"]
    section_dict: dict = {}
    section_dict["name"] = current_section.heading.lower()
    if len(current_section.subsections):
        content = ""
        for subsection_count, subsection_object in enumerate(state["completed_subsections"][-num_subsection:]):
            subsection = subsection_object.content
            subsection = clean_heading_numbers(subsection)
            subsection = subsection.replace(f"## {current_section.heading}\n\n", "")
            if not subsection.startswith("##"):
                if f"## {current_section.subsections[subsection_count].subheading}\n\n" in subsection:
                    subsection = subsection.replace(f"## {current_section.subsections[subsection_count].subheading}\n\n", "")
                subsection = f"## {current_section.subsections[subsection_count].subheading}\n\n" + subsection
            list_heading = re.findall(r"^#+", subsection, flags=re.MULTILINE)
            lines = subsection.splitlines()
            skip_top_level = list_heading.count(list_heading[0]) == 1
            heading_lines = []
            heading_indexes = []
            for idx, line in enumerate(lines):
                if re.match(r'^\s*#+\s*\S', line):
                    heading_lines.append(line)
                    heading_indexes.append(idx)
            numbering = number_headings_relative(list_heading, skip_top_level, section_count, subsection_count)
            for idx, num, original in zip(heading_indexes, numbering, heading_lines):
                match = re.match(r'^(\s*#+)\s*(.*)', original)
                if match:
                    _, title = match.groups()
                    hash_part = ["#"] * (len(num.split(".")) - 1)
                    numbered_title = f"{''.join(hash_part)} {num} {title.strip()}"
                    lines[idx] = numbered_title
                    # logger.info(lines[idx])
            reorder_heading_subsection = '\n'.join(lines)
            reorder_heading_subsection, state["existing_refs"] = edit_refs(
                state["research_papers"],
                reorder_heading_subsection,
                subsection_object.ref,
                state["existing_refs"],
                state["references_style"] if "references_style" in state else state["proposal"].get("references_style", "IEEE")
            )
            if subsection_count != 0:
                subsection = f"\n\n{reorder_heading_subsection}"
            elif subsection_count == 0:
                subsection = f"## {section_count + 1}. {current_section.heading}\n\n{state['current_section'].overview}\n\n" + reorder_heading_subsection
            content += subsection
        section_dict["content"] = content.replace(r'\n', '\n').replace("##", "#", 1)
        section_dict["word_count"] = len(content.split(" "))
        section_dict["input_tokens"] = sum([
            completed_section.input_tokens
            for completed_section in state["completed_subsections"][-num_subsection:]
        ])
        section_dict["output_tokens"] = sum([
            completed_section.output_tokens
            for completed_section in state["completed_subsections"][-num_subsection:]
        ])
        section_dict["web_search_call"] = sum([
            completed_section.web_search_call
            for completed_section in state["completed_subsections"][-num_subsection:]
        ])
        section_dict["embed_tokens"] = sum([
            completed_section.embed_tokens
            for completed_section in state["completed_subsections"][-num_subsection:]
        ])
        section_dict["current_refs"] = state["existing_refs"]
    else:
        subsection_object = state["completed_subsections"][-1]
        subsection = subsection_object.content
        subsection = clean_heading_numbers(subsection)
        subsection = subsection.replace(f"## {current_section.heading}\n\n", "")
        if not subsection.startswith("##"):
            subsection = f"## {current_section.heading}\n\n" + subsection
        list_heading = re.findall(r"^#+", subsection, flags=re.MULTILINE)
        lines = subsection.splitlines()[1:]
        skip_top_level = list_heading.count(list_heading[0]) == 1
        heading_lines = []
        heading_indexes = []
        for idx, line in enumerate(lines):
            if re.match(r'^\s*#+\s*\S', line):
                heading_lines.append(line)
                heading_indexes.append(idx)
        numbering = number_headings_relative(list_heading, skip_top_level, section_count, 0)
        for idx, num, original in zip(heading_indexes, numbering, heading_lines):
            match = re.match(r'^(\s*#+)\s*(.*)', original)
            if match:
                _, title = match.groups()
                hash_part = ["#"] * (len(num.split(".")) - 1)
                numbered_title = f"{''.join(hash_part)} {num} {title.strip()}"
                lines[idx] = numbered_title
                # logger.info(lines[idx])
        reorder_heading_subsection = '\n'.join(lines)
        reorder_heading_subsection, state["existing_refs"] = edit_refs(
            state["research_papers"],
            reorder_heading_subsection,
            subsection_object.ref,
            state["existing_refs"],
            state["references_style"] if "references_style" in state else state["proposal"].get("references_style", "IEEE")
        )
        subsection = f"## {section_count + 1}. {current_section.heading}\n\n{state['current_section'].overview}\n\n" + reorder_heading_subsection
        section_dict["content"] = subsection.replace(r'\n', '\n').replace("##", "#", 1)
        section_dict["word_count"] = len(subsection.split(" "))
        section_dict["input_tokens"] = sum([
            completed_section.input_tokens
            for completed_section in state["completed_subsections"][-1:]
        ])
        section_dict["output_tokens"] = sum([
            completed_section.output_tokens
            for completed_section in state["completed_subsections"][-1:]
        ])
        section_dict["current_refs"] = state["existing_refs"]
    return section_dict, state["existing_refs"]


async def get_key_points(collection: AsyncCollection, papers: list[dict], document_id: str) -> tuple[list[dict], dict]:
    cached_data = await collection.find_one({"_id": document_id})
    documents = cached_data.get("documents", []) if cached_data else []
    doc_map = {d["title"]: d for d in documents if isinstance(d, dict) and "title" in d}
    key_points_map = {}
    for paper in papers:
        title = paper.get("title", "")
        document = doc_map.get(title)
        kp = str(document.get("key_points", "")) if document else ""
        paper["key_points"] = kp
        key_points_map[title] = kp
    return papers, key_points_map


async def _load_model(state: dict):
    logger.info(f"[{state['document_id'][:8]}] Loading refs")
    mongo_client = get_mongodb_client()
    db = mongo_client["user_documents"]
    collection = db["reports_refs"]
    refs, key_points_map = await get_key_points(collection, state["research_papers"], state["document_id"])
    logger.info(f"[{state['document_id'][:8]}] Done loading refs")
    return {
        "existing_refs": [],
        "embed_tokens": 0,
        "embed_uuids": [],
        "key_points_map": key_points_map,
        "research_papers": refs,
        "seen_chunk": {},
    }


async def load_current_section(state: dict):
    mongo_client = get_mongodb_client()
    admin_db = mongo_client["admin"]
    content_collection = admin_db["outlines"]
    try:
        user_report_caches = content_collection.find({"documentId": state["document_id"]}).sort("index", ASCENDING)
        async for doc in user_report_caches:
            if doc["content"]:
                state["other_sections"][doc["index"]]["content"] = markdownify_keep_images(doc["content"])
            else:
                if doc["contentArr"]:
                    section_content = ""
                    for subsection in doc["contentArr"]:
                        section_content += subsection["text"]
                    state["other_sections"][doc["index"]]["content"] = markdownify_keep_images(section_content)
                else:
                    break
    except Exception:
        pass
    collection_chunk = admin_db["reports_refs_chunks"]
    collection_docs = admin_db["processed_docs"]
    heading = state["outline"]["outline"][len(state["other_sections"]) - 1]

    # Check if this section has subheadings
    has_subheadings = heading.get('subheadings') is not None and len(heading.get('subheadings', [])) > 0

    if has_subheadings:
        for section in heading['subheadings']:
            section["refs"] = []

    current_section_record = await content_collection.find_one({"documentId": state["document_id"], "index": len(state["other_sections"])})
    current_section_refs = current_section_record.get("refTable", []) if current_section_record else []

    if has_subheadings:
        subheading_map = {sub['subheading']: sub for sub in heading['subheadings']}
    else:
        subheading_map = {}

    refs_by_subheading = {}
    section_refs = []  # For heading-only sections

    for ref_item in current_section_refs:
        if ref_item["paperChunk"]:
            if ref_item["paperTitle"] not in state["key_points_map"].keys():
                summary_record = await collection_chunk.find_one({"_id": ref_item["summary"]})
                state["key_points_map"][ref_item["paperTitle"]] = str(summary_record["content"])
                doc_records = collection_docs.find({"title": ref_item["paperTitle"]}).sort("createdAt", DESCENDING)
                doc_record = None
                async for record in doc_records:
                    doc_record = record
                    break
                if doc_record:
                    unused_field = ["_id", "isDeleted", "createdBy", "updatedBy", "uuid", "createdAt", "updatedAt", "ref_id", "outline_id", "citation"]
                    for field in unused_field:
                        if field in doc_record:
                            del doc_record[field]
                    doc_record["key_point"] = str(summary_record["content"])
                    state["research_papers"].append(doc_record)
                else:
                    logger.warning(f"[{state['document_id'][:8]}] Processed document not found for '{ref_item['paperTitle']}'")
            ref_item["summary"] = state["key_points_map"][ref_item["paperTitle"]]
            if ref_item["paperChunk"] not in state["seen_chunk"]:
                chunk_record = await collection_chunk.find_one({"_id": ref_item["paperChunk"]})
                state["seen_chunk"][ref_item["paperChunk"]] = chunk_record["content"]
            ref_item["paperChunk"] = state["seen_chunk"][ref_item["paperChunk"]]

            if has_subheadings:
                # Distribute to subheadings
                key = ref_item["subheading"]
                if key not in refs_by_subheading:
                    refs_by_subheading[key] = []
                refs_by_subheading[key].append(ref_item)
            else:
                # Collect for section-level
                section_refs.append(ref_item)

    # Process references
    if has_subheadings:
        for subheading_key, new_refs_list in refs_by_subheading.items():
            subheading_object = subheading_map[subheading_key]
            subheading_object["refs"] = new_refs_list

        subsections: list[SubSectionDescription] = []
        for sub_section in heading["subheadings"]:
            subsection = SubSectionDescription(
                detail_description=sub_section.get("detail_description", ""),
                subheading=sub_section["subheading"].split(" ", 1)[-1] if "." in sub_section["subheading"].split(" ")[0] else sub_section["subheading"],
                subheading_word_count=sub_section.get("subheading_word_count", "0-0"),
                refs=[RefsUsage(title=ref["paperTitle"], ref_chunk=ref["paperChunk"], usage=ref["paperUsage"], summary=ref["summary"]) for ref in sub_section["refs"]],
            )
            subsections.append(subsection)

        current_section = SectionDescription(
            heading=heading["heading"].split(" ", 1)[-1] if "." in heading["heading"].split(" ")[0] else heading["heading"],
            overview=heading["overview"],
            word_count=heading["word_count"],
            subsections=subsections,
            section_refs=None
        )
    else:
        # Heading-only section
        current_section = SectionDescription(
            heading=heading["heading"].split(" ", 1)[-1] if "." in heading["heading"].split(" ")[0] else heading["heading"],
            overview=heading["overview"],
            word_count=heading["word_count"],
            subsections=None,
            section_refs=[RefsUsage(title=ref["paperTitle"], ref_chunk=ref["paperChunk"], usage=ref["paperUsage"], summary=ref["summary"]) for ref in section_refs]
        )
    search_phase = 0
    if "lit_review" in state:
        if len(state["other_sections"]) < state["lit_review"]:
            search_phase = 1
        if "result" in state:
            if state["lit_review"] <= len(state["other_sections"]) < state["result"]:
                search_phase = 2
            else:
                search_phase = 3

    return state, current_section, search_phase


async def get_appendices(state: dict):
    appendices_content = await _get_appendices(state)
    appendices_section = {
        "name": "appendices",
        "content": appendices_content,
        "input_tokens": 0,
        "output_tokens": 0,
        "embed_tokens": 0,
        "section_code": state["outline_code"][-1],
        "current_refs": [],
    }
    state["other_sections"].append(appendices_section)
    mongo_client = get_mongodb_client()
    db = mongo_client["user_documents"]
    refs_collection = db["reports_refs"]
    await refs_collection.find_one_and_delete({"_id": state["document_id"]})
    admin = mongo_client["admin"]
    search_queries_collection = admin["websearch"]
    await search_queries_collection.find_one_and_delete({"_id": state["document_id"]})
    return {
        "other_sections": state["other_sections"],
    }


async def get_need_data(
    model_id: str,
    document_id: str,
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
    system_prompt: str,
    outline: str,
    section: str
) -> tuple[bool, str, str]:
    content = outline + "\n" + section
    error, success, section_with_data, input_tokens, output_tokens = await get_answer_with_schema(
        document_id[:8],
        llm,
        system_prompt,
        content,
        DataSection
    )
    if not success:
        raise error
    if not success:
        section_with_data = DataSection(need_data=False)
        logger.info(f"[{document_id[:8]}] Fail to determine if need data or not, set default to False")
    return section_with_data.need_data, input_tokens, output_tokens


async def get_need_propose_method(
    model_id: str,
    document_id: str,
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
    system_prompt: str,
    outline: str,
    section: str
) -> tuple[bool, str, str]:
    content = outline + "\n" + section
    error, success, method_section, input_tokens, output_tokens = await get_answer_with_schema(
        document_id[:8],
        llm,
        system_prompt,
        content,
        MethodSubSection
    )
    if not success:
        raise error
    if not success:
        method_section = MethodSubSection(new_contribution=False)
        logger.info(f"[{document_id[:8]}] Fail to determine if is method or not, set default to False")
    return method_section.new_contribution, input_tokens, output_tokens


async def write(
    state: dict,
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
    write_prompt: str,
    user_content: str = ""
) -> SectionContentWithTokenCount:
    """Worker writes a section of the report based on given information"""
    current_report = f"Other sections of the report:\n{state['current_report']}" if len(state["current_report"]) else ""
    logger.info(f"[{state['document_id'][:8]}] Writing section {state['section'].heading} - {state['subsection'].subheading} with {state['subsection'].subheading_word_count} word")
    web_search_content, web_search_call, total_input_tokens, total_output_tokens = await search_web(state)
    extra_content = f"Up-to-date information for the subsection:\n{web_search_content}" if web_search_content else ""
    retry = 0
    last_success = SectionContentWithTokenCount(
        content=f"Error writing section {state['section'].heading} - {state['subsection'].subheading}",
        ref=[],
        input_tokens=total_input_tokens,
        output_tokens=total_output_tokens,
        embed_tokens=0,
        web_search_call=web_search_call,
    )
    extra_content = f"Up-to-date information for the subsection:\n{web_search_content}" if web_search_content else ""
    while retry < 3:
        retry += 1
        try:
            content_rewrite = build_write_subsection_content(
                state,
                current_report,
                user_content,
                extra_content,
            )
            error, success, section, input_tokens, output_tokens = await get_answer_with_schema(
                state['document_id'][:8],
                llm,
                write_prompt,
                content_rewrite,
                SectionContent
            )
            if not success:
                raise error
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            if r"\x" in rf"{section.content}" or not success:
                logger.info(f"[{state['document_id'][:8]}] Fail to generate section {state['section'].heading} - {state['subsection'].subheading}, revert to the previous case")
                last_success.input_tokens = total_input_tokens
                last_success.output_tokens = total_output_tokens
                section_with_token_count = last_success
            else:
                last_success.content = section.content
                last_success.ref = section.ref
                titles = [paper.title for paper in state["subsection"].refs]
                section.ref = title_matching(section.ref, titles)
                reorder_ref_content, reorder_ref, total_input_tokens, total_output_tokens = await tidy_up(
                    section,
                    state,
                    total_input_tokens,
                    total_output_tokens,
                    llm
                )
                reorder_ref_content = _clean_text(reorder_ref_content)
                last_success.input_tokens = total_input_tokens
                last_success.output_tokens = total_output_tokens
                # logger.info(reorder_ref_content)
                if r"\x" in rf"{reorder_ref_content}":
                    logger.info(f"[{state['document_id'][:8]}] Fail to generate cleaned section {state['section'].heading} - {state['subsection'].subheading}, revert to the previous case")
                    section_with_token_count = last_success
                else:
                    section_with_token_count = SectionContentWithTokenCount(
                        content=reorder_ref_content,
                        ref=reorder_ref,
                        input_tokens=total_input_tokens,
                        output_tokens=total_output_tokens,
                        embed_tokens=0,
                        web_search_call=web_search_call,
                    )
                    retry = 3
        except Exception as e:
            section_with_token_count = last_success
            logger.info(f"[{state['document_id'][:8]}] {e}")
            logger.info(traceback.format_exc())
    return section_with_token_count


async def write_no_sub(
    state: dict,
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
    write_prompt: str,
    user_content: str = ""
) -> SectionContentWithTokenCount:
    """Worker writes a section of the report based on given information"""
    current_report = f"Other sections of the report:\n{state['current_report']}" if len(state["current_report"]) else ""
    logger.info(f"[{state['document_id'][:8]}] Writing section {state['section'].heading} - {state['section'].word_count} word")
    web_search_content, web_search_call, total_input_tokens, total_output_tokens = await search_web(state)
    extra_content = f"Up-to-date information for the subsection:\n{web_search_content}" if web_search_content else ""
    retry = 0
    last_success = SectionContentWithTokenCount(
        content=f"Error writing section {state['section'].heading}",
        ref=[],
        input_tokens=total_input_tokens,
        output_tokens=total_output_tokens,
        embed_tokens=0,
        web_search_call=web_search_call,
    )
    extra_content = f"Up-to-date information for the subsection:\n{web_search_content}" if web_search_content else ""

    # Determine which references to use: section-level or subsection-level
    refs = state["section"].section_refs if state["section"].has_subsections() == False else state["subsection"].refs

    # Handle case where refs is None (sections without subheadings may not have refs)
    if refs is None:
        refs = []

    while retry < 3:
        retry += 1
        try:
            content_rewrite = build_write_section_content(
                state,
                current_report,
                refs,
                user_content,
                extra_content,
            )
            error, success, section, input_tokens, output_tokens = await get_answer_with_schema(
                state['document_id'][:8],
                llm,
                write_prompt,
                content_rewrite,
                SectionContent
            )
            if not success:
                raise error
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            if r"\x" in rf"{section.content}" or not success:
                logger.info(f"[{state['document_id'][:8]}] Fail to generate section {state['section'].heading}, revert to the previous case")
                last_success.input_tokens = total_input_tokens
                last_success.output_tokens = total_output_tokens
                section_with_token_count = last_success
            else:
                last_success.content = section.content
                last_success.ref = section.ref
            titles = [paper.title for paper in refs]
            section.ref = title_matching(section.ref, titles)
            reorder_ref_content, reorder_ref, total_input_tokens, total_output_tokens = await tidy_up(
                section,
                state,
                total_input_tokens,
                total_output_tokens,
                llm
            )
            reorder_ref_content = _clean_text(reorder_ref_content)

            # Post-process to remove markdown subsections when writing sections with no subsections
            reorder_ref_content = remove_markdown_subsections(reorder_ref_content, delete_subsections=True)
            last_success.input_tokens = total_input_tokens
            last_success.output_tokens = total_output_tokens
            # logger.info(reorder_ref_content)
            if r"\x" in rf"{reorder_ref_content}":
                logger.info(f"[{state['document_id'][:8]}] Fail to generate cleaned section {state['section'].heading}, revert to the previous case")
                section_with_token_count = last_success
            else:
                section_with_token_count = SectionContentWithTokenCount(
                    content=reorder_ref_content,
                    ref=reorder_ref,
                    input_tokens=total_input_tokens,
                    output_tokens=total_output_tokens,
                    embed_tokens=0,
                    web_search_call=web_search_call,
                )
                retry = 3
        except Exception as e:
            section_with_token_count = last_success
            logger.info(f"[{state['document_id'][:8]}] {e}")
            logger.info(traceback.format_exc())
    return section_with_token_count


def are_sections_disjoint(list_of_subsections: list[list[int]]) -> bool:
    seen_elements = set()
    list_of_lists: list = []
    for batch in list_of_subsections:
        list_subheading: list[int] = []
        for subsection in batch:
            list_subheading.append(subsection)
        list_of_lists.append(list_subheading)

    for sublist in list_of_lists:
        if not seen_elements.isdisjoint(sublist):
            return False
        seen_elements.update(sublist)
    return True


def validate_sublists(data: list[list[int]]) -> bool:
    if not data:
        return False

    prev_last = -1
    for sublist in data:
        if any(sublist[i] + 1 != sublist[i + 1] for i in range(len(sublist) - 1)):
            return False
        try:
            if sublist[0] <= prev_last:
                return False
        except Exception:
            logger.info(data)
            return False
        prev_last = sublist[-1]

    return True


async def _get_literature_review_section(_id: str, headings: list[str], model_id: str, llm: ChatGoogleGenerativeAI | ChatOpenAI):
    content = build_literature_review_section_prompt(headings)
    return await get_answer_with_schema(_id, llm, identify_literature_review, content, LitSection)


async def _get_proposed_method_section(_id: str, headings: list[str], model_id: str, llm: ChatGoogleGenerativeAI | ChatOpenAI):
    content = build_proposed_method_section_prompt(headings)
    return await get_answer_with_schema(_id, llm, identify_proposed_method, content, MethodSection)


async def _get_result_section(_id: str, headings: list[str], model_id: str, llm: ChatGoogleGenerativeAI | ChatOpenAI):
    content = build_result_section_prompt(headings)
    return await get_answer_with_schema(_id, llm, identify_results, content, ResultSection)


def get_table(section_dict: dict, section_num: int, current_tab_count: int, language: str) -> tuple[dict, int]:
    content = section_dict["content"]
    language_dict = LANGUAGE_KIT.get(language.lower(), LANGUAGE_KIT["tiếng việt"])

    # Find Markdown tables by position
    md_tables = []
    lines = content.splitlines()
    start_idx, in_table = None, False

    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('|') and not in_table:
            in_table = True
            start_idx = i
        elif stripped and not stripped.startswith('|') and in_table:
            # End of markdown table
            md_tables.append(("\n".join(lines[start_idx:i]), start_idx))
            in_table = False
    if in_table:  # Table ends at EOF
        md_tables.append(("\n".join(lines[start_idx:]), start_idx))

    # Find HTML tables by regex (keep match start positions)
    html_table_pattern = re.compile(r"<table[\s\S]*?</table>", re.IGNORECASE)
    html_tables = [(m.group(0), m.start()) for m in html_table_pattern.finditer(content)]

    # Combine and sort by original position
    all_tables = md_tables + html_tables
    all_tables.sort(key=lambda x: x[1])  # sort by appearance order

    # No tables found
    if not all_tables:
        return section_dict, current_tab_count

    table_title = section_dict.get("name", "").strip()
    if not table_title:
        table_title = "Kết quả thống kê" if language.lower() == "tiếng việt" else "Statistical results"
    table_title = table_title.replace("_", " ").title()

    # Replace each table with formatted version
    for table_index, (table_raw, _) in enumerate(all_tables):
        # Keep the report payload Markdown-only. Convert legacy HTML tables
        # only when an upstream caller still provides one.
        if table_raw.strip().startswith('|'):
            table_markdown = table_raw.strip()
        else:
            try:
                table_markdown = pd.read_html(StringIO(table_raw))[0].to_markdown(
                    index=False, tablefmt="pipe"
                )
            except Exception:
                table_markdown = table_raw

        caption = f'**{language_dict["Table"]} {section_num}.{current_tab_count + table_index + 1}.** {table_title}'
        content = content.replace(table_raw, f"{caption}\n\n{table_markdown}", 1)

    # Update section_dict
    section_dict["content"] = content
    return section_dict, current_tab_count + len(all_tables)


def get_literature_review_table(state: dict, papers: list[dict], section_num: int, language: str, table_count: int):
    seen_papers: dict = {}
    paper_count = 1
    language_dict = LANGUAGE_KIT.get(language.lower(), LANGUAGE_KIT["tiếng việt"])
    for section in state["outline"]["outline"]:
        # Skip sections without subheadings (section-only content)
        if section.get("subheadings") is None:
            continue
        for subsection in section["subheadings"]:
            for ref in subsection["refs"]:
                if "title" in ref:
                    paper_title_key = "title"
                    paper_usage_key = "usage_description"
                else:
                    paper_title_key = "paperTitle"
                    paper_usage_key = "paperUsage"
                if ref[paper_title_key] not in seen_papers:
                    result = next(paper for paper in papers if paper.get("title") == ref[paper_title_key])
                    shorten_authors = shorten_authors_name([result["authors"][0]] if len(result["authors"]) > 2 else result["authors"][0:2])
                    year = f"- {result['year']}" if result["year"] != "Unknown" and result["year"] != "" else ""
                    title_authors_year = f"{ref[paper_title_key]} - {shorten_authors + year}"
                    quality = result["q"] if result["q"] in ["Q1", "Q2", "Q3", "Q4"] else result["type"]
                    note = result["status"]
                    seen_papers[ref[paper_title_key]] = {
                        language_dict["No."]: paper_count,
                        language_dict["Title - Authors - Year"]: title_authors_year,
                        language_dict["Quality"]: quality,
                        language_dict["Use in"]: [section["heading"]],
                        language_dict["Usage"]: [f"{section['heading']} - {subsection['subheading']}: {ref[paper_usage_key]}"],
                        language_dict["Note"]: note,
                    }
                    paper_count += 1
                else:
                    if section["heading"] not in seen_papers[ref[paper_title_key]][language_dict["Use in"]]:
                        seen_papers[ref[paper_title_key]][language_dict["Use in"]].append(section["heading"])
                    seen_papers[ref[paper_title_key]][language_dict["Usage"]].append(f"{section['heading']} - {subsection['subheading']}: {ref[paper_usage_key]}")
    final_table_data: list[dict] = []
    for _, data in seen_papers.items():
        data[language_dict["Use in"]] = "- " + "<br>- ".join(list(set(data[language_dict["Use in"]])))
        data[language_dict["Usage"]] = "- " + "<br>- ".join(data[language_dict["Usage"]])
        final_table_data.append(data)
    df = pd.DataFrame(final_table_data)
    html = format_table(df)
    paper_usage_label = language_dict["Paper's Usage"]
    return f'<div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Table"]} {section_num}.{table_count + 1}.</b> {paper_usage_label}</div>{html}'


def _normalize_survey_questions(survey_questions: list[dict]) -> list[dict]:
    """
    Normalize survey_questions to nested structure.
    Handles both flat (from MongoDB) and nested (from propose_method graph) formats.

    Flat format (MongoDB): [{"variable_name": "Trust", "variable_code": "TR", "question": "Q1", ...},
                             {"variable_name": "", "variable_code": "TR", "question": "Q2", ...}]

    Nested format (Graph): [{"variable_name": "Trust", "variable_code": "TR",
                             "questions": [{"question": "Q1", ...}, {"question": "Q2", ...}]}]

    Returns: Always returns nested format
    Raises: KeyError/ValueError if data structure is invalid
    """
    if not survey_questions:
        return []

    clean_survey_questions = []
    for item in survey_questions:
        if isinstance(item, str):
            clean_survey_questions.append({"variable_name": "General", "variable_code": "GEN", "question": item})
        elif isinstance(item, dict):
            clean_survey_questions.append(item)
    survey_questions = clean_survey_questions
    if not survey_questions:
        return []

    # Check if already nested (first item has "questions" key)
    if "questions" in survey_questions[0]:
        return survey_questions

    # Transform flat to nested - use strict dict access to catch data errors
    grouped = {}
    last_valid_var_code = None

    for row in survey_questions:
        var_code = str(row.get("variable_code", "")).strip()

        # Check if var_code is invalid (NaN, empty, None)
        is_invalid_code = not var_code or var_code.lower() == "nan" or var_code.lower() == "none"

        if is_invalid_code and last_valid_var_code:
            # Fallback to previous valid variable code
            var_code = last_valid_var_code
        elif not is_invalid_code:
            # Update last valid
            last_valid_var_code = var_code
        else:
            # If still invalid and no last valid, skip or use placeholder
            if is_invalid_code:
                var_code = "Unknown"

        # Initialize survey group if it doesn't exist yet
        if var_code not in grouped:
            # Clean fields
            var_name = row.get("variable_name", "")
            if str(var_name).lower() == "nan": var_name = ""

            var_type = row.get("variable_type", "")
            if str(var_type).lower() == "nan": var_type = ""

            measure_type = row.get("measurement_type", "")
            if str(measure_type).lower() == "nan": measure_type = ""

            source = row.get("source", "")
            if str(source).lower() == "nan": source = ""

            # If this is a new group, we need a name. If name is NaN/empty, we try to use code or empty
            if not var_name:
                var_name = var_code if var_code != "Unknown" else ""

            grouped[var_code] = {
                "variable_name": var_name,
                "variable_code": var_code,
                "variable_type": var_type,
                "measurement_type": measure_type,
                "source": source,
                "questions": []
            }

        # Clean question fields
        question = row.get("question", "")
        if str(question).lower() == "nan": question = ""

        q_code = row.get("question_code", "")
        if str(q_code).lower() == "nan": q_code = ""

        scale = row.get("scale_type", "")
        if str(scale).lower() == "nan": scale = ""

        answer = row.get("answer_content", "")
        if str(answer).lower() == "nan": answer = ""

        indicator = row.get("indicator_name", "")
        if str(indicator).lower() == "nan": indicator = ""

        grouped[var_code]["questions"].append({
            "indicator_name": indicator,
            "question": question,
            "question_code": q_code,
            "answer_content": answer,
            "scale_type": scale
        })

    return list(grouped.values())


def get_survey_questions_table(state: dict, section_count: int, table_count: int) -> str:
    language_dict = LANGUAGE_KIT.get(state["language"].lower(), LANGUAGE_KIT["tiếng việt"])
    sub_tab_count = 0
    survey_questions_table = ""
    survey_questions_list: list[dict] = []

    font = """<p style="
        font-family:'Times New Roman', serif;
        font-size:13pt;
        color:#000;
        line-height:1.5;
        text-align:justify;text-indent:1cm;margin:6px 0;">
        """

    # Normalize to nested structure (handles both flat and nested formats)
    surveys = _normalize_survey_questions(state["proposal"]["survey_questions"])

    survey_questions_table += f"{font}{language_dict['propose_method_survey_list']} {language_dict['propose_method_survey_variables_table']}</p>"
    sources: list[str] = []
    for survey in surveys:
        source = survey.get("source", "")
        if source:
            sources.append(source[1:-1])
        key = f"{font}{language_dict['propose_method_name']}: {survey['variable_name']} ({survey['variable_code']}) - {language_dict['propose_method_variable_type']}: {survey['variable_type']} - {language_dict['propose_method_measurement_type']}: {language_dict.get(survey['measurement_type'].lower(), survey['measurement_type'])}</p>"
        variables_list: list[dict] = []
        for question_count, question in enumerate(survey["questions"]):
            variables_list.append(
                {
                    language_dict["No."]: question_count + 1,
                    language_dict["propose_method_question"]: question["question"],
                    language_dict["propose_method_question_code"]: question["question_code"],
                    language_dict["propose_method_scale"]: question["scale_type"],
                    language_dict["propose_method_source"]: source,
                }
            )
        if variables_list:
            df_questions = pd.DataFrame(variables_list)
            questions_html = format_table(df_questions)
            sub_tab_count += 1
            survey_questions_table += f'{key}<div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Table"]} {section_count + 1}.{table_count + sub_tab_count}.</b> {survey["variable_name"].title()}</div>{questions_html}'

    survey_questions_table += f"{font}{language_dict['propose_method_survey_list']} {language_dict['propose_method_survey_questions_table']}</p>"
    for survey in surveys:
        key = f"{font}{language_dict['propose_method_name']}: {survey['variable_name']} ({survey['variable_code']}) - {language_dict['propose_method_variable_type']}: {survey['variable_type']} - {language_dict['propose_method_measurement_type']}: {language_dict.get(survey['measurement_type'].lower(), survey['measurement_type'])}</p>"
        survey_questions_list: list[dict] = []
        for question_count, question in enumerate(survey["questions"]):
            survey_questions_list.append(
                {
                    language_dict["No."]: question_count + 1,
                    language_dict["propose_method_question"]: question["question"],
                    language_dict["propose_method_question_code"]: question["question_code"],
                    language_dict["propose_method_answer_content"]: question["answer_content"],
                    language_dict["propose_method_scale"]: question["scale_type"],
                }
            )
        if survey_questions_list:
            df_questions = pd.DataFrame(survey_questions_list)
            questions_html = format_table(df_questions)
            sub_tab_count += 1
            survey_questions_table += f'{key}<div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Table"]} {section_count + 1}.{table_count + sub_tab_count}.</b> {survey["variable_name"].title()}</div>{questions_html}'
    survey_questions_table, state["existing_refs"] = edit_refs(
        state["research_papers"],
        survey_questions_table,
        sources,
        state["existing_refs"],
        state["references_style"] if "references_style" in state else state["proposal"].get("references_style", "IEEE")
    )

    return survey_questions_table, state["existing_refs"]


def get_used_files(
    replacements: dict[str, str],
    content: str,
    used_files: list[str],
    current_tab_count: int,
    current_fig_count: int,
    language: str,
    section_num: int = 0,
    file_descriptions: dict | None = None,
) -> tuple[str, list, int, int]:
    language_dict = LANGUAGE_KIT.get(language.lower(), LANGUAGE_KIT["tiếng việt"])
    current_files: dict[str, str] = {}
    for key, replacement in replacements.items():
        if key in content:
            if key not in set(used_files):
                current_files[key] = replacement
                used_files.append(key)

    parse_files = ""
    tab_count = 0
    fig_count = 0
    for key, replacement in current_files.items():
        if key.endswith('.csv'):
            tab_count += 1
            table_title = get_generated_file_title(key, file_descriptions)
            if replacement.startswith("Table not available:"):
                parse_files += f"*{language_dict['Table']} {table_title}: output unavailable.*\n"
                continue
            if section_num:
                parse_files += f'**{language_dict["Table"]} {section_num}.{current_tab_count + tab_count}.** {table_title}\n\n{replacement.strip()}\n'
            else:
                parse_files += f'**{language_dict["Table"]} {current_tab_count + tab_count}.** {table_title}\n\n{replacement.strip()}\n'
        elif key.endswith('.png'):
            fig_count += 1
            image_title = get_generated_file_title(key, file_descriptions)
            if replacement.startswith("Image not available:"):
                parse_files += f"*{language_dict['Figure']} {image_title}: image unavailable.*\n"
                continue
            if section_num:
                parse_files += f'![{image_title}]({replacement})\n\n*{language_dict["Figure"]} {section_num}.{current_fig_count + fig_count}. {image_title}*\n'
            else:
                parse_files += f'![{image_title}]({replacement})\n\n*{language_dict["Figure"]} {current_fig_count + fig_count}. {image_title}*\n'

    return parse_files.replace(r'\n', '\n'), used_files, tab_count, fig_count


def get_generated_file_title(file_key: str, file_descriptions: dict | None = None) -> str:
    """Return one stable, human-readable title for a generated artifact."""
    metadata = (file_descriptions or {}).get(file_key, {})
    if isinstance(metadata, dict):
        description = metadata.get("description", "")
    else:
        description = str(metadata or "")
    description = " ".join(description.split())
    if description and not description.lower().startswith("unknown file pattern:"):
        return escape(description)
    fallback = file_key.rsplit("/", 1)[-1]
    return escape(fallback.rsplit(".", 1)[0].replace("_", " ").title())


def renumber_table_captions(
    content: str, section_num: int, language: str, start: int = 0
) -> tuple[str, int]:
    """Renumber Markdown table captions after parallel subsection merge."""
    table_label = LANGUAGE_KIT.get(language.lower(), LANGUAGE_KIT["tiếng việt"])["Table"]
    pattern = re.compile(
        rf"(?m)^(\*\*{re.escape(table_label)}\s+){section_num}\.\d+(\.\*\*.*)$"
    )
    count = start

    def replace(match: re.Match) -> str:
        nonlocal count
        count += 1
        return f"{match.group(1)}{section_num}.{count}{match.group(2)}"

    return pattern.sub(replace, content), count


def count_table_captions(content: str, section_num: int, language: str) -> int:
    table_label = LANGUAGE_KIT.get(language.lower(), LANGUAGE_KIT["tiếng việt"])["Table"]
    pattern = re.compile(
        rf"(?m)^\*\*{re.escape(table_label)}\s+{section_num}\.\d+\.\*\*"
    )
    return len(pattern.findall(content))


# async def match_files_with_llm(
#     file_items: list,
#     available_files: dict[str, str],
#     llm: ChatGoogleGenerativeAI | ChatOpenAI,
#     raw_logs: list[str] = None,
#     draft_context: str = "",
#     file_descriptions: dict[str, dict[str, str]] = None
# ) -> tuple[dict[str, str], int, int]:
#     """
#     Use LLM to match placeholder descriptions to file keys.

#     Args:
#         file_items: List of DraftItem objects with type "table" or "image"
#         available_files: Dict mapping filename keys to HTML/URL
#         llm: Language model to use for matching
#         raw_logs: Optional list of raw analysis logs containing context about the files
#         draft_context: Full draft outline context for better matching
#         file_descriptions: Dict mapping filename keys to their descriptions

#     Returns:
#         Tuple of (mapping dict, input_tokens, output_tokens)
#         mapping dict: {description: matched_key or None}
#     """

#     # Prepare content for LLM
#     available_keys = list(available_files.keys())

#     # Build file context with descriptions
#     if file_descriptions:
#         files_with_descriptions = []
#         for key in available_keys:
#             if key in file_descriptions and 'description' in file_descriptions[key]:
#                 desc = file_descriptions[key]['description']
#                 files_with_descriptions.append(f'"{key}": "{desc}"')
#             else:
#                 files_with_descriptions.append(f'"{key}": "No description available"')
#         files_context = "\n    ".join(files_with_descriptions)
#     else:
#         files_context = "\n    ".join([f'"{key}"' for key in available_keys])

#     # Build log context - extract logs that mention any of the available files
#     log_context = ""
#     if raw_logs:
#         relevant_logs = []
#         for log in raw_logs:
#             # Check if this log mentions any of the available files
#             if any(key in log for key in available_keys):
#                 relevant_logs.append(log)

#         if relevant_logs:
#             log_context = f"""
#     Raw analysis logs (showing context around file generation):
#     {chr(10).join(relevant_logs)}
#     """

#     # Build draft context section
#     draft_section = f"""
#     Full draft outline structure:
#     {draft_context}
#     """ if draft_context else ""

#     content = build_file_matching_content(draft_section, log_context, files_context)

#     # Create dynamic Enum from available keys for better LLM constraint
#     if available_keys:
#         FileKeyEnum = Enum('FileKeyEnum', {k: k for k in available_keys})

#         class FileMappingItem(BaseModel):
#             placeholder_description: str = Field(
#                 description="The original placeholder description from the draft"
#             )
#             matched_key: FileKeyEnum | None = Field(  # type: ignore[valid-type]
#                 description=f"The matched filename key, must be one of: {available_keys}"
#             )
#     else:
#         class FileMappingItem(BaseModel):
#             placeholder_description: str = Field(
#                 description="The original placeholder description from the draft"
#             )
#             matched_key: str | None = Field(
#                 description="The matched filename key from generated_files, or None if no match found"
#             )

#     class FileMapping(BaseModel):
#         mappings: list[FileMappingItem] = Field(
#             description="List of mappings from placeholder descriptions to actual file keys"
#         )

#     error, success, file_mapping, input_tokens, output_tokens = await get_answer_with_schema(
#         llm,
#         file_matching_instructions,
#         content,
#         FileMapping
#     )

#     if not success:
#         logger.warning(f"LLM file matching failed: {error}")
#         return {}, input_tokens, output_tokens

#     # Convert to simple dict (handle enum values)
#     mapping_dict = {}
#     for mapping_item in file_mapping.mappings:
#         key = mapping_item.matched_key
#         # Extract string value if it's an enum
#         if key is not None and hasattr(key, 'value'):
#             key = key.value
#         mapping_dict[mapping_item.placeholder_description] = key

#     return mapping_dict, input_tokens, output_tokens


def summarize_raw_logs_with_files(raw_logs: list[str], generated_files: dict, file_descriptions: dict) -> list[dict]:
    """
    Summarize raw logs by extracting file keys and their descriptions.

    Args:
        raw_logs: List of raw log strings (typically pairs of logs)
        generated_files: Dictionary of generated files {filename: file_info}
        file_descriptions: Dictionary of file descriptions {filename: description}

    Returns:
        List of log summaries with file information
    """
    summaries = []
    file_keys = set(generated_files.keys())

    for idx in range(0, len(raw_logs), 2):
        if idx + 1 < len(raw_logs):
            log_pair = [raw_logs[idx], raw_logs[idx + 1]]
        else:
            log_pair = [raw_logs[idx]]

        # Find all file keys mentioned in this log pair
        mentioned_files = []
        combined_log = '\n'.join(log_pair)

        for file_key in file_keys:
            if file_key in combined_log:
                file_desc = file_descriptions.get(file_key, "No description available")
                mentioned_files.append({
                    "file_key": file_key,
                    "description": file_desc
                })

        # Extract first few lines of the log as a brief summary
        first_log = log_pair[0] if log_pair else ""
        lines = first_log.split('\n')
        brief_summary = '\n'.join(lines[:5]) if len(lines) > 5 else first_log

        summaries.append({
            "log_index": idx // 2,
            "brief_summary": brief_summary,
            "files": mentioned_files,
            "file_count": len(mentioned_files)
        })

    return summaries


async def _assign_logs_for_subsection(
    state: dict,
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
    subsection_idx: int,
    subsection,
    log_summaries_str: str,
    num_logs: int,
) -> tuple[SubsectionLogAssignment, int, int]:
    """
    Assign analysis logs to a single subsection with a condensed prompt.

    The prompt only carries the target subsection's full details plus the
    headings of its siblings (for boundary awareness), so the LLM can focus
    on one matching decision at a time.
    """
    subsection_str = (
        f"Subsection {subsection_idx}: \"{subsection.subheading}\"\n"
        f"  Description: {subsection.detail_description}\n"
        f"  Word count: {subsection.subheading_word_count}"
    )
    other_subsections_str = "\n".join([
        f"Subsection {idx}: \"{other.subheading}\""
        for idx, other in enumerate(state['section'].subsections)
        if idx != subsection_idx
    ]) or "None"

    assignment_content = build_subsection_log_assignment_content(
        state,
        subsection_str,
        other_subsections_str,
        log_summaries_str,
    )

    retry = 0
    assignment = None
    total_input_tokens, total_output_tokens = 0, 0
    while retry < 3 and not assignment:
        retry += 1
        try:
            error, success, assignment, inp_tok, out_tok = await get_answer_with_schema(
                state['document_id'][:8],
                llm,
                assign_logs_to_subsection,
                assignment_content,
                SubsectionLogAssignment
            )
            if success:
                total_input_tokens += inp_tok
                total_output_tokens += out_tok
            else:
                logger.warning(f"[{state['document_id'][:8]}] Log assignment attempt {retry} for subsection {subsection_idx} failed: {error}")
                assignment = None
        except Exception as e:
            logger.warning(f"[{state['document_id'][:8]}] Log assignment exception on attempt {retry} for subsection {subsection_idx}: {e}")
            assignment = None

    if not assignment:
        logger.error(f"[{state['document_id'][:8]}] Log assignment for subsection {subsection_idx} failed after 3 attempts, using fallback (all logs)")
        assignment = SubsectionLogAssignment(
            subsection_index=subsection_idx,
            chosen_logs=list(range(num_logs)),
            reasoning="Fallback: using all logs due to assignment failure"
        )

    # Trust our own index over the LLM's, and keep chosen logs valid
    assignment.subsection_index = subsection_idx
    assignment.chosen_logs = [
        log_idx for log_idx in (assignment.chosen_logs or [])
        if 0 <= log_idx < num_logs
    ]
    return assignment, total_input_tokens, total_output_tokens


async def assign_section_logs(
    state: dict,
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
) -> tuple[dict[int, list[int]], int, int]:
    """
    Assign analysis logs to every subsection in a section, one LLM call per
    subsection (run concurrently) so each decision has a condensed context.

    Args:
        state: State dictionary containing section, subsections, detailed_logs, etc.
        llm: Language model to use for assignment

    Returns:
        Tuple of:
        - Dictionary mapping subsection index to list of chosen log indices
        - Input tokens used
        - Output tokens used
    """
    logger.info(f"[{state['document_id'][:8]}] Assigning logs for section {state['section'].heading} with {len(state['section'].subsections)} subsections")

    # Create log summaries
    log_summaries = summarize_raw_logs_with_files(
        state["detailed_logs"],
        state.get('generated_files', {}),
        state.get('file_descriptions', {})
    )

    # Format log summaries for the prompt (shared by every subsection call)
    log_summary_lines = []
    for log in log_summaries:
        files_text = "\n".join([f"    - {f['file_key']}: {f['description']}" for f in log["files"]])
        log_summary_lines.append(
            f"""Log {log['log_index']}:
            Brief summary: {log['brief_summary']}
            Files ({log['file_count']}):
            {files_text}"""
        )
    log_summaries_str = "\n\n".join(log_summary_lines)

    # One condensed assignment call per subsection, run concurrently
    tasks = [
        _assign_logs_for_subsection(state, llm, idx, subsection, log_summaries_str, len(log_summaries))
        for idx, subsection in enumerate(state['section'].subsections)
    ]
    results = await asyncio.gather(*tasks)

    assignment_dict: dict[int, list[int]] = {}
    total_input_tokens, total_output_tokens = 0, 0
    for assignment, inp_tok, out_tok in results:
        assignment_dict[assignment.subsection_index] = assignment.chosen_logs
        total_input_tokens += inp_tok
        total_output_tokens += out_tok
        logger.info(f"[{state['document_id'][:8]}] Subsection {assignment.subsection_index} assigned logs {assignment.chosen_logs}: {assignment.reasoning}")

    return assignment_dict, total_input_tokens, total_output_tokens


def parse_raw_logs_into_blocks(raw_logs: list[str], generated_files: dict) -> list[LogContentBlock]:
    """
    Parse raw analysis logs into a structured array of text blocks and file references.
    Handles overlapping file names and prevents redundant file blocks for the same file.

    Args:
        raw_logs: List of raw log strings
        generated_files: Dictionary of generated files {filename: file_info}

    Returns:
        List of LogContentBlock objects maintaining order from original logs
    """
    blocks: list[LogContentBlock] = []
    index = 0

    # Sort file keys by length descending to handle substrings correctly
    file_keys = sorted(list(generated_files.keys()), key=len, reverse=True)

    # Track files that have already been converted to 'file' blocks in this subsection
    # We only want to create one 'file' block per unique file to keep the array length manageable
    processed_file_blocks = set()

    for log in raw_logs:
        if not log or not log.strip():
            continue

        lines = log.split('\n')
        current_text_buffer = []

        for line in lines:
            if not line.strip():
                continue

            line_file_matches = []

            for file_key in file_keys:
                start = 0
                while True:
                    idx_match = line.find(file_key, start)
                    if idx_match == -1:
                        break

                    is_covered = False
                    for existing_start, existing_end, _ in line_file_matches:
                        if (idx_match >= existing_start and idx_match < existing_end) or \
                           (idx_match + len(file_key) > existing_start and idx_match + len(file_key) <= existing_end):
                            is_covered = True
                            break

                    if not is_covered:
                        # Only create a separate block if we haven't processed this file yet
                        if file_key not in processed_file_blocks:
                            line_file_matches.append((idx_match, idx_match + len(file_key), file_key))
                            processed_file_blocks.add(file_key)

                    start = idx_match + 1

            line_file_matches.sort()

            if line_file_matches:
                last_pos = 0
                for start, end, file_key in line_file_matches:
                    text_before = line[last_pos:start]
                    if text_before.strip():
                        current_text_buffer.append(text_before)

                    if current_text_buffer:
                        text_content = '\n'.join(current_text_buffer).strip()
                        if text_content:
                            blocks.append(LogContentBlock(index=index, type="text", content=text_content, file=""))
                            index += 1
                        current_text_buffer = []

                    blocks.append(LogContentBlock(index=index, type="file", content="", file=file_key))
                    index += 1
                    last_pos = end

                text_after = line[last_pos:]
                if text_after.strip():
                    current_text_buffer.append(text_after)
            else:
                current_text_buffer.append(line)

        if current_text_buffer:
            text_content = '\n'.join(current_text_buffer).strip()
            if text_content:
                blocks.append(LogContentBlock(index=index, type="text", content=text_content, file=""))
                index += 1

    # Post-process: Merge consecutive text blocks
    if not blocks:
        return []

    merged_blocks: list[LogContentBlock] = []
    current_block = blocks[0]

    for i in range(1, len(blocks)):
        next_block = blocks[i]
        if current_block.type == "text" and next_block.type == "text":
            current_block.content += "\n\n" + next_block.content
        else:
            merged_blocks.append(current_block)
            current_block = next_block

    merged_blocks.append(current_block)

    # Re-index blocks
    for i, block in enumerate(merged_blocks):
        block.index = i

    return merged_blocks


async def write_data(
    state: dict,
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
    write_prompt: str,
    data: str = "",
    chosen_logs: list[int] = None,
    log_cleaning_config: LogCleaningConfig = None,
) -> tuple[SectionContentWithTokenCount, int, int]:
    """
    Write a data analysis subsection by choosing relevant content blocks and
    writing free-form academic prose from them.

    Args:
        state: State dictionary with section, subsection, logs, files, etc.
        llm: Language model to use
        write_prompt: Writing prompt used for the prose-writing phase
        data: Optional additional data
        chosen_logs: Optional pre-selected log indices. If provided, skips Phase 1.
        log_cleaning_config: Configuration for log preprocessing. If None, uses safe defaults.

    Phases:
        Phase 1: Select relevant logs (skipped if chosen_logs provided)
        Phase 1.5: Preprocess logs to remove markers and headers
        Phase 2: Parse logs into structured blocks (text and files)
        Phase 3: Choose which blocks the subsection needs (list of indices)
        Phase 4: Write the subsection prose from the chosen blocks
        Phase 5: Insert chosen files at the writer's placeholders
        Phase 6: Post-process with tidy_up
    """

    # Initialize log cleaner with default config if not provided
    if log_cleaning_config is None:
        log_cleaning_config = LogCleaningConfig()  # Use safe defaults

    log_cleaner = LogCleaner(log_cleaning_config)

    logger.info(f"[{state['document_id'][:8]}] Writing section {state['section'].heading} - {state['subsection'].subheading} with {state['subsection'].subheading_word_count} word")
    logger.info(f"[{state['document_id'][:8]}] Available key files: {list(state.get('generated_files', {}).keys())}")
    logger.info(f"[{state['document_id'][:8]}] Log cleaning config: remove_statistical_markers={log_cleaning_config.remove_statistical_markers}, strip_markdown_headers={log_cleaning_config.strip_markdown_headers}")

    web_search_content, web_search_call, total_input_tokens, total_output_tokens = await search_web(state)
    extra_content = f"Up-to-date information for the subsection:\n{web_search_content}" if web_search_content else ""
    extra_data = f"\nRelevant data:\n{data}" if data else ""

    # ===== PHASE 1: Select Relevant Logs (Optional) =====
    if chosen_logs is not None:
        # Use pre-selected logs
        logger.info(f"[{state['document_id'][:8]}] Using pre-assigned logs {chosen_logs} for subsection")
        log_indices = chosen_logs
    else:
        # Perform log selection
        logger.info(f"[{state['document_id'][:8]}] No pre-assigned logs, performing log selection")
        detail_log_items = [
            f"Log {(math.floor(i / 2))}:\n\n{detailed_log}\n\n"
            for i, detailed_log in enumerate(state["detailed_logs"])
            if i % 2 == 1
        ]
        content = f"{detail_log_items}"
        error, success, logs, input_tokens, output_tokens = await get_answer_with_schema(
            state['document_id'][:8],
            llm,
            select_detail_logs,
            content,
            ChosenLogs
        )
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens

        if not success:
            raise error

        log_indices = logs.chosen_logs
        logger.info(f"[{state['document_id'][:8]}] Selected logs {log_indices} for section {state['section'].heading} - {state['subsection'].subheading}")

    # Build selected raw logs list
    selected_raw_logs: list[str] = []
    if len(log_indices):
        try:
            for log in log_indices:
                if "ERROR" not in state['detailed_logs'][log * 2].split("\n\n")[6]:
                    selected_raw_logs.append(state['detailed_logs'][log * 2])
                    selected_raw_logs.append(state['detailed_logs'][log * 2 + 1])
        except Exception as e:
            logger.warning(f"[{state['document_id'][:8]}] Error building detailed logs: {e}")
            selected_raw_logs = []

    if not selected_raw_logs:
        logger.warning(f"[{state['document_id'][:8]}] No logs selected or available, fallback to normal write")
        return await write(state, llm, section_writer_without_seminar_instructions, user_content=data), 0, 0

    # ===== PHASE 1.5: Preprocess Logs (NEW) =====
    logger.info(f"[{state['document_id'][:8]}] Preprocessing {len(selected_raw_logs)} logs with cleaning config")
    cleaned_logs = []
    for idx, raw_log in enumerate(selected_raw_logs):
        cleaned = log_cleaner.clean_log(raw_log)
        cleaned_logs.append(cleaned)

        # Warn if cleaning removed significant amount of content
        if len(cleaned) < len(raw_log) * 0.5:
            logger.warning(f"[{state['document_id'][:8]}] Log {idx} cleaned significantly: {len(raw_log)} -> {len(cleaned)} chars")

    selected_raw_logs = cleaned_logs
    logger.info(f"[{state['document_id'][:8]}] Log preprocessing complete")

    # ===== PHASE 2: Parse Logs into Structured Blocks =====
    content_blocks = parse_raw_logs_into_blocks(selected_raw_logs, state.get('generated_files', {}))
    logger.info(f"[{state['document_id'][:8]}] Parsed {len(content_blocks)} content blocks ({sum(1 for b in content_blocks if b.type == 'text')} text, {sum(1 for b in content_blocks if b.type == 'file')} files)")

    # ===== PHASE 3: Choose Relevant Blocks for the Subsection =====
    blocks_summary = "\n".join([
        f"Block {block.index} ({block.type}): {block.content[:300] if block.type == 'text' else f'File: {block.file}'}"
        for block in content_blocks
    ])
    selection_content = build_block_selection_content(state, blocks_summary)

    retry = 0
    chosen_blocks = None
    while retry < 3 and not chosen_blocks:
        retry += 1
        try:
            error, success, chosen_blocks, inp_tok, out_tok = await get_answer_with_schema(
                state['document_id'][:8],
                llm,
                select_content_blocks,
                selection_content,
                ChosenBlocks
            )
            total_input_tokens += inp_tok
            total_output_tokens += out_tok
            if not success:
                logger.warning(f"[{state['document_id'][:8]}] Block selection attempt {retry} failed: {error}")
                chosen_blocks = None
        except Exception as e:
            logger.warning(f"[{state['document_id'][:8]}] Block selection exception on attempt {retry}: {e}")
            chosen_blocks = None

    if chosen_blocks is None:
        logger.error(f"[{state['document_id'][:8]}] Block selection failed after 3 attempts")
        chosen_indices = [block.index for block in content_blocks]
    else:
        valid_indices = {block.index for block in content_blocks}
        chosen_indices = sorted({idx for idx in chosen_blocks.chosen_blocks if idx in valid_indices})
        logger.info(f"[{state['document_id'][:8]}] Chosen blocks {chosen_indices} for subsection {state['subsection'].subheading}: {chosen_blocks.reasoning}")

    # The analysis logs have already been assigned to this subsection. Do not
    # let the block selector keep findings while silently dropping their files.
    chosen_indices = sorted(set(chosen_indices) | {
        block.index for block in content_blocks
        if block.type == "file" and block.file in state.get("generated_files", {})
        and block.file not in state["used_files"]
    })

    if not chosen_indices:
        logger.warning(f"[{state['document_id'][:8]}] No content blocks chosen for {state['subsection'].subheading}, fallback to normal write")
        return await write(state, llm, section_writer_without_seminar_instructions, user_content=data), 0, 0

    chosen_set = set(chosen_indices)
    selected_blocks = [block for block in content_blocks if block.index in chosen_set]

    # ===== PHASE 4: Write the Subsection from the Chosen Blocks =====
    current_report = f"Other sections of the report:\n{state['current_report']}" if len(state["current_report"]) else ""

    selected_text_blocks = [block.content for block in selected_blocks if block.type == "text"]
    selected_file_keys: list[str] = []
    for block in selected_blocks:
        if block.type == "file" and block.file:
            if block.file not in state["generated_files"]:
                logger.warning(f"[{state['document_id'][:8]}] File '{block.file}' not found in generated_files")
            elif block.file in state["used_files"] or block.file in selected_file_keys:
                logger.warning(f"[{state['document_id'][:8]}] Skipping duplicate file: '{block.file}' (already used)")
            else:
                selected_file_keys.append(block.file)

    selected_blocks_text = "\n\n---\n\n".join(selected_text_blocks)
    file_descriptions = state.get("file_descriptions", {})
    selected_files_str = "\n".join(
        f"- {file_key}: {get_generated_file_title(file_key, file_descriptions)}"
        for file_key in selected_file_keys
    ) or "None"

    writing_content = build_block_writing_content(
        state,
        selected_blocks_text,
        selected_files_str,
        current_report,
        extra_content,
        extra_data,
    )

    retry = 0
    section = None
    while retry < 3 and not section:
        retry += 1
        try:
            error, success, section, inp_tok, out_tok = await get_answer_with_schema(
                state['document_id'][:8],
                llm,
                write_prompt,
                writing_content,
                SectionContent
            )
            total_input_tokens += inp_tok
            total_output_tokens += out_tok
            if not success:
                logger.warning(f"[{state['document_id'][:8]}] Subsection writing attempt {retry} failed: {error}")
                section = None
            elif not section.content.strip():
                logger.warning(f"[{state['document_id'][:8]}] Subsection writing attempt {retry} returned empty content")
                section = None
        except Exception as e:
            logger.warning(f"[{state['document_id'][:8]}] Subsection writing exception on attempt {retry}: {e}")
            section = None

    if not section:
        logger.error(f"[{state['document_id'][:8]}] Subsection writing failed after 3 attempts")
        logger.error(f"[{state['document_id'][:8]}] Skipping subsection to prevent raw content leakage")

        # Return empty content with informative message instead of using raw content
        return SectionContentWithTokenCount(
            content=f"## {state['section'].heading}\n\n*Content generation failed for this subsection. Please review the analysis logs manually.*\n\n",
            ref=[],
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            embed_tokens=0,
            web_search_call=web_search_call,
        ), 0, 0

    def files_missing_interpretation(content: str) -> list[str]:
        missing = []
        for key in selected_file_keys:
            if content.count(key) != 1:
                missing.append(key)
                continue
            following = content.split(key, 1)[1].lstrip()
            paragraph = following.split("\n\n", 1)[0].strip()
            if not paragraph or paragraph.startswith(("#", *selected_file_keys)):
                missing.append(key)
        return missing

    missing_files = files_missing_interpretation(section.content)
    if missing_files:
        repair_content = f"""{writing_content}

The previous draft omitted or failed to interpret these required generated files: {missing_files}
Rewrite the complete subsection. Put each listed exact file key once on its own line and immediately follow it with a distinct interpretation paragraph grounded only in the supplied logs. Do not invent table/figure numbers.
Previous draft:
{section.content}"""
        try:
            error, success, repaired_section, inp_tok, out_tok = await get_answer_with_schema(
                state['document_id'][:8], llm, write_prompt, repair_content, SectionContent
            )
            total_input_tokens += inp_tok
            total_output_tokens += out_tok
            if success and not files_missing_interpretation(repaired_section.content):
                section = repaired_section
            else:
                logger.warning(f"[{state['document_id'][:8]}] Artifact interpretation repair did not satisfy required file keys: {missing_files}")
        except Exception as e:
            logger.warning(f"[{state['document_id'][:8]}] Artifact interpretation repair failed: {e}")

    # ===== PHASE 5: Insert Chosen Files at the Writer's Placeholders =====
    final_content = section.content
    used_file_keys = []
    tab_count = 0
    fig_count = 0

    logger.info(f"[{state['document_id'][:8]}] Starting file insertion phase")
    logger.info(f"[{state['document_id'][:8]}] Files already used globally: {state['used_files']}")

    for file_key in selected_file_keys:
        # Format this file as a captioned table/figure
        file_html, _, t_count, f_count = get_used_files(
            replacements={file_key: state["generated_files"][file_key]},
            content=file_key,
            used_files=state["used_files"] + used_file_keys,
            current_tab_count=state["tab_count"] + tab_count,
            current_fig_count=state["fig_count"] + fig_count,
            language=state["language"],
            section_num=state["section_num"] + 1,
            file_descriptions=state.get("file_descriptions", {}),
        )

        if not file_html.strip():
            logger.warning(f"[{state['document_id'][:8]}] Failed to insert file '{file_key}' (empty content returned)")
            continue

        if file_key in final_content:
            # Replace the writer's placeholder where the narrative discusses the file,
            # stripping any stray repetitions of the raw key from the remainder
            before, _, after = final_content.partition(file_key)
            after = after.replace(file_key, "")
            final_content = f"{before}\n\n{file_html}\n\n{after}"
        else:
            # Writer did not place it; append so the chosen evidence is not lost
            logger.warning(f"[{state['document_id'][:8]}] File '{file_key}' was not placed by the writer, appending at the end")
            final_content += f"\n\n{file_html}\n\n"

        used_file_keys.append(file_key)
        tab_count += t_count
        fig_count += f_count
        logger.info(f"[{state['document_id'][:8]}] Inserted file '{file_key}' (tab_count={tab_count}, fig_count={fig_count})")

    if not final_content.strip():
        logger.warning(f"[{state['document_id'][:8]}] Writing for {state['subsection'].subheading} resulted in empty content. Fallback to normal write.")
        return await write(state, llm, section_writer_without_seminar_instructions, user_content=data), 0, 0



    # ===== PHASE 6: Post-Processing =====
    titles = [paper.title for paper in state["subsection"].refs]
    section.content = final_content
    section.ref = title_matching(section.ref, titles)

    try:
        reorder_ref_content, reorder_ref, total_input_tokens, total_output_tokens = await tidy_up(
            section,
            state,
            total_input_tokens,
            total_output_tokens,
            llm
        )

        # Enhanced character cleanup - removes statistical markers and normalizes whitespace
        reorder_ref_content = _clean_text_enhanced(reorder_ref_content)

        # Check for invalid characters
        if r"\x" in rf"{reorder_ref_content}":
            logger.warning(f"[{state['document_id'][:8]}] Invalid characters detected in final content, using pre-tidy version")
            final_result_content = final_content
            final_result_ref = section.ref
        else:
            final_result_content = reorder_ref_content
            final_result_ref = reorder_ref

    except Exception as e:
        logger.warning(f"[{state['document_id'][:8]}] Post-processing failed: {e}, using untidied version")
        final_result_content = final_content
        final_result_ref = section.ref

    # Return final result
    section_with_token_count = SectionContentWithTokenCount(
        content=final_result_content,
        ref=final_result_ref,
        input_tokens=total_input_tokens,
        output_tokens=total_output_tokens,
        embed_tokens=0,
        web_search_call=web_search_call,
    )

    # Update state tracking
    state["used_files"] = state.get("used_files", []) + used_file_keys
    logger.info(f"[{state['document_id'][:8]}] Completed data writing: {len(selected_blocks)} blocks used ({len(selected_text_blocks)} text, {len(used_file_keys)} files inserted), {tab_count} tables, {fig_count} figures")

    return section_with_token_count, tab_count, fig_count


async def write_method(
    state: dict,
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
    write_prompt: str,
) -> tuple[SectionContentWithTokenCount, int, int]:
    """Worker writes a section of the report based on given information"""
    web_search_content, web_search_call, total_input_tokens, total_output_tokens = await search_web(state)
    extra_content = f"Up-to-date information for the subsection:\n{web_search_content}" if web_search_content else ""
    method_log = [
        f"Final model - log 0: {state['proposal'].get('final_model', '')}",
        f"Hypotheses - log 1: {state['proposal'].get('hypothesis', '')}",
        f"Variables - log 2: {state['proposal'].get('variables', '')}",
        f"Survey questions - log 3: {state['proposal'].get('survey_questions', '')}",
        f"Final questions - log 4: {state['proposal'].get('questions', '')}",
    ]
    content = build_method_log_selection_content(method_log)
    error, success, logs, input_tokens, output_tokens = await get_answer_with_schema(
        state['document_id'][:8],
        llm,
        select_methodology_logs,
        content,
        ChosenLogs
    )
    if not success:
        if not success:
            raise error
        logs = ChosenLogs(chosen_logs=[])
    logger.info(f"[{state['document_id'][:8]}] Logs {logs} for section {state['section'].heading} - {state['subsection'].subheading}")
    total_input_tokens += input_tokens
    total_output_tokens += output_tokens
    if len(logs.chosen_logs):
        try:
            selected_method_logs = "\n".join([method_log[log] for log in logs.chosen_logs])
            detailed_logs = f"Focus on these information:\n{selected_method_logs}"
        except Exception:
            detailed_logs = ""
    else:
        detailed_logs = ""

    current_report = f"Other sections of the report:\n{state['current_report']}" if len(state["current_report"]) else ""
    if 0 in logs.chosen_logs:
        detailed_logs = detailed_logs + "Only analyze and describe the graph, do not insert the mermaid code as it has been inserted in another section"
        if settings.MINIO_DOMAIN in state["proposal"]["final_model"]["sketch"]:
            img_link = state["proposal"]["final_model"]["sketch"].split("link: ")[-1]
            if img_link not in current_report:
                language_dict = LANGUAGE_KIT.get(state["language"].lower(), LANGUAGE_KIT["tiếng việt"])
                parse_files = f'<img alt="Relationship graph" src="{img_link}" width="90%" style="display:block;margin:auto"/><div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Figure"]} {state["section_num"] + 1}.</b> {language_dict["Relationship Graph"]}</div>\n'
            else:
                parse_files = ""
        else:
            parse_files = ""
    else:
        parse_files = ""
    logger.info(f"[{state['document_id'][:8]}] Writing section {state['section'].heading} - {state['subsection'].subheading} with {state['subsection'].subheading_word_count} word")
    retry = 0
    last_success = SectionContentWithTokenCount(
        content=f"Error writing section {state['section'].heading} - {state['subsection'].subheading}",
        ref=[],
        input_tokens=total_input_tokens,
        output_tokens=total_output_tokens,
        embed_tokens=0,
        web_search_call=web_search_call,
    )
    while retry < 3:
        retry += 1
        try:
            content_rewrite = build_method_write_content(
                state,
                current_report,
                detailed_logs,
                extra_content,
            )
            error, success, section, input_tokens, output_tokens = await get_answer_with_schema(
                state['document_id'][:8],
                llm,
                write_prompt,
                content_rewrite,
                SectionContent
            )
            if not success:
                raise error
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            if r"\x" in rf"{section.content}" or not success:
                logger.info(f"[{state['document_id'][:8]}] Fail to generate section {state['section'].heading} - {state['subsection'].subheading}, revert to the previous case")
                last_success.input_tokens = total_input_tokens
                last_success.output_tokens = total_output_tokens
                section_with_token_count = last_success
            else:
                last_success.content = parse_files + section.content[0].lower() + section.content[1:] if parse_files else section.content
                last_success.ref = section.ref
                titles = [paper.title for paper in state["subsection"].refs]
                section.ref = title_matching(section.ref, titles)
                reorder_ref_content, reorder_ref, total_input_tokens, total_output_tokens = await tidy_up(
                    section,
                    state,
                    total_input_tokens,
                    total_output_tokens,
                    llm
                )
                reorder_ref_content = _clean_text(reorder_ref_content)
                last_success.input_tokens = total_input_tokens
                last_success.output_tokens = total_output_tokens
                # logger.info(reorder_ref_content)
                if r"\x" in rf"{reorder_ref_content}":
                    logger.info(f"[{state['document_id'][:8]}] Fail to generate cleaned section {state['section'].heading} - {state['subsection'].subheading}, revert to the previous case")
                    section_with_token_count = last_success
                else:
                    section_with_token_count = SectionContentWithTokenCount(
                        content=parse_files + reorder_ref_content[0].lower() + reorder_ref_content[1:] if parse_files else reorder_ref_content,
                        ref=reorder_ref,
                        input_tokens=total_input_tokens,
                        output_tokens=total_output_tokens,
                        embed_tokens=0,
                        web_search_call=web_search_call,
                    )
                    retry = 3
        except Exception as e:
            section_with_token_count = last_success
            logger.info(f"[{state['document_id'][:8]}] {e}")
            logger.info(traceback.format_exc())
    return section_with_token_count


def reset_or_add(left: int, right: int | Literal["RESET"]):
    if right == "RESET":
        return 0
    return operator.add(left or 0, right or 0)


async def get_appendix_1(state: dict) -> str:
    language_dict = LANGUAGE_KIT.get(state["language"].lower(), LANGUAGE_KIT["tiếng việt"])
    appendix_1_html = f"""<h2 style="
                font-family:'Times New Roman', serif;
                color:#000;
                line-height:1.5;
                text-align:justify;
            margin:16px 0 8px 0;text-align:justify;">{language_dict["APPENDIX 1. LIST OF TABLES, FIGURES, DIAGRAMS"]}</h2>"""
    content: list[str] = []
    try:
        mongo_client = get_mongodb_client()
        db = mongo_client["admin"]
        content_collection = db["outlines"]
        user_report_caches = content_collection.find({"documentId": state["document_id"]}).sort("index", ASCENDING)
        async for doc in user_report_caches:
            if doc["content"]:
                content.append(doc["content"])
            else:
                if doc["contentArr"]:
                    section_content = ""
                    for subsection in doc["contentArr"]:
                        section_content += subsection["text"]
                    content.append(section_content)
                else:
                    break
    except Exception:
        pass
    appendix_1: list[str] = []
    table_with_caption = re.compile(
        r'<div\b(?=[^>]*text-align\s*:\s*center)[^>]*>.*?</div>\s*<table\b[\s\S]*?</table>',
        re.IGNORECASE,
    )
    for section_num, section in enumerate(content):
        if "result_section" in state and section_num >= state["result_section"] - 1:
            break
        for match in table_with_caption.finditer(section):
            table = match.group(0)
            if language_dict["Paper's Usage"] not in table:
                appendix_1.append(table)
    if appendix_1:
        appendix_1_html += "".join(appendix_1)
    return appendix_1_html


async def get_appendix_2(state: dict) -> str:
    language_dict = LANGUAGE_KIT.get(state["language"].lower(), LANGUAGE_KIT["tiếng việt"])
    appendix_2_html = f"""<h2 style="
                font-family:'Times New Roman', serif;
                color:#000;
                line-height:1.5;
                text-align:justify;
            margin:16px 0 8px 0;text-align:justify;">{language_dict["APPENDIX 2. RESEARCH TOOLS (QUESTIONNAIRE / INTERVIEW / SCALE)"]}</h2>"""
    try:
        mongo_client = get_mongodb_client()
        db = mongo_client["admin"]
        article_collection = db["articles"]
        article = await article_collection.find_one({"_id": ObjectId(state["document_id"])})
        proposal_collection = db["proposal_titles"]
        proposal = await proposal_collection.find_one({"_id": ObjectId(article["title"])})
        appendix_2 = ""
        font = """<p style="
        font-family:'Times New Roman', serif;
        font-size:13pt;
        color:#000;
        line-height:1.5;
        text-align:justify;text-indent:1cm;margin:6px 0;">
        """
        for hypothesis in proposal["hypothesis"]:
            appendix_2 += f'{font}{hypothesis["hypothesis_id"]} - {hypothesis["statement"]}</p>'
        variables_list: list[dict] = []
        for variable_count, variable in enumerate(proposal["variables"]):
            variables_list.append(
                {
                    language_dict["No."]: variable_count + 1,
                    language_dict["propose_method_name"]: variable["name"],
                    language_dict["propose_method_variable_type"]: variable["variable_type"],
                    language_dict["propose_method_measurement_type"]: variable["measurement_type"],
                    language_dict["propose_method_description"]: variable["description"],
                    language_dict["propose_method_scale"]: variable["scale"],
                }
            )
        tab_count = 1
        if variables_list:
            df_variable = pd.DataFrame(variables_list)
            variables_html = format_table(df_variable)
            appendix_2 += rf'<div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Table"]} {tab_count}.</b> {language_dict["propose_method_table"]}</div>{variables_html}'
            tab_count += 1
        try:
            questions_list: list[dict] = []
            for question_count, question in enumerate(proposal["questions"]):
                questions_list.append(
                    {
                        language_dict["No."]: question_count + 1,
                        language_dict["propose_method_questions"]: question,
                    }
                )
            if questions_list:
                df_questions = pd.DataFrame(questions_list)
                questions_html = format_table(df_questions)
                appendix_2 += f'<div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Table"]} {tab_count}.</b> {language_dict["propose_method_questions_table"]}</div>{questions_html}'
                tab_count += 1
        except Exception:
            pass
        if appendix_2:
            appendix_2_html += f"""<p style="
                    font-family:'Times New Roman', serif;
                    font-size:13pt;
                    color:#000;
                    line-height:1.5;
                    text-align:justify;
                text-indent:1cm;margin:6px 0;">{appendix_2}</p>"""
            if settings.MINIO_DOMAIN in proposal["finalModel"]["sketch"]:
                img_link = proposal["finalModel"]["sketch"].split("link: ")[-1]
                appendix_2_html += f'<img alt="Relationship graph" src="{img_link}" width="90%"/><div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Figure"]} 1.</b> {language_dict["Relationship Graph"]}</div>'
                appendix_2_html += f"""<p style="
                    font-family:'Times New Roman', serif;
                    font-size:13pt;
                    color:#000;
                    line-height:1.5;
                    text-align:justify;
                text-indent:1cm;margin:6px 0;">{proposal["finalModel"]["thinking_process"]}</p>"""
    except Exception:
        pass
    return appendix_2_html


async def get_appendix_3(state: dict) -> str:
    language_dict = LANGUAGE_KIT.get(state["language"].lower(), LANGUAGE_KIT["tiếng việt"])
    appendix_3_html = f"""<h2 style="
                font-family:'Times New Roman', serif;
                color:#000;
                line-height:1.5;
                text-align:justify;
            margin:16px 0 8px 0;text-align:justify;">{language_dict["APPENDIX 3. LIST OF SCALES AND ORIGINAL REFERENCES"]}</h2>"""
    try:
        mongo_client = get_mongodb_client()
        db = mongo_client["admin"]
        article_collection = db["articles"]
        article = await article_collection.find_one({"_id": ObjectId(state["document_id"])})
        proposal_collection = db["proposal_titles"]
        proposal = await proposal_collection.find_one({"_id": ObjectId(article["title"])})
        appendix_3 = ""
        font = """<p style="
        font-family:'Times New Roman', serif;
        font-size:13pt;
        color:#000;
        line-height:1.5;
        text-align:justify;text-indent:1cm;margin:6px 0;">
        """
        try:
            # Normalize to nested structure (handles both flat and nested formats)
            surveys = _normalize_survey_questions(proposal.get("surveyQuestions", []))

            sub_tab_count = 0
            appendix_3 += f"{font}{language_dict['propose_method_survey_list']} {language_dict['propose_method_survey_variables_table']}</p>"
            sources: list[str] = []
            for survey in surveys:
                key = f"{font}{language_dict['propose_method_name']}: {survey['variable_name']} ({survey['variable_code']}) - {language_dict['propose_method_variable_type']}: {survey['variable_type']} - {language_dict['propose_method_measurement_type']}: {language_dict.get(survey['measurement_type'].lower(), survey['measurement_type'])}</p>"
                variables_list: list[dict] = []
                source = survey.get("source", "")
                if source:
                    sources.append(source[1:-1])
                for question_count, question in enumerate(survey["questions"]):
                    variables_list.append(
                        {
                            language_dict["No."]: question_count + 1,
                            language_dict["propose_method_question"]: question["question"],
                            language_dict["propose_method_question_code"]: question["question_code"],
                            language_dict["propose_method_scale"]: question["scale_type"],
                            language_dict["propose_method_source"]: survey["source"],
                        }
                    )
                if variables_list:
                    df_questions = pd.DataFrame(variables_list)
                    questions_html = format_table(df_questions)
                    sub_tab_count += 1
                    appendix_3 += f'{key}<div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Table"]} {sub_tab_count}.</b> {survey["variable_name"].title()}</div>{questions_html}'
            appendix_3, state["existing_refs"] = edit_refs(
                state["research_papers"],
                appendix_3,
                sources,
                state["existing_refs"],
                state["references_style"] if "references_style" in state else state["proposal"].get("references_style", "IEEE")
            )
            appendix_3 += f"{font}{language_dict['propose_method_survey_list']} {language_dict['propose_method_survey_questions_table']}</p>"
            for survey in surveys:
                key = f"{font}{language_dict['propose_method_name']}: {survey['variable_name']} ({survey['variable_code']}) - {language_dict['propose_method_variable_type']}: {survey['variable_type']} - {language_dict['propose_method_measurement_type']}: {language_dict.get(survey['measurement_type'].lower(), survey['measurement_type'])}</p>"
                survey_questions_list: list[dict] = []
                for question_count, question in enumerate(survey["questions"]):
                    survey_questions_list.append(
                        {
                            language_dict["No."]: question_count + 1,
                            language_dict["propose_method_question"]: question["question"],
                            language_dict["propose_method_question_code"]: question["question_code"],
                            language_dict["propose_method_answer_content"]: question["answer_content"],
                            language_dict["propose_method_scale"]: question["scale_type"],
                        }
                    )
                if survey_questions_list:
                    df_questions = pd.DataFrame(survey_questions_list)
                    questions_html = format_table(df_questions)
                    sub_tab_count += 1
                    appendix_3 += f'{key}<div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Table"]} {sub_tab_count}.</b> {survey["variable_name"].title()}</div>{questions_html}'
        except Exception:
            pass
        if appendix_3:
            appendix_3_html += f"""<p style="
                    font-family:'Times New Roman', serif;
                    font-size:13pt;
                    color:#000;
                    line-height:1.5;
                    text-align:justify;
                text-indent:1cm;margin:6px 0;">{appendix_3}</p>"""
    except Exception:
        pass
    return appendix_3_html


async def get_appendix_4(state: dict) -> str:
    language_dict = LANGUAGE_KIT.get(state["language"].lower(), LANGUAGE_KIT["tiếng việt"])
    appendix_4_html = f"""<h2 style="
                font-family:'Times New Roman', serif;
                color:#000;
                line-height:1.5;
                text-align:justify;
            margin:16px 0 8px 0;text-align:justify;">{language_dict["APPENDIX 4. STATISTICAL TEST RESULTS"]}</h2>"""
    try:
        generated_files = state.get("generated_files", {})
        file_descriptions = state.get("file_descriptions", {})
        if not file_descriptions:
            try:
                file_descriptions = json.loads(generated_files.get("file_descriptions", "{}"))
            except (TypeError, json.JSONDecodeError):
                file_descriptions = {}

        # Appendix 4 is the complete statistical-output appendix, not only a
        # copy of artifacts selected for the chapter body.
        all_files = [
            key for key, value in generated_files.items()
            if key.endswith((".csv", ".png"))
            and not str(value).startswith(("Table not available:", "Image not available:"))
        ]
        emitted_files: set[str] = set()
        tab_count = 0
        fig_count = 0

        def interpretation_for(title: str) -> str:
            for report_section in state.get("other_sections", []):
                lines = report_section.get("content", "").splitlines()
                for index, line in enumerate(lines):
                    if not line.startswith("**") or title.casefold() not in line.casefold():
                        continue
                    cursor = index + 1
                    while cursor < len(lines) and (not lines[cursor].strip() or lines[cursor].lstrip().startswith("|")):
                        cursor += 1
                    paragraph = []
                    while cursor < len(lines) and lines[cursor].strip() and not lines[cursor].lstrip().startswith(("|", "#", "**")):
                        paragraph.append(lines[cursor].strip())
                        cursor += 1
                    if paragraph:
                        return " ".join(paragraph)
            return ""

        for log_count, log in enumerate(state.get("detailed_logs", [])):
            log_parts = log.split("\n\n")
            log_title = log_parts[6] if len(log_parts) > 6 else f"Analysis {log_count + 1}"
            if "ERROR" not in log_title:
                mentioned_files = [f for f in all_files if f in log]
                parse_files = ""
                for key in mentioned_files:
                    if key in emitted_files:
                        continue
                    emitted_files.add(key)
                    replacement = generated_files[key]
                    title = get_generated_file_title(key, file_descriptions)
                    if key.endswith('.csv'):
                        tab_count += 1
                        parse_files += f'<div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Table"]} {tab_count}.</b> {title}</div>{replacement}'
                    elif key.endswith('.png'):
                        fig_count += 1
                        parse_files += f'<img src="{replacement}" width="90%" alt="{title}" style="display:block;margin:auto"/><div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Figure"]} {fig_count}.</b> {title}</div>'
                for key in mentioned_files:
                    if key.endswith('.csv'):
                        title = get_generated_file_title(key, file_descriptions)
                        interpretation = interpretation_for(title)
                        if interpretation:
                            parse_files += f'<p><b>Interpretation for {escape(title)}:</b> {escape(interpretation)}</p>'
                if parse_files:
                    appendix_4_html += f"""<h3 style="
                    font-family:'Times New Roman', serif;
                    color:#000;
                    line-height:1.5;
                    text-align:justify;
                margin:16px 0 8px 0;text-align:justify;">{log_title.replace("# ", "")}</h3>
                {parse_files}"""
    except Exception:
        pass
    return appendix_4_html


async def get_appendix_5(state: dict) -> str:
    language_dict = LANGUAGE_KIT.get(state["language"].lower(), LANGUAGE_KIT["tiếng việt"])
    appendix_5_html = f"""<h2 style="
                font-family:'Times New Roman', serif;
                color:#000;
                line-height:1.5;
                text-align:justify;
            margin:16px 0 8px 0;text-align:justify;">{language_dict["APPENDIX 5. SURVEY DATA (RAW DATA SUMMARY)"]}</h2>"""
    try:
        mongo_client = get_mongodb_client()
        db = mongo_client["admin"]
        article_collection = db["articles"]
        article = await article_collection.find_one({"_id": ObjectId(state["document_id"])})
        proposal_collection = db["proposal_titles"]
        proposal = await proposal_collection.find_one({"_id": ObjectId(article["title"])})
        appendix_5 = ""
        fig_count = 0
        for key, replacement in proposal["appendix_5"].items():
            fig_count += 1
            image_title = key.replace("_", " ").replace("/", " - ").replace(".png", "").title()
            appendix_5 += f'<img src="{replacement}" width="90%" alt="{image_title}" style="display:block;margin:auto"/><div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Figure"]} {fig_count}.</b> {image_title}</div>'
        if appendix_5:
            appendix_5_html += appendix_5

    except Exception:
        pass
    return appendix_5_html


async def get_appendix_8(state: dict) -> str:
    language_dict = LANGUAGE_KIT.get(state["language"].lower(), LANGUAGE_KIT["tiếng việt"])
    appendix_8_html = f"""<h2 style="
                    font-family:'Times New Roman', serif;
                    color:#000;
                    line-height:1.5;
                    text-align:justify;
                margin:16px 0 8px 0;text-align:justify;">{language_dict["APPENDIX 8. RELATED DOCUMENTS, POLICIES, RESOLUTIONS"]}</h2>"""
    try:
        if "lit_review" in state:
            seen_papers: dict = {}
            paper_count = 1
            for section in state["outline"]["outline"]:
                # Skip sections without subheadings (section-only content)
                if section.get("subheadings") is None:
                    continue
                for subsection in section["subheadings"]:
                    for ref in subsection["refs"]:
                        paper_title_key = "title" if "title" in ref else "paperTitle"
                        if ref[paper_title_key] not in seen_papers:
                            result = next(paper for paper in state["research_papers"] if paper.get("title") == ref[paper_title_key])
                            shorten_authors = shorten_authors_name([result["authors"][0]] if len(result["authors"]) > 2 else result["authors"][0:2])
                            year = result["year"] if result["year"] != "Unknown" and result["year"] != "" else ""
                            seen_papers[ref[paper_title_key]] = {
                                language_dict["No."]: paper_count,
                                language_dict["Title"]: ref[paper_title_key],
                                language_dict["Authors"]: shorten_authors,
                                language_dict["Year"]: year,
                                language_dict["Use in"]: [subsection["subheading"]],
                            }
                            paper_count += 1
                        else:
                            if subsection["subheading"] not in seen_papers[ref[paper_title_key]][language_dict["Use in"]]:
                                seen_papers[ref[paper_title_key]][language_dict["Use in"]].append(subsection["subheading"])
            final_table_data: list[dict] = []
            for title, data in seen_papers.items():
                data[language_dict["Use in"]] = "<br>".join(sorted(set(data[language_dict["Use in"]])))
                final_table_data.append(data)
            df = pd.DataFrame(final_table_data)
            html = format_table(df)
            paper_usage_label = language_dict["Paper's Usage"]
            appendix_8 = f'<div style="text-align:center;font-style:italic;margin:4px 0;"><b>{language_dict["Table"]} 1.</b> {paper_usage_label}</div>{html}'
            if appendix_8:
                appendix_8_html += appendix_8
    except Exception:
        pass
    return appendix_8_html


async def _get_appendices(state: dict) -> str:
    language_dict = LANGUAGE_KIT.get(state["language"].lower(), LANGUAGE_KIT["tiếng việt"])
    tasks = [get_appendix_1(state), get_appendix_2(state), get_appendix_3(state), get_appendix_4(state), get_appendix_5(state), get_appendix_8(state)]
    results = await asyncio.gather(*tasks)
    appendices = f"""<h1 style="
                font-family:'Times New Roman', serif;
                font-size:13pt;
                color:#000;
                line-height:1.5;
                text-align:justify;
            font-weight:bold;margin:16px 0 8px 0;font-size:18pt;text-transform:uppercase;text-align:center;">{language_dict["APPENDICES"]}</h1>"""
    appendices += results[0]
    appendices += results[1]
    appendices += results[2]
    appendices += results[3]
    appendices += results[4]
    appendices += f"""<h2 style="
                    font-family:'Times New Roman', serif;
                    color:#000;
                    line-height:1.5;
                    text-align:justify;
                margin:16px 0 8px 0;text-align:justify;">{language_dict["APPENDIX 6. IN-DEPTH INTERVIEW RESULTS (IF ANY)"]}</h2>"""
    appendices += f"""<h2 style="
                    font-family:'Times New Roman', serif;
                    color:#000;
                    line-height:1.5;
                    text-align:justify;
                margin:16px 0 8px 0;text-align:justify;">{language_dict["APPENDIX 7. CALCULATION FORMULA / MODELING"]}</h2>"""
    appendices += results[5]
    appendices += f"""<h2 style="
                    font-family:'Times New Roman', serif;
                    color:#000;
                    line-height:1.5;
                    text-align:justify;
                margin:16px 0 8px 0;text-align:justify;">{language_dict["APPENDIX 9. RESEARCH ETHICS PROCESS"]}</h2>"""
    appendices += f"""<h2 style="
                    font-family:'Times New Roman', serif;
                    color:#000;
                    line-height:1.5;
                    text-align:justify;
                margin:16px 0 8px 0;text-align:justify;">{language_dict["APPENDIX 10. IMPLEMENTATION PLAN AND SCHEDULE (GANTT CHART)"]}</h2>"""
    appendices += f"""<h2 style="
                    font-family:'Times New Roman', serif;
                    color:#000;
                    line-height:1.5;
                    text-align:justify;
                margin:16px 0 8px 0;text-align:justify;">{language_dict["APPENDIX 11. LIST OF ABBREVIATIONS / SYMBOLS"]}</h2>"""
    appendices = markdownify(appendices)
    return appendices


async def search_web(state: dict) -> tuple[str, int, int, int]:
    search_context = get_search_context(state["search_phase"])
    if not search_context:
        return "", 0, 0, 0

    content = build_search_strategy_content(state, search_context)
    llm = get_llm(state["model_id"], state["llm_key"])
    error, success, search_obj, input_tokens, output_tokens = await get_answer_with_schema(state['document_id'][:8], llm, "", content, AnalysisOutput)
    if not success:
        raise error
    else:
        if search_obj.needs_search:
            logger.info(f"[{state['document_id'][:8]}] Trigged web search for {state['section'].heading} - {state['subsection'].subheading}")
            research_context = build_websearch_research_context(state, search_obj)
            content = build_websearch_content(state)
            error, success, report, web_search_call, search_input_tokens, search_output_tokens = await get_answer_with_websearch(
                state['document_id'][:8],
                llm,
                "",
                content,
                state["search_key"],
                state["language"],
                research_context,
                state["document_id"],
                True
            )
            if not success:
                raise error
            return report, web_search_call, input_tokens + search_input_tokens, output_tokens + search_output_tokens
        else:
            return "", 0, input_tokens, output_tokens


def add_unique(left: list[str], right: list[str]) -> list[str]:
    """
    Appends new items from 'right' to 'left' only if they don't exist in 'left'.
    Preserves order.
    """
    if not left:
        return right
    if not right:
        return left

    # Use a set for O(1) lookups, but append to list to keep order
    existing_set = set(left)
    new_list = left.copy()

    for item in right:
        if item not in existing_set:
            new_list.append(item)
            existing_set.add(item)

    return new_list
