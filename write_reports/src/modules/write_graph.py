import asyncio
from bson import ObjectId
from langgraph.graph import StateGraph, START, END
from langgraph.types import interrupt, Checkpointer, Send
import logging
import json
import operator
from pymongo import ASCENDING
from typing import Annotated, Optional
from typing_extensions import TypedDict

from document_setup.src.modules.ingest_docs import VectorDBProcessor

from write_reports.src.modules.references import format_ref_in_ref_section
from translate import LANGUAGE_KIT
from write_reports.src.modules.utils.utils_func import (
    get_need_data,
    _load_model,
    _section_synthesizer,
    write,
    write_no_sub,
    write_data,
    write_method,
    are_sections_disjoint,
    _get_literature_review_section,
    _get_proposed_method_section,
    _get_result_section,
    get_need_propose_method,
    get_literature_review_table,
    reset_or_add,
    validate_sublists,
    get_table,
    renumber_table_captions,
    count_table_captions,
    get_appendices,
    get_survey_questions_table,
    load_current_section,
    add_unique,
    assign_section_logs,
    section_heading,
    strip_heading_number,
)
from write_reports.src.modules.write_prompt_bank import (
    batch_prompt,
    analyze_subsection_selection, 
    section_writer_without_seminar_instructions, 
    analyze_data_writer,
    method_subsection_selection,
    methodology_writer
)
from write_reports.src.schemas.section import (
    ExecutionPlan,
    SectionDescription,
    SectionContentWithTokenCount,
    SubSectionDescription,
    LitSection,
    MethodSection,
    ResultSection,
)
from write_reports.src.modules.log_cleaner import LogCleaningConfig

from get_llm_response import get_llm, get_answer_with_schema
from utils import markdownify_keep_images, get_mongodb_client

logging.basicConfig(level=logging.info, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


class State(TypedDict):
    analyze_log: str
    change_section: bool
    completed_subsections: Annotated[
        list[SectionContentWithTokenCount],
        operator.add
    ]
    current_count_batch: int
    current_section: SectionDescription
    current_section_batch: list[list[SubSectionDescription]]
    current_section_need_data: bool
    db_key: str
    detailed_logs: list[str]
    document_id: str
    domain: str
    embed_model: str
    embed_tokens: int
    embed_uuids: list[str]
    end_section: bool
    existing_generated_files: dict
    file_descriptions: dict
    existing_refs: list[dict]
    field: str
    generated_files: dict
    input_tokens: int
    language: str
    lit_review: int
    propose_method: int
    llm_key: str
    used_files: Annotated[list[str], add_unique]
    model_id: str
    need_data: bool
    need_method: bool
    new_content: dict
    other_sections: list[dict]
    outline: dict
    outline_code: list[str]
    output_tokens: int
    proposal: dict
    references_style: str
    research_papers: list[dict]
    result_section: int
    sections_need_data: list[bool]
    sections_need_method: list[bool]
    subsection_log_assignments: dict  # Mapping of subsection index -> chosen logs
    title: str
    tab_count_section: int
    tab_count: Annotated[int, reset_or_add]
    fig_count: Annotated[int, reset_or_add]
    key_points_map: dict
    retry: int
    papers_titles: list[str]
    seen_chunk: dict
    search_phase: int
    search_key: str
    use_web_search: bool
    user_content: str
    user_id: str


class Worker(TypedDict):
    analyze_log: str
    completed_subsections: Annotated[
        list[SectionContentWithTokenCount],
        operator.add
    ]
    current_report: str
    domain: str
    document_id: str
    field: str
    language: str
    llm_key: str
    model_id: str
    need_method: bool
    proposal: dict
    generated_files: dict
    used_files: Annotated[list[str], add_unique]
    detailed_logs: list[str]
    file_descriptions: dict
    section_num: int
    section: SectionDescription
    outline: dict
    tab_count: Annotated[int, reset_or_add]
    fig_count: Annotated[int, reset_or_add]
    subsection: SubSectionDescription
    retry: int
    search_phase: int
    search_key: str
    user_content: str
    chosen_logs: list[int]  # Pre-assigned log indices for this subsection
    log_cleaning_config: Optional[LogCleaningConfig]  # Configuration for log preprocessing


async def load_input(state: State):
    return await _load_model(state)


async def choose_refs(state: State):
    logger.info(f"[{state['document_id'][:8]}] Divide sections")
    title_section = {
        "name": "title",
        "content": f"# {state['proposal']['title']}",
        "input_tokens": 0,
        "output_tokens": 0,
        "current_refs": [],
    }
    lit_review = ["related work", "related works", "literature review", "tổng quan tài liệu"]
    headings = [
        strip_heading_number(section_heading(section)) 
        for section in state["outline"]["outline"]
    ]
    found_lit_review = False
    input_tokens = 0
    output_tokens = 0
    lit_input_tokens = 0
    lit_output_tokens = 0
    for section_count, heading in enumerate(headings):
        if heading.lower() in lit_review:
            lit_section = LitSection(lit_review=section_count + 1)
            lit_input_tokens = 0
            lit_output_tokens = 0
            found_lit_review = True
            break
    llm = get_llm(state["model_id"], state["llm_key"])
    if not found_lit_review:
        error, success, lit_section, lit_input_tokens, lit_output_tokens = await _get_literature_review_section(state['document_id'][:8], headings, state["model_id"], llm)
        if not success:
            if error.status_code in [401, 403, 429, 500]:
                raise error
            lit_section = LitSection(lit_review=0)
            lit_input_tokens = 0
            lit_output_tokens = 0
    input_tokens += lit_input_tokens
    output_tokens += lit_output_tokens
    propose_method = ["phương pháp nghiên cứu", "thiết kế nghiên cứu", "research design", "research methodology", "proposed method"]
    found_propose_method = False
    method_input_tokens = 0
    method_output_tokens = 0
    for section_count, heading in enumerate(headings):
        if heading.lower() in propose_method:
            method_section = MethodSection(propose_method=section_count + 1)
            method_input_tokens = 0
            method_output_tokens = 0
            found_propose_method = True
            break
    if not found_propose_method:
        error, success, method_section, method_input_tokens, output_tokens = await _get_proposed_method_section(state['document_id'][:8], headings, state["model_id"], llm)
        if not success:
            if error.status_code in [401, 403, 429, 500]:
                raise error
            method_section = MethodSection(propose_method=0)
            method_input_tokens = 0
            method_output_tokens = 0
    input_tokens += method_input_tokens
    output_tokens += method_output_tokens
    if lit_section.lit_review != 0:
        if method_section.propose_method == 0:
            method_section = MethodSection(propose_method=lit_section.lit_review + 1)
        else:
            if method_section.propose_method <= lit_section.lit_review:
                method_section = MethodSection(propose_method=lit_section.lit_review + 1)
            elif method_section.propose_method > lit_section.lit_review + 1:
                tmp_method = method_section.propose_method
                method_section = MethodSection(propose_method=tmp_method - 1)
    
    result_input_tokens = 0
    result_output_tokens = 0
    error, success, result_section, result_input_tokens, result_output_tokens = await _get_result_section(state['document_id'][:8], headings, state["model_id"], llm)
    if not success:
        if error.status_code in [401, 403, 429, 500]:
            raise error
        result_section = ResultSection(result=0)
        result_input_tokens = 0
        result_output_tokens = 0
    input_tokens += result_input_tokens
    output_tokens += result_output_tokens
    if result_section.result <= method_section.propose_method and method_section.propose_method != 0:
        result_section = ResultSection(result=method_section.propose_method + 1)
    logger.info(f"[{state['document_id'][:8]}] Lit review: {lit_section.lit_review}")
    logger.info(f"[{state['document_id'][:8]}] Method: {method_section.propose_method}")
    logger.info(f"[{state['document_id'][:8]}] Result: {result_section.result}")
    for key in ["variables", "final_model", "hypothesis", "survey_questions", "questions"]:
        state["proposal"].pop(key, None)
    mongo_client = get_mongodb_client()
    admin_db = mongo_client["admin"]
    content_collection = admin_db["outlines"]
    articles_collection = admin_db["articles"]
    article = await articles_collection.find_one({"_id": ObjectId(state["document_id"])})
    user_report_str = ""
    if article and "topicRefs" in article:
        user_report_cache: list[str] = []
        if len(article["topicRefs"]):
            seminar_id = article["topicRefs"][0]
            seminar_user_report_caches = content_collection.find({"documentId": seminar_id}).sort("index", ASCENDING)
            async for doc in seminar_user_report_caches:
                if doc.get("content"):
                    user_report_cache.append(doc["content"])
                else:
                    break
        user_report_str += "\n".join(user_report_cache)
    return {
        "other_sections": [title_section],
        "change_section": True,
        "need_data": False,
        "need_method": False,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "lit_review": lit_section.lit_review,
        "propose_method": method_section.propose_method,
        "result_section": result_section.result,
        "used_files": [],
        "user_content": user_report_str,
    }
    

async def write_section(state: State):
    if len(state["other_sections"]):
        result = interrupt({
            "break_point": "chapter_break",
            "ref_table": [],
            "disable_edit": False if (len(state["other_sections"]) < state["propose_method"] or state["propose_method"] == 0) else True,
        })
        state["model_id"] = result["model_id"]
        state["llm_key"] = result["llm_key"]
        state["db_key"] = result["db_key"]
        state["use_web_search"] = result["use_web_search"]
        state["search_key"] = result["search_key"]
    state, current_section, search_phase = await load_current_section(state)
    sections_with_data = []
    sections_need_method = []
    if current_section.has_subsections():
        if len(state["other_sections"]) >= state["result_section"]:
            content = f"""
            Report detail section:
            Section {len(state["other_sections"])}:
            Heading: 
            {current_section.heading}
            Overview: 
            {current_section.overview}
            Current subsection:
            """
            llm = get_llm(state["model_id"], state["llm_key"])
            tasks = [
                get_need_data(
                    state["model_id"], 
                    state["document_id"], 
                    llm, 
                    analyze_subsection_selection, 
                    content, 
                    str(dict(subsection))
                ) for subsection in current_section.subsections
            ]
            results = await asyncio.gather(*tasks)
            sections_with_data = [result[0] for result in results]
            input_tokens = [result[1] for result in results]
            output_tokens = [result[2] for result in results]
            state["input_tokens"] += sum(input_tokens)
            state["output_tokens"] += sum(output_tokens)
            state["need_data"] = any(sections_with_data)                
        elif len(state["other_sections"]) == state["result_section"]:
            state["need_data"] = True
        else:
            state["need_data"] = False
        if not state["need_data"]:
            if len(state["other_sections"]) >= state["propose_method"]:
                content = f"""
                Report detail section:
                Section {len(state["other_sections"])}:
                Heading: 
                {current_section.heading}
                Overview: 
                {current_section.overview}
                Current subsection:
                """
                llm = get_llm(state["model_id"], state["llm_key"])
                tasks = [
                    get_need_propose_method(
                        state["model_id"], 
                        state["document_id"], 
                        llm, 
                        method_subsection_selection, 
                        content, 
                        str(dict(subsection))
                    ) for subsection in current_section.subsections
                ]
                results = await asyncio.gather(*tasks)
                sections_need_method = [result[0] for result in results]
                input_tokens = [result[1] for result in results]
                output_tokens = [result[2] for result in results]
                if not any(sections_need_method) and len(state["other_sections"]) == state["propose_method"]:
                    start_index = len(sections_need_method) // 2
                    for i in range(start_index, len(sections_need_method)):
                        sections_need_method[i] = True
                state["input_tokens"] += sum(input_tokens)
                state["output_tokens"] += sum(output_tokens)
                state["need_method"] = True
    else:
        state["need_data"] = False
    return {
        "other_sections": state["other_sections"],
        "model_id": state["model_id"],
        "llm_key": state["llm_key"],
        "db_key": state["db_key"],
        "change_section": False,
        "current_section": current_section,
        "sections_need_data": sections_with_data or [],
        "sections_need_method": sections_need_method or [],
        "need_data": state["need_data"],
        "need_method": state["need_method"],
        "input_tokens": state["input_tokens"],
        "output_tokens": state["output_tokens"],
        "tab_count_section": 0,
        "tab_count": "RESET",
        "fig_count": "RESET",
        "seen_chunk": state["seen_chunk"],
        "research_papers": state["research_papers"],
        "key_points_map": state["key_points_map"],
        "outline": state["outline"],
        "search_phase": search_phase,
        "use_web_search": state["use_web_search"],
        "search_key": state["search_key"],
    }


async def create_subsection_batch(state: State):
    logger.info(f"[{state['document_id'][:8]}] Creating batches for section: '{state['current_section'].heading}'")
    if state["current_section"].has_subsections():
        if state["need_method"] and not state["need_data"]:
            try:
                split_point = state["sections_need_method"].index(True)
                final_batch = [state["current_section"].subsections[:split_point]] + [
                    [item] for item in state["current_section"].subsections[split_point:]
                ] if split_point else [[item] for item in state["current_section"].subsections]
            except Exception:
                final_batch: list[list[SubSectionDescription]] = []
                for batch in state["current_section"].subsections:
                    if batch:
                        final_batch.append([batch])
            return {
                "current_count_batch": 0,
                "current_section_batch": final_batch,
                "input_tokens": state["input_tokens"],
                "output_tokens": state["output_tokens"],
            }
        if not state["need_data"] or (state["need_data"] and sum(state["sections_need_data"]) == len(state["current_section"].subsections)):
            content = f"Here is the section data:\n{state['current_section']}\nGenerate from 0 to {len(state['current_section'].subsections) - 1}"
            llm = get_llm(state["model_id"], state["llm_key"])
            success = False
            retry = 0
            while not success and retry < 3:
                retry += 1
                try:
                    error, success, current_section_batch, input_tokens, output_tokens = await get_answer_with_schema(
                        state["document_id"][:8],
                        llm,
                        batch_prompt,
                        content,
                        ExecutionPlan
                    )
                    if not success:
                        if error.status_code in [401, 403, 429, 500]:
                            raise error
                    state["input_tokens"] += input_tokens
                    state["output_tokens"] += output_tokens
                    current_section_batch.plan = [sorted(set(batch)) for batch in current_section_batch.plan]
                    batch_len = sum([len(batch) for batch in current_section_batch.plan])
                    success = batch_len == len(state["current_section"].subsections)
                except Exception as e:
                    logger.error(f"[{state['document_id'][:8]}] {e}")
            if not success or not are_sections_disjoint(current_section_batch.plan) or not validate_sublists(current_section_batch.plan):                    
                current_section_batch = ExecutionPlan(plan=[[i] for i in range(len(state["current_section"].subsections))])
            if current_section_batch.plan[0][0] != 0 and success:
                current_section_batch.plan = [[[item - current_section_batch.plan[0][0]] for item in batch] for batch in current_section_batch.plan]
            final_batch: list[list[SubSectionDescription]] = []
            for batch in current_section_batch.plan:
                subsection_batch: list[SubSectionDescription] = []
                for item in batch:
                    subsection_batch.append(state["current_section"].subsections[item])
                if subsection_batch:
                    final_batch.append(subsection_batch)
            return {
                "current_count_batch": 0,
                "current_section_batch": final_batch,
                "input_tokens": state["input_tokens"],
                "output_tokens": state["output_tokens"],
            }
        elif state["need_data"]:
            try:
                split_point = state["sections_need_data"].index(True)
                final_batch = [state["current_section"].subsections[:split_point]] + [
                    [item] for item in state["current_section"].subsections[split_point:]
                ] if split_point else [[item] for item in state["current_section"].subsections]
            except Exception:
                final_batch: list[list[SubSectionDescription]] = []
                for batch in state["current_section"].subsections:
                    if batch:
                        final_batch.append([batch])
            return {
                "current_count_batch": 0,
                "current_section_batch": final_batch,
                "input_tokens": state["input_tokens"],
                "output_tokens": state["output_tokens"],
            }
    else:
        return {
            "current_count_batch": 0,
            "current_section_batch": [],
            "input_tokens": state["input_tokens"],
            "output_tokens": state["output_tokens"],
        }


async def check_propose_method(state: State):
    if state["current_section"].has_subsections():
        current_batch_subsections = state["current_section_batch"][state["current_count_batch"]]
        current_subsections_count = sum([len(state["current_section_batch"][batch]) for batch in range(state["current_count_batch"])])
        need_method = any(
            state["sections_need_method"][current_subsections_count:current_subsections_count + len(current_batch_subsections)]
        )
        if need_method and not state["proposal"].get("final_model"):
            result = interrupt({
                "break_point": "propose_method",
                "ref_table": [],
                "disable_edit": True,
            })
            return {
                "proposal": result["proposal"],
                "model_id": result["model_id"],
                "llm_key": result["llm_key"],
                "user_content": "",
            }
        else:
            return {}
    else:
        return {}


async def check_for_data(state: State):
    if len(state["current_section"].subsections):
        current_batch_subsections = state["current_section_batch"][state["current_count_batch"]]
        current_subsections_count = sum([len(state["current_section_batch"][batch]) for batch in range(state["current_count_batch"])])
        need_data = any(
            state["sections_need_data"][current_subsections_count:current_subsections_count + len(current_batch_subsections)]
        )
        result = {}
        if need_data and not len(state["analyze_log"]):
            result = interrupt({
                "break_point": "analyzer",
                "ref_table": [],
                "disable_edit": True,
            })
        if need_data:
            qdrant_processor = VectorDBProcessor(
                state["llm_key"], 
                state["db_key"], 
                10000, 
                state["language"], 
                state["document_id"], 
                state["model_id"],
            )
            input_data, _, _ = await qdrant_processor.load_datas_chat(state["user_id"], "article")
            # Fallback to state if result is missing keys (e.g., from partial resume)
            def get_val(key, default=None):
                if result and isinstance(result, dict) and key in result:
                    return result[key]
                return state.get(key, default)

            # ===== NEW: Assign logs to all subsections at section level =====
            file_descriptions = get_val("file_descriptions", {})
            if not file_descriptions:
                try:
                    file_descriptions = json.loads(get_val("generated_files", {}).get("file_descriptions", "{}"))
                except (TypeError, json.JSONDecodeError):
                    file_descriptions = {}
            get_val("generated_files", {}).pop("file_descriptions", None)
            llm = get_llm(get_val("model_id"), get_val("llm_key"))
            temp_state = {
                "section": state["current_section"],
                "document_id": state["document_id"],
                "detailed_logs": get_val("detailed_logs", []),
                "generated_files": get_val("generated_files", {}),
                "file_descriptions": file_descriptions,
                "proposal": state.get("proposal", {}),
                "field": state.get("field", ""),
                "domain": state.get("domain", ""),
            }
            log_assignments, inp_tok, out_tok = await assign_section_logs(temp_state, llm)
            logger.info(f"[{state['document_id'][:8]}] Section-level log assignment completed: {log_assignments}")

            return {
                "document_id": state["document_id"],
                "analyze_log": get_val("analyze_log", ""),
                "detailed_logs": get_val("detailed_logs", []),
                "generated_files": get_val("generated_files", {}),
                "file_descriptions": file_descriptions,
                "model_id": get_val("model_id"),
                "llm_key": get_val("llm_key"),
                "user_content": input_data,
                "subsection_log_assignments": log_assignments,
                "input_tokens": state["input_tokens"] + inp_tok,
                "output_tokens": state["output_tokens"] + out_tok,
            }
        else:
            return {}
    else:
        return {}


def assign_workers_subsection(state: State):
    if len(state["current_section"].subsections):
        current_batch_subsections = state["current_section_batch"][state["current_count_batch"]]
        current_subsections_count = sum([len(state["current_section_batch"][batch]) for batch in range(state["current_count_batch"])])
        need_data = any(
            state["sections_need_data"][current_subsections_count:current_subsections_count + len(current_batch_subsections)]
        )
        need_method = any(
            state["sections_need_method"][current_subsections_count:current_subsections_count + len(current_batch_subsections)]
        )
        current_report = "\n\n".join(
            [
                f"{other_section['content']}" for other_section in state["other_sections"]
            ]
        )

        # ===== NEW: Get the subsection index and chosen logs =====
        workers = []
        for batch_idx, subsection in enumerate(current_batch_subsections):
            # Calculate the global subsection index
            subsection_global_idx = current_subsections_count + batch_idx

            # Get the chosen logs for this subsection (if available)
            chosen_logs = state.get("subsection_log_assignments", {}).get(subsection_global_idx, [])

            workers.append(Send(
                "write_subsection",
                {
                    "analyze_log": state["analyze_log"] if need_data else "",
                    "generated_files": state["generated_files"] if need_data else {},
                    "detailed_logs": state["detailed_logs"] if need_data else [],
                    "file_descriptions": state.get("file_descriptions", {}) if need_data else {},
                    "used_files": state["used_files"],
                    "section_num": len(state["other_sections"]) - 1 if not state["current_count_batch"] else len(state["other_sections"]) - 2,
                    "current_report": markdownify_keep_images(current_report),
                    "domain": state["domain"],
                    "field": state["field"],
                    "language": state["language"],
                    "llm_key": state["llm_key"],
                    "model_id": state["model_id"],
                    "need_method": need_method,
                    "proposal": state["proposal"],
                    "section": state["current_section"],
                    "outline": state["outline"],
                    "subsection": subsection,
                    "tab_count": state["tab_count"],
                    "fig_count": state["fig_count"],
                    "retry": state["retry"],
                    "search_phase": state["search_phase"] if state["use_web_search"] else 0,
                    "search_key": state["search_key"],
                    "document_id": state["document_id"],
                    "user_content": state["user_content"],
                    "chosen_logs": chosen_logs,  # ← NEW: Pass pre-assigned logs
                },
            ))

        return workers
    else:
        return Send(
            "write_subsection",
            {
                "analyze_log": "",
                "generated_files": {},
                "detailed_logs": [],
                "used_files": state["used_files"],
                "section_num": 0,
                "current_report": markdownify_keep_images(current_report),
                "domain": state["domain"],
                "field": state["field"],
                "language": state["language"],
                "llm_key": state["llm_key"],
                "model_id": state["model_id"],
                "need_method": False,
                "proposal": state["proposal"],
                "section": state["current_section"],
                "outline": state["outline"],
                "tab_count": state["tab_count"],
                "fig_count": state["fig_count"],
                "retry": state["retry"],
                "subsection": SubSectionDescription(detail_description="", subheading="", subheading_word_count="0-0"),
                "search_phase": state["search_phase"] if state["use_web_search"] else 0,
                "search_key": state["search_key"],
                "document_id": state["document_id"],
                "user_content": state["user_content"],
            },
        )


async def write_subsection(state: Worker):
    """Worker writes a section of the report based on given information"""
    llm = get_llm(state["model_id"], state["llm_key"])
    if len(state["subsection"].subheading):
        if state["analyze_log"]:
            # ===== Pass pre-assigned logs and log cleaning config to write_data =====
            chosen_logs = state.get("chosen_logs", None)

            # Get or create log cleaning config
            log_cleaning_config = state.get("log_cleaning_config", None)
            if log_cleaning_config is None:
                # Default: enable all cleaning features
                log_cleaning_config = LogCleaningConfig(
                    remove_statistical_markers=True,
                    remove_log_headers=True,
                    remove_diagnostic_columns=True,
                    remove_section_markers=True,
                    strip_markdown_headers=True,
                )

            section_with_token_count, tab_count, fig_count = await write_data(
                state,
                llm,
                analyze_data_writer,
                state["user_content"],
                chosen_logs=chosen_logs,  # Pass pre-assigned logs
                log_cleaning_config=log_cleaning_config,  # Pass log cleaning config
            )
            return {
                "completed_subsections": [section_with_token_count],
                "tab_count": tab_count,
                "fig_count": fig_count,
                "used_files": state["used_files"],
            }
        elif state["need_method"]:
            section_with_token_count = await write_method(state, llm, methodology_writer)
            return {
                "completed_subsections": [section_with_token_count],
                "tab_count": 0,
                "fig_count": 0,
                "used_files": state["used_files"],
            }
        else:
            section_with_token_count = await write(state, llm, section_writer_without_seminar_instructions)
            return {
                "completed_subsections": [section_with_token_count],
                "tab_count": 0,
                "fig_count": 0,
                "used_files": state["used_files"],
            }
    else:
        section_with_token_count = await write_no_sub(state, llm, section_writer_without_seminar_instructions)
        return {
            "completed_subsections": [section_with_token_count],
            "tab_count": 0,
            "fig_count": 0,
            "used_files": state["used_files"],
        }


def continue_to_write_subsection(state: State):
    if state["current_count_batch"] == len(state["current_section_batch"]) - 1 or not state["current_section"].has_subsections():
        if len(state["other_sections"]) - 1 != len(state["outline"]["outline"]):
            return "write_section"
        else:
            return "add_ref"
    else:
        return "increment_count_batch"


async def increment_count_batch(state: State):
    return {"current_count_batch": state["current_count_batch"] + 1}


async def section_synthesizer(state: State):
    logger.info(f"[{state['document_id'][:8]}] Synthesize section {state['current_section'].heading}")
    end_section = False
    if state["current_count_batch"] == 0:
        if state["current_section"].has_subsections():
            num_subsection = len(state["current_section_batch"][state["current_count_batch"]])
        else:
            num_subsection = 1
        section_count = len(state["other_sections"]) - 1
        section_dict, state["existing_refs"] = await _section_synthesizer(state, section_count, num_subsection)
        section_dict["section_code"] = state["outline_code"][section_count]
        if state["need_data"]:
            section_dict["content"], table_count = renumber_table_captions(
                section_dict["content"], section_count + 1, state["language"]
            )
        else:
            section_dict, table_count = get_table(section_dict, section_count + 1, state["tab_count_section"], state["language"])
        if state["current_count_batch"] == len(state["current_section_batch"]) - 1:
            if section_count == state["lit_review"] - 1:
                lit_review_table = get_literature_review_table(state, state["research_papers"], state["lit_review"], state["language"], table_count)
                section_dict["content"] = section_dict["content"] + "\n" + lit_review_table
            if section_count == state["propose_method"] - 1:
                survey_questions_table, state["existing_refs"] = get_survey_questions_table(state, section_count, table_count)
                section_dict["content"] = section_dict["content"] + "\n" + survey_questions_table
            if len(state["other_sections"]) - 1 == len(state["outline"]["outline"]):
                end_section = True
        state["other_sections"].append(section_dict)
        return {
            "tab_count_section": table_count,
            "new_content": section_dict,
            "end_section": end_section,
            "other_sections": state["other_sections"],
            "existing_refs": state["existing_refs"],
            "proposal": state["proposal"],
        }
    else:
        num_subsection = sum([len(batch) for batch in state["current_section_batch"][:state["current_count_batch"] + 1]])
        existing_subsection = num_subsection - len(state["current_section_batch"][state["current_count_batch"]])
        section_count = len(state["other_sections"]) - 2
        section_dict, state["existing_refs"] = await _section_synthesizer(state, section_count, num_subsection)
        section_dict["section_code"] = state["outline_code"][section_count]
        content = section_dict["content"].split(f"## {section_count + 1}.{existing_subsection + 1}. ")[-1]
        section_dict["content"] = f"## {section_count + 1}.{existing_subsection + 1}. " + content
        if state["need_data"]:
            previous_count = count_table_captions(
                state["other_sections"][-1]["content"], section_count + 1, state["language"]
            )
            section_dict["content"], table_count = renumber_table_captions(
                section_dict["content"], section_count + 1, state["language"], previous_count
            )
        else:
            section_dict, table_count = get_table(section_dict, section_count + 1, state["tab_count_section"], state["language"])
        if state["current_count_batch"] == len(state["current_section_batch"]) - 1:
            if section_count == state["lit_review"] - 1:
                lit_review_table = get_literature_review_table(state, state["research_papers"], state["lit_review"], state["language"], table_count)
                section_dict["content"] = section_dict["content"] + "\n" + lit_review_table
            if section_count == state["propose_method"] - 1:
                survey_questions_table, state["existing_refs"] = get_survey_questions_table(state, section_count, table_count)
                section_dict["content"] = section_dict["content"] + "\n" + survey_questions_table
            if len(state["other_sections"]) - 1 == len(state["outline"]["outline"]):
                end_section = True
        state["other_sections"][-1]["content"] = state["other_sections"][-1]["content"] + "\n\n" + section_dict["content"]
        return {
            "new_content": section_dict,
            "tab_count_section": table_count,
            "end_section": end_section,
            "other_sections": state["other_sections"],
            "existing_refs": state["existing_refs"],
            "proposal": state["proposal"],
        }


def continue_to_write(state: State):
    return len(state["other_sections"]) - 1 != len(state["outline"]["outline"])


async def add_ref(state: State):
    result = interrupt({
        "break_point": "chapter_break",
        "disable_edit": True,
    })
    state["model_id"] = result["model_id"]
    state["llm_key"] = result["llm_key"]
    state["db_key"] = result["db_key"]
    tasks = [
        format_ref_in_ref_section(
            title, 
            state["research_papers"], 
            state["references_style"]
        ) for title in state["existing_refs"]
    ]
    refs = await asyncio.gather(*tasks)
    if state["references_style"] in ["Vancouver", "IEEE"]:
        refs = [f"[{i + 1}] {ref}" for i, ref in enumerate(refs)]
    language_dict = LANGUAGE_KIT.get(state["language"].lower(), LANGUAGE_KIT["tiếng việt"])
    refs_text = "\n\n".join(refs)
    ref_section = {
        "name": "references",
        "content": f"# {language_dict['References']}\n\n{refs_text}".replace(r'\n', '\n'),
        "input_tokens": 0,
        "output_tokens": 0,
        "embed_tokens": 0,
        "section_code": state["outline_code"][-2],
        "current_refs": [],
    }
    state["other_sections"].append(ref_section)
    return {
        "other_sections": state["other_sections"],
        "change_section": True,
    }


async def write_appendices(state: State):
    return await get_appendices(state)


async def get_graph(checkpointer: Checkpointer):
    writer_builder = StateGraph(State)

    writer_builder.add_node("load_input", load_input)
    writer_builder.add_node("choose_refs", choose_refs)
    writer_builder.add_node("check_propose_method", check_propose_method)
    writer_builder.add_node("write_section", write_section)
    writer_builder.add_node("create_subsection_batch", create_subsection_batch)
    writer_builder.add_node("check_for_data", check_for_data)
    writer_builder.add_node("write_subsection", write_subsection)
    writer_builder.add_node("increment_count_batch", increment_count_batch)
    writer_builder.add_node("section_synthesizer", section_synthesizer)
    writer_builder.add_node("add_ref", add_ref)
    writer_builder.add_node("write_appendices", write_appendices)

    writer_builder.add_edge(START, "load_input")
    writer_builder.add_edge("load_input", "choose_refs")
    writer_builder.add_edge("choose_refs", "write_section")
    writer_builder.add_edge("write_section", "create_subsection_batch")
    writer_builder.add_edge("create_subsection_batch", "check_propose_method")
    writer_builder.add_edge("check_propose_method", "check_for_data")

    writer_builder.add_conditional_edges(
        "check_for_data",
        assign_workers_subsection,
        ["write_subsection"],
    )
    writer_builder.add_edge("write_subsection", "section_synthesizer")
    writer_builder.add_conditional_edges(
        "section_synthesizer", continue_to_write_subsection, {
            "add_ref": "add_ref", 
            "write_section": "write_section",
            "increment_count_batch": "increment_count_batch",
        }
    )
    writer_builder.add_edge("increment_count_batch", "check_propose_method")
    writer_builder.add_edge("add_ref", "write_appendices")
    writer_builder.add_edge("write_appendices", END)

    return writer_builder.compile(checkpointer=checkpointer)
