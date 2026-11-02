import asyncio
from langgraph.graph import StateGraph, START, END
from langgraph.types import interrupt, Checkpointer, Send
import logging
import operator
from typing import Annotated
from typing_extensions import TypedDict

from write_reports.src.modules.references import format_ref_in_ref_section
from translate import LANGUAGE_KIT
from write_reports.src.modules.utils.utils_func import (
    _load_model,
    _section_synthesizer,
    write,
    write_no_sub,
    are_sections_disjoint,
    write_method,
    _get_proposed_method_section,
    validate_sublists,
    get_appendices,
    get_table,
    get_survey_questions_table,
    load_current_section,
)
from write_reports.src.modules.write_prompt_bank import (
    batch_prompt,
    section_writer_without_seminar_instructions,
    methodology_writer,
)
from write_reports.src.schemas.section import (
    ExecutionPlan,
    SectionDescription, 
    SubSectionDescription, 
    SectionContentWithTokenCount, 
    MethodSection,
)

from get_llm_response import get_llm, get_answer_with_schema
from utils import markdownify_keep_images

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


class State(TypedDict):
    change_section: bool
    completed_subsections: Annotated[
        list[SectionContentWithTokenCount],
        operator.add
    ]
    current_count_batch: int
    current_section: SectionDescription
    current_section_batch: ExecutionPlan
    db_key: str
    document_id: str
    domain: str
    end_section: bool
    embed_model: str
    embed_tokens: int
    embed_uuids: list[str]
    existing_refs: list[dict]
    field: str
    input_tokens: int
    language: str
    llm_key: str
    lit_review: int
    model_id: str
    new_content: dict
    other_sections: list[dict]
    outline: dict
    outline_code: list[str]
    output_tokens: int
    section_num: int
    proposal: dict
    propose_method: int
    references_style: str
    research_papers: dict
    title: str
    tab_count_section: int
    retry: int
    key_points_map: dict
    seen_chunk: dict
    use_web_search: bool
    search_key: str


class Worker(TypedDict):
    llm_key: str
    completed_subsections: Annotated[
        list[SectionContentWithTokenCount],
        operator.add
    ]
    current_report: str
    domain: str
    document_id: str
    field: str
    language: str
    model_id: str
    need_method: bool
    proposal: dict
    retry: int
    section: SectionDescription
    outline: dict
    section_num: int
    subsection: SubSectionDescription
    search_phase: int
    search_key: str


async def load_model(state: State):
    return await _load_model(state)


async def choose_refs(state: State):
    title_section = {
        "name": "title",
        "content": f"# {state["proposal"]["title"]}",
        "input_tokens": 0,
        "output_tokens": 0,
        "current_refs": [],
    }
    llm = get_llm(state["model_id"], state["llm_key"])
    headings = [
        (section.get("heading") or section.get("name") or section.get("title") or "").split(" ", 1)[-1] if "." in (section.get("heading") or section.get("name") or section.get("title") or "").split(" ")[0] else (section.get("heading") or section.get("name") or section.get("title") or "") 
        for section in state["outline"]["outline"]
    ]
    propose_method = ["phương pháp nghiên cứu", "thiết kế nghiên cứu", "research design", "research methodology", "proposed method"]
    found_propose_method = False
    for section_count, heading in enumerate(headings):
        if heading.lower() in propose_method:
            method_section = MethodSection(propose_method=section_count + 1)
            input_tokens = 0
            output_tokens = 0
            found_propose_method = True
            break
    if not found_propose_method:
        error, success, method_section, input_tokens, output_tokens = await _get_proposed_method_section(state['document_id'][:8], headings, state["model_id"], llm)
        if not success:
            if error.status_code in [401, 403, 429, 500]:
                raise error
            method_section = MethodSection(propose_method=2)
    return {
        "other_sections": [title_section],
        "change_section": True,
        "need_method": False,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "propose_method": method_section.propose_method,
    }


async def write_section(state: State):
    if len(state["other_sections"]) > 1:
        result = interrupt({
            "break_point": "chapter_break",
            "ref_table": [],
            "disable_edit": True,
        })
        state["model_id"] = result["model_id"]
        state["llm_key"] = result["llm_key"]
        state["db_key"] = result["db_key"]
        state["use_web_search"] = result["use_web_search"]
        state["search_key"] = result["search_key"]
    state, current_section, _ = await load_current_section(state)
    return {
        "other_sections": state["other_sections"],
        "model_id": state["model_id"],
        "llm_key": state["llm_key"],
        "db_key": state["db_key"],
        "change_section": False,
        "current_count_batch": 0,
        "current_section": current_section,
        "tab_count_section": 0,
        "seen_chunk": state["seen_chunk"],
        "research_papers": state["research_papers"],
        "key_points_map": state["key_points_map"],
        "outline": state["outline"],
        "use_web_search": state["use_web_search"],
        "search_key": state["search_key"],
    }


async def create_subsection_batch(state: State):
    logger.info(f"[{state['document_id'][:8]}] Creating batches for section: '{state['current_section'].heading}'")
    subsections = getattr(state["current_section"], "subsections", None) or []
    if len(subsections):
        content = f"Here is the section data:\n{state['current_section']}\nGenerate from 0 to {len(subsections) - 1}"
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
                success = batch_len == len(subsections)
            except Exception as e:
                logger.error(f"[{state['document_id'][:8]}] {e}")
        if not success or not are_sections_disjoint(current_section_batch.plan) or not validate_sublists(current_section_batch.plan):                    
            current_section_batch = ExecutionPlan(plan=[[i] for i in range(len(subsections))])
        if current_section_batch.plan[0][0] != 0 and success:
            current_section_batch.plan = [[[item - current_section_batch.plan[0][0]] for item in batch] for batch in current_section_batch.plan]
        final_batch: list[list[SubSectionDescription]] = []
        for batch in current_section_batch.plan:
            subsection_batch: list[SubSectionDescription] = []
            for item in batch:
                subsection_batch.append(subsections[item])
            if subsection_batch:
                final_batch.append(subsection_batch)
        return {
            "current_count_batch": 0,
            "current_section_batch": final_batch,
            "input_tokens": state["input_tokens"],
            "output_tokens": state["output_tokens"],
        }
    else:
        return {
            "current_section_batch": [],
            "input_tokens": state["input_tokens"],
            "output_tokens": state["output_tokens"],
        }


def assign_workers_subsection(state: State):
    # Tính MỘT lần TRƯỚC if/else: nhánh không có subsection vẫn cần current_report.
    # Trước đây biến chỉ gán trong nhánh if → UnboundLocalError (lỗi 633 chuyen_de_2).
    current_report = markdownify_keep_images(
        "\n\n".join([f"{other_section["content"]}" for other_section in state["other_sections"]])
    )
    if len(state["current_section_batch"]):
        return [
            Send(
                "write_subsection",
                {
                    "outline": state["outline"],
                    "section": state["current_section"],
                    "subsection": subsection,
                    "proposal": state["proposal"],
                    "language": state["language"],
                    "field": state["field"],
                    "domain": state["domain"],
                    "current_report": current_report,
                    "retry": state["retry"],
                    "section_num": len(state["other_sections"]) - 1 if not state["current_count_batch"] else len(state["other_sections"]) - 2,
                    "model_id": state["model_id"],
                    "llm_key": state["llm_key"],
                    "need_method": len(state["other_sections"]) >= state["propose_method"],
                    "search_phase": 2 if state["use_web_search"] else 0,
                    "search_key": state["search_key"],
                    "document_id": state["document_id"],
                },
            ) for subsection in state["current_section_batch"][state["current_count_batch"]]
        ]
    else:
        return Send(
            "write_subsection",
            {
                "outline": state["outline"],
                "section": state["current_section"],
                "proposal": state["proposal"],
                "language": state["language"],
                "field": state["field"],
                "domain": state["domain"],
                "current_report": current_report,
                "subsection": SubSectionDescription(detail_description="", subheading="", subheading_word_count="0-0"),
                "retry": state["retry"],
                "model_id": state["model_id"],
                "llm_key": state["llm_key"],
                "section_num": 0,
                "need_method": False,
                "search_phase": 2 if state["use_web_search"] else 0,
                "search_key": state["search_key"],
                "document_id": state["document_id"],
            },
        )


async def write_subsection(state: Worker):
    """Worker writes a section of the report based on given information"""
    llm = get_llm(state["model_id"], state["llm_key"])
    if len(state["subsection"].subheading):
        if state["need_method"]:
            section_with_token_count = await write_method(state, llm, methodology_writer)
        else:
            section_with_token_count = await write(state, llm, section_writer_without_seminar_instructions)
    else:
        section_with_token_count = await write_no_sub(state, llm, section_writer_without_seminar_instructions)
    return {"completed_subsections": [section_with_token_count]}


def continue_to_write_subsection(state: State):
    has_sub = bool(getattr(state["current_section"], "subsections", None))
    if state["current_count_batch"] == len(state["current_section_batch"]) - 1 or not has_sub:
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
        has_sub = bool(getattr(state["current_section"], "subsections", None))
        if has_sub:
            num_subsection = len(state["current_section_batch"][state["current_count_batch"]])
        else:
            num_subsection = 1
        section_count = len(state["other_sections"]) - 1
        section_dict, state["existing_refs"] = await _section_synthesizer(state, section_count, num_subsection)
        section_dict["section_code"] = state["outline_code"][section_count]
        section_dict, table_count = get_table(section_dict, section_count + 1, state["tab_count_section"], state["language"])
        state["other_sections"].append(section_dict)
        if state["current_count_batch"] == len(state["current_section_batch"]) - 1:
            if len(state["other_sections"]) == 2:
                survey_questions_table, state["existing_refs"] = get_survey_questions_table(state, section_count, table_count)
                section_dict["content"] = section_dict["content"] + "\n" + survey_questions_table
            if len(state["other_sections"]) - 1 == len(state["outline"]["outline"]):
                end_section = True
        return {
            "new_content": section_dict,
            "tab_count_section": table_count,
            "end_section": end_section,
            "other_sections": state["other_sections"],
            "existing_refs": state["existing_refs"],
        }
    else:
        num_subsection = sum([len(batch) for batch in state["current_section_batch"][:state["current_count_batch"] + 1]])
        existing_subsection = num_subsection - len(state["current_section_batch"][state["current_count_batch"]])
        section_count = len(state["other_sections"]) - 2
        section_dict, state["existing_refs"] = await _section_synthesizer(state, section_count, num_subsection)
        section_dict["section_code"] = state["outline_code"][section_count]
        content = section_dict["content"].split(f"## {section_count + 1}.{existing_subsection + 1}. ")[-1]
        section_dict["content"] = f"## {section_count + 1}.{existing_subsection + 1}. " + content
        section_dict, table_count = get_table(section_dict, section_count + 1, state["tab_count_section"], state["language"])
        if state["current_count_batch"] == len(state["current_section_batch"]) - 1:
            if len(state["other_sections"]) == 3:
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
    ref_section = {
        "name": "references",
        "content": f"# {language_dict["References"]}\n\n{"\n\n".join(refs)}".replace(r'\n', '\n'),
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

    writer_builder.add_node("load_model", load_model)
    writer_builder.add_node("choose_refs", choose_refs)
    writer_builder.add_node("write_section", write_section)
    writer_builder.add_node("create_subsection_batch", create_subsection_batch)
    writer_builder.add_node("write_subsection", write_subsection)
    writer_builder.add_node("increment_count_batch", increment_count_batch)
    writer_builder.add_node("section_synthesizer", section_synthesizer)
    writer_builder.add_node("add_ref", add_ref)
    writer_builder.add_node("write_appendices", write_appendices)

    writer_builder.add_edge(START, "load_model")
    writer_builder.add_edge("load_model", "choose_refs")
    writer_builder.add_edge("choose_refs", "write_section")
    writer_builder.add_edge("write_section", "create_subsection_batch")
    writer_builder.add_conditional_edges(
        "create_subsection_batch",
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
    writer_builder.add_conditional_edges(
        "increment_count_batch", 
        assign_workers_subsection,
        ["write_subsection"],
    )
    writer_builder.add_edge("add_ref", "write_appendices")
    writer_builder.add_edge("write_appendices", END)

    return writer_builder.compile(checkpointer=checkpointer)
