import asyncio
from langgraph.graph import StateGraph, START, END
from langgraph.types import interrupt, Checkpointer, Send
import logging
import operator
import json
from typing import Annotated
from typing_extensions import TypedDict

from document_setup.src.modules.ingest_docs import VectorDBProcessor
from write_reports.src.modules.references import format_ref_in_ref_section
from write_reports.src.modules.utils.utils_func import (
    get_need_data,
    _load_model,
    _section_synthesizer,
    write,
    write_no_sub,
    write_data,
    are_sections_disjoint,
    reset_or_add,
    validate_sublists,
    get_appendices,
    get_table,
    renumber_table_captions,
    count_table_captions,
    load_current_section,
    add_unique,
    assign_section_logs,
)
from write_reports.src.modules.write_prompt_bank import (
    batch_prompt,
    analyze_subsection_selection, 
    section_writer_without_seminar_instructions, 
    analyze_data_writer,
)
from write_reports.src.schemas.section import (
    ExecutionPlan,
    SectionDescription, 
    SubSectionDescription, 
    SectionContentWithTokenCount, 
)

from translate import LANGUAGE_KIT
from get_llm_response import get_llm, get_answer_with_schema
from utils import markdownify_keep_images

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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
    current_section_batch: ExecutionPlan
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
    existing_refs: list[dict]
    file_descriptions: dict
    field: str
    generated_files: dict
    input_tokens: int
    language: str
    llm_key: str
    lit_review: int
    model_id: str
    need_data: bool
    new_content: dict
    other_sections: list[dict]
    outline: dict
    outline_code: list[str]
    output_tokens: int
    proposal: dict
    tab_count_section: int
    used_files: Annotated[list[str], add_unique]
    references_style: str
    research_papers: list[dict]
    sections_need_data: list[bool]
    subsection_log_assignments: dict
    title: str
    tab_count: Annotated[int, reset_or_add]
    fig_count: Annotated[int, reset_or_add]
    retry: int
    key_points_map: dict
    seen_chunk: dict
    use_web_search: bool
    search_key: str
    user_content: str
    user_id: str


class Worker(TypedDict):
    analyze_log: str
    completed_subsections: Annotated[
        list[SectionContentWithTokenCount],
        operator.add
    ]
    llm_key: str
    current_report: str
    domain: str
    document_id: str
    field: str
    language: str
    proposal: dict
    section: SectionDescription
    generated_files: dict
    used_files: Annotated[list[str], add_unique]
    detailed_logs: list[str]
    file_descriptions: dict
    section_num: int
    model_id: str
    outline: dict
    tab_count_section: int
    tab_count: Annotated[int, reset_or_add]
    fig_count: Annotated[int, reset_or_add]
    subsection: SubSectionDescription
    retry: int
    search_phase: int
    search_key: str
    user_content: str
    chosen_logs: list[int]  # Pre-assigned log indices for this subsection


async def load_model(state: State):
    return await _load_model(state)


async def choose_refs(state: State):
    qdrant_processor = VectorDBProcessor(state["llm_key"], state["db_key"], 10000, state["language"], state["document_id"], state["model_id"])
    input_data, _, _ = await qdrant_processor.load_datas_chat(state["user_id"], "article")
    title_section = {
        "name": "title",
        "content": f"# {state['proposal']['title']}",
        "input_tokens": 0,
        "output_tokens": 0,
        "current_refs": [],
    }
    return {
        "other_sections": [title_section],
        "change_section": True,
        "need_data": False,
        "input_tokens": 0,
        "output_tokens": 0,
        "used_files": [],
        "user_content": input_data,
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
    subsections = getattr(current_section, "subsections", None) or []
    if len(subsections):
        if len(state["other_sections"]) > 2:
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
                ) for subsection in subsections
            ]
            results = await asyncio.gather(*tasks)
            sections_with_data = [result[0] for result in results]
            input_tokens = [result[1] for result in results]
            output_tokens = [result[2] for result in results]
            logger.info(f"[{state['document_id'][:8]}] {len(subsections)}")
            logger.info(f"[{state['document_id'][:8]}] {sections_with_data}")
            state["input_tokens"] += sum(input_tokens)
            state["output_tokens"] += sum(output_tokens)
            state["need_data"] = True
        else:
            state["need_data"] = False
            sections_with_data = []
    else:
        state["need_data"] = False
        sections_with_data = []
    return {
        "other_sections": state["other_sections"],
        "model_id": state["model_id"],
        "llm_key": state["llm_key"],
        "db_key": state["db_key"],
        "change_section": False,
        "current_section": current_section,
        "sections_need_data": sections_with_data,
        "need_data": state["need_data"],
        "input_tokens": state["input_tokens"],
        "output_tokens": state["output_tokens"],
        "tab_count_section": 0,
        "tab_count": "RESET",
        "fig_count": "RESET",
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
        if not state["need_data"] or (state["need_data"] and sum(state["sections_need_data"]) == len(subsections)):
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
        elif state["need_data"]:
            try:
                split_point = state["sections_need_data"].index(True)
                final_batch = [subsections[:split_point]] + [
                    [item] for item in subsections[split_point:]
                ] if split_point else [[item] for item in subsections]
            except Exception:
                final_batch: list[list[SubSectionDescription]] = []
                for batch in subsections:
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


async def check_for_data(state: State):
    updates = {}
    if "current_section" not in state:
        logger.warning(f"[{state['document_id'][:8]}] 'current_section' missing in state during check_for_data. Reloading.")
        state, current_section, _ = await load_current_section(state)
        state["current_section"] = current_section
        updates["current_section"] = current_section

    subsections = getattr(state["current_section"], "subsections", None) or []
    if len(subsections):
        current_batch_subsections = state["current_section_batch"][state["current_count_batch"]]
        current_subsections_count = sum([len(state["current_section_batch"][batch]) for batch in range(state["current_count_batch"])])
        need_data = any(
            state["sections_need_data"][current_subsections_count:current_subsections_count + len(current_batch_subsections)]
        )
        if need_data:
            qdrant_processor = VectorDBProcessor(state["llm_key"], state["db_key"], 10000, state["language"], state["document_id"], state["model_id"])
            input_data, _, _ = await qdrant_processor.load_datas_chat(state["user_id"], "article")
            llm = get_llm(state["model_id"], state["llm_key"])
            file_descriptions = state.get("file_descriptions", {})
            if not file_descriptions:
                try:
                    file_descriptions = json.loads(state.get("generated_files", {}).get("file_descriptions", "{}"))
                except (TypeError, json.JSONDecodeError):
                    file_descriptions = {}
            state.get("generated_files", {}).pop("file_descriptions", None)
            state["file_descriptions"] = file_descriptions
            temp_state = {
                "section": state["current_section"],
                "document_id": state["document_id"],
                "detailed_logs": state["detailed_logs"],
                "generated_files": state.get("generated_files", {}),
                "file_descriptions": file_descriptions,
                "proposal": state.get("proposal", {}),
                "field": state.get("field", ""),
                "domain": state.get("domain", ""),
            }
            log_assignments, inp_tok, out_tok = await assign_section_logs(temp_state, llm)
            logger.info(f"[{state['document_id'][:8]}] Section-level log assignment completed: {log_assignments}")
            return {
                **updates,
                "document_id": state["document_id"],
                "user_content": input_data,
                "subsection_log_assignments": log_assignments,
                "file_descriptions": file_descriptions,
                "input_tokens": state["input_tokens"] + inp_tok,
                "output_tokens": state["output_tokens"] + out_tok,
            }
        else:
            return updates
    return updates


def assign_workers_subsection(state: State):
    current_report = "\n\n".join(
        [
            f"{other_section['content']}" for other_section in state["other_sections"]
        ]
    )
    subsections = getattr(state["current_section"], "subsections", None) or []
    if len(subsections):
        current_batch_subsections = state["current_section_batch"][state["current_count_batch"]]
        current_subsections_count = sum([len(state["current_section_batch"][batch]) for batch in range(state["current_count_batch"])])
        need_data = any(
            state["sections_need_data"][current_subsections_count:current_subsections_count + len(current_batch_subsections)]
        )

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
                    "model_id": state["model_id"],
                    "proposal": state["proposal"],
                    "section": state["current_section"],
                    "outline": state["outline"],
                    "subsection": subsection,
                    "tab_count": state["tab_count"],
                    "fig_count": state["fig_count"],
                    "retry": state["retry"],
                    "llm_key": state["llm_key"],
                    "search_phase": 3 if state["use_web_search"] else 0,
                    "search_key": state["search_key"],
                    "document_id": state["document_id"],
                    "user_content": state["user_content"],
                    "chosen_logs": chosen_logs,
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
                "model_id": state["model_id"],
                "proposal": state["proposal"],
                "section": state["current_section"],
                "outline": state["outline"],
                "tab_count": state["tab_count"],
                "fig_count": state["fig_count"],
                "retry": state["retry"],
                "subsection": SubSectionDescription(detail_description="", subheading="", subheading_word_count="0-0", refs=[]),
                "llm_key": state["llm_key"],
                "search_phase": 3 if state["use_web_search"] else 0,
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
            chosen_logs = state.get("chosen_logs", None)
            section_with_token_count, tab_count, fig_count = await write_data(
                state,
                llm,
                analyze_data_writer,
                state["user_content"],
                chosen_logs=chosen_logs,
            )
            return {
                "completed_subsections": [section_with_token_count],
                "tab_count": tab_count,
                "fig_count": fig_count,
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
    has_sub = bool(getattr(state["current_section"], "subsections", None))
    if state["current_count_batch"] == 0:
        if has_sub:
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
        state["other_sections"].append(section_dict)
        if state["current_count_batch"] == len(state["current_section_batch"]) - 1 or not has_sub:
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
    return len(state["other_sections"]) - 1 != len(state["sections"])


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

    writer_builder.add_node("load_model", load_model)
    writer_builder.add_node("choose_refs", choose_refs)
    writer_builder.add_node("write_section", write_section)
    writer_builder.add_node("create_subsection_batch", create_subsection_batch)
    writer_builder.add_node("check_for_data", check_for_data)
    writer_builder.add_node("write_subsection", write_subsection)
    writer_builder.add_node("increment_count_batch", increment_count_batch)
    writer_builder.add_node("section_synthesizer", section_synthesizer)
    writer_builder.add_node("add_ref", add_ref)
    writer_builder.add_node("write_appendices", write_appendices)

    writer_builder.add_edge(START, "load_model")
    writer_builder.add_edge("load_model", "choose_refs")
    writer_builder.add_edge("choose_refs", "write_section")
    writer_builder.add_edge("write_section", "create_subsection_batch")
    writer_builder.add_edge("create_subsection_batch", "check_for_data")
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
    writer_builder.add_conditional_edges(
        "increment_count_batch", 
        assign_workers_subsection,
        ["write_subsection"],
    )
    writer_builder.add_edge("add_ref", "write_appendices")
    writer_builder.add_edge("write_appendices", END)

    return writer_builder.compile(checkpointer=checkpointer)
