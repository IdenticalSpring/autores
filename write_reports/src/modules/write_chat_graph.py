import asyncio
from langgraph.graph import StateGraph, START, END
from langgraph.types import Send, Checkpointer
import logging
import operator
from typing import Annotated
from typing_extensions import TypedDict

from exception_type import AIERROR
from document_setup.src.modules.ingest_docs import VectorDBProcessor
from document_setup.src.modules.outline_generation import OutlineGeneration
from ai_chatbot.src.modules.ai_in_doc import AIInDoc
from write_reports.src.modules.references import format_ref_in_ref_section
from translate import LANGUAGE_KIT
from write_reports.src.modules.utils.utils_func import (
    _section_synthesizer,
    write,
    write_no_sub,
    are_sections_disjoint,
    validate_sublists,
    get_table,
)
from write_reports.src.modules.write_prompt_bank import (
    batch_prompt,
    section_writer_without_seminar_instructions, 
    GET_USER_INFO_PROMPT,
)
from write_reports.src.schemas.section import (
    ExecutionPlan,
    SectionDescription, 
    SectionContentWithTokenCount, 
    SubSectionDescription, 
    UserInfo,
    RefsUsage,
)

from get_llm_response import get_llm, get_answer_with_schema
from utils import get_mongodb_client, markdownify_keep_images

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


class State(TypedDict):
    completed_subsections: Annotated[
        list[SectionContentWithTokenCount],
        operator.add
    ]
    current_count_batch: int
    current_section: SectionDescription
    current_section_batch: list[list[SubSectionDescription]]
    end_section: bool
    new_content: dict
    retry: int

    db_key: str
    document_id: str
    user_id: str
    conv_id: str

    embed_model: str
    llm_key: str
    model_id: str

    embed_tokens: int
    embed_uuids: list[str]
    input_tokens: int
    output_tokens: int

    existing_refs: list[dict]
    key_points_map: dict
    language: str
    lit_review: int
    other_sections: list[dict]
    outline: dict
    outline_code: list[str]
    outline_str: str
    proposal: dict
    research_papers: list[dict]
    summary: dict
    user_content: str

    domain: str
    field: str
    tab_count_section: int
    retry: int
    use_web_search: bool


class Worker(TypedDict):
    completed_subsections: Annotated[
        list[SectionContentWithTokenCount],
        operator.add
    ]

    current_report: str
    domain: str
    document_id: str
    field: str
    language: str
    outline: dict
    proposal: dict
    section: SectionDescription
    subsection: SubSectionDescription
    user_content: str

    llm_key: str
    model_id: str
    retry: int
    search_phase: int


async def load_refs(state: State):
    logger.info(f"[{state['document_id'][:8]}] Load files")
    qdrant_processor = VectorDBProcessor(state["llm_key"], state["db_key"], 10000, state["language"], state["document_id"], state["model_id"])
    mongo_client = get_mongodb_client()
    admin = mongo_client["admin"]
    outlines_collection = admin["outlines"]
    doc_record = await outlines_collection.find_one({"documentId": state["document_id"]})
    if doc_record is None:
        # Trước đây: doc_record["code"] → 'NoneType' object is not subscriptable (error 653 khó hiểu)
        raise AIERROR(
            653,
            f"Không tìm thấy outline cho document_id={state['document_id']}. "
            "Cần tạo document/outline trước khi gọi event 'writer'.",
        )
    logger.info(f"[{state['document_id'][:8]}] Load refs")
    research_papers, input_tokens, output_tokens, embed_tokens = await qdrant_processor.load_refs_chat(state["user_id"])
    
    logger.info(f"[{state['document_id'][:8]}] Load data")
    input_data, data_input_tokens, data_output_tokens = await qdrant_processor.load_datas_chat(state["user_id"], "document-request")
    # data_helper = DataHelper(state["user_id"], state["model_id"], state["llm_key"], state["conv_id"])
    # input_idea, idea_input_tokens, idea_output_tokens = await data_helper.load_file_content()
    # await data_helper.collection.drop()
    key_points_map = {doc["title"]: doc["key_points"] for doc in research_papers}
    return {
        "research_papers": research_papers,
        "input_tokens": input_tokens + data_input_tokens,
        "output_tokens": output_tokens + data_output_tokens,
        "embed_tokens": embed_tokens,
        "user_content": input_data,
        "key_points_map": key_points_map,
        "existing_refs": [],
        "outline_code": [doc_record["code"]],
    }


def route_input(state: State):
    logger.info(f"[{state['document_id'][:8]}] Routing")
    if "research_topic" in state["summary"]:
        return "write"
    else:
        return "edit"
    

async def edit_paragraph(state: State):
    mongo_client = get_mongodb_client()
    module = AIInDoc(mongo_client, state["model_id"], state["llm_key"], state["db_key"], state["document_id"], state["language"], 10000)
    response = await module.process_paragraph("", "", "better", None, state["summary"], state["user_content"])
    section = {
        "name": "",
        "content": f"{response[0]}",
        "outline_code": state["outline_code"][0],
        "input_tokens": module.input_tokens,
        "output_tokens": module.output_tokens,
        "embed_tokens": module.embed_tokens,
        "current_refs": [],
    }
    return {
        "other_sections": [section],
        "input_tokens": state["input_tokens"],
        "output_tokens": state["output_tokens"],
        "embed_tokens": state["embed_tokens"],
    }
    

async def parse_outline(state: State):
    mongo_client = get_mongodb_client()
    module_outline = OutlineGeneration(state["document_id"], mongo_client, state["model_id"], state["language"], state["llm_key"], 10000)
    outline, state["summary"], input_tokens, output_tokens, embed_tokens = await module_outline.generate_outline_description_chatbot(
        state["summary"], 
        state["outline_str"], 
        state["research_papers"], 
        state["user_content"], 
        state["db_key"],
    )
    title_section = {
        "name": "title",
        "content": f"# {outline["title"]}",
        "input_tokens": 0,
        "output_tokens": 0,
        "embed_tokens": 0,
        "current_refs": [],
    }
    llm = get_llm(state["model_id"], state["llm_key"])
    error, success, user_info, info_input_tokens, info_output_tokens = await get_answer_with_schema(
        state["document_id"][:8],
        llm,
        GET_USER_INFO_PROMPT,
        f"User's research brief: {state.get("research_brief", "")}",
        UserInfo,
    )
    if not success:
        raise error
    return {
        "retry": 3,
        "other_sections": [title_section],
        "proposal": state["summary"],
        "field": user_info.field,
        "domain": user_info.domain,
        "outline": outline,
        "input_tokens": state["input_tokens"] + input_tokens + info_input_tokens,
        "output_tokens": state["output_tokens"] + output_tokens + info_output_tokens,
        "embed_tokens": state["embed_tokens"] + embed_tokens,
    }


async def write_section(state: State):

    heading = state["outline"]["outline"][len(state["other_sections"]) - 1]
    subsections: list[SubSectionDescription] = []

    # Check if this section has subheadings (subsections)
    # If subheadings is None, this is a section-only content (no subsections)
    if heading.get("subheadings") is not None:
        mongo_client = get_mongodb_client()
        admin_db = mongo_client["admin"]
        collection_chunk = admin_db["reports_refs_chunks"]
        for sub_section in heading["subheadings"]:
            parsed_refs: list[RefsUsage] = []
            for ref in sub_section["refs"]:
                ref_chunk = await collection_chunk.find_one({"_id": ref["ref_chunk"]})
                key_points = state["key_points_map"][ref["title"]]
                ref = RefsUsage(title=ref["title"], ref_chunk=ref_chunk["content"], usage=ref["usage_description"], summary=str(key_points))
                parsed_refs.append(ref)
            subsection = SubSectionDescription(
                detail_description=sub_section.get("detail_description", ""),
                subheading=sub_section["subheading"].split(" ", 1)[-1] if "." in sub_section["subheading"].split(" ")[0] else sub_section["subheading"],
                subheading_word_count=sub_section.get("subheading_word_count", "0-0"),
                refs=parsed_refs,
            )
            subsections.append(subsection)

    current_section = SectionDescription(
        heading=heading["heading"].split(" ", 1)[-1] if "." in heading["heading"].split(" ")[0] else heading["heading"],
        overview=heading["overview"],
        word_count=heading["word_count"],
        subsections=subsections  # Will be empty list if no subheadings
    )
    return {
        "current_section": current_section,
        "tab_count_section": 0,
    }


async def create_subsection_batch(state: State):
    logger.info(f"[{state['document_id'][:8]}] Creating batches for section: '{state["current_section"].heading}'")
    content = f"Here is the section data:\n{state["current_section"]}\nGenerate from 0 to {len(state["current_section"].subsections) - 1}"
    if len(state["current_section"].subsections):
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
    else:
        # Section without subsections - set current_count_batch to 0
        return {
            "current_count_batch": 0,
            "current_section_batch": [],
            "input_tokens": state["input_tokens"],
            "output_tokens": state["output_tokens"],
        }


def assign_workers_subsection(state: State):
    # Build current report from existing sections (needed for both paths)
    current_report = "\n\n".join(
        [
            f"{other_section["content"]}" for other_section in state["other_sections"]
        ]
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
                    "current_report": markdownify_keep_images(current_report),
                    "retry": state["retry"],
                    "model_id": state["model_id"],
                    "llm_key": state["llm_key"],
                    "user_content": state["user_content"],
                    "search_phase": 4 if state["use_web_search"] else 0,
                    "document_id": state["document_id"],
                },
            ) for subsection in state["current_section_batch"][state["current_count_batch"]]
        ]
    else:
        # Section without subheadings - write as single section
        return Send(
            "write_subsection",
            {
                "outline": state["outline"],
                "section": state["current_section"],
                "proposal": state["proposal"],
                "language": state["language"],
                "field": state["field"],
                "domain": state["domain"],
                "current_report": markdownify_keep_images(current_report),
                "subsection": SubSectionDescription(detail_description="", subheading="", subheading_word_count="0-0", refs=[]),
                "retry": state["retry"],
                "model_id": state["model_id"],
                "llm_key": state["llm_key"],
                "user_content": state["user_content"],
                "search_phase": 4 if state["use_web_search"] else 0,
                "document_id": state["document_id"],
            },
        )


async def write_subsection(state: Worker):
    """Worker writes a section of the report based on given information"""
    llm = get_llm(state["model_id"], state["llm_key"])
    if len(state["subsection"].subheading):
        section_with_token_count = await write(state, llm, section_writer_without_seminar_instructions, state["user_content"])
    else:
        section_with_token_count = await write_no_sub(state, llm, section_writer_without_seminar_instructions, state["user_content"])
    return {"completed_subsections": [section_with_token_count]}


def continue_to_write_subsection(state: State):
    if state["current_count_batch"] == len(state["current_section_batch"]) - 1 or not len(state["current_section"].subsections):
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
        if len(state["current_section"].subsections):
            num_subsection = len(state["current_section_batch"][state["current_count_batch"]])
        else:
            num_subsection = 1
        section_count = len(state["other_sections"]) - 1
        section_dict, state["existing_refs"] = await _section_synthesizer(state, section_count, num_subsection)
        section_dict["section_code"] = state["outline_code"][0]
        section_dict, table_count = get_table(section_dict, section_count + 1, state["tab_count_section"], state["language"])
        if state["current_count_batch"] == len(state["current_section_batch"]) - 1:
            if len(state["other_sections"]) - 1 == len(state["outline"]["outline"]):
                end_section = True
        state["other_sections"].append(section_dict)
        return {
            "tab_count_section": table_count,
            "new_content": section_dict,
            "end_section": end_section,
            "other_sections": state["other_sections"],
            "existing_refs": state["existing_refs"],
        }
    else:
        num_subsection = sum([len(batch) for batch in state["current_section_batch"][:state["current_count_batch"] + 1]])
        existing_subsection = num_subsection - len(state["current_section_batch"][state["current_count_batch"]])
        section_count = len(state["other_sections"]) - 2
        section_dict, state["existing_refs"] = await _section_synthesizer(state, section_count, num_subsection)
        section_dict["section_code"] = state["outline_code"][0]
        content = section_dict["content"].split(f"## {section_count + 1}.{existing_subsection + 1}. ")[-1]
        section_dict["content"] = f"## {section_count + 1}.{existing_subsection + 1}. " + content
        section_dict, table_count = get_table(section_dict, section_count + 1, state["tab_count_section"], state["language"])
        if state["current_count_batch"] == len(state["current_section_batch"]) - 1:
            if len(state["other_sections"]) - 1 == len(state["outline"]["outline"]):
                end_section = True
        state["other_sections"][-1]["content"] = state["other_sections"][-1]["content"] + "\n\n" + section_dict["content"]
        return {
            "tab_count_section": table_count,
            "new_content": section_dict,
            "end_section": end_section,
            "other_sections": state["other_sections"],
            "existing_refs": state["existing_refs"],
        }


def continue_to_write(state: State):
    return len(state["other_sections"]) - 1 != len(state["outline"]["outline"])


async def add_ref(state: State):
    if state["research_papers"]:
        tasks = [
            format_ref_in_ref_section(
                title, 
                state["research_papers"], 
                state["proposal"].get("references_style", "IEEE")
            ) for title in state["existing_refs"]
        ]
        refs = await asyncio.gather(*tasks)
        if state["proposal"].get("references_style", "IEEE") in ["Vancouver", "IEEE"]:
            refs = [f"[{i + 1}] {ref}" for i, ref in enumerate(refs)]
        language_dict = LANGUAGE_KIT.get(state["language"].lower(), LANGUAGE_KIT["tiếng việt"])
        ref_section = {
            "name": "references",
            "content": f"# {language_dict["References"]}\n\n{"\n\n".join(refs)}".replace(r'\n', '\n'),
            "input_tokens": 0,
            "output_tokens": 0,
            "embed_tokens": 0,
            "section_code": state["outline_code"][0],
            "current_refs": [],
        }
    else:
        language_dict = LANGUAGE_KIT.get(state["language"].lower(), LANGUAGE_KIT["tiếng việt"])
        ref_section = {
            "name": "",
            "content": "",
            "input_tokens": 0,
            "output_tokens": 0,
            "embed_tokens": 0,
            "section_code": state["outline_code"][0],
            "current_refs": [],
        }
    state["other_sections"].append(ref_section)
    return {
        "other_sections": state["other_sections"],
        "input_tokens": state["input_tokens"],
        "output_tokens": state["output_tokens"],
        "embed_tokens": state["embed_tokens"],
    }


async def get_graph(checkpointer: Checkpointer):
    writer_builder = StateGraph(State)

    writer_builder.add_node("load_refs", load_refs)
    writer_builder.add_node("edit_paragraph", edit_paragraph)
    writer_builder.add_node("parse_outline", parse_outline)
    writer_builder.add_node("write_section", write_section)
    writer_builder.add_node("create_subsection_batch", create_subsection_batch)
    writer_builder.add_node("write_subsection", write_subsection)
    writer_builder.add_node("increment_count_batch", increment_count_batch)
    writer_builder.add_node("section_synthesizer", section_synthesizer)
    writer_builder.add_node("add_ref", add_ref)

    writer_builder.add_edge(START, "load_refs")
    writer_builder.add_conditional_edges(
        "load_refs", route_input, {
            "write": "parse_outline", 
            "edit": "edit_paragraph",
        }
    )
    writer_builder.add_edge("edit_paragraph", END)
    writer_builder.add_edge("parse_outline", "write_section")
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
    writer_builder.add_edge("add_ref", END)

    return writer_builder.compile(checkpointer=checkpointer)