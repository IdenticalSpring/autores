from bson import ObjectId
from langchain_core.messages import HumanMessage, SystemMessage, AnyMessage, AIMessage
from langgraph.graph import StateGraph, START
from langgraph.graph.message import add_messages
from langgraph.types import Command, interrupt, Checkpointer
from typing_extensions import TypedDict, Annotated, Literal
from typing import Optional, Dict, Any, List, Union
from pydantic import BaseModel, Field
from enum import Enum
import logging
import json
import re
from pymongo import ASCENDING

from data_analysis.src.modules.prompt_bank import (
    assumptions_instructions,
    model_instructions,
    core_variables_instructions,
    survey_instructions,
    questions_instructions,
    quantitative_questions_instructions,
    finalize_instructions,
    reference_selection_instructions
)
from data_analysis.src.configs.app import settings
from data_analysis.src.modules.utils import extract_mermaid, process_text_with_sketch, parse_raw_variables, manually_parse_survey_to_variables, title_matching, get_uncite_survey
from get_llm_response import get_llm, get_answer_with_schema, get_answer, calculate_token_scaling
from utils import get_s3_client, get_mongodb_client, markdownify_keep_images
from translate import LANGUAGE_KIT
from langchain_core.tools import tool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inline Schemas
def append_list(left: List, right: List) -> List:
    """Append list to left list."""
    return left + right

def unique_list(left: List, right: List) -> List:
    """Return a list with unique items, preserving order."""
    seen = set(left)
    result = left.copy()
    for item in right:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result

# Updated ModelRecommendation schema - simplified
class ModelRecommendation(BaseModel):
    thinking_process: str = Field(..., description="Detailed thinking process and justification for the model, including classification, justification, measurement, and analysis.")
    sketch: str = Field(..., description="The complete sketch diagram using the exact sketch syntax format.")
    model_type: Literal["Simple", "Extended", "Comprehensive"] = Field(
        default="Simple",
        description="Exactly one of Simple, Extended, or Comprehensive. Never use a theory, estimator, or method name here."
    )


def _normalize_model_recommendation(result: ModelRecommendation, state: Dict[str, Any]) -> ModelRecommendation:
    """Keep the persisted model class within the three supported categories."""
    sketch = result.sketch or ""
    dependent_variables = len(re.findall(r"^\s*DV\s*:", sketch, re.IGNORECASE | re.MULTILINE))
    has_mediation_or_moderation = bool(re.search(
        r"^\s*(?:M\d+|H\d+\s*\(\s*moderate\s*\))\s*:",
        sketch,
        re.IGNORECASE | re.MULTILINE,
    ))

    has_latent_variable = False
    accepted_dependent_variables = 0
    for element in state.get("elements", []):
        if element.get("node") != "recommend_variables" or not element.get("accepted"):
            continue
        variables = element.get("object")
        variables = variables.get("variables", []) if isinstance(variables, dict) else getattr(variables, "variables", [])
        for variable in variables:
            measurement_type = variable.get("measurement_type", "") if isinstance(variable, dict) else getattr(variable, "measurement_type", "")
            variable_type = variable.get("variable_type", "") if isinstance(variable, dict) else getattr(variable, "variable_type", "")
            if any(term in str(variable_type).lower() for term in ("dependent", "phụ thuộc", "dv")):
                accepted_dependent_variables += 1
            if "latent" in str(measurement_type).lower() or "tiềm ẩn" in str(measurement_type).lower():
                has_latent_variable = True

    model_type = (
        "Comprehensive" if max(dependent_variables, accepted_dependent_variables) > 1 or has_latent_variable
        else "Extended" if has_mediation_or_moderation
        else "Simple"
    )
    result.model_type = model_type
    if re.search(r"^\s*ModelType\s*:", sketch, re.IGNORECASE | re.MULTILINE):
        result.sketch = re.sub(
            r"^\s*ModelType\s*:\s*.*$",
            f"ModelType: {model_type}",
            sketch,
            count=1,
            flags=re.IGNORECASE | re.MULTILINE,
        )
    elif sketch:
        result.sketch = f"ModelType: {model_type}\n{sketch}"
    return result

# Updated VariableRecommend schema - table format
class VariableRecommend(BaseModel):
    name: str
    variable_type: str  # Độc lập, Phụ thuộc, Trung gian, Điều tiết, Kiểm soát
    measurement_type: str  # Tiềm ẩn (Latent) or Quan sát (Observable)
    description: str
    scale: str

class VariablesRecommend(BaseModel):
    variables: list[VariableRecommend]

class HypothesisType(str, Enum):
    null = "null"
    primary = "primary"
    mediator = "mediator"
    moderator = "moderator"
    control = "control"

class AssumptionRecommend(BaseModel):
    hypothesis_id: str = Field(..., description='e.g., "H0", "H1", "M1", "C1"')
    statement: str = Field(..., description='Full hypothesis text')
    type: HypothesisType = Field(..., description='e.g., "null", "primary", "mediator"')
    independent_variable: Optional[str] = Field(None, description='Independent Variable, e.g., "Price"')
    dependent_variable: Optional[str] = Field(None, description='Dependent Variable, e.g., "Purchase Intention"')
    secondary_variables: Optional[List[str]] = Field(default_factory=list, description='For secondaries: e.g., ["Perceived Value (mediator)"] or ["Age, gender (controls)"]')
    affected_path: Optional[str] = Field(None, description='Path notation for secondaries, e.g., "Price → Perceived Value → Purchase Intention". Leave None for primaries/null.')
    role: Optional[str] = Field(None, description='Secondary role with conditions, e.g., "Mediator" or "Moderator (stronger for high levels)". Leave None for primaries/null.')
    relationship: Optional[str] = Field(None, description='Directional summary, e.g., "Positive direct effect" or "Indirect effect via mediator"')
    theoretical_basis: Optional[str] = Field(None, description='Brief rationale, e.g., "Theory of Planned Behavior"')

class AssumptionsRecommend(BaseModel):
    assumptions: list[AssumptionRecommend]
    inherited_from_papers_count: Optional[int] = Field(0, description='Number of papers that these hypothesis/assumption was inherited from. Set to 0 if all original design.')
    
class QuestionsRecommend(BaseModel):
    questions: List[str]

class SurveyQuestion(BaseModel):
    indicator_name: str = Field(..., description="Name of the indicator for latent variables (e.g., 'Security' for trust construct); use a descriptive sub-component label.")
    question: str = Field(..., description="The survey question or measurement item text; clear, concise, neutral.")
    question_code: str = Field(..., description="Unique code: For latent indicators, variable_code + sequential number (e.g., 'CS1'). For observable, just variable_code.")
    answer_content: str = Field(..., description="Detailed description of answer options or scale options for Likert).")
    scale_type: str = Field(..., description="Core scale type (e.g., 'Likert', 'Nominal', 'Ordinal', 'Interval', 'Ratio'). Inherit from the variable's scale")

class SurveyForVariable(BaseModel):
    variable_name: str = Field(..., description="Full name of the variable from input.")
    variable_code: str = Field(..., description="Concise uppercase code (2-4 letters) from variable name (e.g., 'HQKT' for 'Hiệu quả kỹ thuật'; ensure uniqueness across all).")
    variable_type: str = Field(..., description="Type from input: 'Độc lập', 'Phụ thuộc', etc.")
    measurement_type: str = Field(..., description="'Latent' ('Tiềm ẩn' in vietnamese) with multiple indicators questions or 'Observable' ('Quan sát' in vietnamese) with single question.")
    questions: List[SurveyQuestion] = Field(..., description="List of 3-5 questions/indicators for latent; exactly 1 for observable. All share the same scale.")
    source: str = Field(..., description="Paper title if adapted or reused from provided materials, or 'Original design' if newly designed; required for traceability.")

class SurveyRecommend(BaseModel):
    surveys: List[SurveyForVariable]

class ReferencePaper(BaseModel):
    title: str = Field(..., description="Title of the reference paper")
    author: str = Field(..., description="Author(s) of the reference paper")
    year: str = Field(..., description="Year of publication")
    hypotheses: Optional[List[str]] = Field(
        default=[],
        description="List of hypotheses from this paper that are relevant to the current research"
    )
    variables: Optional[List[str]] = Field(
        default=[],
        description="List of variables from this paper that can be used in the current research"
    )
    measurement_items: Optional[List[Dict[str, List[str]]]] = Field(
        default=[],
        description=(
            "A list of dictionaries where each key is a variable name and the corresponding value "
            "is a list of measurement item questions used to measure that variable. These items "
            "can be reused in the current research."
        )
    )

class ReferenceSelection(BaseModel):
    reference_papers: List[ReferencePaper] = Field(..., description="List of selected reference papers with their relevant hypotheses and variables")
    selection_rationale: str = Field(..., description="Overall explanation of why these papers and their elements were selected for the current research")

class FeedbackIntent(BaseModel):
    intent: Literal["accept", "edit", "not_related", "clarify"]
    reasoning: str
    extracted_feedback: Optional[str] = None  # Only for edit intent
     
class FinalizedResults(BaseModel):
    final_model: Optional[ModelRecommendation] = None
    final_assumptions: List[AssumptionRecommend] = Field(default_factory=list)
    final_variables: List[VariableRecommend] = Field(default_factory=list)
    final_surveys: List[SurveyForVariable] = Field(default_factory=list)
    final_questions: List[str] = Field(default_factory=list)
    parsed_variables: List[Dict[str, Any]] = Field(default_factory=list)  # Parsed variables for analyzer
    summary: str = ""
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    
class StateElement(TypedDict):
    accepted: bool
    node: str
    object: Union[ModelRecommendation, AssumptionsRecommend, VariablesRecommend, QuestionsRecommend, SurveyRecommend, ReferenceSelection, FinalizedResults, Dict[str, Any]]

class AnalysisState(TypedDict):
    model_id: str
    llm_key: str
    max_tokens: int
    research_context: str
    language: str
    auto_mode: bool
    auto_accept_nodes: List[str]  # Specific nodes to auto-accept in phase 2 (regardless of auto_mode)
    research_type: str
    elements: List[StateElement]
    document_id: str
    relevant_papers: list[dict]
    current_content: str
    current_content_summary: str  # Concise but detailed summary of current_content

    # Inherit mode fields
    inherit_mode: bool
    inherit_context: Optional[Dict[str, Any]]  # FinalizedResults-like structure
    
    # Node-specific message arrays using add_messages
    model_recommendation_messages: Annotated[List[AnyMessage], add_messages]
    assumptions_recommendation_messages: Annotated[List[AnyMessage], add_messages]
    variable_recommendation_messages: Annotated[List[AnyMessage], add_messages]
    survey_recommendation_messages: Annotated[List[AnyMessage], add_messages]
    question_recommendation_messages: Annotated[List[AnyMessage], add_messages]
    reference_selection_messages: Annotated[List[AnyMessage], add_messages]
    finalize_messages: Annotated[List[AnyMessage], add_messages]
    inherit_messages: Annotated[List[AnyMessage], add_messages]

    # Assistant-specific message arrays using add_messages
    model_assistant_messages: Annotated[List[AnyMessage], add_messages]
    assumptions_assistant_messages: Annotated[List[AnyMessage], add_messages]
    variable_assistant_messages: Annotated[List[AnyMessage], add_messages]
    survey_assistant_messages: Annotated[List[AnyMessage], add_messages]
    question_assistant_messages: Annotated[List[AnyMessage], add_messages]
    reference_selection_assistant_messages: Annotated[List[AnyMessage], add_messages]
    finalize_assistant_messages: Annotated[List[AnyMessage], add_messages]
    
    final_model_count: int

    # Phase tracking
    current_phase: Optional[Literal["phase_1", "phase_2"]] = None
    current_assistant_node: Optional[str] = None

    # Token tracking
    input_tokens: int
    output_tokens: int
    
    # Temp data for review
    temp_review_data: Optional[Dict[str, Any]]
    temp_reask_type: Optional[str]

def get_message_key(node_name: str) -> str:
    """Get the message array key for a given node."""
    message_keys = {
        "recommend_assumptions": "assumptions_recommendation_messages",
        "model_recommend": "model_recommendation_messages",
        "recommend_variables": "variable_recommendation_messages",
        "recommend_survey": "survey_recommendation_messages",  # New
        "recommend_questions": "question_recommendation_messages",
        "reference_selection": "reference_selection_messages",
        "finalize_analysis": "finalize_messages",
        "inherit_display": "inherit_messages"
    }
    return message_keys.get(node_name, "")

def get_assistant_message_key(node_name: str) -> str:
    """Get the assistant message array key for a given node."""
    assistant_message_keys = {
        "recommend_assumptions": "assumptions_assistant_messages",
        "model_recommend": "model_assistant_messages",
        "recommend_variables": "variable_assistant_messages",
        "recommend_survey": "survey_assistant_messages",
        "recommend_questions": "question_assistant_messages",
        "reference_selection": "reference_selection_assistant_messages"
    }
    return assistant_message_keys.get(node_name, "")

def get_node_prompt(node_name: str) -> str:
    """Get the system prompt for a given node."""
    prompts = {
        "recommend_assumptions": assumptions_instructions,
        "model_recommend": model_instructions,
        "recommend_variables": core_variables_instructions,
        "recommend_survey": survey_instructions,
        "recommend_questions": questions_instructions,
        "reference_selection": reference_selection_instructions,
        "finalize_analysis": finalize_instructions,
    }
    return prompts.get(node_name, "")

def get_assistant_prompt(base_prompt: str, language: str) -> str:
    """Cleaner, unambiguous version of the proactive assistant prompt."""
    
    return f"""
You are a proactive research assistant working iteratively. Respond only in {language}.  
For each task: briefly state your recommendation, then provide the full draft (assumptions, methods, variables). After each draft, invite feedback and refine.

### Confirmation Rule (very important)
At the end of every message, append exactly one confirmation sentence in {language}.  
Do not include any English version unless the chosen language is English.  
The confirmation sentence meaning must be:

"If you agree with what we've discussed so far, please respond 'OK', and I will propose the content for the next section."

### Interaction Logic
- If the user clearly accepts ("OK", "looks good", etc.): proceed directly or invoke tools when appropriate.
- If the user shows hesitation (“maybe”, “OK but change X”): ask a focused clarifying question.
- If unsure once: ask a concise clarification; otherwise proceed confidently.

-----
Base task:
{base_prompt}
"""


@tool
def handoff_to_confirm() -> str:
    """Call this tool when you are ready to generate the structured output based on the conversation so far. Do not use any parameters."""
    return "Handoff to structured generation confirmed."

@tool
def accept_inherited_content() -> str:
    """Call this tool when user accepts the inherited content as-is without modifications and wants to finish. Do not use any parameters."""
    return "Inherited content accepted."

@tool
def modify_inherited_content() -> str:
    """Call this tool when user wants to modify or add to the inherited content and proceed with the normal research flow."""
    return "User wants to modify inherited content."

def get_next_node(current_node: str, research_type: str) -> Literal["reference_selection", "model_recommend", "recommend_assumptions", "recommend_variables", "recommend_survey", "recommend_questions", "finalize_analysis", "parse_variables", "__end__"]:
    """Determine the next node based on current node and research type."""

    # Define different flows for different research types
    flows = {
        "dinh_luong": {
            "reference_selection": "recommend_assumptions",
            "recommend_assumptions": "recommend_variables",
            "recommend_variables": "model_recommend",
            "model_recommend": "recommend_survey",
            "recommend_survey": "finalize_analysis",
            "finalize_analysis": "parse_variables",
            "parse_variables": "__end__"
        },
        "thu_cap": {
            "model_recommend": "recommend_survey",
            "recommend_survey": "finalize_analysis",
            "finalize_analysis": "parse_variables",
            "parse_variables": "__end__"
        },
        "dinh_tinh": {
            "reference_selection": "recommend_questions",
            "recommend_questions": "finalize_analysis",
            "finalize_analysis": "__end__"
        },
        "hon_hop": {
            "reference_selection": "recommend_assumptions",
            "recommend_assumptions": "recommend_variables",
            "recommend_variables": "model_recommend",
            "model_recommend": "recommend_survey",
            "recommend_survey": "recommend_questions",
            "recommend_questions": "finalize_analysis",
            "finalize_analysis": "parse_variables",
            "parse_variables": "__end__"
        },
    }

    # Get the flow for the current research type
    current_flow = flows.get(research_type, flows["dinh_luong"])

    return current_flow.get(current_node, "__end__")

async def get_key_points(state: AnalysisState) -> dict:
    mongo_client = get_mongodb_client()
    db = mongo_client["user_documents"]
    collection = db["reports_refs"]
    admin_db = mongo_client["admin"]
    content_collection = admin_db["outlines"]
    # Get content
    user_report_cache: list[str] = []
    user_report_str = ""
    try:
        user_report_caches = content_collection.find({"documentId": state["document_id"]}).sort("index", ASCENDING)
        async for doc in user_report_caches:
            if doc["content"]:
                user_report_cache.append(doc["content"])
            else:
                if doc["contentArr"]:
                    section_content = ""
                    for subsection in doc["contentArr"]:
                        section_content += subsection["text"]
                    user_report_cache.append(section_content)
                else:
                    break
        user_report_str += "\n".join(user_report_cache)
    except Exception as e:
        pass
    articles_collection = admin_db["articles"]
    article = await articles_collection.find_one({"_id": ObjectId(state["document_id"])})
    if article and "topicRefs" in article and article["category"] == 2:
        for seminar_id in article["topicRefs"]:
            seminar_user_report_caches = content_collection.find({"documentId": seminar_id}).sort("index", ASCENDING)
            async for doc in seminar_user_report_caches:
                if doc.get("content"):
                    user_report_cache.append(doc["content"])
                else:
                    break
        user_report_str += "\n".join(user_report_cache)
    user_report_str_md = markdownify_keep_images(user_report_str)

    # Get inherited papers
    try:
        use_papers: set = set()
        user_report_caches = content_collection.find({"documentId": state["document_id"]}).sort("index", ASCENDING)
        async for doc in user_report_caches:
            if doc["refTable"]:
                use_papers.update([row["paperTitle"] for row in doc["refTable"] if row.get("inherited", False)])
    except Exception as e:
        pass
    cached_data = await collection.find_one({"_id": state["document_id"]})
    documents = cached_data.get("documents", []) if cached_data else []
    papers_title_keypoints: list[dict] = []

    def process_key_point_field(field_value):
        """Helper function to handle both string and list types for key_points fields."""
        if isinstance(field_value, str):
            return field_value.replace("</br>", "\n")
        elif isinstance(field_value, list):
            # If it's a list, join items with newlines
            return "\n".join(str(item) for item in field_value)
        else:
            # Fallback to string representation
            return str(field_value) if field_value else ""

    for document in documents:
        if document["title"] in use_papers:
            paper_title_keypoints: dict = {}
            paper_title_keypoints["title"] = document["title"]
            paper_title_keypoints["authors"] = document["authors"]
            paper_title_keypoints["year"] = document["year"]
            paper_title_keypoints["key_points"] = {
                "objectives": process_key_point_field(document["key_points"]["objectives"]),
                "results": process_key_point_field(document["key_points"]["results"]),
                "variables": process_key_point_field(document["key_points"]["variables"]),
                "hypotheses": process_key_point_field(document["key_points"]["hypotheses"]),
                "measurement_items": process_key_point_field(document["key_points"]["measurement_items"]),
                "model_design": process_key_point_field(document["key_points"]["model_design"]),
            }
            papers_title_keypoints.append(paper_title_keypoints)
    logger.info(f"[{state['document_id'][:8]}] Use {len(papers_title_keypoints)}/{len(documents)} documents")
    if len(documents) != 0 and len(papers_title_keypoints) == 0:
        for document in documents:
            paper_title_keypoints: dict = {}
            paper_title_keypoints["title"] = document["title"]
            paper_title_keypoints["authors"] = document["authors"]
            paper_title_keypoints["year"] = document["year"]
            paper_title_keypoints["key_points"] = {
                "objectives": process_key_point_field(document["key_points"]["objectives"]),
                "results": process_key_point_field(document["key_points"]["results"]),
                "variables": process_key_point_field(document["key_points"]["variables"]),
                "hypotheses": process_key_point_field(document["key_points"]["hypotheses"]),
                "measurement_items": process_key_point_field(document["key_points"]["measurement_items"]),
                "model_design": process_key_point_field(document["key_points"]["model_design"]),
            }
            papers_title_keypoints.append(paper_title_keypoints)
        logger.info(f"[{state['document_id'][:8]}] Use all of {len(documents)} documents")

    # Generate summary of current_content
    current_content_summary = ""
    if user_report_str_md and user_report_str_md.strip():
        try:
            llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
            summary_prompt = f"""You are a research assistant. Please create a concise but detailed summary of the following research report content.

Requirements:
- Keep all important details, findings, methodologies, variables, and key points
- Use bullet points and structured format for clarity
- Be concise but comprehensive - capture the essence without losing critical information
- Focus on: research objectives, methodology, key variables, main findings, and conclusions
- Maintain technical accuracy and specific details (numbers, percentages, variable names, etc.)

Report content:
{user_report_str_md}

Please provide a structured summary in {state["language"]}."""

            error, success, summary_result, _, _ = await get_answer(state['document_id'][:8], llm, summary_prompt, [])
            if success and summary_result:
                current_content_summary = summary_result
                logger.info(f"[{state['document_id'][:8]}] Successfully generated current_content summary")
            else:
                logger.info(f"[{state['document_id'][:8]}] Failed to generate summary, using truncated content")
                # Fallback: use first 2000 characters as summary
                current_content_summary = user_report_str_md[:2000] + ("..." if len(user_report_str_md) > 2000 else "")
        except Exception as e:
            logger.info(f"[{state['document_id'][:8]}] Error generating summary: {str(e)}")
            # Fallback: use first 2000 characters as summary
            current_content_summary = user_report_str_md[:2000] + ("..." if len(user_report_str_md) > 2000 else "")

    return {
        "relevant_papers": papers_title_keypoints,
        "current_content": user_report_str_md,
        "current_content_summary": current_content_summary,
        # "auto_accept_nodes": state.get("auto_accept_nodes", ["model_recommend", "recommend_assumptions"])
    }

async def coordinator(state: AnalysisState) -> Command[Literal["reference_selection", "model_recommend", "recommend_assumptions", "recommend_variables", "recommend_survey", "recommend_questions", "finalize_analysis", "inherit_display", "__end__"]]:
    """
    Coordinator node that determines the first node in the flow based on research type.
    Checks inherit_mode first to route to inherit_display if needed.
    """
    logger.info(f"[{state['document_id'][:8]}] Coordinator node: determining flow based on research type")

    # Check inherit mode first
    inherit_mode = state.get("inherit_mode", False)
    if inherit_mode:
        logger.info(f"[{state['document_id'][:8]}] Inherit mode enabled, routing to inherit_display")
        return Command(goto="inherit_display")

    research_type = state.get("research_type", "dinh_luong")  # Default to dinh_luong if not specified

    # Define the starting nodes for each research type
    flow_starting_nodes = {
        "dinh_luong": "reference_selection",
        "thu_cap": "model_recommend",
        "dinh_tinh": "reference_selection",
        "hon_hop": "reference_selection",
    }

    starting_node = flow_starting_nodes.get(research_type, "reference_selection")
    logger.info(f"[{state['document_id'][:8]}] Research type: {research_type}, starting with node: {starting_node}")

    return Command(goto=starting_node)

def parse_inherit_context_to_elements(inherit_context: Dict[str, Any]) -> List[StateElement]:
    """Parse inherit_context (FinalizedResults-like dict) into individual StateElements,
    and create a final FinalizedResults object as the last element."""
    elements = []

    # Parse and collect all data
    final_model = None
    final_assumptions = []
    final_variables = []
    final_surveys = []
    final_questions = []

    # Parse assumptions
    if inherit_context.get("final_assumptions"):
        assumptions = []
        for a in inherit_context["final_assumptions"]:
            if isinstance(a, dict):
                assumption_obj = AssumptionRecommend(**a)
                assumptions.append(assumption_obj)
                final_assumptions.append(assumption_obj)
            else:
                assumptions.append(a)
                final_assumptions.append(a)
        if assumptions:
            elements.append(StateElement(
                accepted=True,
                node="recommend_assumptions",
                object=AssumptionsRecommend(assumptions=assumptions)
            ))

    # Parse variables
    if inherit_context.get("final_variables"):
        variables = []
        for v in inherit_context["final_variables"]:
            if isinstance(v, dict):
                variable_obj = VariableRecommend(**v)
                variables.append(variable_obj)
                final_variables.append(variable_obj)
            else:
                variables.append(v)
                final_variables.append(v)
        if variables:
            elements.append(StateElement(
                accepted=True,
                node="recommend_variables",
                object=VariablesRecommend(variables=variables)
            ))

    # Parse model
    if inherit_context.get("final_model"):
        model_data = inherit_context["final_model"]
        if isinstance(model_data, dict):
            if model_data.get("model_type") not in {"Simple", "Extended", "Comprehensive"}:
                model_data = {key: value for key, value in model_data.items() if key != "model_type"}
            final_model = ModelRecommendation(**model_data)
        else:
            final_model = model_data
        if isinstance(final_model, ModelRecommendation):
            final_model = _normalize_model_recommendation(final_model, {"elements": elements})
        elements.append(StateElement(
            accepted=True,
            node="model_recommend",
            object=final_model
        ))

    # Parse surveys
    if inherit_context.get("final_surveys"):
        surveys = []
        for s in inherit_context["final_surveys"]:
            if isinstance(s, dict):
                # Convert questions list
                questions = []
                for q in s.get("questions", []):
                    if isinstance(q, dict):
                        questions.append(SurveyQuestion(**q))
                    else:
                        questions.append(q)
                s_copy = s.copy()
                s_copy["questions"] = questions
                survey_obj = SurveyForVariable(**s_copy)
                surveys.append(survey_obj)
                final_surveys.append(survey_obj)
            else:
                surveys.append(s)
                final_surveys.append(s)
        if surveys:
            elements.append(StateElement(
                accepted=True,
                node="recommend_survey",
                object=SurveyRecommend(surveys=surveys)
            ))

    # Parse questions
    if inherit_context.get("final_questions"):
        questions = inherit_context["final_questions"]
        if questions:
            final_questions = questions
            elements.append(StateElement(
                accepted=True,
                node="recommend_questions",
                object=QuestionsRecommend(questions=questions)
            ))

    # Create FinalizedResults object as the last element
    parsed_variables = inherit_context.get("parsed_variables", [])
    finalized_results = FinalizedResults(
        final_model=final_model,
        final_assumptions=final_assumptions,
        final_variables=final_variables,
        final_surveys=final_surveys,
        final_questions=final_questions,
        parsed_variables=parsed_variables,
        summary="Inherited content accepted as-is.",
        total_input_tokens=0,
        total_output_tokens=0
    )

    elements.append(StateElement(
        accepted=True,
        node="finalize_analysis",
        object=finalized_results
    ))

    return elements

async def inherit_display(state: AnalysisState) -> Command[Literal["model_recommend", "recommend_assumptions", "recommend_variables", "recommend_survey", "recommend_questions", "finalize_analysis", "parse_variables", "__end__"]]:
    """
    Display inherited content to user and ask if they want to modify or proceed.
    Uses handoff_to_normal_flow tool to detect user agreement.
    """
    logger.info(f"[{state['document_id'][:8]}] Inherit display node: showing inherited content")

    inherit_context = state.get("inherit_context", {})
    if not inherit_context:
        logger.info(f"[{state['document_id'][:8]}] No inherit context found, proceeding to normal flow")
        # Disable inherit mode and go back to coordinator
        return Command(
            update={"inherit_mode": False},
            goto="coordinator"
        )

    inherit_messages = state.get("inherit_messages", [])

    # Build initial context if first time
    if not inherit_messages:
        # Format inherit context as readable text
        model_str = ""
        if inherit_context.get("final_model"):
            model = inherit_context["final_model"]
            if isinstance(model, dict):
                model_str = f"**Model:**\n- Thinking: {model.get('thinking_process', 'N/A')}\n- Sketch: {model.get('sketch', 'N/A')}\n"
            else:
                model_str = f"**Model:**\n- Thinking: {model.thinking_process}\n- Sketch: {model.sketch}\n"

        assumptions_str = ""
        if inherit_context.get("final_assumptions"):
            assumptions_str = "**Hypotheses/Assumptions:**\n"
            for a in inherit_context["final_assumptions"]:
                if isinstance(a, dict):
                    assumptions_str += f"- {a.get('hypothesis_id', 'N/A')}: {a.get('statement', 'N/A')}\n"
                else:
                    assumptions_str += f"- {a.hypothesis_id}: {a.statement}\n"

        variables_str = ""
        if inherit_context.get("final_variables"):
            variables_str = "**Variables:**\n| Name | Type | Description | Scale |\n|------|------|-------------|-------|\n"
            for v in inherit_context["final_variables"]:
                if isinstance(v, dict):
                    variables_str += f"| {v.get('name', 'N/A')} | {v.get('variable_type', 'N/A')} | {v.get('description', 'N/A')} | {v.get('scale', 'N/A')} |\n"
                else:
                    variables_str += f"| {v.name} | {v.variable_type} | {v.description} | {v.scale} |\n"

        surveys_str = ""
        if inherit_context.get("final_surveys"):
            surveys_str = "**Survey Questions:**\n"
            for s in inherit_context["final_surveys"]:
                if isinstance(s, dict):
                    surveys_str += f"### {s.get('variable_name', 'N/A')}\n"
                    for q in s.get("questions", []):
                        if isinstance(q, dict):
                            surveys_str += f"- {q.get('question', 'N/A')}\n"
                        else:
                            surveys_str += f"- {q.question}\n"
                elif isinstance(s, str):
                    surveys_str += f"- {s}\n"
                else:
                    surveys_str += f"### {s.variable_name}\n"
                    for q in s.questions:
                        surveys_str += f"- {q.question}\n"

        questions_str = ""
        if inherit_context.get("final_questions"):
            questions_str = "**Research Quantitative Questions:**\n"
            for i, q in enumerate(inherit_context["final_questions"], 1):
                questions_str += f"{i}. {q}\n"

        inherited_content = f"""
        {assumptions_str}
        {variables_str}
        {model_str}
        {surveys_str}
        {questions_str}
        """

        user_content = f"""System message:
        Here is the inherited research content:
        {inherited_content}
        
        Please display this to me and ask me if I want to modify or accept it. 
        """
        
        inherit_messages = [HumanMessage(content=user_content)]

    system_prompt = f"""You are a research assistant. The user is inheriting data from a previous phase.
    Your Task:
    1. Present the INHERITED DATA provided by the user clearly and beautifully in Markdown. For the model design info (the skecth), keep it in fenced code blocks exactly as it is - do not modify or reformat it.
    2. Then ask the user: "Do you want to keep this design as is? If you want to edit it, I will help you start over from scratch to ensure consistency and logic across the entire content."
    3. If they accept, they wrote anything like "keep", "accept", "yes", "ok" → call tool `accept_inherited_content`
    4. If they modify, they wrote anything like "edit", "modify", "change" → call tool `modify_inherited_content`

    Do not generate new content. Only display what is provided.
    Respond in {state.get('language', 'Vietnamese')}.
    """

    # Check if we have real user feedback (not just the initial system message)
    has_user_feedback = len(inherit_messages) > 2
    
    llm_with_tool = None
    if has_user_feedback:
        llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
        llm_with_tool = llm.bind_tools([accept_inherited_content, modify_inherited_content])
    else:
        llm_with_tool = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
        
    invoke_messages = [SystemMessage(content=system_prompt)] + inherit_messages

    response = await llm_with_tool.ainvoke(invoke_messages)
    input_tokens = getattr(response, 'usage_metadata', {}).get('input_tokens', 0)
    output_tokens = getattr(response, 'usage_metadata', {}).get('output_tokens', 0)
    
    # Only check for tool calling if we have real user feedback
    if has_user_feedback and response.tool_calls:
        tool_name = response.tool_calls[0]['name']

        # Check if user accepts via tool call
        if tool_name == 'accept_inherited_content':
            logger.info(f"[{state['document_id'][:8]}] User accepts inherited content as-is")

            # Parse inherit_context into elements (including FinalizedResults as last element)
            inherited_elements = parse_inherit_context_to_elements(inherit_context)
            elements = state.get("elements", [])
            updated_elements = elements + inherited_elements

            return Command(
                update={
                    "elements": updated_elements,
                    "inherit_mode": False,
                    "input_tokens": state.get("input_tokens", 0) + input_tokens,
                    "output_tokens": state.get("output_tokens", 0) + output_tokens,
                },
                goto="__end__"
            )

        # Check if user wants to modify via tool call
        elif tool_name == 'modify_inherited_content':
            logger.info(f"[{state['document_id'][:8]}] User wants to modify inherited content, proceeding to normal flow with inherited context")

            # # Parse inherit_context into elements so the normal flow has context
            # inherited_elements = parse_inherit_context_to_elements(inherit_context)
            # elements = state.get("elements", [])
            # updated_elements = elements + inherited_elements

            return Command(
                update={
                    # "elements": updated_elements,
                    "inherit_mode": False,
                    "input_tokens": state.get("input_tokens", 0) + input_tokens,
                    "output_tokens": state.get("output_tokens", 0) + output_tokens,
                },
                goto="coordinator"
            )

    # Always show LLM response to user via review_interrupt
    updated_messages = inherit_messages + [AIMessage(content=response.content)]

    readable_message = process_text_with_sketch(response.content)

    temp_review_data = {
        "readable_message": readable_message,
        "obj": None,
        "node_name": "inherit_display",
        "message_key": "inherit_messages"
    }

    return Command(
        update={
            "inherit_messages": updated_messages,
            "temp_review_data": temp_review_data,
            "input_tokens": state.get("input_tokens", 0) + input_tokens,
            "output_tokens": state.get("output_tokens", 0) + output_tokens,
        },
        goto="review_interrupt"
    )

async def _generate_node_output(
    state: AnalysisState,
    node_name: str,
    message_key: str,
    system_prompt: str,
    output_schema: type,
    filter_context_nodes: Optional[List[str]] = None,
    include_research_context: bool = False,
    confirmation_mode: bool = False,
    additional_update: Optional[Dict[str, Any]] = None
    ) -> Command[Literal["review_interrupt", "model_recommend", "recommend_assumptions", "recommend_variables", "recommend_survey", "recommend_questions", "finalize_analysis", "parse_variables", "__end__"]]:
    """
    Generic function to handle node output generation with consistent logic.
    
    Args:
        state: Current analysis state
        node_name: Name of the current node
        message_key: Key for the message array in state
        system_prompt: System prompt template (already formatted)
        output_schema: Pydantic schema for structured output
        filter_context_nodes: List of node names to filter for context (if None, includes all accepted elements)
        include_research_context: Whether to include research_context in user content (default: True)
        confirmation_mode: If True, use assistant message history instead of main message key (default: False)
        additional_update: Optional additional updates to include in the Command
    """
    logger.info(f"[{state['document_id'][:8]}] Generating output for {node_name}, confirmation_mode={confirmation_mode}")
    
    # === NEW: Get auto_mode and research_type from state ===
    auto_mode = state.get("auto_mode", True)
    auto_accept_nodes = state.get("auto_accept_nodes", [])
    research_type = state.get("research_type", "dinh_luong")

    llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])

    # Get existing messages for this node
    if confirmation_mode:
        ass_msg_key = get_assistant_message_key(node_name)
        if ass_msg_key is None:
            raise ValueError(f"No assistant message key found for node {node_name} in confirmation mode")
        node_messages = state.get(ass_msg_key, [])
        # Clean up if last message has tool calls (remove tool_calls, keep content)
        if node_messages and isinstance(node_messages[-1], AIMessage) and node_messages[-1].tool_calls:
            last_msg = AIMessage(content=node_messages[-1].content)
            node_messages = node_messages[:-1] + [last_msg]
        
        # Build string history starting from the first AI message, formatted as requested
        # Skip initial HumanMessage (heavy context) and start from first AI response
        first_ai_index = next((i for i, msg in enumerate(node_messages) if isinstance(msg, AIMessage)), None)
        if first_ai_index is not None:
            conversation_history = []
            for msg in node_messages[first_ai_index:]:
                if isinstance(msg, HumanMessage):
                    conversation_history.append(f"- Human: {msg.content}")
                elif isinstance(msg, AIMessage):
                    conversation_history.append(f"- AI: {msg.content}")
            history_str = "\n".join(conversation_history)
        else:
            history_str = "No conversation history available."
        
        # Use the formatted history as a single HumanMessage
        node_messages = [HumanMessage(content=history_str)]
    else:
        node_messages = state.get(message_key, [])
    
    # Add user language config to system prompt
    system_prompt = system_prompt + f"\nResponse in {state["language"]}\n"
    
    # Prepare invoke messages: system prompt + node messages
    if not node_messages:
        if filter_context_nodes is not None:
            accepted_objects = [
                elem["object"] 
                for elem in state.get("elements", []) 
                if elem["accepted"] and elem["node"] in filter_context_nodes
            ]
        else:
            accepted_objects = [elem["object"] for elem in state.get("elements", []) if elem["accepted"]]
        
        context_content = "\n".join([str(obj) for obj in accepted_objects]) if accepted_objects else ""
        
        if include_research_context:
            user_content = f"""Research context:
            {str(state["research_context"])}
            ----
            Other's relevant research context (use as references):
            {str(state["relevant_papers"])}
            ----
            Current report's content (ensure the generated content is coherent and cohesive and relevant to the report content):
            {state["current_content_summary"]}
            ----
            Accepted elements:
            {context_content}
            """
        else:
            user_content = f"Consider these contexts that I accepted: {context_content}" if context_content else ""
        
        node_messages = [HumanMessage(content=user_content)] if user_content else []
    
    invoke_messages = [
        *node_messages
    ]
    
    error, success, result, input_tokens, output_tokens = await get_answer_with_schema(state['document_id'][:8], llm, system_prompt, invoke_messages, output_schema)
    if not success:
        raise error 
    if node_name == "model_recommend":
        result = _normalize_model_recommendation(result, state)

    elements = state.get("elements", [])
    existing_element_index = None
    for i, elem in enumerate(elements):
        if elem["node"] == node_name:
            existing_element_index = i
            break
   
    # Generate readable message for history and potential review
    readable_message, readable_input, readable_output = await generate_readable_message(
        state, result, node_name
    )
    total_input = input_tokens + readable_input
    total_output = output_tokens + readable_output
    
    ai_message = AIMessage(content=readable_message)
    updated_node_messages = node_messages + [ai_message]
    
    # Process sketch
    readable_message =  process_text_with_sketch(readable_message)
    
    input_update = state.get("input_tokens", 0) + total_input
    output_update = state.get("output_tokens", 0) + total_output

    # Check if this node should be auto-accepted (either global auto_mode or specific node in list)
    should_auto_accept = (auto_mode and node_name != "finalize_analysis") or (node_name in auto_accept_nodes)

    if should_auto_accept:
        # Auto-mode is ON or node is in auto_accept list, so we auto-accept and proceed.
        logger.info(f"[{state['document_id'][:8]}] Auto-accepting '{node_name}' (auto_mode={auto_mode}, in auto_accept_nodes={node_name in auto_accept_nodes})")
        
        if existing_element_index is not None:
            elements[existing_element_index]["object"] = result
            elements[existing_element_index]["accepted"] = True # Auto-accept
        else:
            new_element = StateElement(
                accepted=True, # Auto-accept
                node=node_name,
                object=result
            )
            elements.append(new_element)
        
        next_node = get_next_node(node_name, research_type)
        
        update = {
            "elements": elements,
            message_key: updated_node_messages,
            "input_tokens": input_update,
            "output_tokens": output_update,
        }
        if additional_update:
            update.update(additional_update)
        
        return Command(
            update=update,
            goto=next_node
        )
    else:
        # Manual mode OR the final node, so we interrupt for review.
        logger.info(f"[{state['document_id'][:8]}] Manual mode or final node. Proceeding to review for '{node_name}'.")
        if node_name == "finalize_analysis":
            if result.final_model is None:
                result.final_model = ModelRecommendation(
                thinking_process="Qualitative research does not have relational model",
                sketch=""
            )
        if existing_element_index is not None:
            elements[existing_element_index]["object"] = result
            elements[existing_element_index]["accepted"] = False
        else:
            new_element = StateElement(
                accepted=False,
                node=node_name,
                object=result
            )
            elements.append(new_element)
        render_input = 0
        render_output = 0
        final_model_count = state.get("final_model_count", 0)

        if "```" in readable_message:
            mermaid_code = readable_message.split("```")[1].split("mermaid")[-1]
            if "graph" in mermaid_code or "flowchart" in mermaid_code:
                logger.info(f"[{state['document_id'][:8]}] Start render image")
                success_render, fixed_mermaid_code, image_content, render_input, render_output = await extract_mermaid(mermaid_code, state, llm)
                if success_render:
                    mongo_client = get_mongodb_client()
                    db = mongo_client["admin"]
                    collection = db["document_configurations"]
                    document_config = await collection.find_one({"documentId": state["document_id"]})
                    user_id = document_config["createdBy"]
                    s3_client = get_s3_client()
                    s3_client.put_object(
                        Bucket="users",
                        Key=f"{user_id}/article/{state["document_id"]}/{settings.MINIO_BUCKET_ANALYSIS}/{state["document_id"]}_{final_model_count}.png",
                        Body=image_content,
                        ContentType="image/png"
                    )
                    public_url = f"{settings.MINIO_DOMAIN}/users/{user_id}/article/{state["document_id"]}/{settings.MINIO_BUCKET_ANALYSIS}/{state["document_id"]}_{final_model_count}.png"
                    img_ref = f'<img loading="lazy" src="{public_url}" alt="Relationship Graph">'
                    if node_name == "finalize_analysis":
                        result.final_model.sketch = fixed_mermaid_code + "\nlink: " + public_url
                        
                    #### TURN OFF TO KEEP RAW CONTENT OF OBJECT IN MODEL RECOMMEND NODE ###
                    # elif node_name == "model_recommend":
                    #     result.sketch = fixed_mermaid_code + "\nlink: " + public_url
                    
                    readable_message = readable_message.replace(mermaid_code, f"\n{fixed_mermaid_code}\n")
                    final_model_count += 1
            else:
                readable_message = readable_message.replace(mermaid_code, mermaid_code.replace("\n", "</br>"))

        display_object = result.model_dump() if hasattr(result, 'model_dump') else result
        temp_review_data = {
            "readable_message": readable_message,
            "obj": display_object,
            "node_name": node_name,
            "message_key": message_key
        }
        update = {
            "elements": elements,
            message_key: updated_node_messages,
            "input_tokens": input_update + render_input,
            "output_tokens": output_update + render_output,
            "temp_review_data": temp_review_data,
            "final_model_count": final_model_count,
        }
        if additional_update:
            update.update(additional_update)
        return Command(
            update=update,
            goto="review_interrupt"
        )

async def _node_assistant(
    state: AnalysisState,
    node_name: str,
    system_prompt: str,
    output_schema: type,
    filter_context_nodes: Optional[List[str]] = None,
    include_research_context: bool = False,
    include_papers_only: bool = False,
    include_context_no_papers: bool = False,
    include_context_reference_selection: bool = False,
) -> Command[Literal["review_interrupt", "model_recommend", "recommend_assumptions", "recommend_variables", "recommend_survey", "recommend_questions", "finalize_analysis", "__end__"]]:
    """
    Handles the two-phase process for a node:
    1. Conversational phase to refine requirements.
    2. Handoff to the generation phase for structured output.
    """
    logger.info(f"[{state['document_id'][:8]}] Entering assistant for node: {node_name}")
    
    model_name = state["model_id"]
    if model_name.startswith("gpt-") and node_name in ["model_recommend", "recommend_assumptions", "recommend_survey"]:
        llm = get_llm("gpt-4.1-2025-04-14", state["llm_key"], state["max_tokens"], reasoning_effort='none')
    else:
        llm = get_llm(model_name, state["llm_key"], state["max_tokens"])
    llm_with_tool = llm.bind_tools([handoff_to_confirm])
    
    assistant_message_key = get_assistant_message_key(node_name)
    assistant_messages = state.get(assistant_message_key, [])
    
    model_recommend_note = "" if node_name == "model_recommend" else  "- For diagrams (e.g., the sketch in model recommend), use exact fenced code block from latest version—do not modify or convert to other formats."
    confirmation_prompt = f"""You are finalizing structured output using full conversation history.

**Key Rules**:
- Integrate all user updates/corrections/preferences from history—do not add/remove/alter unapproved elements.
- Copy exactly from the most recent proposal (last AI message before final user input), as it mostly includes all refinements.
{model_recommend_note}
- If history shows fully approved output, emit that exactly as final.
- Use {state["language"]} for labels."""


    # Initialize phase and node if first entry
    current_assistant_node = state.get("current_assistant_node")
    current_phase = state.get("current_phase")
    phase_update = {}
    if current_assistant_node != node_name:
        phase_update["current_assistant_node"] = node_name
        phase_update["current_phase"] = "phase_1"
        current_phase = "phase_1"
    else:
        current_phase = current_phase or "phase_1"

    # Check if handoff already triggered (phase 2 branch)
    handoff_triggered = False
    if assistant_messages and isinstance(assistant_messages[-1], AIMessage) and assistant_messages[-1].tool_calls and assistant_messages[-1].tool_calls[0]['name'] == 'handoff_to_confirm':
        logger.info(f"[{state['document_id'][:8]}] Handoff already triggered for node {node_name}. Proceeding to structured generation.")
        phase_update["current_phase"] = "phase_2"
        handoff_triggered = True
        
        main_message_key = get_message_key(node_name)
        
        sub_command = await _generate_node_output(
            state=state,
            node_name=node_name,
            message_key=main_message_key,
            system_prompt=confirmation_prompt,
            output_schema=output_schema,
            include_research_context=False,
            filter_context_nodes=[],
            confirmation_mode=True,
            additional_update=phase_update
        )
        return sub_command
    
    # Phase 1: Conversational
    # If first time, build initial context
    if not assistant_messages:
        logger.info(f"[{state['document_id'][:8]}] First entry for {node_name}, building initial context.")
        if filter_context_nodes is not None:
            accepted_objects = [
                elem["object"] 
                for elem in state.get("elements", []) 
                if elem["accepted"] and elem["node"] in filter_context_nodes
            ]
        else:
            accepted_objects = [elem["object"] for elem in state.get("elements", []) if elem["accepted"]]
        
        context_content = "\n".join([str(obj) for obj in accepted_objects]) if accepted_objects else ""

        # Build inherit context string if inherit_context exists
        # Only append the corresponding field based on node type
        inherit_context_str = ""
        inherit_context = state.get("inherit_context", {})
        if inherit_context:
            # Map node names to their corresponding inherit_context fields
            node_to_field_map = {
                "recommend_assumptions": "final_assumptions",
                "recommend_variables": "final_variables",
                "model_recommend": "final_model",
                "recommend_survey": "final_surveys",
                "recommend_questions": "final_questions"
            }

            field_key = node_to_field_map.get(node_name)
            if field_key and inherit_context.get(field_key):
                inherit_context_str = f"""
            ----
            Inherited content from previous research (for reference only). Prioritize building from existing elements above if they exist. Use this as supplementary reference:
            {str(inherit_context[field_key])}
            """

        base_content = f"Consider these contexts that I accepted: {context_content}" if context_content else ""
        user_content = base_content + inherit_context_str if inherit_context_str else base_content
        
        # logger.info(f"[{state['document_id'][:8]}] ""RELEVANT PAPERS :
        # {state["relevant_papers"]}
        # """)
        
        if include_research_context:
            user_content = f"""Research context:
            {str(state["research_context"])}
            ----
            Other's relevant research context (use as references):
            {str(state["relevant_papers"])}
            ----
            Current report's content (ensure the generated content is coherent and cohesive and relevant to the report content):
            {state["current_content_summary"]}
            ----
            Accepted elements:
            {context_content}
            {inherit_context_str}
            """
            
        if include_context_reference_selection:
            user_content = f"""Current Research context:
            {str(state["research_context"])}
            
            ----
            Relevant research papers (if not relevant at raise your issue):
            {str(state["relevant_papers"])}
            ----
            """
            
        if include_papers_only:

            logger.info(f"[{state['document_id'][:8]}] Paper input for citation:{str(state["relevant_papers"])}")

            user_content = f"""Reference papers list:
            The following is the complete and exclusive list of papers you may cite. Do not cite any papers not in this list. If you cite a paper, you must use the exact title as it appears below:
            {str(state["relevant_papers"])}

            If you cannot find a relevant scale in the above papers, use "Original design" as the source.

            ----
            Accepted elements:
            {context_content}
            {inherit_context_str}
            """
        if include_context_no_papers:
            user_content = f"""Research context:
            {str(state["research_context"])}
            ----
            Current report's content (ensure the generated content is coherent and cohesive and relevant to the report content):
            {state["current_content_summary"]}
            ----
            Accepted elements:
            {context_content}
            {inherit_context_str}
            """

        initial_message = [HumanMessage(content=user_content)] if user_content else []
        assistant_messages = initial_message[:]  # Start fresh with initial

    # Prepare and invoke the conversational LLM
    assistant_system_prompt = get_assistant_prompt(system_prompt, state["language"])
    invoke_messages = [SystemMessage(content=assistant_system_prompt)] + assistant_messages
    
    response = await llm_with_tool.ainvoke(invoke_messages)
    input_tokens = getattr(response, 'usage_metadata', {}).get('input_tokens', 0)
    output_tokens = getattr(response, 'usage_metadata', {}).get('output_tokens', 0)
    
    base_model = state["model_id"]
    target_model = base_model

    if base_model.startswith("gpt-") and node_name in ["model_recommend", "recommend_assumptions", "recommend_variables"]:
        target_model = "gpt-4.1-2025-04-14"
        input_tokens, output_tokens = calculate_token_scaling(
            input_tokens, output_tokens, target_model, base_model
        )
    
    # Check if the LLM decided to handoff to the generation phase
    if response.tool_calls and response.tool_calls[0]['name'] == 'handoff_to_confirm':
        logger.info(f"[{state['document_id'][:8]}] Handoff triggered for node {node_name}. Proceeding to structured generation.")
        phase_update["current_phase"] = "phase_2"
        
        # Add response content (without tool calls) to assistant history
        handoff_ai = AIMessage(content=response.content)
        updated_assistant_messages = assistant_messages + [handoff_ai]
        
        # Update token counts from the handoff call before generating
        token_update = {
            "input_tokens": state.get("input_tokens", 0) + input_tokens,
            "output_tokens": state.get("output_tokens", 0) + output_tokens,
        }
        
        
        main_message_key = get_message_key(node_name)
        
        sub_command = await _generate_node_output(
            state=state,
            node_name=node_name,
            message_key=main_message_key,
            system_prompt=confirmation_prompt,
            output_schema=output_schema,
            include_research_context=False,
            filter_context_nodes=[],
            confirmation_mode=True,
            additional_update={**phase_update, **token_update, assistant_message_key: updated_assistant_messages}
        )
        return sub_command
    else:
        # Still in Phase 1: Continue conversation
        logger.info(f"[{state['document_id'][:8]}] Continuing conversation for node {node_name}.")
        
        # Add AI's chat response to history (raw, with code intact)
        updated_assistant_messages = assistant_messages + [AIMessage(content=response.content)]
        
        
        # Prepare readable_message for interrupt: start with raw content, then render Mermaid if present
        readable_message = process_text_with_sketch(response.content)
        render_input = 0
        render_output = 0
        model_count_update = 0
        if node_name == "model_recommend" and "```" in readable_message:
            mermaid_code = readable_message.split("```")[1].split("mermaid")[-1]
            if "graph" in mermaid_code or "flowchart" in mermaid_code:
                logger.info(f"[{state['document_id'][:8]}] Start render image - Normal convo")
                success_render, fixed_mermaid_code, image_content, render_input, render_output = await extract_mermaid(mermaid_code, state, llm)
                if success_render:
                    # s3_client = get_s3_client()
                    # current_count = state.get("final_model_count", 0)
                    # key = f"{state['document_id']}_{current_count}.png"
                    # s3_client.put_object(
                    #     Bucket=settings.MINIO_BUCKET_ANALYSIS,
                    #     Key=key,
                    #     Body=image_content,
                    #     ContentType="image/png"
                    # )
                    # public_url = f"{settings.MINIO_DOMAIN}/{settings.MINIO_BUCKET_ANALYSIS}/{key}"
                    # img_ref = f'<img loading="lazy" src="{public_url}" alt="Relationship Graph">'
                    # Replace ONLY in readable_message (preserve code in history)
                    # new_content = full_block + "\n\n" + img_ref
                    readable_message = readable_message.replace(mermaid_code, f"\n{fixed_mermaid_code}\n")
                    model_count_update = 1
            else:
                readable_message = readable_message.replace(mermaid_code, mermaid_code.replace("\n", "</br>"))
                    
        # Set temp_review_data for review_interrupt (now with rendered img)
        temp_review_data = {
            "readable_message": readable_message,
            "obj": None,
            "node_name": f"{node_name}_phase_1",
            "message_key": assistant_message_key
        }
        
        token_update = {
            "input_tokens": state.get("input_tokens", 0) + input_tokens + render_input,
            "output_tokens": state.get("output_tokens", 0) + output_tokens + render_output,
        }
        
        update = {
            assistant_message_key: updated_assistant_messages,
            "temp_review_data": temp_review_data,
            **token_update,
            **phase_update
        }
        
        if model_count_update:
            update["final_model_count"] = state.get("final_model_count", 0) + model_count_update
        
        return Command(
            update=update,
            goto="review_interrupt"
        )
    
async def reference_selection(state: AnalysisState) -> Command[Literal["review_interrupt"]]:
    """Select relevant reference papers with their hypotheses and variables via conversational assistant."""
    return await _node_assistant(
        state=state,
        node_name="reference_selection",
        system_prompt=reference_selection_instructions,
        output_schema=ReferenceSelection,
        include_context_reference_selection=True
    )

async def recommend_assumptions(state: AnalysisState) -> Command[Literal["review_interrupt"]]:
    """Recommend research assumptions and hypotheses via conversational assistant."""
    return await _node_assistant(
        state=state,
        node_name="recommend_assumptions",
        system_prompt=assumptions_instructions,
        output_schema=AssumptionsRecommend,
        filter_context_nodes=['reference_selection']
    )

async def model_recommend(state: AnalysisState) -> Command[Literal["review_interrupt"]]:
    """Recommend conceptual model with thinking process and mermaid diagram via conversational assistant."""
    return await _node_assistant(
        state=state,
        node_name="model_recommend",
        system_prompt=model_instructions,
        output_schema=ModelRecommendation,
        filter_context_nodes=["recommend_assumptions", "recommend_variables"],
        include_research_context=False
    )

async def recommend_survey(state: AnalysisState) -> Command[Literal["review_interrupt"]]:
    """Recommend survey questions for each variable based on model, variables, and research context via conversational assistant."""
    return await _node_assistant(
        state=state,
        node_name="recommend_survey",
        system_prompt=survey_instructions,
        output_schema=SurveyRecommend,
        filter_context_nodes=["model_recommend", "recommend_variables", "reference_selection"],
        include_research_context=False,
        include_papers_only=False,
        include_context_no_papers=True
    )
    
async def recommend_variables(state: AnalysisState) -> Command[Literal["review_interrupt"]]:
    """Recommend variables in table format with Vietnamese structure via conversational assistant."""
    return await _node_assistant(
        state=state,
        node_name="recommend_variables",
        system_prompt=core_variables_instructions,
        output_schema=VariablesRecommend,
        filter_context_nodes=["recommend_assumptions"],
        include_research_context=False
    )

async def recommend_questions(state: AnalysisState) -> Command[Literal["review_interrupt"]]:
    """Recommend research questions based on research type via conversational assistant."""

    research_type = state.get("research_type", "dinh_luong")
    user_lang = state.get("language", "en")

    if research_type == "dinh_tinh":
        system_prompt = questions_instructions
        filter_context_nodes = []
        include_research_context = True  # Only research context for qualitative
    else:
        # Format the quantitative prompt with the state language
        system_prompt = quantitative_questions_instructions.format(user_lang=user_lang)
        filter_context_nodes = ["recommend_variables", "recommend_survey"]  # Include variables for dependents
        include_research_context = True  # Include both context and variables

    return await _node_assistant(
        state=state,
        node_name="recommend_questions",
        system_prompt=system_prompt,
        output_schema=QuestionsRecommend,
        filter_context_nodes=filter_context_nodes,
        include_research_context=include_research_context
    )

async def generate_readable_message(state: AnalysisState, ai_output: Any, node_name: str) -> tuple[str, int, int]:
    """Convert AI output to readable message in user language, optionally including latest user message. Returns (message, input_tokens, output_tokens)."""
    llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
    
    # Fetch the message key and latest user message for this node
    message_key = get_message_key(node_name)
    node_messages = state.get(message_key, [])
    latest_user_message = None
    if node_messages:
        for msg in reversed(node_messages):
            if isinstance(msg, HumanMessage):
                latest_user_message = msg.content
                break
    
    # Prepare latest user message string if it exists
    user_message_str = latest_user_message if latest_user_message else ""
    
    # NEW: Dynamic intro for questions based on research_type
    research_type = state.get("research_type", "dinh_luong")
    if research_type == "dinh_tinh":
        questions_intro = "To explore your research themes deeply, here are the guiding research questions I propose:"
    elif research_type == "hon_hop":
        questions_intro = "To guide your mixed-methods analysis effectively, these are the qualitative research questions I propose (blending quantitative measurability with qualitative depth):"
    else:  # dinh_luong or thu_cap (quantitative defaults)
        questions_intro = "To guide your quantitative analysis effectively, these are the qualitative research questions I propose:"
    
    # Updated: Use node-specific finalized intro phrase, especially for first-time or post-conversation generation
    node_specific_finalized_intro = {
        "recommend_assumptions": "Before I recommend the next element, here is the finalized assumptions and hypotheses we’ve discussed so far:",
        "model_recommend": "Before I recommend the next element, here is the finalized conceptual model we’ve discussed so far:",
        "recommend_variables": "Before I recommend the next element, here are the finalized core variables we’ve discussed so far:",
        "recommend_survey": "Before I recommend the next element, here are the finalized survey questions we’ve discussed so far:",
        "recommend_questions": "Before I recommend the next element, here are the finalized qualitative questions we’ve discussed so far:",
        "finalize_analysis": "We've come a long way! Here's a polished summary of what we agreed so far, pulling everything together:"
    }
    
    finalized_intro_example = node_specific_finalized_intro.get(node_name, "Before I recommend the next element, here is the finalized output we’ve discussed so far:")
    
    # Determine if first generation: check if node_messages contain any HumanMessage that is feedback (not starting with "Research context:")
    has_feedback = any(
        isinstance(msg, HumanMessage) and not msg.content.startswith("Research context:")
        for msg in node_messages
    )
    is_first_generation = not has_feedback
    
    # Set the starting phrase based on whether it's the first output for this element type
    if is_first_generation:
        start_phrase = finalized_intro_example
        feedback_nod_instruction = "No user feedback has been provided yet, so do not include any nod to recent input."
    else:
        start_phrase = f"Based on your recent feedback, here is the updated {node_name.replace('_recommend', '').replace('_', ' ')} we’ve discussed so far:"
        feedback_nod_instruction = "Weave in a brief nod to the recent user feedback right after the intro (e.g., 'Incorporating your note on adding more variables, ...')."
    
    
    recommend_assumptions_note =  ""
    model_recommend_note =  ""
    recommend_variables_note =  ""
    recommend_survey_note =  ""
    recommend_questions_note =  ""
    finalize_analysis_note = ""

    if node_name == "recommend_assumptions":
        recommend_assumptions_note = ""
    if node_name == "model_recommend":
        model_recommend_note = "    - For diagrams (e.g., the sketch in model recommend), preserve the exact code block as-is within a proper fenced code block, including all nodes, edges, labels, and structure from the output—do not regenerate or alter."
    if node_name == "recommend_variables":
        recommend_variables_note = "    - For variables, display names, types, descriptions, and scales in a **Markdown table** (columns: *Name | Type | Description | Scale*), copying each row exactly."
    if node_name == "recommend_survey":
        recommend_survey_note = "    - For surveys, group by variable with subheadings (e.g., ### Variable: [Name]), then list questions with scale types and answer contents in a table or formatted list (e.g., **Question:** [text] | **Scale Type:** [type] | **Answer Content:** [description if any])."
    if node_name == "recommend_questions":
        recommend_questions_note = "    - For questions, output a numbered list (1., 2., etc.) copying each question verbatim from the output—match the exact count and wording."
    if node_name == "finalize_analysis":
        # Check if there are any inherited hypotheses from the state elements
        total_inherited = 0
        elements = state.get("elements", [])
        for elem in elements:
            if elem.get("accepted") and elem.get("node") == "recommend_assumptions":
                obj = elem.get("object")
                if obj and hasattr(obj, 'inherited_from_papers_count') and obj.inherited_from_papers_count:
                    total_inherited = max(total_inherited, obj.inherited_from_papers_count)

        if total_inherited > 0:
            finalize_analysis_note = f"""
    - IMPORTANT: After presenting all the content, you must add a clear warning section with the following message (translate to {state["language"]}):

      "**Important Notice**: Your model and hypotheses have been inherited from {total_inherited} research paper(s). autoresearching has explained the basis of inheritance for each model. Please add images and diagrams of the original models from these papers to your article for proper attribution and theoretical grounding."
"""

    system_prompt = f"""You are a research assistant who rewrites structured outputs into clear, user-friendly messages in {state["language"]}. 
Your goal is to make the content easy to read, review, and provide feedback on.

Always format the output using markdown syntax for readability (use headings, bullet points, numbered lists, and tables where appropriate).  

Guidelines:
1. Start with an introduction that acknowledges the user's progress and introduces the content warmly, matching the node type. Use a finalizing tone since this is the structured output.
    - Begin with this phrase: "{start_phrase}" (adapted to {state["language"]})
    {feedback_nod_instruction}

2. Present the content accessibly:
    - Use exactly all items from the provided output—do not omit any, do not add any, do not rephrase, consolidate, interpret, or adjust. Copy every single item verbatim, preserving exact wording, order, count, and details (e.g., if the output reflects user-refined updates like added/edited questions, variables, or diagram code, replicate those precisely without deviation). The provided output is the authoritative, history-refined version—treat it as final.
{recommend_variables_note}
{model_recommend_note}
{recommend_survey_note}
{recommend_questions_note}
{finalize_analysis_note}

3. End with a call to action similar to the following, adapted to {state["language"]}:
    - For English: "If you agree with this proposed content, please enter OK. If you want to change or adjust anything, please let me know!"

General principles:
- Be concise but informative.
- Use bullet points or numbered lists for clarity.
- Adjust tone to be supportive and collaborative.
- Fidelity to the provided output is paramount—any deviation (even minor) invalidates the response.
"""
    
    user_content = f"""
Node type: {node_name}
Latest user message (if any): {user_message_str}

Convert this output to a readable (message-like) format: 
{ai_output}

---

Please respond in {state["language"]}.
    """
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_content)
    ]
    
    response = await llm.ainvoke(messages)
    
    # Extract token counts
    input_tokens = getattr(response, 'usage_metadata', {}).get('input_tokens', 0)
    output_tokens = getattr(response, 'usage_metadata', {}).get('output_tokens', 0)
    
    return response.content, input_tokens, output_tokens

async def check_feedback_intent(state: AnalysisState, feedback: str, message_history: List[AnyMessage]) -> tuple[FeedbackIntent, int, int]:
    """Check the intent of user feedback using LLM. Returns (intent, input_tokens, output_tokens)."""
    llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
    
    # Convert message history to string context
    history_context = "\n".join([
        f"{'Human' if isinstance(msg, HumanMessage) else 'AI'}: {msg.content}"
        for msg in message_history[-3:]  # Last 3 messages for context
    ])
    
    system_prompt = """
You are an intent classifier for user feedback in a research analysis workflow. Classify the *latest user feedback* as one of four intents, prioritizing forward momentum, based on conversation history (for context) and latest feedback ONLY. For SHORT feedbacks (1-5 words), DEFAULT to "accept" unless explicitly requesting change, clarity, or off-topic—do not overthink or infer from history.

Intents:
1. "accept" - User fully agrees/approves current output (e.g., ready to move on). Default for neutral/positive short responses.
2. "edit" - User wants specific modifications (e.g., "add X", "change Y").
3. "not_related" - Feedback unrelated/off-topic (e.g., random question, joke).
4. "clarify" - User expresses confusion/needs explanation (e.g., "what does this mean?", "unclear").

**Rules**:
- Standalone affirmations (e.g., "OK", "yes", "good", "fine", "proceed", "looks good", "that's it", "approve") → "accept"; ignore history unless direct contradiction.
- "edit" requires actionable details (e.g., "edit diagram to add Z"); vague ("maybe" alone) → "accept".
- "clarify" needs explicit confusion words (e.g., "explain", "why", "don't understand"); "OK but..." → "edit" if "but" has specifics.
- Reasoning: BRIEF. Explain choice, especially if NOT "accept" for short feedback.

Examples (focus on latest feedback):
- "OK" / "ok" / "Ok" / "Yes" / "Fine" / "Good" (case-insensitive) → accept (clear affirmation, default for short input)
- "OK, proceed" / "Looks good, next" → accept
- "Change variable to Price" / "Add more assumptions" / "Fix the sketch" → edit (specific request)
- "What's the weather?" / "LOL unrelated" → not related
- "I don't get the model" / "Clarify H1" / "Explain diagram?" → clarify (Explicit confusion)
- "OK but add gender control" → edit (Affirmation + specific change)
- "Maybe, what do you think?" → clarify (Hesitation seeking more info)

For "edit", extract exact feedback text into `extracted_feedback`.
"""
    
    user_content = f"""
    Conversation History:
    ```
    {history_context}
    ```
    Latest User Feedback: {feedback}
    
    Classify the intent and reasoning.
    """

    error, success, response, input_tokens, output_tokens = await get_answer_with_schema(state['document_id'][:8], llm, system_prompt, user_content, FeedbackIntent)
    if not success:
        raise error
    return response, input_tokens, output_tokens

async def review_interrupt(state: AnalysisState) -> Command[Literal["human_feedback"]]:
    """Dedicated node for interrupting with readable output—minimizes re-execution risk."""
    logger.info(f"[{state['document_id'][:8]}] Review interrupt node: presenting output for feedback")
    
    temp_data = state.get("temp_review_data", {})
    if not temp_data:
        logger.info(f"[{state['document_id'][:8]}] No temp review data—skipping interrupt")
        return Command(update={"temp_review_data": None}, goto="human_feedback")
    
    readable_message = temp_data["readable_message"]
    display_object = temp_data["obj"]
    node_name = temp_data["node_name"]  # For logging
    
    logger.info(f"[{state['document_id'][:8]}] Interrupting for {node_name} review")
    
    feedback = interrupt(readable_message)

    # Clear temp data UNLESS it's inherit_display (which needs it to route back)
    update = {}
    if node_name != "inherit_display":
        update["temp_review_data"] = None

    if feedback:
        # Immediately add feedback to the relevant message history
        message_key = temp_data["message_key"]
        current_messages = state.get(message_key, [])
        updated_messages = current_messages + [HumanMessage(content=str(feedback["user_input"]))]
        update[message_key] = updated_messages
        update["model_id"] = feedback["model_id"]
        update["llm_key"] = feedback["llm_key"]
        update["input_tokens"] = 0
        update["output_tokens"] = 0
        logger.info(f"[{state['document_id'][:8]}] Feedback added to {message_key}: {feedback["user_input"]}")

    # Always goto human_feedback for intent processing
    return Command(update=update, goto="human_feedback")

async def reask_node(state: AnalysisState) -> Command[Literal["human_feedback"]]:
    """Reask for relevant feedback when user input is not related or needs clarity—caches base prompt if history stable."""
    logger.info(f"[{state['document_id'][:8]}] Reask node: asking for relevant feedback")
    
    elements = state.get("elements", [])
    user_lang = state.get("language", "Vietnamese")
    
    # Find the current unaccepted element
    current_element = None
    for elem in elements:
        if not elem["accepted"]:
            current_element = elem
            break
    
    if not current_element:
        return Command(goto="human_feedback")
    
    # Get message history for current node (latest 5 messages only)
    message_key = get_message_key(current_element["node"])
    message_history = state.get(message_key, [])[-5:]  # Latest 5
    
    # Simple cache check: hash last 3 messages to avoid full re-gen if irrelevant again
    history_hash = hash("".join([msg.content for msg in message_history[-3:]]))
    cached_reask = state.get("cached_reask_hash") == history_hash
    input_tokens = output_tokens = 0
    
    reask_type = state.get("temp_reask_type", "not_related")
    
    if cached_reask:
        reask_message = state.get("cached_reask_message", "Please focus on the current output and provide feedback.")
        logger.info(f"[{state['document_id'][:8]}] Using cached reask message")
    else:
        # Generate reask message using LLM
        llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
        
        node_prompt = get_node_prompt(current_element["node"])
        
        system_prompt = f"""
        You are helping a user provide relevant feedback for a research analysis task.
        The user's previous response was {reask_type} to the current task.
        
        Original task instructions for domain knowledge:
        {node_prompt}
        
        The research's context:
        {state["research_context"]}
        Other's relevant research context (use as references):
        {str(state["relevant_papers"])}
        Based on the conversation history, generate a helpful message in {state["language"]} that:
        """
        if reask_type == "clarify":
            system_prompt += """
            1. Acknowledges the user's confusion and provides a brief, clearer explanation of the current output, drawing from the original task instructions.
            2. Gives 2-3 specific examples of the type of feedback or information needed (e.g., for assumptions: "Example feedback: 'Add a null hypothesis for variable X' or 'This assumption seems unclear—can you simplify?'").
            3. Clearly asks for acceptance, specific edits, or further questions to clarify.
            
            Be empathetic, use simple language, and guide them back to the task with domain-specific examples.
            """
        else:
            system_prompt += """
            1. Politely indicates their previous input wasn't related to the current task
            2. Summarizes what they need to review based on the conversation and original task
            3. Clearly asks for either acceptance or specific feedback
            
            Be friendly and guide them back to the task at hand, incorporating domain knowledge from the original instructions.
            """
        system_prompt += "Important: If you don't know the answer or don't have enough information, just say you don't know, do not guess."
        # Prepare messages: system prompt + message history
        invoke_messages = [SystemMessage(content=system_prompt)] + message_history
        
        response = await llm.ainvoke(invoke_messages)
        
        reask_message = response.content
        input_tokens = getattr(response, 'usage_metadata', {}).get('input_tokens', 0)
        output_tokens = getattr(response, 'usage_metadata', {}).get('output_tokens', 0)
        
        # Cache for next time
        cache_update = {
            "cached_reask_hash": history_hash,
            "cached_reask_message": reask_message
        }
        logger.info(f"[{state['document_id'][:8]}] Generated and cached new reask message")
    
    # Create interrupt for reask
    display_object = current_element["object"].model_dump() if hasattr(current_element["object"], 'model_dump') else current_element["object"]
    
    feedback = interrupt(reask_message)
    
    if feedback:
        # Add the feedback to message history and update token counts
        current_messages = state.get(message_key, [])
        updated_messages = current_messages + [HumanMessage(content=str(feedback))]
        # Reset token count when interrupt
        base_update = {
            message_key: updated_messages,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "temp_reask_type": None,
        }
        if not cached_reask:
            base_update.update(cache_update)
        
        return Command(update=base_update, goto="human_feedback")
    
    # Update token counts even if no feedback received
    base_update = {
        "input_tokens": state.get("input_tokens", 0) + input_tokens,
        "output_tokens": state.get("output_tokens", 0) + output_tokens,
        "temp_reask_type": None,
    }
    if not cached_reask:
        base_update.update(cache_update)
    
    return Command(update=base_update, goto="human_feedback")

async def human_feedback_node(state: AnalysisState) -> Command[Literal["model_recommend", "recommend_assumptions", "recommend_variables", "recommend_survey", "recommend_questions", "finalize_analysis", "parse_variables", "reask_node", "inherit_display", "__end__"]]:
    """Post-resume handler: adds feedback, checks intent, decides next step."""
    logger.info(f"[{state['document_id'][:8]}] Human feedback node: processing resume")

    elements = state.get("elements", [])
    research_type = state.get("research_type", "dinh_luong")

    current_phase = state.get("current_phase")

    # --- BRANCH 0: Handle inherit_display feedback ---
    # Check if we're coming from inherit_display (via temp_review_data)
    temp_data = state.get("temp_review_data", {})
    if temp_data and temp_data.get("node_name") == "inherit_display":
        # Always route back to inherit_display - it handles the logic via tool calls
        logger.info(f"[{state['document_id'][:8]}] Routing back to inherit_display to process user feedback")
        return Command(
            update={"temp_review_data": None},
            goto="inherit_display"
        )

    # --- BRANCH 1: Phase 1 - Conversational Turn ---
    # If we are in a conversation, we don't need to check intent.
    # We just pass the user's message back to the assistant.
    if current_phase == "phase_1":
        current_assistant_node = state.get("current_assistant_node")
        if not current_assistant_node:
            logger.error(f"[{state['document_id'][:8]}] Logic Error: In Phase 1 but no current_assistant_node is set. Cannot continue. Ending.")
            return Command(update={"current_phase": None, "current_assistant_node": None}, goto="__end__")

        logger.info(f"[{state['document_id'][:8]}] Phase 1: Continuing conversation. Routing back to '{current_assistant_node}'.")
        return Command(goto=current_assistant_node)
    
    # --- BRANCH 2: Phase 2 / Default - Review and Decision ---
    # Early exit if no elements (all done, including finalize if present)
    if not elements:
        logger.info(f"[{state['document_id'][:8]}] No elements in state. All processing complete.")
        return Command(update={"current_phase": None, "current_assistant_node": None}, goto="__end__")
    
    # Find the latest unaccepted element (only for phase 2 or no phase)
    latest_element = None
    latest_index = -1
    for i, elem in enumerate(elements):
        if not elem["accepted"]:
            latest_element = elem
            latest_index = i
            break
    
    if latest_element is None:
        # All elements are accepted (including finalize if present), proceed to end
        logger.info(f"[{state['document_id'][:8]}] All elements accepted. Proceeding to end.")
        return Command(update={"current_phase": None, "current_assistant_node": None}, goto="__end__")
    
    # Check if this node should be auto-accepted
    auto_mode = state.get("auto_mode", True)
    auto_accept_nodes = state.get("auto_accept_nodes", [])
    current_node = latest_element["node"]

    should_auto_accept = auto_mode and (current_node != "finalize_analysis" or current_node in auto_accept_nodes)
    
    if should_auto_accept:
        # Auto-accept the current element
        updated_elements = elements.copy()
        updated_elements[latest_index]["accepted"] = True
        next_node = get_next_node(current_node, research_type)
        logger.info(f"[{state['document_id'][:8]}] Auto-accepting '{current_node}' and proceeding to '{next_node}'.")
        return Command(
            update={
                "elements": updated_elements,
                "current_phase": None,
                "current_assistant_node": None
            },
            goto=next_node
        )
    
    # Get message history for current node
    message_key = get_message_key(latest_element["node"])
    message_history = state.get(message_key, [])
    
    # Since we resume after interrupt, latest should be HumanMessage (feedback)
    # But to be robust, check if there's a pending human message
    if not message_history or not isinstance(message_history[-1], HumanMessage):
        # Rare edge: no feedback yet—re-interrupt or end
        logger.info(f"[{state['document_id'][:8]}] No human feedback found—ending")
        return Command(update={"current_phase": None, "current_assistant_node": None}, goto="__end__")
    
    updated_elements = elements.copy()
    latest_human_message = message_history[-1].content
    
    # Before token init or intent check ***
    stripped_feedback = latest_human_message.strip().upper()
    if stripped_feedback in ["HOANTOANDONGY"]:
        # Force accept to avoid LLM flakiness
        updated_elements[latest_index]["accepted"] = True
        next_node = get_next_node(latest_element["node"], research_type)
        logger.info(f"[{state['document_id'][:8]}] Hard fallback: Accepting '{latest_element["node"]}' due to explicit trigger response.")
        base_update = {
            "elements": updated_elements,
            "current_phase": None,
            "current_assistant_node": None,
        }
        return Command(update=base_update, goto=next_node)
    
    total_input_tokens = 0
    total_output_tokens = 0
    
    # Await intent check (only for phase 2 / structured output)
    try:
        intent_result, input_tokens, output_tokens = await check_feedback_intent(
            state, latest_human_message, message_history
        )
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens
    except Exception as e:
        logger.info(f"[{state['document_id'][:8]}] Error checking feedback intent: {e}")
        # Default to edit intent if LLM fails
        intent_result = FeedbackIntent(
            intent="edit",
            reasoning="LLM intent check failed, defaulting to edit",
            extracted_feedback=latest_human_message
        )
    
    # Base update with token tracking
    base_update = {
        "elements": updated_elements,
        "input_tokens": state.get("input_tokens", 0) + total_input_tokens,
        "output_tokens": state.get("output_tokens", 0) + total_output_tokens,
        "current_phase": None,
        "current_assistant_node": None,
    }
    
    if intent_result.intent == "accept":
        # Accept the current element
        updated_elements[latest_index]["accepted"] = True
        next_node = get_next_node(latest_element["node"], research_type)
        logger.info(f"[{state['document_id'][:8]}] Accepting '{latest_element["node"]}' and proceeding to '{next_node}'.")
        return Command(
            update=base_update,
            goto=next_node
        )
    elif intent_result.intent == "edit":
        # Process edit feedback - go back to the same node
        if state.get("current_phase") == "phase_2":
            assistant_key = get_assistant_message_key(latest_element["node"])
            # Get the proposed output (last AI message before feedback or from element)
            main_messages = state.get(message_key, [])
            proposed_output = ""
            if len(main_messages) >= 2 and isinstance(main_messages[-2], AIMessage):
                proposed_output = main_messages[-2].content
            else:
                proposed_output = str(latest_element["object"])
            ass_messages = state.get(assistant_key, [])
            # Append proposal and feedback to continue conversation
            proposal_ai = AIMessage(content=f"Based on our discussion, here is the proposed structured output:\n{proposed_output}")
            feedback_human = HumanMessage(content=latest_human_message)
            ass_messages += [proposal_ai, feedback_human]
            base_update[assistant_key] = ass_messages
            base_update["current_phase"] = "phase_1"
        logger.info(f"[{state['document_id'][:8]}] Editing '{latest_element["node"]}' - looping back.")
        return Command(
            update=base_update,
            goto=latest_element["node"]
        )
    else:  # not_related or clarify
        reask_type = "clarify" if intent_result.intent == "clarify" else "not_related"
        # Go to reask node
        logger.info(f"[{state['document_id'][:8]}] Reasking for '{reask_type}' on '{latest_element["node"]}'.")
        return Command(
            update={**base_update, "temp_reask_type": reask_type},
            goto="reask_node"
        )

async def finalize_analysis(state: AnalysisState) -> Command[Literal["review_interrupt"]]:
    """
    Finalize the analysis by generating aggregated results from all accepted elements.
    For first time: hardcoded aggregation + LLM for summary only.
    For subsequent: full LLM regeneration via _generate_node_output.
    No token updates to state.
    """
    logger.info(f"[{state['document_id'][:8]}] Finalizing analysis results")
    
    elements = state.get("elements", [])
    research_type = state.get("research_type", "dinh_luong")  # Fetch research_type early
    existing_finalize_index = next((i for i, elem in enumerate(elements) if elem["node"] == "finalize_analysis"), None)
    
    message_key = "finalize_messages"
    llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
    
    if existing_finalize_index is None:
        logger.info(f"[{state['document_id'][:8]}] First time for finalizing analysis results")
        # First time: hardcoded aggregation + LLM summary
        accepted_elements = [elem for elem in elements if elem["accepted"]]
        
        # Initialize collections for final results
        final_model = ModelRecommendation(
            thinking_process="Qualitative research does not have relational model",
            sketch=""
        )
        final_assumptions = []
        final_variables = []
        final_surveys = []
        final_questions = []
        total_inherited_count = 0

        # Process each accepted element and combine results
        get_cite_in = 0
        get_cite_out = 0
        for element in accepted_elements:
            node_name = element["node"]
            obj = element["object"]

            if node_name == "model_recommend" and isinstance(obj, ModelRecommendation):
                final_model = obj
            elif node_name == "recommend_assumptions" and isinstance(obj, AssumptionsRecommend):
                final_assumptions.extend(obj.assumptions)
                # Track the inherited count from AssumptionsRecommend
                if hasattr(obj, 'inherited_from_papers_count') and obj.inherited_from_papers_count:
                    total_inherited_count = max(total_inherited_count, obj.inherited_from_papers_count)
            elif node_name == "recommend_variables" and isinstance(obj, VariablesRecommend):
                final_variables.extend(obj.variables)
            elif node_name == "recommend_survey" and isinstance(obj, SurveyRecommend):
                paper_titles = [paper["title"] for paper in state["relevant_papers"]]
                for survey in obj.surveys:
                    survey.source = title_matching(survey.source, paper_titles)
                    if survey.source != "Original design":
                        survey.source = "[" + survey.source + "]"
                    else:
                        survey.source = ""
                obj.surveys, get_cite_in, get_cite_out = await get_uncite_survey(state['document_id'][:8], llm, obj.surveys, state["relevant_papers"])
                final_surveys.extend(obj.surveys)
            elif node_name == "recommend_questions" and isinstance(obj, QuestionsRecommend):
                final_questions.extend(obj.questions)
        
        # Post-processing: Remove duplicates
        # Deduplicate assumptions by hypothesis_id
        unique_assumptions = []
        seen_assumption_ids = set()
        for assumption in final_assumptions:
            if assumption.hypothesis_id not in seen_assumption_ids:
                unique_assumptions.append(assumption)
                seen_assumption_ids.add(assumption.hypothesis_id)
        
        # Deduplicate variables by name
        unique_variables = []
        seen_variable_names = set()
        for variable in final_variables:
            if variable.name not in seen_variable_names:
                unique_variables.append(variable)
                seen_variable_names.add(variable.name)
        
        # Deduplicate surveys by variable_name (keep all questions)
        unique_surveys = []
        seen_survey_vars = set()
        for survey_var in final_surveys:
            if survey_var.variable_name not in seen_survey_vars:
                unique_surveys.append(survey_var)
                seen_survey_vars.add(survey_var.variable_name)
        
        # No deduplication for questions
        unique_questions = final_questions

        # Conditional summary prompt based on research_type and model presence
        summary_prompt = f"""
You are a helpful assistant that synthesizes multiple research analysis components
into a clear, user-friendly final summary in {state["language"]}.

### Guidelines:
1. Start with a short introduction phrase in {state["language"]} introducing the summary.

2. Structure the content with Markdown:
   - Key Insights / Totals: Use bullet points for listing, including the provided counts for assumptions, variables, surveys, questions, etc.
   - Assumptions: Present as a bullet list using ALL provided items verbatim, and include the count.
   - Variables: Use a compact Markdown table with columns: Name | Type | Description | Scale using all provided items verbatim, and include the count.
   - Surveys: Group by variable with subheadings (### Variable: [Name]), then list questions with scale types and answer contents using ALL provided items verbatim, and include the total count of questions.
   - Questions: Use a numbered list using ALL provided items verbatim, and include the count.
"""
        if research_type != "dinh_tinh":
            summary_prompt += """
   - Models: Include model description (inherit from thinking process) and sketch details if available (preserve the exact fenced code block).
"""

        # Add inheritance warning if applicable
        if total_inherited_count > 0:
            summary_prompt += f"""

3. IMPORTANT - Inheritance Notice: After presenting all the content, add a clear warning section with this exact message (translate to {state["language"]}):

   "**Important Notice**: Your model and hypotheses have been inherited from {total_inherited_count} research paper(s). autoresearching has explained the basis of inheritance for each model. Please add images and diagrams of the original models from these papers to your article for proper attribution and theoretical grounding."

"""

        summary_prompt += f"""
{"4" if total_inherited_count > 0 else "3"}. End with a friendly, interactive call to action in {state["language"]}.

### General Principles:
- Always ensure the summary is clear, balanced, and easy to review.
- If some components are missing or incomplete, still present what is available gracefully.
- Prioritize readability and coherence — keep it concise but comprehensive.
"""

        # Prepare FULL data for summary (no truncation)
        model_str = str(final_model.model_dump())
        assump_full = "\n".join([f"- {a.hypothesis_id}: {a.statement}" for a in unique_assumptions]) if unique_assumptions else "None"
        var_full = "| Name | Type | Description | Scale |\n|------|------|-------------|-------|\n" + "\n".join([f"| {v.name} | {v.variable_type} | {v.description} | {v.scale} |" for v in unique_variables]) if unique_variables else "None"
        survey_full = ""
        total_survey_questions = 0
        if unique_surveys:
            for sv in unique_surveys:
                survey_full += f"### Variable: {sv.variable_name} ({sv.measurement_type})\n"
                for q in sv.questions:
                    q_str = f"- **Question:** {q.question}"
                    if q.scale_type:
                        q_str += f" | **Scale Type:** {q.scale_type}"
                    if q.answer_content:
                        q_str += f" | **Answer Content:** {q.answer_content}"
                    survey_full += q_str + "\n"
                total_survey_questions += len(sv.questions)
        survey_full = f"Surveys (Total Questions: {total_survey_questions}):\n{survey_full}" if unique_surveys else "None"
        quest_full = "\n".join([f"{i+1}. {q}" for i, q in enumerate(unique_questions)]) if unique_questions else "None"
        
        user_content = f"""Aggregated Data:
- Model:
{model_str}
- Assumptions ({len(unique_assumptions)}): 
{assump_full}
- Variables ({len(unique_variables)}): 
{var_full}
- Surveys: 
{survey_full}
- Questions ({len(unique_questions)}): 
{quest_full}
"""
        logger.info(f"[{state['document_id'][:8]}] Finalize Model: \n{model_str}")
        messages = [
            SystemMessage(content=summary_prompt),
            HumanMessage(content=user_content)
        ]

        error, success, summary, input_tokens, output_tokens = await get_answer(state['document_id'][:8], llm, messages)
        if not success:
            raise error

        # Prepare messages for first time
        node_messages = []
        ai_message = AIMessage(content=summary)
        updated_node_messages = node_messages + [ai_message]
        
        # Process the sketch for display (readable_message) and final results
        summary = process_text_with_sketch(summary)  
        
        # Guarded rendering - skip for qualitative or no sketch
        render_input = 0
        render_output = 0
        final_model_count = state.get("final_model_count", 0)
        if (research_type != "dinh_tinh" and final_model.sketch and "```" in summary):
            mermaid_code = summary.split("```")[1].split("mermaid")[-1]
            if "graph" in mermaid_code or "flowchart" in mermaid_code:
                logger.info(f"[{state['document_id'][:8]}] Start render image - Final analysis")
                success_render, fixed_mermaid_code, image_content, render_input, render_output = await extract_mermaid(mermaid_code, state, llm)
                if success_render:
                    s3_client = get_s3_client()
                    mongo_client = get_mongodb_client()
                    db = mongo_client["admin"]
                    collection = db["document_configurations"]
                    document_config = await collection.find_one({"documentId": state["document_id"]})
                    user_id = document_config["createdBy"]
                    s3_client.put_object(
                        Bucket="users",
                        Key=f"{user_id}/article/{state["document_id"]}/{settings.MINIO_BUCKET_ANALYSIS}/{state["document_id"]}_{final_model_count}.png",
                        Body=image_content,
                        ContentType="image/png"
                    )
                    public_url = f"{settings.MINIO_DOMAIN}/users/{user_id}/article/{state["document_id"]}/{settings.MINIO_BUCKET_ANALYSIS}/{state["document_id"]}_{final_model_count}.png"
                    img_ref = f'<img loading="lazy" src="{public_url}" alt="Relationship Graph">'
                    final_model_count += 1
                    final_model.sketch = fixed_mermaid_code + "\nlink: " + public_url
                    summary = summary.replace(mermaid_code, f"\n{fixed_mermaid_code}\n")
            else:
                summary = summary.replace(mermaid_code, mermaid_code.replace("\n", "</br>"))
        
        finalized_results = FinalizedResults(
            final_model=final_model,
            final_assumptions=unique_assumptions,
            final_variables=unique_variables,
            final_surveys=unique_surveys,
            final_questions=unique_questions,
            summary=summary.strip(),
            total_input_tokens=input_tokens,
            total_output_tokens=output_tokens
        )
        
        # Create new element
        new_element = StateElement(
            accepted=False,
            node="finalize_analysis",
            object=finalized_results
        )
        elements.append(new_element)
        # Update without token changes
        display_object = finalized_results.model_dump()
        temp_review_data = {
            "readable_message": summary,
            "obj": display_object,
            "node_name": "finalize_analysis",
            "message_key": message_key
        }
        return Command(
            update={
                "elements": elements,
                message_key: updated_node_messages,
                "temp_review_data": temp_review_data,
                "input_tokens": state.get("input_tokens", 0) + input_tokens + get_cite_in + render_input,
                "output_tokens": state.get("output_tokens", 0) + output_tokens + get_cite_out + render_output,
                "final_model_count": final_model_count,
            },
            goto="review_interrupt"
        )
    else:
        # Not first time: regenerate via full LLM
        return await _generate_node_output(
            state=state,
            node_name="finalize_analysis",
            message_key=message_key,
            system_prompt=finalize_instructions,
            output_schema=FinalizedResults,
            filter_context_nodes=[],  # Include all for re-aggregation if needed
            include_research_context=False,
        )

async def parse_variables_node(state: AnalysisState) -> Command[Literal["__end__"]]:
    """
    Parse the finalized variables and surveys into the format required by the analyzer.
    This node runs after finalize_analysis is accepted.
    """
    logger.info(f"[{state['document_id'][:8]}] Parsing variables for analyzer")

    elements = state.get("elements", [])

    # Find the finalize_analysis element
    finalize_element = None
    finalize_index = None
    for i, elem in enumerate(elements):
        if elem["node"] == "finalize_analysis" and elem["accepted"]:
            finalize_element = elem
            finalize_index = i
            break

    if not finalize_element:
        logger.info(f"[{state['document_id'][:8]}] No accepted finalize_analysis element found")
        return Command(goto="__end__")

    finalized_results = finalize_element["object"]

    # Convert final_variables to the format expected by parse_raw_variables
    variables_for_parsing = []
    for var in finalized_results.final_variables:
        var_dict = {
            "name": var.name,
            "variable_type": var.variable_type,
            "measurement_type": var.measurement_type,
            "description": var.description,
            "scale": var.scale
        }
        variables_for_parsing.append(var_dict)

    # Convert final_surveys to the format expected by parse_raw_variables
    survey_questions_for_parsing = []
    for survey in finalized_results.final_surveys:
        for question in survey.questions:
            survey_q = {
                "variable_name": survey.variable_name,
                "variable_code": survey.variable_code,
                "variable_type": survey.variable_type,
                "measurement_type": survey.measurement_type,
                "questions": question.question,
                "answer_content": question.answer_content
            }
            survey_questions_for_parsing.append(survey_q)

    # First, manually parse the survey questions into variable objects
    logger.info(f"[{state['document_id'][:8]}] Manually parsing survey data into variable structures")
    pre_parsed_variables = manually_parse_survey_to_variables(
        variables=variables_for_parsing,
        survey_questions=survey_questions_for_parsing
    )
    logger.info(f"[{state['document_id'][:8]}] Pre-parsed {len(pre_parsed_variables)} variables from survey data")

    # Get LLM and use it to fix/refine the pre-parsed variables
    llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
    language = state.get("language", "Vietnamese")

    try:
        logger.info(f"[{state['document_id'][:8]}] Using LLM to fix and refine pre-parsed variables")
        error, success, parsed_variables, input_tokens, output_tokens = await parse_raw_variables(
            _id=state['document_id'][:8],
            variables=variables_for_parsing,
            survey_questions=survey_questions_for_parsing,
            llm=llm,
            language=language,
            pre_parsed_variables=pre_parsed_variables
        )

        if not success:
            logger.info(f"[{state['document_id'][:8]}] Failed to parse variables: {error}")
            parsed_variables = []
            input_tokens = 0
            output_tokens = 0
        else:
            logger.info(f"[{state['document_id'][:8]}] Successfully parsed {len(parsed_variables)} variables")

    except Exception as e:
        logger.info(f"[{state['document_id'][:8]}] Error parsing variables: {e}")
        parsed_variables = []
        input_tokens = 0
        output_tokens = 0

    # Update the finalized results with parsed variables
    finalized_results.parsed_variables = parsed_variables
    finalized_results.total_input_tokens += input_tokens
    finalized_results.total_output_tokens += output_tokens

    # Update the element in state
    updated_elements = elements.copy()
    updated_elements[finalize_index]["object"] = finalized_results

    return Command(
        update={
            "elements": updated_elements,
            "input_tokens": state.get("input_tokens", 0) + input_tokens,
            "output_tokens": state.get("output_tokens", 0) + output_tokens,
        },
        goto="__end__"
    )

# Define and compile the graph - Updated with simplified structure
async def get_graph(checkpointer: Checkpointer):
    builder = StateGraph(AnalysisState)
    # Add nodes - Updated with new review_interrupt; removed go_end as redundant
    builder.add_node("get_key_points", get_key_points)
    builder.add_node("coordinator", coordinator)
    builder.add_node("inherit_display", inherit_display)  # New for inherit mode
    builder.add_node("reference_selection", reference_selection)  # New reference selection node
    builder.add_node("model_recommend", model_recommend)
    builder.add_node("human_feedback", human_feedback_node)
    builder.add_node("reask_node", reask_node)
    builder.add_node("recommend_assumptions", recommend_assumptions)
    builder.add_node("recommend_variables", recommend_variables)
    builder.add_node("recommend_survey", recommend_survey)  # New
    builder.add_node("recommend_questions", recommend_questions)
    builder.add_node("finalize_analysis", finalize_analysis)
    builder.add_node("review_interrupt", review_interrupt)
    builder.add_node("parse_variables", parse_variables_node)

    # Define only essential edges - Command.goto handles the rest
    builder.add_edge(START, "get_key_points")
    builder.add_edge("get_key_points", "coordinator")

    return builder.compile(checkpointer=checkpointer)
