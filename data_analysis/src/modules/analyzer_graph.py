import json
import logging
from typing import Annotated, Literal
from pydantic import BaseModel
from langgraph.graph import StateGraph, START, END
from langgraph.types import Command, interrupt, Checkpointer, Send
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool
import re
import json

from data_analysis.src.modules.tools.analysis.pipeline import execute_pipeline
from data_analysis.src.configs.documents import JSON_DOCUMENT, TOOLS_DOCUMENT
from data_analysis.src.modules.utils import (
    get_current_dataframe, 
    search_document,
    process_generated_files,
    serialize_dict
)

from data_analysis.src.modules.tools.analysis.tool_availability import get_tool_availability

from data_analysis.src.schemas.analyzer_states import (
    Plan, 
    AnalysisStepType, 
    ExecutionMode, 
    LogEntry,
    State, 
    Query, 
    Doc, 
    AnalyzeStep, 
    ExecutionLog, 
    SimplifiedPlan, 
    SimplifiedPlanStep,
    Action, 
    ReportSection, 
    ReportSectionInput,
    ReportSectionNoToken,
)

from data_analysis.src.modules.prompt_bank import (
    COORDINATOR_PROMPT, 
    PLANNER_PROMPT, 
    PARAMETER_REFINEMENT_PROMPT,
    REPORT_PROMPT, 
    HEURISTIC_GUIDE,
)

from get_llm_response import get_llm, get_answer_with_schema

logger = logging.getLogger(__name__)


FRIENDLY_TOOL_NAME_MAP = {
    "English": {
        "single_factor_scale_reliability_testing": "Single Factor Reliability Test",
        "run_reliability_analysis": "Reliability Analysis",
        "run_efa_analysis": "EFA Analysis",
        "run_anova_ttest_analysis": "ANOVA & T-Test Analysis",
        "run_regression_analysis": "Regression Analysis",
        "run_logistic_regression_analysis": "Logistic Regression Analysis",
        "run_clustering_analysis": "Clustering Analysis",
        "run_cfa_analysis": "CFA Analysis",
        "run_cb_sem_analysis": "CB-SEM Analysis",
        "run_pls_sem_analysis": "PLS-SEM Analysis",
        "run_gsca_analysis": "GSCA Analysis",
        "run_stationarity_assessment": "Stationarity Assessment",
        "run_model_structure_identification": "Model Structure Identification",
        "run_model_estimation_and_diagnostics": "Model Estimation & Diagnostics",
        "run_panel_model_selection": "Panel Model Selection",
        "run_advanced_panel_analysis": "Advanced Panel Analysis",
        "run_overview_charts_analysis": "Overview Charts Analysis",
        "perform_kmo_bartlett_tool": "EFA - KMO & Bartlett Test",
        "determine_number_of_factors_tool": "EFA - Factor Selection",
        "perform_pca_varimax_tool": "EFA - PCA & Rotation",
        "analyze_factor_loadings_tool": "EFA - Loading Analysis",
        "calculate_variance_explained_tool": "EFA - Variance Assessment",
        "assign_factors_and_compute_scores_tool": "EFA - Scoring",
    },
    "Tiếng Việt": {
        "single_factor_scale_reliability_testing": "Kiểm định Độ tin cậy Nhân tố Đơn lẻ",
        "run_reliability_analysis": "Phân tích Độ tin cậy",
        "run_efa_analysis": "Phân tích Nhân tố Khám phá (EFA)",
        "run_anova_ttest_analysis": "Phân tích ANOVA & T-Test",
        "run_regression_analysis": "Phân tích Hồi quy",
        "run_logistic_regression_analysis": "Phân tích Hồi quy Logistic",
        "run_clustering_analysis": "Phân tích Phân cụm",
        "run_cfa_analysis": "Phân tích Nhân tố Khẳng định (CFA)",
        "run_cb_sem_analysis": "Phân tích SEM dựa trên Hiệp phương sai (CB-SEM)",
        "run_pls_sem_analysis": "Phân tích SEM dựa trên Bình phương Tối thiểu (PLS-SEM)",
        "run_gsca_analysis": "Phân tích Cấu trúc Tổng quát (GSCA)",
        "run_stationarity_assessment": "Đánh giá Tính dừng",
        "run_model_structure_identification": "Xác định Cấu trúc Mô hình",
        "run_model_estimation_and_diagnostics": "Ước lượng & Chẩn đoán Mô hình",
        "run_panel_model_selection": "Lựa chọn Mô hình Dữ liệu Bảng",
        "run_advanced_panel_analysis": "Phân tích Dữ liệu Bảng Nâng cao",
        "run_overview_charts_analysis": "Phân tích Biểu đồ Tổng quan",
        "perform_kmo_bartlett_tool": "EFA - Kiểm định KMO & Bartlett",
        "determine_number_of_factors_tool": "EFA - Lựa chọn Số lượng Nhân tố",
        "perform_pca_varimax_tool": "EFA - PCA & Phép quay",
        "analyze_factor_loadings_tool": "EFA - Phân tích Hệ số Tải",
        "calculate_variance_explained_tool": "EFA - Đánh giá Phương sai Trình bày",
        "assign_factors_and_compute_scores_tool": "EFA - Tính điểm Nhân tố",
    }
}

FRIENDLY_ACTION_TYPE_MAP = {
    "English": {
        "remove_variable": "Remove Variable",
        "discard_factor": "Discard Factor",
        "recheck_data": "Recheck Data",
        "reanalyze": "Reanalyze",
        "split_variable": "Split Variable",
        "combine_variables": "Combine Variables",
        "transform_variables": "Transform Variables",
        "inverse_transform_variables": "Inverse Transform Variables",
        "handle_outliers": "Handle Outliers",
        "handle_missing_values": "Handle Missing Values",
        "modify_model_specification": "Modify Model Specification",
        "update_variable_properties": "Update Variable Properties",
        "handle_duplicates": "Handle Duplicates",
        "convert_data_type": "Convert Data Type",
    },
    "Tiếng Việt": {
        "remove_variable": "Loại bỏ Biến",
        "discard_factor": "Loại bỏ Nhân tố",
        "recheck_data": "Kiểm tra lại Dữ liệu",
        "reanalyze": "Phân tích lại",
        "split_variable": "Chia Biến",
        "combine_variables": "Gộp Biến",
        "transform_variables": "Biến đổi Biến",
        "inverse_transform_variables": "Nghịch biến đổi Biến",
        "handle_outliers": "Xử lý Giá trị ngoại lệ",
        "handle_missing_values": "Xử lý Giá trị thiếu",
        "modify_model_specification": "Điều chỉnh Đặc điểm Mô hình",
        "update_variable_properties": "Cập nhật Thuộc tính Biến",
        "handle_duplicates": "Xử lý Dữ liệu trùng lặp",
        "convert_data_type": "Chuyển đổi Kiểu dữ liệu",
    }
}

FRIENDLY_ACTION_MSG_TEMPLATES = {
    "English": {
        "remove_variable": "{variable}",
        "discard_factor": "{factor}",
        "recheck_data": "{target}",
        "transform_variables": "{transform_type} on {variable}",
        "handle_outliers": "{outlier_count} items in {variable} (Method: {method})",
        "handle_missing_values": "{missing_count} items in {variable} (Method: {method})",
        "update_variable_properties": "{variables}",
        "convert_data_type": "{variable} to {target_type}",
    },
    "Tiếng Việt": {
        "remove_variable": "{variable}",
        "discard_factor": "{factor}",
        "recheck_data": "{target}",
        "transform_variables": "{transform_type} cho {variable}",
        "handle_outliers": "{outlier_count} giá trị tại {variable} (Phương pháp: {method})",
        "handle_missing_values": "{missing_count} giá trị tại {variable} (Phương pháp: {method})",
        "update_variable_properties": "{variables}",
        "convert_data_type": "{variable} sang {target_type}",
    }
}

FRIENDLY_ISSUE_TYPE_MAP = {
    "English": {
        "kmo_low": "KMO Low",
        "bartlett_failed": "Bartlett Test Failed",
        "alpha_high": "Alpha Too High",
        "alpha_low": "Alpha Too Low",
        "missing_values": "Missing Values",
        "outliers": "Outliers Detected",
        "Dataset": " ",
    },
    "Tiếng Việt": {
        "kmo_low": "KMO Thấp",
        "bartlett_failed": "Kiểm định Bartlett Thất bại",
        "alpha_high": "Alpha Quá Cao",
        "alpha_low": "Alpha Quá Thấp",
        "missing_values": "Giá trị Thiếu",
        "outliers": "Phát hiện Ngoại lệ",
        "Dataset": " ",
    }
}

def find_non_serializable(obj, path=""):
    """Recursively find non-serializable fields in a Pydantic model or nested data."""
    try:
        json.dumps(obj)
        return []
    except TypeError as e:
        errors = []
        if isinstance(obj, BaseModel):
            for field_name, field_value in obj.model_dump().items():
                errors.extend(find_non_serializable(field_value, f"{path}.{field_name}" if path else field_name))
        elif isinstance(obj, dict):
            for k, v in obj.items():
                errors.extend(find_non_serializable(v, f"{path}['{k}']"))
        elif isinstance(obj, (list, tuple)):
            for i, item in enumerate(obj):
                errors.extend(find_non_serializable(item, f"{path}[{i}]"))
        else:
            errors.append(f"Non-serializable value at path '{path}' (type: {type(obj)}): {repr(obj)}")
        return errors
    
@tool
def handoff_to_planner(
    task_title: Annotated[str, "The title of the task to be handed off."],
    # locale: Annotated[str, "The user's detected language locale (e.g., en-US, zh-CN)."],
    query: Annotated[Query, "The query object containing keywords for background investigation."],
):
    """Handoff to planner agent to do plan, routing through background investigator."""
    return

async def coordinator_node(state: State) -> Command[Literal["background_investigator"]]:
    """Coordinator node that communicates with customers, summarizes context, and generates query."""
    logger.info(f"[{state['document_id'][:8]}] Coordinator node running.")
    # await send_health_check(state["document_id"], AIStatus.PROCESSING)
    # Check if plan mode is manual and tool_configs are provided
    if state["plan_mode"] == ExecutionMode.MANUAL and state["tool_configs"]:
        logger.info(f"[{state['document_id'][:8]}] Manual mode detected with tool configs, routing directly to planner.")
        return Command(goto="planner")

    # Access DataFrames
    current_data = get_current_dataframe(state)
    # Summarize dataset
    data_summary = f"Dataset shape: {current_data.shape}\n"
    variable_summary = "Variables:\n"
    for var in state["original_variables"]:
        # Initialize variable description with mandatory fields
        var_desc = f"- Name / Description: {var.name}, Code: {var.code}, Type={var.variable_type}, Role={var.role}, Scale={var.scale}"
        
        # Add optional fields only if they are non-empty/non-null
        optional_fields = []
        if var.parent_code:
            optional_fields.append(f"Parent Code={var.parent_code}")
        if var.statement:
            optional_fields.append(f"Statement={var.statement}")
            
        # Append optional fields if any
        if optional_fields:
            var_desc += ", " + ", ".join(optional_fields)
            
        variable_summary += var_desc + "\n"

    # Get user query (only if last message is from user)
    user_query = ""
    if "messages" in state and len(state["messages"]) and isinstance(state["messages"][-1], HumanMessage):
        user_query = state["messages"][-1].content

    # Construct invoke messages using the reformatted COORDINATOR_PROMPT
    invoke_messages = [
        SystemMessage(content=COORDINATOR_PROMPT["system"].format(
            heuristic_guide=HEURISTIC_GUIDE)),
        *state["messages"],  # Include existing messages from state
        HumanMessage(content=COORDINATOR_PROMPT["user"].format(
            data_summary=data_summary,
            variable_summary=variable_summary,
            user_query=user_query
        ))
    ]

    # Invoke the LLM with the handoff tool
    llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
    response = llm.bind_tools([handoff_to_planner]).invoke(invoke_messages)
    state["input_tokens"] += response.usage_metadata["input_tokens"]
    state["output_tokens"] += response.usage_metadata["output_tokens"]
    
    logger.debug(f"[{state['document_id'][:8]}] Current state messages: {state['messages']}")

    # goto = "__end__"
    goto = "background_investigator" # always go to background investigator
    query_to_pass = Query(keywords=[])  # Default empty query
    queries = state["queries"]

    if len(response.tool_calls) > 0:
        goto = "background_investigator"
        for tool_call in response.tool_calls:
            if tool_call.get("name", "") != "handoff_to_planner":
                continue
            args = tool_call.get("args", {})
            if tool_query := args.get("query"):
                try:
                    query_to_pass = Query.model_validate(tool_query)
                    queries.append(query_to_pass)  # Store the query in state
                except Exception as e:
                    logger.warning(f"[{state['document_id'][:8]}] Invalid query in tool call: {e}")
            break
    else:
        logger.info(
            f"[{state['document_id'][:8]}] Non-analysis query received: {user_query}. Terminating workflow."
        )
        logger.debug(f"[{state['document_id'][:8]}] Coordinator response: {response}")

    return Command(
        update={
            "queries": queries,
            "input_tokens": state["input_tokens"],
            "output_tokens": state["output_tokens"],
        },
        goto=goto,
    )

async def background_investigation_node(state: State) -> Command[Literal["planner"]]:
    """Background investigation node using local document search."""
    logger.info(f"[{state['document_id'][:8]}] Background investigation node is running.")
    # await send_health_check(state["document_id"], AIStatus.PROCESSING)
    
    # Get the latest query from the queries list
    if "queries" not in state or ("queries" in state and not state["queries"]):
        logger.warning(f"[{state['document_id'][:8]}] No queries found in state.")
        return Command(
            update={"background_results": {}},
            goto="planner",
        )
    
    query = state["queries"][-1]  # Use the most recent query
    
    # Perform local document search
    try:
        search_results = search_document(query, JSON_DOCUMENT)
        
        # Convert search results to Document objects
        documents = []
        for result in search_results:
            doc = Doc(
                id=result.get("id", ""),
                title=result.get("title", ""),
                content=result.get("content", ""),
                keywords=result.get("keywords", []),
                variable_content=result.get("variable_content", None),
                subsections=[]  # Assuming no subsections are needed for simplicity
            )
            documents.append(doc)
        
        # Store results in state
        return Command(
            update={
                "docs": state["docs"] + documents,  # Append new documents to existing ones
                "background_results": {
                    "documents": [doc.model_dump() for doc in documents]
                }
            },
            goto="planner",
        )
    
    except Exception as e:
        logger.error(f"[{state['document_id'][:8]}] Error during document search: {e}")
        return Command(
            update={"background_results": {}},
            goto="planner",
        )

async def refine_plan_parameters(state: State, conversation_history: str) -> tuple[SimplifiedPlan, int, int]:
    """Refine the parameters of a given plan using the LLM."""
    logger.info(f"[{state['document_id'][:8]}] Refining plan parameters.")
    
    llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
    
    tools_params_docs = "Available tools:\n" + "\n".join(
        f"- {tool['name']}: \nDescription: {tool['description']}. \nParameters: {', '.join(f'{k}: {v}' for k, v in tool['params'].items()) or 'None'}"
        for tool in TOOLS_DOCUMENT
    )

    # Create stage 2 messages
    stage2_messages = [
        SystemMessage(content=PARAMETER_REFINEMENT_PROMPT.format(tools_params_docs=tools_params_docs)),
        HumanMessage(content=conversation_history)  # Full conversation history including the plan to refine
    ]
    
    error, success, refined_plan, input_tokens, output_tokens = await get_answer_with_schema( 
        state['document_id'][:8], 
        llm, 
        stage2_messages[0].content, 
        [stage2_messages[1].content], 
        SimplifiedPlan
    )
    
    if not success:
        raise error
    
    # Log the refined plan
    logger.info(f"[{state['document_id'][:8]}] Refined plan with {len(refined_plan.steps)} steps")
    logger.info(f"[{state['document_id'][:8]}] Refined analysis description: {refined_plan.analysis_description}")
    for i, step in enumerate(refined_plan.steps, 1):
        logger.info(f"[{state['document_id'][:8]}]   Refined Step {i}: Tool='{step.tool}', Parameters={step.parameters}")
    
    return refined_plan, input_tokens, output_tokens


async def planner_node(state: State) -> Command[Literal["human_feedback"]]:
    """Planner node that generates or adjusts a simplified plan with analysis_description and steps."""
    logger.info(f"[{state['document_id'][:8]}] Planner node running")
    # await send_health_check(state["document_id"], AIStatus.PROCESSING)
    current_plan = state["current_plan"]
     # Check if we're in manual mode with tool_configs
    if state["plan_mode"] == ExecutionMode.MANUAL and state["tool_configs"]:
        logger.info(f"[{state['document_id'][:8]}] Manual mode detected with tool_configs, creating plan from provided configurations.")
        
        # Log the existing tool configurations before creating the plan
        logger.info(f"[{state['document_id'][:8]}] Tool configs being used to create manual plan: {len(state['tool_configs'])} steps")
        for i, tool_config in enumerate(state["tool_configs"], 1):
            logger.info(f"[{state['document_id'][:8]}]   Step {i}: Tool='{tool_config.tool}', Parameters={tool_config.parameters}")

        # Generate a unique plan_id
        plan_id = len(state["execution_log"].plans if state["execution_log"] is not None else ExecutionLog().plans) + 1

        # Map tool_configs (SimplifiedPlanStep) to AnalyzeStep
        seen_tools: set = set()
        analyze_steps: list[AnalyzeStep] = []
        for step in state["tool_configs"]:
            # Handle None parameters by using empty dict
            params = step.parameters if step.parameters is not None else {}
            key_to_find = (step.tool, json.dumps(params, sort_keys=True, default=str))
            if key_to_find not in seen_tools:
                analyze_steps.append(AnalyzeStep(
                step_type=AnalysisStepType.TOOL,
                tool=step.tool,
                parameters=params,
                comment=f"Execute {step.tool} with provided parameters",
                plan_id=plan_id,
                output=None  # Output will be populated during execution
            ))
                seen_tools.add(key_to_find)
        # Create full Plan with null analysis_description for manual mode
        new_plan = Plan(
            plan_id=plan_id,
            analysis_description="", # Empty description for manual mode
            steps=analyze_steps,
            parameters={},  # Default global parameters
            accepted=False  # Default to False, requires human feedback
        )
        # Log the final plan creation
        logger.info(f"[{state['document_id'][:8]}] Created manual plan with ID {plan_id} containing {len(analyze_steps)} steps")

        # Append AI message indicating manual plan creation
        updated_messages = state["messages"].copy()
        updated_messages.append(
            AIMessage(content="Created plan from manually provided tool configurations.")
        )

        return Command(
            update={
                "current_plan": new_plan,
                "messages": updated_messages,
                "available_tools": get_tool_availability(state)
            },
            goto="human_feedback"
        )

    # Non-manual modes

    # Prepare context
    data_summary = f"      "
    variable_summary = "Variables:\n" + "\n".join(
        f"- Name / Description: {var.name}, Code: {var.code}, Type={var.variable_type}, Role={var.role}, Scale={var.scale}"
        + (f", Parent Code={var.parent_code}" if var.parent_code else "")
        for var in state["original_variables"]
    )
    docs_summary = "Relevant documents:\n" + (
        "\n".join(f"- {doc.title}: {doc.content}..." for doc in state["docs"])
        if "docs" in state else "No documents available."
    )
    tools_docs = "Available tools:\n" + "\n".join(
        f"- {tool['name']}: \nDescription: {tool['description']}."
        for tool in TOOLS_DOCUMENT
    )

    # Invoke LLM with simplified schema
    try:
        llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"],temperature=0.000001)
        # STAGE 1: Create initial plan
        logger.info(f"[{state['document_id'][:8]}] Stage 1: Creating initial plan")
        
        if state["final_proposal"]:
            human_message = HumanMessage(content=PLANNER_PROMPT["user_proposal"].format(
                user_proposal=state["final_proposal"],
                data_summary=data_summary, 
                variable_summary=variable_summary,
                language=state["language"],
            ))
        else:
            human_message = HumanMessage(content=PLANNER_PROMPT["user"].format(
                data_summary=data_summary, 
                variable_summary=variable_summary,
                language=state["language"],
            ))
        # Construct invoke messages with full message history
        stage1_invoke_messages = [
            SystemMessage(content=PLANNER_PROMPT["system"].format(
                heuristic_guide=HEURISTIC_GUIDE,
                tools_summary=tools_docs,
                docs_summary=docs_summary,
            )),
            *state["messages"],
            human_message
        ]

        error, success, initial_plan, input_tokens, output_tokens = await get_answer_with_schema( 
            state['document_id'][:8], 
            llm, 
            "", 
            stage1_invoke_messages, 
            SimplifiedPlan
        )
        if not success:
            raise error
        else:
            state["input_tokens"] += input_tokens
            state["output_tokens"] += output_tokens
            # Log the initial plan
            logger.info(f"[{state['document_id'][:8]}] Stage 1 completed - Generated initial plan with {len(initial_plan.steps)} steps")
            logger.info(f"[{state['document_id'][:8]}] Initial analysis description: {initial_plan.analysis_description}")
            for i, step in enumerate(initial_plan.steps, 1):
                logger.info(f"[{state['document_id'][:8]}]   Initial Step {i}: Tool='{step.tool}', Parameters={step.parameters}")
            
            # # STAGE 2: Refine tool parameters
            # logger.info(f"[{state['document_id'][:8]}] Stage 2: Refining tool parameters")
            
            # # Build conversation history string with role prefixes
            # conversation_history_base  = ""
            # for msg in state["messages"]:
            #     if isinstance(msg, HumanMessage):
            #         conversation_history_base += f"---\nUser: {msg.content}\n\n"
            #     elif isinstance(msg, AIMessage):
            #         conversation_history_base += f"---\nAI: {msg.content}\n\n"
            
            # # Add the user proposal and data context
            # if state["final_proposal"]:
            #     conversation_history_base += f"""---\nUser: {
            #         PLANNER_PROMPT['user_proposal'].format(
            #             user_proposal=state['final_proposal'], 
            #             data_summary=data_summary, 
            #             variable_summary=variable_summary, 
            #             language=state['language']
            #         )
            #     }\n\n"""
            # else:
            #     conversation_history_base += f"""---\nUser: {
            #         PLANNER_PROMPT['user'].format(
            #             data_summary=data_summary, 
            #             variable_summary=variable_summary, 
            #             language=state['language']
            #         )
            #     }\n\n"""
            # # Add the initial plan from stage 1
            # initial_plan_content = f"---\nAI: Generated initial plan:\nAnalysis: {initial_plan.analysis_description}\nSteps:\n"
            # for i, step in enumerate(initial_plan.steps, 1):
            #     initial_plan_content += f"{i}. Tool: {step.tool}, Parameters: {step.parameters}\n"
            # conversation_history = conversation_history_base + initial_plan_content
            
            # # Call the refinement function
            # refined_plan, input_tokens, output_tokens = await refine_plan_parameters(state, conversation_history)
            # state["input_tokens"] += input_tokens
            # state["output_tokens"] += output_tokens
            plan_id = len(state["execution_log"].plans if "execution_log" in state and state["execution_log"] is not None else ExecutionLog().plans) + 1

            # Map SimplifiedPlanStep to AnalyzeStep (using initial_plan instead of refined_plan)
            seen_tools: set = set()
            analyze_steps: list[AnalyzeStep] = []
            for step in initial_plan.steps:
                # Handle None parameters by using empty dict
                params = step.parameters if step.parameters is not None else {}
                key_to_find = (step.tool, json.dumps(params, sort_keys=True, default=str))
                if key_to_find not in seen_tools:
                    analyze_steps.append(AnalyzeStep(
                    step_type=AnalysisStepType.TOOL,
                    tool=step.tool,
                    parameters={},  # Empty parameters as requested
                    comment=f"Execute {step.tool}",  # Simplified comment
                    plan_id=plan_id,
                    output=None  # Output will be populated during execution
                ))
                    seen_tools.add(key_to_find)

            # Create full Plan using initial plan (not refined)
            new_plan = Plan(
                plan_id=plan_id,
                analysis_description=initial_plan.analysis_description,
                steps=analyze_steps,
                parameters={},  # Default global parameters (can be customized if needed)
                accepted=False  # Default to False, requires human feedback
            )

            # Log the final plan creation
            logger.info(f"[{state['document_id'][:8]}] Created plan with ID {plan_id} containing {len(analyze_steps)} steps")

            plan_message = f"Generated plan: {initial_plan.analysis_description}"
            for i, step in enumerate(initial_plan.steps, 1):
                plan_message += f"\nStep {i}: Tool='{step.tool}'"
                
            # Append AI message with analysis_description
            updated_messages = state["messages"].copy()
            updated_messages.append(AIMessage(content=plan_message))

            return Command(
                update={
                    "current_plan": new_plan,
                    "messages": updated_messages,
                    "input_tokens": state["input_tokens"],
                    "output_tokens": state["output_tokens"],
                    "available_tools": get_tool_availability(state)
                },
                goto="human_feedback"
            )
    except Exception as e:
        logger.exception(f"[{state['document_id'][:8]}] Failed to generate/adjust plan")
        raise e


async def human_feedback_node(state: State) -> Command[Literal["planner", "analyzer"]]:
    """Human feedback node to review plan or the first pending action in interactive mode."""
    logger.info(f"[{state['document_id'][:8]}] Human feedback node running")
    # await send_health_check(state["document_id"], AIStatus.PROCESSING)

    current_plan = state["current_plan"]
    execution_log_data = state.get("execution_log")
    if isinstance(execution_log_data, dict):
        execution_log = ExecutionLog(**execution_log_data)
    elif execution_log_data:
        execution_log = execution_log_data
    else:
        execution_log = ExecutionLog()
    
    # Case 1: No current plan, route to planner
    if not current_plan:
        logger.warning(f"[{state['document_id'][:8]}] No current plan found to review.")
        return Command(goto="planner")

    # Case 2: Plan not yet accepted
    if not current_plan.accepted:
        if state["plan_mode"] == ExecutionMode.INTERACTIVE:
            logger.info(f"[{state['document_id'][:8]}] Prompting for plan approval")
            
            # Construct interrupt dictionary for plan review
            # feedback = interrupt(current_plan.model_dump())
            feedback = interrupt({
                **current_plan.model_dump(), 
                "available_tools": state.get("available_tools", get_tool_availability(state))
            })
            if feedback:
                updated_messages = state["messages"]
                feedback_str = str(feedback).strip()
                
                if feedback_str.upper().startswith("[ACCEPT]"):
                    logger.info(f"[{state['document_id'][:8]}] Plan accepted by user")
                    current_plan.accepted = True
                    updated_messages.append(HumanMessage(content=feedback))
                    return Command(
                        update={
                            "current_plan": current_plan,
                            "messages": updated_messages
                        },
                        goto="analyzer"
                    )
                elif feedback_str.upper().startswith("[EDIT_PLAN]"):
                    logger.info(f"[{state['document_id'][:8]}] User requested plan edit")
                    updated_messages.append(HumanMessage(content=feedback))
                    return Command(
                        update={"messages": updated_messages},
                        goto="planner"
                    )
                else:
                    # Check if feedback is already a dict/JSON object with tool_configs
                    try:
                        if isinstance(feedback, dict):
                            feedback_json = feedback
                        else:
                            feedback_json = json.loads(feedback_str)
                        
                        # Support both formats: list directly or wrapped in tool_configs
                        if isinstance(feedback_json, list):
                            tool_configs_list = feedback_json
                        elif "tool_configs" in feedback_json and isinstance(feedback_json["tool_configs"], list):
                            tool_configs_list = feedback_json["tool_configs"]
                        else:
                            logger.warning(f"[{state['document_id'][:8]}] Feedback does not contain valid tool_configs: {feedback}")
                            raise TypeError(f"Invalid format. Expected list or 'tool_configs' list, got: {feedback}")

                        if tool_configs_list:
                            logger.info(f"[{state['document_id'][:8]}] Received tool_configs format with {len(tool_configs_list)} steps")

                            # Log the existing tool configurations before creating the plan
                            logger.info(f"[{state['document_id'][:8]}] Tool configs being used to create plan from feedback: {len(tool_configs_list)} steps")
                            for i, tool_config in enumerate(tool_configs_list, 1):
                                logger.info(f"[{state['document_id'][:8]}]   Step {i}: Tool='{tool_config.get('tool', '')}', Parameters={tool_config.get('parameters', {})}")

                            # Create initial SimplifiedPlan from user-provided tool_configs
                            initial_plan = SimplifiedPlan(
                                analysis_description="",  # Empty initial description; refinement may generate one
                                steps=[
                                    SimplifiedPlanStep(tool=step.get("tool", ""), parameters=step.get("parameters", {}))
                                    for step in tool_configs_list
                                ]
                            )
                            
                            # Generate a unique plan_id
                            plan_id = len(state["execution_log"].plans if state["execution_log"] is not None else ExecutionLog().plans) + 1
                            
                            # # Build conversation history base from current messages
                            # conversation_history_base = ""
                            # for msg in state["messages"]:
                            #     if isinstance(msg, HumanMessage):
                            #         conversation_history_base += f"---\nUser: {msg.content}\n\n"
                            #     elif isinstance(msg, AIMessage):
                            #         conversation_history_base += f"---\nAI: {msg.content}\n\n"
                            
                            # # Add the user-provided tool_configs as part of the conversation
                            # user_plan_content = f"---\nUser: Provided initial plan to refine:\nAnalysis: {initial_plan.analysis_description}\nSteps:\n"
                            # for i, step in enumerate(initial_plan.steps, 1):
                            #     user_plan_content += f"{i}. Tool: {step.tool}, Parameters: {step.parameters}\n"
                            
                            # conversation_history = conversation_history_base + user_plan_content
                            
                            # # Refine the parameters using the shared function
                            # refined_plan, input_tokens, output_tokens = await refine_plan_parameters(state, conversation_history)
                            # state["input_tokens"] += input_tokens
                            # state["output_tokens"] += output_tokens

                            # # Map refined stepsy to AnalyzeStep
                            # analyze_steps = [
                            #     AnalyzeStep(
                            #         step_type=AnalysisStepType.TOOL,
                            #         tool=step.tool,
                            #         parameters=step.parameters,
                            #         comment=f"Execute {step.tool} with refined parameters",
                            #         plan_id=plan_id,
                            #         output=None  # Output will be populated during execution
                            #     )
                            #     for step in refined_plan.steps
                            # ]

                            # # Create full Plan with refined analysis_description
                            # new_plan = Plan(
                            #     plan_id=plan_id,
                            #     analysis_description=refined_plan.analysis_description,
                            #     steps=analyze_steps,
                            #     parameters={},  # Default global parameters
                            #     accepted=True  # Auto-accept user-provided plan after refinement
                            # )
                            
                            # Map initial steps to AnalyzeStep (using user-provided parameters directly)
                            analyze_steps: list[AnalyzeStep] = []
                            for step in initial_plan.steps:
                                analyze_steps.append(AnalyzeStep(
                                step_type=AnalysisStepType.TOOL,
                                tool=step.tool,
                                parameters=step.parameters,
                                comment=f"Execute {step.tool} with user-provided parameters",
                                plan_id=plan_id,
                                output=None  # Output will be populated during execution
                            ))
                            # Create full Plan using initial plan (no refinement)
                            new_plan = Plan(
                                plan_id=plan_id,
                                analysis_description=initial_plan.analysis_description,
                                steps=analyze_steps,
                                parameters={},  # Default global parameters
                                accepted=True  # Auto-accept user-provided plan
                            )
                            
                            # Add human message and AI response (convert dict to string for message)
                            feedback_content = str(feedback) if isinstance(feedback, dict) else feedback_str
                            updated_messages.append(HumanMessage(content=feedback_content))
                            updated_messages.append(
                                AIMessage(content="Refined and accepted plan from user-provided tool configurations.")
                            )

                            return Command(
                                update={
                                    "current_plan": new_plan,
                                    "messages": updated_messages,
                                    "input_tokens": state["input_tokens"],
                                    "output_tokens": state["output_tokens"],
                                },
                                goto="analyzer"
                            )
                        else:
                            logger.warning(f"[{state['document_id'][:8]}] Feedback does not contain valid tool_configs: {feedback}")
                            raise TypeError(f"Invalid format. Expected 'tool_configs' list, got: {feedback}")
                            
                    except json.JSONDecodeError:
                        logger.warning(f"[{state['document_id'][:8]}] Unsupported feedback received (not valid JSON): {feedback}")
                        raise TypeError(f"Interrupt value of {feedback} is not supported. Expected [ACCEPT], [EDIT_PLAN], or dict/JSON with tool_configs.")
                    except Exception as e:
                        logger.error(f"[{state['document_id'][:8]}] Error processing tool_configs feedback: {e}")
                        raise TypeError(f"Error processing feedback: {e}")
            else:
                raise TypeError(f"Interrupt value of {feedback} is empty !")
        else:  # AUTO mode
            logger.info(f"[{state['document_id'][:8]}] Auto-accepting plan in AUTO mode")
            current_plan.accepted = True
            updated_messages = state["messages"].copy()
            updated_messages.append(
                AIMessage(content="Plan auto-accepted in AUTO mode")
            )
            return Command(
                update={
                    "current_plan": current_plan,
                    "messages": updated_messages
                },
                goto="analyzer"
            )

    # Case 3: Check for pending action at head of queue in interactive mode
    if state["action_mode"] == ExecutionMode.INTERACTIVE and execution_log.pending_steps:
        action_step = execution_log.pending_steps[0]  # Check head of queue
        if action_step.step_type == AnalysisStepType.ACTION:
            action = Action(**action_step.parameters["action"])
            if action.status == "pending":
                # Preprocess action to use friendly names for display
                action_data = action.model_dump()
                method_name = action_data.get("method")

                # Normalize language detection (resilient to different formats)
                raw_lang = state.get("language")
                logger.info(f"[{state['document_id'][:8]}] Language to debug: {raw_lang}")
                if not raw_lang:
                    raw_lang = "English"
                
                # Use a more flexible detection logic (handles "vi", "vn", "Tiếng Việt", "Vietnamese", etc.)
                raw_lang_lower = str(raw_lang).lower()
                is_vietnamese = (
                    raw_lang_lower.startswith("vi") or 
                    raw_lang_lower.startswith("vn") or
                    "việt" in raw_lang_lower or 
                    "viet" in raw_lang_lower or 
                    "tiếng" in raw_lang_lower or 
                    "tieng" in raw_lang_lower
                )
                
                if is_vietnamese:
                    language_key = "Tiếng Việt"
                else:
                    language_key = "English"
                
                tool_map = FRIENDLY_TOOL_NAME_MAP.get(language_key, FRIENDLY_TOOL_NAME_MAP["English"])
                action_map = FRIENDLY_ACTION_TYPE_MAP.get(language_key, FRIENDLY_ACTION_TYPE_MAP["English"])

                # Traceback to find the most recent main tool executed (TOOL or REFLECTION)
                # This helps map sub-tools (like efa loading analysis) to their parent tool name
                parent_tool = None
                for step in reversed(execution_log.executed_steps):
                    if step.step_type in [AnalysisStepType.TOOL, AnalysisStepType.REFLECTION]:
                        parent_tool = step.tool
                        break
                
                friendly_name = None
                
                # 1. Try mapping the parent tool first to get the "main" context if it exists
                if parent_tool and parent_tool in tool_map:
                    friendly_name = tool_map[parent_tool]
                
                # 2. Fallback to mapping the technical method name directly if no parent mapping found
                if not friendly_name and method_name in tool_map:
                    friendly_name = tool_map[method_name]
                
                # 3. Apply the friendly name to the display data
                if friendly_name:
                    action_data["method"] = friendly_name
                
                if "action_type" in action_data and action_data["action_type"] in action_map:
                    raw_action_type = action_data["action_type"]
                    action_data["action_type"] = action_map[raw_action_type]
                    
                    # Apply friendly message if parameters exist
                    msg_templates = FRIENDLY_ACTION_MSG_TEMPLATES.get(language_key, FRIENDLY_ACTION_MSG_TEMPLATES["English"])
                    if raw_action_type in msg_templates and "action_params" in action_data and action_data["action_params"]:
                        params = action_data["action_params"]
                        try:
                            # Standardize variable name access
                            variable = params.get("variable") or params.get("variable_code") or params.get("factor")
                            
                            if raw_action_type == "update_variable_properties":
                                vars_list = params.get("variable_codes", [])
                                if not vars_list and variable:
                                    vars_list = [variable]
                                variables_str = ", ".join(vars_list)
                                action_data["action_params"] = msg_templates[raw_action_type].format(variables=variables_str)
                            elif raw_action_type == "recheck_data":
                                # For recheck_data, show variable/factor if available, otherwise show issue_type or "Dataset"
                                if variable:
                                    target = variable
                                else:
                                    issue_type = params.get("issue_type", "Dataset")
                                    issue_type_map = FRIENDLY_ISSUE_TYPE_MAP.get(language_key, FRIENDLY_ISSUE_TYPE_MAP["English"])
                                    target = issue_type_map.get(issue_type, issue_type.replace("_", " ").title() if issue_type else "Dataset")
                                action_data["action_params"] = msg_templates[raw_action_type].format(target=target)
                            elif raw_action_type == "transform_variables":
                                transform_type = params.get("transform_type", "transformation")
                                action_data["action_params"] = msg_templates[raw_action_type].format(variable=variable, transform_type=transform_type)
                            elif raw_action_type in ["handle_missing_values", "handle_outliers"]:
                                count = params.get("missing_count") or params.get("outlier_count") or "N/A"
                                method = params.get("method", "standard")
                                action_data["action_params"] = msg_templates[raw_action_type].format(variable=variable, missing_count=count, outlier_count=count, method=method)
                            else:
                                if variable:
                                    action_data["action_params"] = msg_templates[raw_action_type].format(variable=variable, factor=variable)
                        except Exception as e:
                            logger.warning(f"[{state['document_id'][:8]}] Error formatting friendly action message for {raw_action_type}: {e}")
                            # Keep original action_params if formatting fails

                feedback = interrupt(action_data)
                
                # action_str = action.model_dump_json(indent=2)
                # feedback = interrupt(
                #     f"Please review the pending action:\n{action_str}\n"
                #     f"Respond with [APPROVE], [REJECT] to proceed."
                # )
                
                updated_steps = execution_log.pending_steps.copy()
                updated_logs = execution_log.logs.copy()
                updated_messages = state["messages"].copy()

                if feedback:
                    feedback = str(feedback).upper()
                    if feedback.startswith("[APPROVE]"):
                        action.status = "approved"
                        logger.info(f"[{state['document_id'][:8]}] Action {action.action_type} approved")
                        updated_messages.append(
                            HumanMessage(content=f"[APPROVED] Action: {action.action_type}")
                        )
                    elif feedback.startswith("[REJECT]"):
                        action.status = "rejected"
                        logger.info(f"[{state['document_id'][:8]}] Action {action.action_type} rejected")
                        updated_messages.append(
                            HumanMessage(content=f"[REJECTED] Action: {action.action_type}")
                        )
                    else:
                        logger.warning(f"[{state['document_id'][:8]}] Unsupported action feedback: {feedback}")
                        raise TypeError(f"Action feedback {feedback} is not supported.")
                    
                    # Update the action in the step
                    action_step.parameters["action"] = action.model_dump() # Update with modified action
                    updated_steps[0] = action_step  # Update head of queue
                    
                    # Log the action resolution
                    log_entry = LogEntry(
                        step_type=AnalysisStepType.ACTION,
                        action_type=action.action_type,
                        plan_id=action_step.plan_id,
                        content=f"Action {action.action_type} set to {action.status} by user"
                    )
                    state["logs"].append(log_entry)
                    updated_logs.append(log_entry.content)
                    
                    # Update execution_log
                    updated_execution_log = ExecutionLog(
                        plans=execution_log.plans,
                        executed_steps=execution_log.executed_steps,
                        pending_steps=updated_steps,
                        logs=updated_logs
                    )
                    
                    return Command(
                        update={
                            "messages": updated_messages,
                            "execution_log": updated_execution_log
                        },
                        goto="analyzer"
                    )
                else:
                    # No feedback, pause and keep action in queue
                    logger.info(f"[{state['document_id'][:8]}] No feedback provided, pausing for action")
                    return Command(
                        update={"messages": updated_messages},
                        goto="__end__"
                    )
    
    # Case 4: No pending actions or in AUTO mode, proceed to analyzer
    logger.info(f"[{state['document_id'][:8]}] No pending action at head or in AUTO mode, proceeding to analyzer")
    return Command(goto="analyzer")

async def analyzer_node(state: State) -> Command[Literal["analyzer", "human_feedback", "section_reporter"]]:
    logger.info(f"[{state['document_id'][:8]}] Analyzer node running to execute next pipeline step")
    # await send_health_check(state["document_id"], AIStatus.PROCESSING)
    try:
        result_state = execute_pipeline(state)

        # Serialize execution_log to handle non-JSON-serializable types (numpy arrays, pandas DataFrames, etc.)
        execution_log_dict = result_state["execution_log"].model_dump()
        execution_log_serialized = serialize_dict(execution_log_dict)

        # Check for non-serializable fields after serialization
        non_serializable = find_non_serializable(execution_log_serialized, "execution_log")
        if non_serializable:
            logger.error(f"[{state['document_id'][:8]}] Non-serializable fields found after serialization: {non_serializable}")
            raise ValueError(f"Non-serializable fields in execution_log: {non_serializable}")

        # Prepare updated state
        updated_state = {
            "current_data": result_state["current_data"],
            "current_variables": result_state["current_variables"],
            "execution_log": execution_log_serialized,
            "logs": result_state["logs"],
            "generated_files": result_state["generated_files"],
            "file_descriptions": result_state.get("file_descriptions", {}),
            "current_plan": result_state["current_plan"]
        }

        # Check for pending actions
        has_pending_actions = any(
            step.step_type == AnalysisStepType.ACTION
            for step in result_state["execution_log"].pending_steps
        )

        # Routing logic
        if has_pending_actions and state["action_mode"] == ExecutionMode.INTERACTIVE:
            logger.info(f"[{state['document_id'][:8]}] Pending actions detected, routing to human_feedback")
            return Command(
                update=updated_state,
                goto="human_feedback"
            )
        elif result_state["execution_log"].pending_steps:
            logger.info(f"[{state['document_id'][:8]}] Non-action pending steps remain, continuing in analyzer")
            return Command(
                update=updated_state,
                goto="analyzer"
            )
        else:
            logger.info(f"[{state['document_id'][:8]}] No pending steps, pipeline complete, dispatching to reporting subgraph")

            # Define filter parameters for report sections
            filter_params_list = []

            # Step 1: Filter logs by the latest plan_id
            if state["execution_log"].plans:
                latest_plan_id = max(plan.plan_id for plan in state["execution_log"].plans)
                plan_indices = [
                    i for i, log in enumerate(state["logs"]) if log.plan_id == latest_plan_id
                ]
            else:
                plan_indices = list(range(len(state["logs"])))  # Use all logs if no plans

            if plan_indices:
                # Check if custom indices are provided in state
                if hasattr(state, 'filter_params') and state["filter_params"] and 'indices' in state["filter_params"]:
                    # Use custom indices directly from filter_params
                    custom_indices = state["filter_params"].get('indices', [])
                    if isinstance(custom_indices, list) and all(isinstance(i, int) for i in custom_indices):
                        # Validate that custom indices are within plan_indices
                        valid_indices = [i for i in custom_indices if i in plan_indices]
                        if valid_indices:
                            filter_params_list.append({"indices": valid_indices})  # Use as a single segment
                        else:
                            logger.warning(f"[{state['document_id'][:8]}] Custom indices provided but none match plan_indices, falling back to default logic")
                    else:
                        logger.warning(f"[{state['document_id'][:8]}] Invalid custom indices format, falling back to default logic")

                # Fallback to default segmenting logic if no valid custom indices
                if not filter_params_list:
                    # Step 2: Identify tool steps within the filtered logs
                    tool_indices = [
                        i for i in plan_indices if state["logs"][i].step_type == AnalysisStepType.TOOL
                    ]

                    # Step 3: Create a segment for logs before the first tool step, if any
                    if tool_indices and tool_indices[0] > min(plan_indices):
                        filter_params_list.append({"indices": list(range(min(plan_indices), tool_indices[0] + 1))})

                    # Step 4: Create segments for each tool step
                    for i in range(len(tool_indices)):
                        start_idx = tool_indices[i]
                        end_idx = tool_indices[i + 1] if i + 1 < len(tool_indices) else max(plan_indices) + 1
                        if start_idx < end_idx:
                            filter_params_list.append({"indices": list(range(start_idx, end_idx))})

            logger.info(f"[{state['document_id'][:8]}] Generated filter_params_list: {filter_params_list}")

            # Create ReportSectionState instances for each filter
            sends = [
                Send(
                    "section_reporter",
                    ReportSectionInput(
                        document_id=state["document_id"],
                        model_id=state["model_id"],
                        original_variables=state["original_variables"],
                        current_variables=state["current_variables"],
                        execution_log=state["execution_log"],
                        current_plan=state["current_plan"],
                        queries=state["queries"],
                        docs=state["docs"],
                        logs=state["logs"],
                        filter_params=params,
                        section_index=i + 1,
                        get_detailed_report=state["get_detailed_report"],
                        llm_key=state["llm_key"],
                        max_tokens=state["max_tokens"],
                    )
                )
                for i, params in enumerate(filter_params_list)
            ]

            return Command(
                update=updated_state,
                goto=sends
            )

    except Exception as e:
        logger.error(f"[{state['document_id'][:8]}] Failed to execute pipeline: {e}")
        raise e

async def section_reporter(state: ReportSectionInput) -> Command[Literal["collector"]]:
    """
    Generate a report section based on filtered logs using an LLM with structured output.
    Assumes 'filter_params' contains 'indices' key with a list of specific indices.
    Enhanced for hierarchical, nested bullet-point structure in content.
    """
    logger.info(f"[{state['document_id'][:8]}] Generating report section with filter_params: {state['filter_params']}")

    # Prepare variable summary (full version for LLM context)
    full_variable_summary = "#### Variable Summary:\n\nVariables:\n" + "\n".join(
        f"- Name / Description: {var.name}, Code: {var.code}, Type: {var.variable_type}, Role: {var.role}, Scale: {var.scale}"
        + (f", Parent Code: {var.parent_code}" if var.parent_code else "")
        for var in state["original_variables"]
    ) if "original_variables" in state else "No variables available."

    # Short version for non-first sections (codes only, single line joined by "; ")
    short_variable_summary = (
        "#### Variable Codes:\n\n" + "; ".join(var.code for var in state["original_variables"])
        if "original_variables" in state
        else "No variables available."
    )

    # Filter logs based on filter_params
    filter_params = state["filter_params"] if "filter_params" in state and state["filter_params"] is not None else {}
    indices = filter_params.get("indices", list(range(len(state["logs"]))))  # Default to all logs if no indices
    # Ensure indices are valid
    valid_indices = [i for i in indices if 0 <= i < len(state["logs"])]
    if not valid_indices:
        logger.warning(f"[{state['document_id'][:8]}] No valid indices provided in filter_params, using empty log set")
        filtered_logs = []
    else:
        filtered_logs = [state["logs"][i] for i in valid_indices]
    
    # Set section title based on indices (outside, as h3)
    section_title = f"### Log Segment {', '.join(str(i + 1) for i in valid_indices)}" if valid_indices else "### Empty Log Segment"

    # Check if the segment corresponds to the latest plan_id
    if filtered_logs and state["execution_log"].plans:
        latest_plan_id = max(plan.plan_id for plan in state["execution_log"].plans)
        if all(log.plan_id == latest_plan_id for log in filtered_logs):
            section_title = f"### Summary for Plan {latest_plan_id}"

    # Prepare log items with table formatting and proper separation
    log_items = []
    for log in filtered_logs:
        formatted_log = format_tables_in_log(log.content)  # Apply improved formatting
        log_items.append(f"**Type:** {log.step_type}\n\n{formatted_log}")

    # Build as separated paragraphs (no lists for block safety)
    log_contents = "#### Execution Logs:\n\n" + ("\n\n".join(log_items) if log_items else "No relevant logs found for this section.")

    # Use full or short variable summary based on section_index, with nesting
    if state["section_index"] == 0:
        section_markdown = f"{section_title}\n\n{full_variable_summary}\n\n{log_contents}\n\n"  # Extra \n\n for separation
    else:
        section_markdown = f"{section_title}\n\n{short_variable_summary}\n\n{log_contents}\n\n"

    # Prepare messages for LLM (instruct for nested bullets; full for context)
    if state["get_detailed_report"]:
        invoke_messages = [
            SystemMessage(content=REPORT_PROMPT["system"] + "\n\nOutput in nested bullet points for deeper hierarchy: Use - for top level, -- for sub, --- for sub-sub, etc. Structure sections outside with headings."),
            HumanMessage(content=REPORT_PROMPT["user"].format(
                section_index=state["section_index"],
                variable_summary=full_variable_summary,  # Full for LLM
                log_contents=log_contents,
                language="English",
            ))
        ]
        llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
        error, success, section_no_token_count, input_tokens, output_tokens = await get_answer_with_schema( 
            state['document_id'][:8], 
            llm, 
            invoke_messages[0], 
            [invoke_messages[1]], 
            ReportSectionNoToken
        )
        if not success:
            if error.status_code in [401, 403, 429, 500]:
                raise error
            section_no_token_count = ReportSectionNoToken(
                title=section_title,
                content="Failed to generate summary due to an error.",
                input_tokens=0,
                output_tokens=0,
            )
        logger.info(f"[{state['document_id'][:8]}] Generated section: {section_no_token_count.title}")
        
        # Serialize ONLY the title and content fields, not the entire structure
        new_section = ReportSection(
            title=serialize_dict(section_no_token_count.title),
            content=serialize_dict(section_no_token_count.content),
            input_tokens=input_tokens,  # Keep as int
            output_tokens=output_tokens,  # Keep as int
        )  
    else:
        new_section = ReportSection(title="", content="", input_tokens=0, output_tokens=0)

    # Return the dict directly, don't serialize it
    return {
        "detailed_report_sections": [new_section],
        "report_sections": [section_markdown],
    }

async def collector_node(state: State) -> Command[Literal["__end__"]]:
    """Collect report sections from subgraph and update the main state."""
    logger.info(f"[{state['document_id'][:8]}] Collecting report sections from subgraph")

    # # Log state size for debugging checkpoint issues
    # try:
    #     state_json = json.dumps(dict(state), default=str)
    #     state_size_mb = len(state_json) / (1024 * 1024)
    #     logger.info(f"[{state['document_id'][:8]}] State size before processing: {state_size_mb:.2f} MB")

    #     # Log individual component sizes
    #     generated_files_size = len(json.dumps(state.get("generated_files", {}), default=str)) / (1024 * 1024)
    #     original_data_size = len(str(state.get("original_data", ""))) / (1024 * 1024)
    #     current_data_size = len(str(state.get("current_data", ""))) / (1024 * 1024)
    #     logger.info(f"[{state['document_id'][:8]}] Component sizes - generated_files: {generated_files_size:.2f} MB, original_data: {original_data_size:.2f} MB, current_data: {current_data_size:.2f} MB")
    # except Exception as e:
    #     logger.warning(f"Could not calculate state size: {e}")

    # await send_health_check(state["document_id"], AIStatus.PROCESSING)
    final_report = "# Data analysis report\n\n"
    detailed_report: list[str] = []
    for section_count, (section, ai_section) in enumerate(zip(state["report_sections"], state["detailed_report_sections"])):
        # FOR FINAL REPORT: Keep headers for human readability
        final_report += f"## Raw report of log {section_count + 1}\n\n{section}\n\n"

        # FOR DETAILED_REPORT (consumed by write_data): NO HEADERS
        # This prevents headers from interfering with document structure
        detailed_report.append(section)  # Just the section content, no header

        if state["get_detailed_report"]:
            final_report += f"## AI report of log {section_count + 1}\n\n{ai_section.content}\n\n"
            detailed_report.append(ai_section.content)  # Just the content, no header

        # Add separator after each section (except last) to close lists/blocks
        if section_count < len(state["report_sections"]) - 1:
            final_report += "---\n\n"

    state = await process_generated_files(state)

    # # Log final state size
    # try:
    #     final_state_size_mb = len(json.dumps(dict(state), default=str)) / (1024 * 1024)
    #     logger.info(f"[{state['document_id'][:8]}] Final state size after processing: {final_state_size_mb:.2f} MB")
    #     if final_state_size_mb > 16:  # Redis default proto-max-bulk-len is 512MB, but warn at 16MB
    #         logger.warning(f"State size ({final_state_size_mb:.2f} MB) is large and may cause checkpoint save issues")
    # except Exception as e:
    #     logger.warning(f"Could not calculate final state size: {e}")

    generated_files = dict(state["generated_files"])
    generated_files["file_descriptions"] = json.dumps(
        state.get("file_descriptions", {}), ensure_ascii=False
    )
    return {
        "final_report": final_report,
        "generated_files": generated_files,
        "detailed_report": detailed_report,
        "input_tokens": state["input_tokens"] + sum([section.input_tokens for section in state["detailed_report_sections"]]),
        "output_tokens": state["output_tokens"] + sum([section.output_tokens for section in state["detailed_report_sections"]])
    }
    
def format_tables_in_log(log_content: str) -> str:
    """
    Ensures proper Markdown table rendering by inserting a blank line before the table header
    if it follows non-blank text directly (without separation). Handles standard Markdown tables.
    """
    import re
    if not log_content or '|' not in log_content:
        return log_content

    lines = log_content.splitlines()
    new_lines = []
    in_table = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('|') and not in_table:
            # Potential start of table: insert blank line if previous line is non-empty/non-blank
            if new_lines and new_lines[-1].strip():
                new_lines.append('')  # Blank line for separation
            in_table = True
        elif stripped and not stripped.startswith('|'):
            in_table = False
        new_lines.append(line)
    
    return '\n'.join(new_lines) + '\n' if new_lines else log_content

async def get_graph(checkpointer: Checkpointer):
    builder = StateGraph(State)

    builder.add_node("coordinator", coordinator_node)
    builder.add_node("background_investigator", background_investigation_node)
    builder.add_node("planner", planner_node)
    builder.add_node("human_feedback", human_feedback_node)
    builder.add_node("analyzer", analyzer_node)
    builder.add_node("section_reporter", section_reporter)
    builder.add_node("collector", collector_node)

    builder.add_edge(START, "coordinator")
    builder.add_edge("section_reporter", "collector")
    builder.add_edge("collector", END)
    return builder.compile(checkpointer=checkpointer)
