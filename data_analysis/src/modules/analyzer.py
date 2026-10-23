import asyncio
import json
import logging
import os
from langgraph.types import Command
from bson import ObjectId
import pandas as pd
from pymongo import AsyncMongoClient
import markdown
from data_analysis.src.configs.app import settings
from data_analysis.src.modules.analyzer_graph import get_graph
from data_analysis.src.schemas.analyzer_states import ExecutionMode, Variable, SimplifiedPlanStep, VariableCondition
from data_analysis.src.modules.utils import create_state_from_dataframes, _check_variable_logic, upload_image
from data_analysis.src.modules.tools.analysis.chart.overview_chart import run_overview_charts_analysis
from data_analysis.src.modules.prompt_bank import (
    variable_objective_match, 
    scale_format_match, 
    scope_target_match, 
    sample_size_adequacy,
)

from utils import get_async_redis_checkpoint, get_async_mongo_checkpoint, get_s3_client
from get_llm_response import get_llm, get_answer_with_schema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

class Analyzer():
    def __init__(self, model_id: str, language: str, llm_key: str, max_tokens: int, mongo_client: AsyncMongoClient):
        self.model_id = model_id
        self.language = language.lower()
        self.llm_key = llm_key
        self.max_tokens = max_tokens
        self.llm = get_llm(self.model_id, self.llm_key, self.max_tokens)
        self.mongo_client = mongo_client

    async def check_variable_logic(self, variables_data: list[dict]) -> dict[str, list[str]]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _check_variable_logic, variables_data, self.language)

    async def check_variable_condition(self, condition_name: str, system_prompt: str, content: str) -> VariableCondition:
        logger.info(f"[variable_condition] Checking condition: {condition_name}")
                
        # Get structured response from LLM
        error, success, result, input_tokens, output_tokens = await get_answer_with_schema(
            _id="variable_condition", 
            llm=self.llm,
            system_prompt=system_prompt,
            content=content,
            structure=VariableCondition
        )
        
        if not success:
            raise error
        
        # Accumulate tokens
        logger.info(f"Condition {condition_name} - Input tokens: {input_tokens}, Output tokens: {output_tokens}")
        
        # Ensure condition_name is set
        result.condition_name = condition_name

        return result, input_tokens, output_tokens
    
    async def check_variable_conditions(
        self,
        data_dict: dict,
        variables_data: list[dict],
        final_proposal: dict,
    ) -> tuple[list[VariableCondition], int, int]:
        """
        Check all 4 variable conditions and return consolidated results with token tracking.
        
        Args:
            data_dict: Dictionary containing the data
            variables_data: List of variable configurations
            research_context: Context describing what conditions to check
            
        Returns:
            Tuple of (success, result, total_input_tokens, total_output_tokens) where result is list of VariableCondition on success
            or error message1 on failure
        """
        logger.info("[variable_condition] Starting multiple variable condition checks")             
        # Create DataFrame for data analysis (same for all conditions)
        data = pd.DataFrame(data_dict)
        logger.info(f"[variable_condition] DataFrame shape: {data.shape}, columns: {data.columns.tolist()}")
        
        # Prepare data summary for LLM (same for all conditions)
        data_summary = {
            "shape": data.shape,
            "columns": data.columns.tolist(),
            "dtypes": data.dtypes.to_dict(),
            "missing_values": data.isnull().sum().to_dict()
        }
        
        # Create content (same for all conditions)
        content = f"""
        RESEARCH CONTEXT:
        {final_proposal}
        
        VARIABLES TO EVALUATE:
        {variables_data}

        DATA SUMMARY:
        {json.dumps(data_summary, indent=2, default=str)}

        Please evaluate this specific condition based on the research context, variables, and data provided.
        Provide a thorough explanation and a clear boolean conclusion.

        Generate response in:
        {self.language}
        """
        
        # Get condition prompts
        condition_prompts = {
            "variable_objective_match": variable_objective_match,
            "scale_format_match": scale_format_match,
            "scope_target_match": scope_target_match,
            "sample_size_adequacy": sample_size_adequacy
        }
        
        # Results collection
        tasks = [self.check_variable_condition(condition_name, system_prompt, content) for condition_name, system_prompt in condition_prompts.items()]
        results = await asyncio.gather(*tasks)
        condition_results = [result[0] for result in results]
        input_tokens = [result[1] for result in results]
        output_tokens = [result[2] for result in results]
        return condition_results, sum(input_tokens), sum(output_tokens)

    def _build_analyze_log(
        self,
        message: dict,
        data_dict: dict,
        variables_data: list[dict],
        generated_files: dict,
        result_html: str,
    ) -> str:
        """Build the plain-text analysis summary (`analyze_log`).

        The text is assembled ONLY from data actually produced by the graph
        (logs / execution log / variables / generated files). Anything not
        available is reported as missing instead of being invented.
        """
        # Inspect the dataset without building a DataFrame (keeps this helper
        # dependency-free and cheap for large payloads).
        columns = len(data_dict) if isinstance(data_dict, dict) else 0
        column_lengths = (
            [len(values) for values in data_dict.values() if isinstance(values, (list, tuple, set))]
            if isinstance(data_dict, dict)
            else []
        )
        rows = max(column_lengths) if column_lengths else 0

        variables = message.get("current_variables") or variables_data or []
        variable_codes: list[str] = []
        for variable in variables:
            if isinstance(variable, dict):
                code = variable.get("code") or variable.get("name")
            else:
                code = getattr(variable, "code", None) or getattr(variable, "name", None)
            if code:
                variable_codes.append(str(code))

        raw_logs = message.get("logs") or []
        if isinstance(raw_logs, str):
            raw_logs = [raw_logs]
        log_lines = [str(line).strip() for line in raw_logs if str(line).strip()]

        execution_log = message.get("execution_log")
        if execution_log is None:
            execution_text = ""
        elif isinstance(execution_log, str):
            execution_text = execution_log
        else:
            execution_text = json.dumps(execution_log, ensure_ascii=False, default=str)

        warning_markers = ("WARNING", "CẢNH BÁO", "ERROR", "FAILED")
        warning_lines = [
            line for line in log_lines if any(marker in line.upper() for marker in warning_markers)
        ]

        file_names = [name for name in generated_files if name != "file_descriptions"]

        summary_parts = [
            f"Dataset: {rows} rows x {columns} columns.",
            (
                f"Variables used in the analysis ({len(variable_codes)}): "
                f"{', '.join(variable_codes) if variable_codes else 'not reported by the graph'}."
            ),
        ]

        if log_lines:
            summary_parts.append("Analysis results (latest steps):")
            summary_parts.extend(f"- {line}" for line in log_lines[-40:])
        elif execution_text:
            summary_parts.append("Analysis results (execution log):")
            summary_parts.append(execution_text[:3000])
        else:
            summary_parts.append("Analysis results: the graph did not return log entries.")

        if file_names:
            summary_parts.append(f"Generated artefacts: {', '.join(file_names)}.")
        if result_html:
            summary_parts.append("Full HTML report was uploaded successfully.")
        else:
            summary_parts.append("Full HTML report upload failed; only the artefacts above are available.")

        if warning_lines:
            summary_parts.append("Warnings:")
            summary_parts.extend(f"- {line}" for line in warning_lines[:10])
        else:
            summary_parts.append("Warnings: none reported by the analysis tools.")

        return "\n".join(summary_parts)

    async def run_graph(
            self,
            document_id: str,
            thread_id: str,
            db: str,
            get_detailed_report: bool,
            resume: bool,
            resume_error: bool,
            feedback: str | dict,
            final_proposal: dict,
            data_dict: dict,
            variables_data: list[dict],
            auto_plan_mode: bool,
            auto_action_mode: bool,
            manual_plan_mode: bool = False,
            tool_configs: list[dict] = None,
            ) -> tuple[bool, str | dict]:
        checkpointer = await get_async_redis_checkpoint() if db == "redis" else await get_async_mongo_checkpoint()
        graph = await get_graph(checkpointer)
        thread = {
            "configurable": {
                "thread_id": thread_id,
            },
        }
        _id = document_id if document_id else thread_id
        try:
            if resume or resume_error:
                checkpoint = await graph.aget_state(thread)
                if not checkpoint.values or "plan_mode" not in checkpoint.values:
                    raise ValueError(f"Analyzer checkpoint not found for thread_id '{thread_id}'")
            if resume_error:
                logger.info(f"[{_id}] Invoking graph with resume_error mode")
                message = await graph.ainvoke(None, thread)
            elif resume and feedback and not resume_error:
                resume_input = Command(resume=feedback)
                logger.info(f"[{_id}] Invoking graph with resume command")
                message = await graph.ainvoke(resume_input, thread)
            else:
                for var_data in variables_data:
                    if "values" in var_data and isinstance(var_data["values"], str):
                        values_str = var_data["values"].strip()

                        # Skip empty strings
                        if not values_str:
                            logger.warning(f"[{_id}] Empty values field for variable {var_data.get('name', 'Unknown')}, skipping")
                            continue

                        # Only parse if it looks like JSON (starts with '{')
                        if values_str.startswith("{"):
                            try:
                                values_dict = json.loads(values_str)
                                formatted_values = ", ".join(f"{k}: {v}" for k, v in values_dict.items())
                                var_data["values"] = "{" + formatted_values + "}"
                            except json.JSONDecodeError as e:
                                logger.error(f"[{_id}] Error parsing values JSON for variable {var_data.get('name', 'Unknown')}: {e}")
                                logger.error(f"[{_id}] Problematic values content: '{var_data['values']}'")
                                raise
                        # else: keep plain text strings as-is (like "Nhập tỷ lệ phần trăm: ____%")
                variables = [Variable(**var_data) for var_data in variables_data]

                data = pd.DataFrame(data_dict)
                logger.info(f"[{_id}] DataFrame shape: {data.shape}, columns: {data.columns.tolist()}")

                # Determine plan mode based on priority: manual > auto > interactive
                if manual_plan_mode:
                    plan_mode = ExecutionMode.MANUAL
                    logger.info(f"[{_id}] Using MANUAL plan mode with tool configs")
                elif auto_plan_mode:
                    plan_mode = ExecutionMode.AUTO
                    logger.info(f"[{_id}] Using AUTO plan mode")
                else:
                    plan_mode = ExecutionMode.INTERACTIVE
                    logger.info(f"[{_id}] Using INTERACTIVE plan mode")

                # Convert tool_configs if provided
                tool_configs_list = []
                if tool_configs and manual_plan_mode:
                    try:
                        tool_configs_list = [SimplifiedPlanStep(**config) for config in tool_configs]
                        logger.info(f"[{_id}] Loaded {len(tool_configs_list)} tool configurations for manual mode")
                    except Exception as e:
                        logger.error(f"[{_id}] Error parsing tool configs: {e}")
                        raise
            
                initial_state_model = create_state_from_dataframes(
                    original_data=data,
                    current_data=data.copy(),
                    original_variables=variables,
                    current_variables=[v.model_copy(deep=True) for v in variables], # Ensure deep copy for mutable objects
                    action_mode=ExecutionMode.AUTO if auto_action_mode else ExecutionMode.INTERACTIVE,
                    plan_mode=plan_mode,
                    tool_configs=tool_configs_list,
                    current_plan=None,
                    report_sections=[],
                    model_id=self.model_id,
                    language=self.language,
                    input_tokens=0,
                    output_tokens=0,
                    messages=[],
                    queries=[],
                    docs=[],
                    generated_files={},
                    execution_log=None,
                    logs=[],
                    get_detailed_report=get_detailed_report,
                    final_proposal=final_proposal,
                    llm_key=self.llm_key,
                    document_id=document_id,
                    max_tokens=self.max_tokens
                )
                logger.info(f"[{_id}] Invoking graph with initial state model")
                message = await graph.ainvoke(initial_state_model, thread)
            logger.info(f"[{_id}] Finish run graph")
            interruptted = message.get("__interrupt__")
            if interruptted:
                interrupt_message = interruptted[-1].value
                logger.info(f"[{_id}] Interrupt message: {str(interrupt_message)}")
                final_state = await graph.aget_state(thread)
                result = {
                    "ai_message": interrupt_message,
                    "input_tokens": final_state.values["input_tokens"],
                    "output_tokens": final_state.values["output_tokens"]
                }
                return False, result
            else:                    
                # Start with original markdown report
                original_markdown = message["final_report"]
                
                # Keep metadata in the persisted payload, but never render it as a result file.
                generated_files = dict(message["generated_files"])
                raw_file_descriptions = generated_files.pop("file_descriptions", "{}")
                try:
                    file_descriptions = (
                        raw_file_descriptions
                        if isinstance(raw_file_descriptions, dict)
                        else json.loads(raw_file_descriptions or "{}")
                    )
                except (TypeError, json.JSONDecodeError):
                    file_descriptions = {}

                # Identify actual generated artifacts and classify them
                all_files = list(generated_files.keys())
                
                # Find mentioned files (those referenced in original markdown)
                mentioned_files = [f for f in all_files if f in original_markdown]
                other_files = [f for f in all_files if f not in mentioned_files]
                
                # Step 1: Create HTML snippets for all files (consistent caption placement: below content)
                def create_file_snippet(file_name: str, prefix: str = "", count: dict = None) -> str:
                    if count is not None:
                        if file_name.endswith('.csv'):
                            count['tab'] += 1
                            title = file_name.replace("_", " ").replace("/", " - ").replace(".csv", "").title()
                            table_type = "Table" if prefix == "" else f"{prefix} Table"
                            snippet = f'<div>{generated_files[file_name]}<div class="caption"><b>{table_type} {count["tab"]}</b>: {title}</div></div>'
                        elif file_name.endswith('.png'):
                            count['fig'] += 1
                            title = file_name.replace("_", " ").replace("/", " - ").replace(".png", "").title()
                            figure_type = "Figure" if prefix == "" else f"{prefix} Figure"
                            snippet = f'<div><img src="{generated_files[file_name]}" width="90%" alt="{title}"><div class="caption"><b>{figure_type} {count["fig"]}</b>: {title}</div></div>'
                        elif file_name.endswith('.txt'):
                            snippet = f'<div>{generated_files[file_name]}<div class="caption"><b>{prefix} Text Output</b>: {file_name}</div></div>'
                        else:
                            snippet = f'<div>{generated_files[file_name]}<div class="caption"><b>{prefix} File</b>: {file_name}</div></div>'
                    else:
                        # For inline replacements, no numbering
                        if file_name.endswith('.csv'):
                            title = file_name.replace("_", " ").replace("/", " - ").replace(".csv", "").title()
                            snippet = f'{generated_files[file_name]}<div class="caption"><b>Table: {title}</b></div>'
                        elif file_name.endswith('.png'):
                            title = file_name.replace("_", " ").replace("/", " - ").replace(".png", "").title()
                            snippet = f'<img src="{generated_files[file_name]}" width="90%" alt="{title}"><div class="caption"><b>Figure: {title}</b></div>'
                        elif file_name.endswith('.txt'):
                            snippet = generated_files[file_name]
                        else:
                            snippet = generated_files[file_name]
                    return snippet
                
                # Step 2: FIRST convert original_markdown to HTML (pure markdown, no snippets yet)
                md_converter = markdown.Markdown(
                    extensions=['tables', 'fenced_code', 'codehilite', 'nl2br'],
                    output_format='html',
                    safe_mode=False
                )
                main_html = md_converter.convert(original_markdown)
                
                # THEN replace placeholders in the HTML version with HTML snippets
                main_html_with_inline = main_html
                for file_name in mentioned_files:
                    snippet = create_file_snippet(file_name, count=None)
                    main_html_with_inline = main_html_with_inline.replace(file_name, snippet)
                
                # Step 3: Build Used Files and Other Files sections (separate, numbered)
                used_files_html = '<h2>II. Used Files</h2>\n'
                used_counts = {'tab': 0, 'fig': 0}
                for file_name in mentioned_files:
                    used_files_html += create_file_snippet(file_name, "2.", used_counts) + '\n'
                
                other_files_html = '<h2>III. Other Files</h2>\n'
                other_counts = {'tab': 0, 'fig': 0}
                for file_name in other_files:
                    other_files_html += create_file_snippet(file_name, "3.", other_counts) + '\n'
                
                # Step 4: Structure the full body with hierarchical sections
                full_body_html = f'<h2>I. Main Analysis</h2>\n{main_html_with_inline}\n\n{used_files_html}\n\n{other_files_html}'
                
                # Step 5: Plug directly into HTML template (no further markdown conversion)
                html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Analysis Report</title>
    <style>
        body { font-family: sans-serif; line-height: 1.6; margin: 20px; }
        h1, h2, h3 { color: #333; }
        img { border: 1px solid #ddd; border-radius: 4px; padding: 5px; max-width: 90%; height: auto; display: block; margin-left: auto; margin-right: auto; }
        .caption { text-align: center; display: block; margin-top: 10px; margin-bottom: 30px; font-style: italic; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        ul { list-style-type: disc; margin-left: 20px; }
        li ul { list-style-type: circle; margin-left: 20px; }
        li li ul { list-style-type: square; margin-left: 20px; }
        /* Deeper nesting support */
        li li li ul { list-style-type: disc; margin-left: 20px; }
        li li li li ul { list-style-type: circle; margin-left: 20px; }
    </style>
</head>
<body>
<h1>ANALYSIS RESULTS</h1>
{body}
</body>
</html>
"""
                parse_files = html_template.replace('{body}', full_body_html)
                
                # Step 6: Upload to S3
                try:
                    s3_client = get_s3_client()
                    db = self.mongo_client["admin"]
                    collection = db["document_configurations"]
                    document_config = await collection.find_one({"documentId": document_id})
                    if document_config:
                        variables = [Variable(**var_data) for var_data in variables_data]
                        data = pd.DataFrame(data_dict)
                        appendices = run_overview_charts_analysis(data, variables, {})
                        file_contents = appendices.file_contents 
                        image_tasks = []
                        replacements: dict[str, str] = {}
                        for key, content in file_contents.items():
                            file_name = os.path.basename(key)
                            task = upload_image(content, file_name, document_id)
                            image_tasks.append((key, task))
                        # Execute all image uploads concurrently
                        if image_tasks:
                            results = await asyncio.gather(*(task for _, task in image_tasks), return_exceptions=True)
                            for (key, _), result in zip(image_tasks, results):
                                if isinstance(result, Exception):
                                    logger.error(f"[{_id}] Error in concurrent upload for {key}: {result}")
                                    replacements[key] = f"Image not available: {key}"
                                elif result:
                                    replacements[key] = result
                                else:
                                    replacements[key] = f"Image not available: {key}"
                            db = self.mongo_client["admin"]
                            article_collection = db["articles"]
                            article = await article_collection.find_one({"_id": ObjectId(document_id)})
                            proposal_collection = db["proposal_titles"]
                            await proposal_collection.update_one(
                                {
                                    "_id": ObjectId(article["title"])
                                },
                                {
                                    "$set": {
                                        "appendix_5": replacements
                                    }
                                },
                            )
                        user_id = document_config["createdBy"]
                        s3_client.put_object(
                            Bucket="users",
                            Key=f"{user_id}/article/{document_id}/{settings.MINIO_BUCKET_ANALYSIS}/{document_id}.html",
                            Body=parse_files.encode('utf-8'),
                            ContentType='text/html'
                        )
                        public_url = f"{settings.MINIO_DOMAIN}/users/{user_id}/article/{document_id}/{settings.MINIO_BUCKET_ANALYSIS}/{document_id}.html"
                    else:
                        tools_collection = db["tools"]
                        document_config = await tools_collection.find_one({"_id": ObjectId(document_id)})
                        user_id = document_config["createdBy"]
                        s3_client.put_object(
                            Bucket="users",
                            Key=f"{user_id}/tools/{document_id}/{settings.MINIO_BUCKET_ANALYSIS}/{document_id}.html",
                            Body=parse_files.encode('utf-8'),
                            ContentType='text/html'
                        )
                        public_url = f"{settings.MINIO_DOMAIN}/users/{user_id}/tools/{document_id}/{settings.MINIO_BUCKET_ANALYSIS}/{document_id}.html"

                    
                    logger.info(f"[{_id}] Successfully uploaded HTML report. URL: {public_url}")
                except Exception as e:
                    logger.error(f"[{_id}] Error uploading file {document_id} to MinIO: {e}")
                    public_url = ""
                
                # Remove the redundant embedding loop - no longer needed

                analyze_log = self._build_analyze_log(
                    message=message,
                    data_dict=data_dict,
                    variables_data=variables_data,
                    generated_files=generated_files,
                    result_html=public_url,
                )

                return True, {
                    "current_variables": [
                        variable.model_dump() if hasattr(variable, "model_dump") else dict(variable)
                        for variable in (message.get("current_variables") or [])
                    ],
                    "generated_files": {
                        **generated_files,
                        "file_descriptions": json.dumps(file_descriptions, ensure_ascii=False),
                    },
                    "result_html": public_url,
                    "upload_failed": not bool(public_url),
                    "analyze_log": analyze_log,
                    "detailed_logs": message["detailed_report"],
                    "input_tokens": message["input_tokens"],
                    "output_tokens": message["output_tokens"],
                }
        except Exception as e:
            logger.info(f"[{_id}] {e}")
            raise(e)
