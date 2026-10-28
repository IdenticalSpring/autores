from collections.abc import Iterable
from datetime import datetime
from typing import Any, Optional
import logging
from bson import ObjectId
import os
import base64
import pandas as pd
import numpy as np
import math
import rapidfuzz
import uuid
import asyncio
from playwright.async_api import async_playwright
from data_analysis.src.configs.app import settings
import json
from data_analysis.src.schemas.analyzer_states import State, VariableRole, Variable, ScaleType, VariableType, Query
from data_analysis.src.schemas.proposed_method import Mermaid, Source
from exception_type import AIERROR
import io
from pydantic import ValidationError
from collections import defaultdict
from get_llm_response import get_answer_with_schema
import unicodedata
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

from typing import List, Dict, Any
import re
import traceback
from utils import get_s3_client, get_mongodb_client

def search_document(query: Query, document: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Search the JSON document for entries matching a single query based on keywords.
    Args:
        query: A Query object specifying section_ids, method, and keywords.
        document: List of dictionaries representing the JSON document structure.
    Returns:
        List of matching document entries (dictionaries) preserving document order.
    """
    matches = []

    def matches_query(item: Dict[str, Any], query: Query) -> bool:
        """Check if a document item matches the query based on keywords."""
        if not query.keywords:
            return False

        keywords_lower = [k.lower() for k in query.keywords]
        item_keywords_lower = [k.lower() for k in item.get("keywords", [])]

        # Require at least one query keyword to be in the item's keywords list
        keyword_match = any(k in item_keywords_lower for k in keywords_lower)
        return keyword_match
    # Traverse document in order
    for item in document:
        if matches_query(item, query):
            match = {
                "id": item.get("id", ""),
                "title": item.get("title", ""),
                "content": item.get("content", ""),
                "keywords": item.get("keywords", [])
            }
            if "variable_content" in item:
                match["variable_content"] = item.get("variable_content", "")
            matches.append(match)

        # Check subsections recursively
        for subitem in item.get("subsections", []):
            if matches_query(subitem, query):
                match = {
                    "id": subitem.get("id", ""),
                    "title": subitem.get("title", ""),
                    "content": subitem.get("content", ""),
                    "keywords": subitem.get("keywords", [])
                }
                if "variable_content" in subitem:
                    match["variable_content"] = subitem.get("variable_content", "")
                matches.append(match)

            # Check nested subsections
            for subsubitem in subitem.get("subsections", []):
                if matches_query(subsubitem, query):
                    match = {
                        "id": subsubitem.get("id", ""),
                        "title": subsubitem.get("title", ""),
                        "content": subsubitem.get("content", ""),
                        "keywords": subsubitem.get("keywords", [])
                    }
                    if "variable_content" in subsubitem:
                        match["variable_content"] = subsubitem.get("variable_content", "")
                    matches.append(match)

    return matches

async def upload_image(base64_str: str, file_name: str, document_id: str) -> str | None:
    try:
        image_data = base64.b64decode(base64_str)
        unique_filename = f"{uuid.uuid4().hex}_{file_name}"
        s3_client = get_s3_client()
        mongo_client = get_mongodb_client()
        db = mongo_client["admin"]
        collection = db["document_configurations"]
        document_config = await collection.find_one({"documentId": document_id})
        if document_config:
            user_id = document_config["createdBy"]
        else:
            tools_collection = db["tools"]
            document_config = await tools_collection.find_one({"_id": ObjectId(document_id)})
            user_id = document_config["createdBy"]
        s3_client.put_object(
            Bucket="users",
            Key=f"{user_id}/article/{document_id}/{settings.MINIO_BUCKET_ANALYSIS}/{unique_filename}",
            Body=image_data,
            ContentType='image/png'
        )
        public_url = f"{settings.MINIO_DOMAIN}/users/{user_id}/article/{document_id}/{settings.MINIO_BUCKET_ANALYSIS}/{unique_filename}"
        return public_url
    except Exception as e:
        logger.error(f"Error uploading image {file_name} to MinIO: {e}")
        return None

def parse_csv_to_markdown(csv_str: str) -> str | None:
    """Parse a CSV string into a real Markdown table."""
    try:
        # Read CSV into DataFrame
        df = pd.read_csv(io.StringIO(csv_str))
        
        # Drop unnamed index columns created by CSV exports; they are not data fields.
        df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed:")]
        
        # Replace NaN/null with empty string
        df = df.fillna(" ")

        # Format numeric columns to 3 decimal places
        for col in df.columns:
            if col != " ":  # Skip empty column names
                # Try to convert to numeric and format
                try:
                    # Convert column to numeric, errors='ignore' keeps non-numeric values as-is
                    numeric_col = pd.to_numeric(df[col], errors='ignore')
                    
                    # Check if the column is actually numeric (not all strings)
                    if numeric_col.dtype in ['float64', 'int64', 'float32', 'int32']:
                        # Format floats with scientific notation for very small values
                        def format_number(x):
                            if isinstance(x, float) and not pd.isna(x):
                                # If the original value is actually zero
                                if x == 0.0:
                                    return "0.0"
                                # If the value would round to 0.000 but isn't actually zero, use scientific notation
                                elif abs(x) < 0.0005 and x != 0.0:  # Would round to 0.000 but not zero
                                    return f"{x:.1e}"
                                # Otherwise format to 3 decimal places
                                else:
                                    return f"{x:.3f}"
                            return x
                        
                        df[col] = numeric_col.apply(format_number)
                except:
                    # If conversion fails, keep original values
                    pass
        
        # Keep the Kafka payload Markdown-only; BE owns Markdown -> HTML.
        markdown_table = df.to_markdown(index=False, tablefmt="pipe")
        
        return markdown_table
    except Exception as e:
        logger.error(f"Error parsing CSV to markdown: {e}")
        return None

async def process_generated_files(state: dict[str, str]) -> dict:
    """Process generated files and update report sections and generated_files with markdown content concurrently."""
    replacements: dict[str, str] = {}

    # Process images concurrently
    image_tasks = []
    for key, value in state["generated_files"].items():
        if key.endswith('.png'):
            file_name = os.path.basename(key)
            task = upload_image(value, file_name, state["document_id"])
            image_tasks.append((key, task))
    
    # Execute all image uploads concurrently
    if image_tasks:
        results = await asyncio.gather(*(task for _, task in image_tasks), return_exceptions=True)
        for (key, _), result in zip(image_tasks, results):
            if isinstance(result, Exception):
                logger.error(f"Error in concurrent upload for {key}: {result}")
                replacements[key] = f"Image not available: {key}"
            elif result:
                replacements[key] = result
            else:
                replacements[key] = f"Image not available: {key}"

    # Process tables sequentially
    for key, value in state["generated_files"].items():
        # Handle CSVs
        if key.endswith('.csv'):
            markdown_table = parse_csv_to_markdown(value)
            if markdown_table:
                # Add newlines for markdown formatting
                replacements[key] = f"\n\n{markdown_table}"
            else:
                replacements[key] = f"Table not available: {key}" 
        # Process text files
        elif key.endswith('.txt'):
            # Format as an 'info' code block with newlines
            txt_content = f"\n```info\n{value}\n```\n"
            replacements[key] = txt_content

    # Update generated_files with replacements
    state["generated_files"] = replacements

    # ---  Update Report Content ---
    if state.get("get_detailed_report"):
        for section in state["detailed_report_sections"]:
            for key, replacement in replacements.items():
                if key.endswith('.csv'):
                    # STEP A: Use regex to prepare the area.
                    # It finds the key (with or without backticks), ensures newlines exist,
                    # and REMOVES the backticks using .strip('`') in the lambda.
                    pattern = rf"(^|[^\n])(\n{{0,}})(`{key}`|{key})(\n{{0,}}|$)"
                    section.content = re.sub(
                        pattern,
                        lambda m: f"{m.group(1)}\n\n{m.group(3).strip('`')}\n{m.group(4)}",
                        section.content
                    )
                    
                    # STEP B: Now that backticks are stripped, replace the plain key with the table.
                    # We also try replacing `{key}` just in case the regex didn't match (safety net).
                    section.content = section.content.replace(f"`{key}`", replacement)
                    section.content = section.content.replace(key, replacement)
                else:
                    # Images don't need the regex newline logic, just direct replacement
                    section.content = section.content.replace(f"`{key}`", replacement)
                    section.content = section.content.replace(key, replacement)
    
    return state

def serialize_dict(data: Any, path: str = "") -> Any:
    """
    Recursively serialize any data structure, converting pandas DataFrames/Series to JSON-compatible formats,
    NumPy arrays/scalars to Python equivalents, and handling nested iterables and custom objects.

    Args:
        data: Input data (dict, list, tuple, set, or any other type).
        path: Current path in the data structure for logging (default: "").

    Returns:
        Serialized data with all NumPy and pandas types converted to JSON-compatible Python types.

    Raises:
        ValueError: If an object cannot be serialized and no fallback is possible.
    """
    # Handle None
    if data is None:
        return None

    # --- FIX: Handle special float values (NaN, Infinity) which are not valid in JSON ---
    # This check is placed early to catch these values before other checks pass them through.
    if isinstance(data, float) and (math.isnan(data) or math.isinf(data)):
        return None  # Represent NaN/Inf as null in JSON

    # Handle pandas DataFrame
    if isinstance(data, pd.DataFrame):
        # The result of to_dict might contain NaNs, which will be handled by recursive calls
        return data.to_dict(orient="records")

    # Handle pandas Series
    if isinstance(data, pd.Series):
        # The result of to_list might contain NaNs, which will be handled by recursive calls
        return data.to_list()

    # Handle NumPy array
    if isinstance(data, np.ndarray):
        # tolist() converts np.nan to float('nan'), which our new check above will handle
        return data.tolist()

    # Handle NumPy scalar types (np.float64, np.int32, np.bool_, etc.)
    if isinstance(data, np.generic):
        # item() converts to a standard Python type. If it's a float nan/inf,
        # it will be caught by the recursive call or the check at the top.
        return data.item()

    # Handle dictionaries
    if isinstance(data, dict):
        return {str(key): serialize_dict(value, f"{path}[{key!r}]") for key, value in data.items()}

    # Handle iterables (lists, tuples, sets, etc.), excluding strings
    if isinstance(data, Iterable) and not isinstance(data, (str, bytes)):
        return [serialize_dict(item, f"{path}[{i}]") for i, item in enumerate(data)]

    # Handle datetime objects
    if isinstance(data, datetime):
        return data.isoformat()

    # Handle Pydantic BaseModel objects
    if isinstance(data, BaseModel):
        return serialize_dict(data.model_dump(), path)

    # Handle Enum objects
    if hasattr(data, 'value') and hasattr(data, 'name'):
        return data.value

    # Handle standard Python types that are JSON-serializable
    if isinstance(data, (str, int, float, bool)):
        return data

    # Handle other types (e.g., custom objects)
    try:
        json.dumps(data)  # Test if JSON-serializable
        return data
    except (TypeError, ValueError):
        logger.warning(f"Non-serializable type {type(data)} at path '{path}' with value {data}. Converting to string.")
        return str(data)
    
    
def serialize_state(state: Any) -> dict:
    """
    Create a JSON-serializable dictionary from a state object, handling any data type or field.
    
    Args:
        state: The state object to serialize.
    
    Returns:
        dict: A JSON-serializable dictionary representation of the state.
    """
    def serialize_value(obj: Any) -> Any:
        """Recursively serialize any value into a JSON-compatible format."""
        if obj is None:
            return None
        if isinstance(obj, str):
            return obj
        try:
            json.dumps(obj)
            return obj
        except (TypeError, OverflowError):
            pass
        if isinstance(obj, pd.DataFrame):
            return {
                "_type": "DataFrame",
                "shape": obj.shape,
                "columns": obj.columns.tolist()
            }
        if isinstance(obj, datetime):
            return obj.isoformat()
        if hasattr(obj, "model_dump"):
            return serialize_value(obj.model_dump())
        if isinstance(obj, (list, tuple)):
            return [serialize_value(item) for item in obj]
        if isinstance(obj, dict):
            return {str(k): serialize_value(v) for k, v in obj.items()}
        if hasattr(obj, "value"):
            return obj.value
        return str(obj)

    # Handle different state types
    if isinstance(state, str):
        try:
            state = json.loads(state)
        except json.JSONDecodeError:
            return {"_type": "string", "value": state}
    if isinstance(state, dict):
        return serialize_value(state)
    if hasattr(state, "model_dump"):
        return serialize_value(state.model_dump())
    if hasattr(state, "__dict__"):
        return serialize_value(vars(state))
    return serialize_value(state)

def parse_variable_values(input_str: str = None) -> Dict[str, str]:
    """Parse the 'values' or 'statements' field of a Variable into a dictionary mapping codes to labels.

    Args:
        input_str: String like "0: Nam, 1: Nữ", "{1: Nam},{2: Nữ},{3: Khác}", 
                  "{{1: Rất không đồng ý},{2: Không đồng ý},{3: Trung lập},{4: Đồng ý},{5: Rất đồng ý}}",
                  or "{1: Rất không ảnh hưởng, 2: Không ảnh hưởng, 3: Ảnh hưởng ít, 4: Ảnh hưởng nhiều, 5: Ảnh hưởng nghiêm trọng}", or None.

    Returns:
        Dictionary mapping value codes to labels (e.g., {"0": "Nam", "1": "Nữ"}).
    """
    if not input_str:
        return {}

    value_map = {}
    try:
        # Handle double-brace JSON-like format: "{{1: Rất không đồng ý},{2: Không đồng ý}}"
        if input_str.startswith('{{') and input_str.endswith('}}'):
            pairs = input_str.strip('{}').strip('{}').split('},{')
            for pair in pairs:
                if ':' not in pair:
                    continue
                parts = pair.split(':', 1)
                if len(parts) != 2:
                    continue
                code, label = parts[0].strip(), parts[1].strip()
                value_map[code] = label
        # Handle single-brace format: "{1: Rất không ảnh hưởng, 2: Không ảnh hưởng, ...}"
        elif input_str.startswith('{') and input_str.endswith('}'):
            # Remove outer braces and split by comma, accounting for spaces
            content = input_str.strip('{}').strip()
            if not content:
                return value_map
            # Split by comma, ensuring we don't split within labels that might contain commas
            pairs = [p.strip() for p in content.split(',') if p.strip()]
            for pair in pairs:
                # Find first colon to separate key and value
                if ':' not in pair:
                    continue
                parts = pair.split(':', 1)
                if len(parts) != 2:
                    continue
                code, label = parts[0].strip(), parts[1].strip()
                value_map[code] = label
        # Handle original format: "0: Nam, 1: Nữ"
        else:
            pairs = [pair.strip() for pair in input_str.split(',') if pair.strip()]
            for pair in pairs:
                if ':' not in pair:
                    continue
                parts = pair.split(':', 1)
                if len(parts) != 2:
                    continue
                code, label = parts[0].strip(), parts[1].strip()
                value_map[code] = label

    except Exception:
        # Return what we've parsed so far if any error occurs
        pass

    return value_map

def create_state_from_dataframes(
    original_data: pd.DataFrame, 
    current_data: pd.DataFrame,
    **kwargs: Any
) -> State:
    state: State = {
        "original_data": original_data.to_csv(index=False),
        "current_data": current_data.to_csv(index=False),
        **kwargs
    }
    return state

def get_current_dataframe(state: State) -> pd.DataFrame:
    return pd.read_csv(io.StringIO(state["current_data"]))

def set_current_data(state: State, df: pd.DataFrame):
    state["current_data"] = df.to_csv(index=False)

# ----------------------------------------------------- #
#### PARSE EXCEL, RAW VARIABLES ###
# ----------------------------------------------------- #

async def parse_excel_template(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _parse_excel_template(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse raw JSON data into a list of Variable dictionaries.
        Creates latent variables for groups of indicator variables with sequential numbering.
        
        Args:
            raw_data: List of dictionaries containing variable information
            
        Returns:
            List of dictionaries representing Variable objects (both observed and latent)
            
        Raises:
            ValueError: When validation fails with detailed error information
        """
        
        # Mapping Vietnamese scale types to English enum values
        scale_mapping = {
            "Thang đo Danh nghĩa": ScaleType.NOMINAL.value,
            "Thang đo Thứ bậc": ScaleType.ORDINAL.value,
            "Thang đo Khoảng": ScaleType.INTERVAL.value,
            "Thang đo Tỉ lệ": ScaleType.RATIO.value,
            "Thang đo Guttman": ScaleType.GUTTMAN.value,
            "Thang đo Semantic Differential": ScaleType.SEMANTIC_DIFFERENTIAL.value,
            "Thang đo Mức độ hiệu suất": ScaleType.PERFORMANCE_LEVEL.value,
        }
        
        # Mapping Vietnamese variable roles to English enum values
        role_mapping = {
            "Biến độc lập": VariableRole.INDEPENDENT.value,
            "Biến phụ thuộc": VariableRole.DEPENDENT.value,
            "Biến nội sinh": VariableRole.ENDOGENOUS.value,
            "Biến trung gian": VariableRole.INTERMEDIATE.value,
            "Biến điều tiết": VariableRole.MODERATOR.value,
            "Biến kiểm soát": VariableRole.CONTROL.value,
        }
        
        parsed_variables = []
        group_analysis = {}  # To analyze groups for latent variable creation
        validation_errors = []  # Collect all validation errors
        
        # First pass: Parse all variables and analyze groups
        for row_index, item in enumerate(raw_data, start=1):  # Start from 1 for user-friendly row numbers
            try:
                current_errors = []  # Track errors for this specific row
                
                # Validate and extract CODE field
                code = item.get('CODE', '')
                if code is None or str(code).strip() == '':
                    current_errors.append(f"CODE field is required and cannot be empty at row {row_index}")
                    continue  # Skip this row as CODE is essential
                
                code = str(code).strip()
                
                # Check for duplicate codes
                existing_codes = set([var.get('code') for var in parsed_variables])
                if code in existing_codes:
                    current_errors.append(f"Duplicate CODE '{code}' found at row {row_index}. Each variable must have a unique code.")
                
                # Validate CODE format (basic validation - can be enhanced)
                if not code.replace('_', '').replace('-', '').isalnum():
                    current_errors.append(f"CODE '{code}' contains invalid characters at row {row_index}. Only alphanumeric characters, underscores, and hyphens are allowed.")
                
                # Extract and validate SCALE field
                scale_vietnamese = item.get('SCALE', '')
                if scale_vietnamese is None or str(scale_vietnamese).strip() == '':
                    current_errors.append(f"SCALE field is required for variable '{code}' at row {row_index}")
                else:
                    scale_vietnamese = str(scale_vietnamese).strip()
                    scale_english = scale_mapping.get(scale_vietnamese)
                    if not scale_english:
                        valid_scales = list(scale_mapping.keys())
                        current_errors.append(
                            f"Invalid SCALE value '{scale_vietnamese}' for variable '{code}' at row {row_index}, column 'SCALE'. "
                            f"Valid values are: {', '.join(valid_scales)}"
                        )
                
                # Extract and validate VARIABLE_TYPE field
                role_vietnamese = item.get('VARIABLE_TYPE', '')
                role_english = None
                if role_vietnamese is None or str(role_vietnamese).strip() == '':
                    current_errors.append(f"VARIABLE_TYPE field is required for variable '{code}' at row {row_index}")
                else:
                    role_vietnamese = str(role_vietnamese).strip()
                    role_english = role_mapping.get(role_vietnamese)
                    if not role_english:
                        valid_roles = list(role_mapping.keys())
                        current_errors.append(
                            f"Invalid VARIABLE_TYPE value '{role_vietnamese}' for variable '{code}' at row {row_index}, column 'VARIABLE_TYPE'. "
                            f"Valid values are: {', '.join(valid_roles)}"
                        )
                        role_english = VariableRole.INDEPENDENT.value  # Default fallback
                
                # Validate NAME field
                name = item.get('NAME', '')
                if name is None or str(name).strip() == '':
                    name = code  # Use code as fallback
                else:
                    name = str(name).strip()
                    if len(name) > 255:  # Reasonable length limit
                        current_errors.append(f"NAME field is too long (max 255 characters) for variable '{code}' at row {row_index}")
                
                # Validate optional fields with length limits
                statement = item.get('STATEMENT', item.get('Câu hỏi'))
                if statement is not None:
                    statement = str(statement).strip() if statement else None
                    if statement and len(statement) > 1000:  # Reasonable length limit
                        current_errors.append(f"STATEMENT field is too long (max 1000 characters) for variable '{code}' at row {row_index}")
                
                values = item.get('VALUES', item.get('Giá trị'))
                if values is not None:
                    values = str(values).strip() if values else None
                    if values and len(values) > 500:  # Reasonable length limit
                        current_errors.append(f"VALUES field is too long (max 500 characters) for variable '{code}' at row {row_index}")
                
                unit = item.get('UNIT', item.get('Tỉ lệ'))
                if unit is not None:
                    unit = str(unit).strip() if unit else None
                    if unit and len(unit) > 100:  # Reasonable length limit
                        current_errors.append(f"UNIT field is too long (max 100 characters) for variable '{code}' at row {row_index}")
                
                # Extract and validate GROUP field
                group_name = item.get('GROUP', item.get('Nhóm'))
                if group_name is not None:
                    group_name = str(group_name).strip() if group_name else None
                    if group_name and len(group_name) > 200:  # Reasonable length limit
                        current_errors.append(f"GROUP field is too long (max 200 characters) for variable '{code}' at row {row_index}")
                
                # If there are validation errors for this row, collect them and continue
                if current_errors:
                    validation_errors.extend(current_errors)
                    continue
                
                # Create variable dictionary (initially without parent_code)
                variable_dict = {
                    'name': name,
                    'code': code,
                    'variable_type': VariableType.OBSERVED.value,
                    'role': role_english,
                    'statement': statement,
                    'values': values,
                    'scale': scale_english,
                    'unit': unit,
                    'group_name': group_name  # Temporary field for analysis
                }
                
                # Remove None values to keep the JSON clean
                variable_dict = {k: v for k, v in variable_dict.items() if v is not None}
                
                parsed_variables.append(variable_dict)
                
                # Analyze group for potential latent variable creation
                if group_name:
                    if group_name not in group_analysis:
                        group_analysis[group_name] = []
                    group_analysis[group_name].append({
                        'code': code,
                        'role': role_english,
                        'variable_dict': variable_dict,
                        'row_index': row_index
                    })
                
            except Exception as e:
                validation_errors.append(f"Unexpected error parsing variable at row {row_index}: {str(e)}")
                continue
        
        # If there are validation errors, raise them all at once
        if validation_errors:
            error_message = "Validation errors found in Excel template:\n" + "\n".join(validation_errors)
            raise AIERROR(638, error_message)
        
        # Check if we have any valid variables
        if not parsed_variables:
            raise AIERROR(639, "No valid variables found in the Excel template. Please check your data format.")
        
        # Second pass: Analyze groups and create latent variables
        latent_variables = []
        latent_creation_errors = []
        
        for group_name, group_vars in group_analysis.items():
            if len(group_vars) <= 1:
                continue  # Skip groups with only one variable
            
            try:
                # Extract codes and check for sequential numbering pattern
                codes = [var['code'] for var in group_vars]
                
                # Try to find a common prefix and sequential numbering
                potential_latent_code = None
                sequential_vars = []
                
                # Sort codes to analyze pattern
                codes.sort()
                
                # Look for pattern: prefix + sequential numbers starting from 1
                for i, code in enumerate(codes):
                    # Try to extract prefix and number
                    import re
                    match = re.match(r'^(.+?)(\d+)$', code)
                    if match:
                        prefix = match.group(1)
                        number = int(match.group(2))
                        
                        if i == 0:  # First code sets the pattern
                            if number == 1:  # Must start from 1
                                potential_latent_code = prefix
                                sequential_vars.append((code, number))
                            else:
                                break  # Pattern doesn't start from 1
                        else:
                            # Check if this follows the pattern
                            if prefix == potential_latent_code and number == sequential_vars[-1][1] + 1:
                                sequential_vars.append((code, number))
                            else:
                                potential_latent_code = None
                                break
                    else:
                        potential_latent_code = None
                        break
                
                # If we found a valid sequential pattern, create latent variable
                if potential_latent_code and len(sequential_vars) >= 2:
                    # Check if latent code already exists
                    existing_codes = [var.get('code') for var in parsed_variables + latent_variables]
                    if potential_latent_code in existing_codes:
                        rows_in_group = [str(var['row_index']) for var in group_vars]
                        latent_creation_errors.append(
                            f"Cannot create latent variable with code '{potential_latent_code}' for group '{group_name}' "
                            f"(rows: {', '.join(rows_in_group)}) because this code already exists."
                        )
                        continue
                    
                    # Validate that all variables in the group have the same role
                    roles = [var['role'] for var in group_vars]
                    if len(set(roles)) > 1:
                        rows_in_group = [str(var['row_index']) for var in group_vars]
                        latent_creation_errors.append(
                            f"Variables in group '{group_name}' (rows: {', '.join(rows_in_group)}) "
                            f"have different VARIABLE_TYPE values. All indicators of a latent variable must have the same role."
                        )
                        continue
                    
                    # Create latent variable
                    first_var_role = group_vars[0]['role']
                    
                    latent_var = {
                        'name': group_name,
                        'code': potential_latent_code,
                        'variable_type': VariableType.LATENT.value,
                        'role': first_var_role  # Inherit role from indicators
                    }
                    
                    latent_variables.append(latent_var)
                    
                    # Update indicator variables to point to latent variable
                    sequential_codes = [var_code for var_code, _ in sequential_vars]
                    for var_dict in parsed_variables:
                        if var_dict['code'] in sequential_codes:
                            var_dict['parent_code'] = potential_latent_code
                
            except Exception as e:
                rows_in_group = [str(var['row_index']) for var in group_vars]
                latent_creation_errors.append(
                    f"Error creating latent variable for group '{group_name}' (rows: {', '.join(rows_in_group)}): {str(e)}"
                )
        
        # Report latent variable creation errors as warnings (don't stop processing)
        if latent_creation_errors:
            print("Warnings during latent variable creation:")
            for error in latent_creation_errors:
                print(f"  - {error}")
        
        # Clean up temporary group_name field from all variables
        for var_dict in parsed_variables:
            var_dict.pop('group_name', None)
        
        # Combine latent variables and observed variables
        all_variables = latent_variables + parsed_variables
        
        # Final validation: ensure we have at least one variable
        if not all_variables:
            raise ValueError("No valid variables could be created from the Excel template.")
        
        print(f"Successfully parsed {len(parsed_variables)} observed variables and created {len(latent_variables)} latent variables.")
        
        return all_variables
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _parse_excel_template, raw_data)

class Variable(BaseModel):
    name: str
    code: str
    statement: Optional[str] = None  # The survey question text for this variable
    variable_type: VariableType = VariableType.OBSERVED
    role: VariableRole = VariableRole.INDEPENDENT
    parent_code: Optional[str] = None  # For grouping variables
    values: Optional[str] = None  # Possible values for categorical variables, or range
    scale: ScaleType  # e.g., "interval", "nominal", "ordinal", "ratio", "guttman", "semantic differential", "hybrid", "performance level"
    unit: Optional[str] = None  # e.g., "USD", "count", date format it can be "DayOfWeek", "DayOfMonth", "Month", "Hour", "%YYYY-%mm-%dd" ...

class Variables(BaseModel):
    variables: List[Variable]

def manually_parse_survey_to_variables(
    variables: List[Dict[str, Any]],
    survey_questions: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Manually parse survey questions and variables into temporary variable format.
    Uses ALL STRING FIELDS (no enums) to keep content exactly as-is, even if in Vietnamese or incorrect format.
    LLM will later convert to proper Variable schema with enums.

    Field Mappings (original → temp → final):
    - variable_type (Độc lập/Phụ thuộc/etc) → role → INDEPENDENT/DEPENDENT/etc
    - measurement_type (Latent/Observable or Tiềm ẩn/Quan sát) → variable_type → LATENT/OBSERVED
    - scale_type or scale → scale → NOMINAL/ORDINAL/INTERVAL/RATIO/etc

    Args:
        variables: List of dicts with variable schema (name, variable_type, measurement_type, description, scale).
        survey_questions: List of dicts with survey questions (variable_name, variable_code, variable_type, measurement_type, questions, answer_content).

    Returns:
        List of dicts with all string fields, keeping ALL content exactly as provided.
    """
    parsed_vars = []

    # # First, process standalone variables (not from surveys)
    # for var in variables:
    #     var_dict = {
    #         "name": var.get("name"),
    #         "code": var.get("name"),  # Use name as code if not provided
    #         "statement": var.get("description"),  # Keep description exactly
    #         "variable_type": var.get("measurement_type"),  # Latent/Observable (or Tiềm ẩn/Quan sát) - keep as-is
    #         "role": var.get("variable_type"),  # Độc lập/Phụ thuộc/etc - keep as-is
    #         "parent_code": None,
    #         "values": None,
    #         "scale": var.get("scale"),  # Keep scale exactly as-is
    #         "unit": None
    #     }
    #     parsed_vars.append(var_dict)

    # Group survey questions by variable_code to identify latent variables
    survey_groups = {}
    for sq in survey_questions:
        var_code = sq.get("variable_code", sq.get("variable_name"))
        if var_code not in survey_groups:
            survey_groups[var_code] = []
        survey_groups[var_code].append(sq)

    # Process survey questions
    for var_code, questions in survey_groups.items():
        first_q = questions[0]
        measurement_type = first_q.get("measurement_type", "").lower()

        # Check if it's latent (multiple questions OR measurement_type indicates latent)
        is_latent = len(questions) > 1 or "latent" in measurement_type or "tiềm ẩn" in measurement_type

        if is_latent:
            # Create latent parent variable
            latent_var = {
                "name": first_q.get("variable_name", var_code),
                "code": var_code,
                "statement": None,  # Latent variables don't have statements
                "variable_type": first_q.get("measurement_type"),  # Keep exactly: "Latent" or "Tiềm ẩn"
                "role": first_q.get("variable_type"),  # Keep exactly: "Độc lập", "Phụ thuộc", etc
                "parent_code": None,
                "values": None,
                "scale": None,  # Latent doesn't have scale directly
                "unit": None
            }
            parsed_vars.append(latent_var)

            # Create child observed variables for each question
            for idx, q in enumerate(questions, 1):
                # Use question_code if available, otherwise generate
                child_code = q.get("question_code") or f"{var_code}{idx}"
                child_var = {
                    "name": q.get("indicator_name") or q.get("variable_name", f"{var_code} Item {idx}"),
                    "code": child_code,
                    "statement": q.get("questions") or q.get("question"),  # The actual question text
                    "variable_type": "Observable",  # Child is always observable
                    "role": first_q.get("variable_type"),  # Inherit role from parent
                    "parent_code": var_code,
                    "values": q.get("answer_content"),  # Keep exact answer content
                    "scale": q.get("scale_type"),  # Keep scale exactly as-is
                    "unit": None
                }
                parsed_vars.append(child_var)
        else:
            # Single observable variable
            q = questions[0]
            single_var = {
                "name": q.get("variable_name", var_code),
                "code": var_code,
                "statement": q.get("questions") or q.get("question"),
                "variable_type": q.get("measurement_type"),  # Keep exactly: "Observable" or "Quan sát"
                "role": q.get("variable_type"),  # Keep exactly: "Độc lập", "Phụ thuộc", etc
                "parent_code": None,
                "values": q.get("answer_content"),  # Keep exact answer content
                "scale": q.get("scale_type"),  # Keep scale exactly as-is
                "unit": None
            }
            parsed_vars.append(single_var)

    return parsed_vars

async def parse_raw_variables(
    _id: str,
    variables: List[Dict[str, Any]],
    survey_questions: List[Dict[str, Any]],
    llm: Any,
    language: str,
    pre_parsed_variables: Optional[List[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """
    Processes the raw input variables and survey questions into a list of valid Variable schemas

    Args:
        variables: List of dicts with old variable schema (name, variable_type, measurement_type, description, scale).
        survey_questions: List of dicts with survey questions (variable_name, variable_code, variable_type, measurement_type, questions, answer_content).
        llm: Initialized LLM instance.
        language: Language for processed output (e.g., "en", "vi").
        pre_parsed_variables: Optional pre-parsed variable list that needs fixing/refinement.

    Returns:
        List of dicts representing parsed Variables (JSON-serializable), with textual fields in the specified language.
    """

    # If pre-parsed variables are provided, use a different prompt focused on fixing/refining
    if pre_parsed_variables:
        pre_parsed_json = json.dumps(pre_parsed_variables, ensure_ascii=False, indent=2)
        content = f"""
Pre-parsed Variables (manually extracted - ALL original content preserved):
{pre_parsed_json}

IMPORTANT: Data above uses strings and may contain Vietnamese. All content is already extracted - DO NOT hallucinate!

FIELD CONVERSION MAPPINGS:
1. variable_type: Convert Vietnamese/string to VariableType enum
   - "Latent"/"Tiềm ẩn" → LATENT
   - "Observable"/"Quan sát" → OBSERVED

2. role: Convert Vietnamese/string to VariableRole enum
   - "Độc lập"/"Independent" → INDEPENDENT
   - "Phụ thuộc"/"Dependent" → DEPENDENT
   - "Trung gian"/"Mediator" → MEDIATOR
   - "Điều tiết"/"Moderator" → MODERATOR
   - "Kiểm soát"/"Control" → CONTROL
   - If null, infer from context

3. scale: Convert Vietnamese/string to ScaleType enum
   - "Likert" → INTERVAL
   - "Danh nghĩa"/"Nominal" → NOMINAL
   - "Thứ tự"/"Ordinal" → ORDINAL
   - "Khoảng"/"Interval" → INTERVAL
   - "Tỷ lệ"/"Ratio" → RATIO
   - Infer from values/answer_content if needed

Your tasks:
1. Convert enum fields using mappings above
2. Translate text fields (name, statement, values, unit) to {language}
3. Validate data structure and parent-child relationships
4. DO NOT change, add, or remove actual content

General Parsing Requirements:
- Translate all human-readable text fields (name, labels, unit, statements) into the target language: {language}.
- All enums must be in English.
- Infer missing details based on context when needed.

Variable Interpretation Rules:

1. Latent Variables:
    - Detected if a variable in survey_questions groups multiple indicators/questions.
    - Fields:
        - variable_type = LATENT
        - statement = null
        - role: must not be null. Infer from variable_type or context (e.g., independent vs dependent).
        - code: variable_code from the survey questions.
        - scale: infer based on the scales of its child indicators.
        - parent_code = null
        - values = null
        - unit = null
    - Each survey question under a latent variable becomes a child observed variable.
    - All child variables inherit the same role as the latent parent.

2. Child Observed Variables (Indicators of Latent Variables):
    - One child created per survey question belonging to the latent variable.
    - Fields:
        - variable_type = OBSERVED
        - parent_code = code of the latent variable
        - role: same as the latent parent
        - statement: the question text, translated
        - code:
            - use the survey variable_code if provided,
            - or generate sequential codes (e.g., ABC1, ABC2).
        - scale:
            - Infer from "scale" or "answer_content"
            - Examples:
                - Likert (e.g., 1–5 agreement) → INTERVAL
                - ordered categories → ORDINAL
                - categorical names → NOMINAL
                - continuous (numeric) → RATIO
        - values:
            - For scales with discrete categories:
                - Provide a JSON representation mapping values → translated labels.
            - For continuous ranges:
                - Provide descriptive range text in {language}.
            - If no explicit value options exist, set to null.
        - unit: translated if present, otherwise null.

3. Normal Single Variables (not latent and not indicators):
    - Set:
        - variable_type = OBSERVED
        - parent_code = null
        - role: infer (must not be null)
        - statement: use the question text if available
        - other fields follow the same scale/value/unit inference rules.

4. Time Series Variables (time index or date/time data):
    - Fields:
        - variable_type = time_index
        - parent_code = null
        - role = "null"
        - unit: must be a valid date or time format (e.g., "MM-YYYY", "DD-MM-YYYY", "HH:mm:ss", etc.)
        - values: normal text content describing
        - statement: describe the time period or temporal aspect

5. ID Variables (entity identifiers for panel data):
    - Fields:
        - variable_type = entity_index
        - parent_code = null
        - role = "null"
        - unit = "ID"
        - values: normal text content describing the entity type
        - statement: describe what the ID represents (e.g., "Company identifier", "Individual ID")

Additional Rules:
- If the original data includes multilingual or shorthand values (e.g., "same as above"), expand them completely using context before translating.
- Ensure every variable always has:
    - type
        - role (must not be null, except for time series variables and ID variables)
- Latent variables must not have statements; only child observed variables carry the actual question text.
- Demographic or background variables (e.g., age, gender, income, education, marital status, etc...) must always be parsed as standalone single variables:
    - Do not convert demographic variables into latent constructs.
    - Do not create child indicator variables for them.
    - They must be simple observed variables with parent_code = null.
"""
    else:
        # Original behavior - parse from raw variables and survey questions
        variables_json = json.dumps(variables, ensure_ascii=False, indent=2)
        survey_json = json.dumps(survey_questions, ensure_ascii=False, indent=2)
        content = f"""
Raw Variables:
{variables_json}

Raw Questions (including answer_content for scales/values):
{survey_json}

Task: Carefully parse all provided variables and survey questions into a list of new Variable objects, following strict structural rules and correcting inconsistencies logically.

General Parsing Requirements:
- Translate all human-readable text fields (name, labels, unit, statements) into the target language: {language}.
- All enums must be in English.
- Infer missing details based on context when needed.

Variable Interpretation Rules:

1. Latent Variables:
    - Detected if a variable in survey_questions groups multiple indicators/questions.
    - Fields:
        - variable_type = LATENT
        - statement = null
        - role: must not be null. Infer from variable_type or context (e.g., independent vs dependent).
        - code: variable_code from the survey questions.
        - scale: infer based on the scales of its child indicators.
        - parent_code = null
        - values = null
        - unit = null
    - Each survey question under a latent variable becomes a child observed variable.
    - All child variables inherit the same role as the latent parent.

2. Child Observed Variables (Indicators of Latent Variables):
    - One child created per survey question belonging to the latent variable.
    - Fields:
        - variable_type = OBSERVED
        - parent_code = code of the latent variable
        - role: same as the latent parent
        - statement: the question text, translated
        - code:
            - use the survey variable_code if provided,
            - or generate sequential codes (e.g., ABC1, ABC2).
        - scale:
            - Infer from "scale" or "answer_content"
            - Examples:
                - Likert (e.g., 1–5 agreement) → INTERVAL
                - ordered categories → ORDINAL
                - categorical names → NOMINAL
                - continuous (numeric) → RATIO
        - values:
            - For scales with discrete categories:
                - Provide a JSON representation mapping values → translated labels.
            - For continuous ranges:
                - Provide descriptive range text in {language}.
            - If no explicit value options exist, set to null.
        - unit: translated if present, otherwise null.

3. Normal Single Variables (not latent and not indicators):
    - Set:
        - variable_type = OBSERVED
        - parent_code = null
        - role: infer (must not be null)
        - statement: use the question text if available
        - other fields follow the same scale/value/unit inference rules.

4. Time Series Variables (time index or date/time data):
    - Fields:
        - variable_type = time_index
        - parent_code = null
        - role = "null" ( the string of "null" )
        - unit: must be a valid date or time format (e.g., "MM-YYYY", "DD-MM-YYYY", "HH:mm:ss", etc.)
        - values: normal text content describing
        - statement: describe the time period or temporal aspect

5. ID Variables (entity identifiers for panel data):
    - Fields:
        - variable_type = entity_index
        - parent_code = null
        - role = "null" ( the string of "null" )
        - unit = "ID"
        - values: normal text content describing the entity type
        - statement: describe what the ID represents (e.g., "Company identifier", "Individual ID")

Additional Rules:
- If the original data includes multilingual or shorthand values (e.g., "same as above"), expand them completely using context before translating.
- Ensure every variable always has:
    - type
    - role (must not be null, EXCEPT for time series variables and ID variables which should have role with string "null" )
- Latent variables must not have statements; only child observed variables carry the actual question text.
- Demographic or background variables (e.g., age, gender, income, education, marital status, etc...) must always be parsed as standalone single variables:
    - Do not convert demographic variables into latent constructs.
    - Do not create child indicator variables for them.
    - They must be simple observed variables with parent_code = null.
"""

    # System prompt for structured output
    if pre_parsed_variables:
        system_prompt = f"""You are an expert in research methodology and variable modeling. Convert the pre-parsed variable list (with Vietnamese/string fields) into the proper Variable schema with English enums. All content is already extracted - focus ONLY on: (1) converting enum fields from Vietnamese to English using the provided mappings, (2) translating text fields to {language}, (3) validating structure. DO NOT hallucinate, add, or remove any content."""
    else:
        system_prompt = f"""You are an expert in research methodology and variable modeling. Parse the provided input into the new schema with contextual understanding. Translate/map textual content (names, labels) to {language}; keep enums in English. Handle ambiguities by inferring from context (e.g., incomplete answer_content -> full expansion, then translate)."""
    
    # Call the LLM function
    error, success, parsed_variables, tokens_in, tokens_out = await get_answer_with_schema(
        _id=_id,
        llm=llm,
        system_prompt=system_prompt,
        content=content,
        structure=Variables
    )
    
    # Convert to list of dicts for JSON-serializable output
    variable_dicts = [var.model_dump() for var in parsed_variables.variables] if success else []

    return error, success, variable_dicts, tokens_in, tokens_out

# ----------------------------------------------------- #
#### CHECK VARIABLE LOGIC ###
# ----------------------------------------------------- #

MESSAGES = {
    # Step 2: Unique Codes
    'duplicate_codes': {
        'english': "ERROR: Duplicate codes: {codes}",
        'tiếng việt': "LỖI: Các mã trùng lặp: {codes}"
    },
    # Step 3: Parent Existence
    'missing_parent': {
        'english': "ERROR: Missing parent '{parent_code}'.",
        'tiếng việt': "LỖI: Thiếu biến tiềm ẩn '{parent_code}'."
    },
    # Step 4: Cycles
    'cycle_detected': {
        'english': "ERROR: Cycle in parent hierarchy.",
        'tiếng việt': "LỖI: Vòng lặp trong hệ thống phân cấp biến tiềm ẩn và chỉ báo."
    },
    # Step 5: Enum Validity
    'invalid_scale': {
        'english': "ERROR: Invalid scale '{scale}' for '{code}'.",
        'tiếng việt': "LỖI: Thang đo không hợp lệ '{scale}' cho '{code}'."
    },
    'invalid_type': {
        'english': "ERROR: Invalid type '{variable_type}' for '{code}'.",
        'tiếng việt': "LỖI: Loại biến không hợp lệ '{variable_type}' cho '{code}'."
    },
    'invalid_role': {
        'english': "ERROR: Invalid role '{role}' for '{code}'.",
        'tiếng việt': "LỖI: Vai trò không hợp lệ '{role}' cho '{code}'."
    },
    # Step 6: Multiple Dependents
    'multiple_dependents': {
        'english': "WARNING: {count} dependent vars (typically expect 1).",
        'tiếng việt': "CẢNH BÁO: {count} biến phụ thuộc (thường chỉ mong đợi 1 biến)."
    },
    # Step 7: VariableType + Role Compatibility
    'invalid_type_role_latent_dependent': {
        'english': "ERROR: Invalid type-role: '{code}' (LATENT can't be DEPENDENT).",
        'tiếng việt': "LỖI: Kết hợp loại-vai trò không hợp lệ: '{code}' (TIỀM ẨN không thể là biến PHỤ THUỘC)."
    },
    'invalid_type_role_index': {
        'english': "ERROR: Invalid type-role: '{code}' (index types should be INDEPENDENT/NULL).",
        'tiếng việt': "LỖI: Kết hợp loại-vai trò không hợp lệ: '{code}' (các loại chỉ số nên là ĐỘC LẬP hoặc KHÔNG)."
    },
    # Step 8: Scale + VariableType
    'scale_type_mismatch_time_index': {
        'english': "WARNING: Scale-type mismatch: '{code}' (TIME_INDEX expects ORDINAL/INTERVAL).",
        'tiếng việt': "CẢNH BÁO: Không khớp thang đo-loại: '{code}' (CHỈ SỐ THỜI GIAN mong đợi thang ORDINAL/INTERVAL)."
    },
    # Step 9: Scale + Values (for categorical)
    'missing_values_categorical': {
        'english': "WARNING: Missing values: '{code}' (categorical scale needs 'values').",
        'tiếng việt': "CẢNH BÁO: Thiếu giá trị: '{code}' (thang đo phân loại cần trường 'Giá Trị')."
    },
    # Step 10: Statement for Key Roles
    'missing_statement_key_role': {
        'english': "WARNING: Missing statement: '{code}' ({role} needs 'statement').",
        'tiếng việt': "CẢNH BÁO: Thiếu mô tả: '{code}' (vai trò {role} cần trường 'statement')."
    },
    # Step 12: Basic Research Design
    'no_independents_for_dependent': {
        'english': "WARNING: Dependent var exists but no independents (basic model incomplete).",
        'tiếng việt': "CẢNH BÁO: Có biến phụ thuộc nhưng không có biến độc lập (mô hình cơ bản chưa hoàn chỉnh)."
    },
    # Step 13: Unit Consistency in Hierarchy
    'unit_mismatch_hierarchy': {
        'english': "WARNING: Unit mismatch under '{parent_code}': {children}",
        'tiếng việt': "CẢNH BÁO: Đơn vị không nhất quán dưới giữa tiềm ẩn và chỉ báo '{parent_code}': {children}"
    },
    # Step 14: Unique Names
    'duplicate_names': {
        'english': "WARNING: Duplicate names: {names}",
        'tiếng việt': "CẢNH BÁO: Các tên trùng lặp: {names}"
    },
    # Step 15: Latent should not have values
    'invalid_latent_values': {
        'english': "ERROR: Invalid latent values: '{code}' (LATENT should not have 'values').",
        'tiếng việt': "LỖI: Giá trị không hợp lệ cho biến tiềm ẩn: '{code}' (TIỀM ẨN không nên có trường 'Giá Trị')."
    },
    # Step 16: Indicators for Latent
    'invalid_latent_indicator_type': {
        'english': "ERROR: Invalid latent indicators: '{child_code}' (indicator of LATENT '{parent_code}' must be OBSERVED).",
        'tiếng việt': "LỖI: Chỉ báo tiềm ẩn không hợp lệ: '{child_code}' (chỉ báo của TIỀM ẨN '{parent_code}' phải là QUAN SÁT)."
    },
    'invalid_latent_indicator_naming': {
        'english': "ERROR: Invalid latent indicators: '{child_code}' (indicator of LATENT '{parent_code}' must end with a number, e.g., {parent_code}1).",
        'tiếng việt': "LỖI: Chỉ báo tiềm ẩn không hợp lệ: '{child_code}' (chỉ báo của TIỀM ẨN '{parent_code}' phải kết thúc bằng số, ví dụ: {parent_code}1)."
    },
    # Generic for schema validation (keep dynamic)
    'schema_validation_error': {
        'english': "ERROR (index {index}): {error}",
        'tiếng việt': "LỖI định dạng không hợp lệ (vị trí {index}): {error}"
    }
}


def _get_message(key: str, language: str, **kwargs) -> str:
    """
    Helper to get translated message.
    
    Args:
        key: Message key.
        language: 'english' or 'tiếng việt'.
        **kwargs: Placeholders for formatting.
    
    Returns:
        Formatted message string.
    """
    if language not in ['english', 'tiếng việt']:
        language = 'english'  # Fallback
    msg_template = MESSAGES.get(key, {}).get(language, f"Missing message: {key}")
    return msg_template.format(**kwargs)


def _check_variable_logic(variables_data: List[dict], language: str = 'english') -> Dict[str, List[str]]:
    """
    Enhanced check for variable logic flaws. Returns dict with 'errors' (fatal) and 'warnings' (advisory).
    
    Args:
        variables_data: List of dicts.
        language: Language for messages ('english' or 'tiếng việt').
    
    Returns:
        {'errors': [msgs], 'warnings': [msgs]}
    """
    errors: List[str] = []
    warnings: List[str] = []
    
    # Existing Step 1: Schema Validation
    variables = []
    for i, var_data in enumerate(variables_data):
        try:
            var = Variable(**var_data)
            variables.append(var)
        except ValidationError as e:
            error_msg = _get_message('schema_validation_error', language, index=i, error=str(e))
            errors.append(error_msg)
    
    if errors:
        return {'errors': errors, 'warnings': warnings}
    
    # Existing Step 2: Unique Codes
    codes = [v.code for v in variables]
    unique_codes = set(codes)
    if len(codes) != len(unique_codes):
        duplicates = [c for c in set(codes) if codes.count(c) > 1]
        error_msg = _get_message('duplicate_codes', language, codes=', '.join(duplicates))
        errors.append(error_msg)
    
    # Existing Step 3: Parent Existence
    parent_codes = [v.parent_code for v in variables if v.parent_code]
    missing_parents = [pc for pc in set(parent_codes) if pc not in unique_codes]
    for pc in missing_parents:
        error_msg = _get_message('missing_parent', language, parent_code=pc)
        errors.append(error_msg)
    
    # Existing Step 4: Cycles (unchanged)
    graph = defaultdict(list)
    for v in variables:
        if v.parent_code:
            graph[v.parent_code].append(v.code)
    
    def has_cycle(node: str, visited: set, rec_stack: set) -> bool:
        visited.add(node)
        rec_stack.add(node)
        for child in graph[node]:
            if child not in visited:
                if has_cycle(child, visited, rec_stack):
                    return True
            elif child in rec_stack:
                return True
        rec_stack.remove(node)
        return False
    
    visited = set()
    for code in codes:
        if code not in visited:
            temp_visited = set()
            temp_rec_stack = set()
            if has_cycle(code, temp_visited, temp_rec_stack):
                error_msg = _get_message('cycle_detected', language)
                errors.append(error_msg)
                break
            visited.update(temp_visited)
    
    # Existing Step 5: Enum Validity
    valid_scales = {s.value for s in ScaleType}
    valid_types = {t.value for t in VariableType}
    valid_roles = {r.value for r in VariableRole}
    for v in variables:
        if v.scale not in valid_scales:
            error_msg = _get_message('invalid_scale', language, scale=v.scale, code=v.code)
            errors.append(error_msg)
        if v.variable_type not in valid_types:
            error_msg = _get_message('invalid_type', language, variable_type=v.variable_type, code=v.code)
            errors.append(error_msg)
        if v.role not in valid_roles:
            error_msg = _get_message('invalid_role', language, role=v.role, code=v.code)
            errors.append(error_msg)
    
    # Existing Step 6: Multiple Dependents
    dependent_count = sum(1 for v in variables if v.role == VariableRole.DEPENDENT)
    if dependent_count > 1:
        warning_msg = _get_message('multiple_dependents', language, count=dependent_count)
        warnings.append(warning_msg)
    
    # Existing Step 7: VariableType + Role Compatibility
    invalid_type_role = []
    for v in variables:
        if v.variable_type == VariableType.LATENT and v.role == VariableRole.DEPENDENT:
            msg = _get_message('invalid_type_role_latent_dependent', language, code=v.code)
            invalid_type_role.append(msg)
        elif v.variable_type in [VariableType.TIME_INDEX, VariableType.ENTITY_INDEX] and v.role not in [VariableRole.INDEPENDENT, VariableRole.NULL]:
            msg = _get_message('invalid_type_role_index', language, code=v.code)
            invalid_type_role.append(msg)
    errors.extend(invalid_type_role)
    
    # Existing Step 8: Scale + VariableType
    invalid_scale_type = []
    for v in variables:
        if v.variable_type == VariableType.TIME_INDEX and v.scale not in [ScaleType.ORDINAL.value, ScaleType.INTERVAL.value]:
            msg = _get_message('scale_type_mismatch_time_index', language, code=v.code)
            invalid_scale_type.append(msg)
    warnings.extend(invalid_scale_type)
    
    # Existing Step 9: Scale + Values (for categorical)
    missing_values = []
    for v in variables:
        if v.scale in [ScaleType.NOMINAL.value, ScaleType.ORDINAL.value] and (not v.values or v.values.strip() == ''):
            msg = _get_message('missing_values_categorical', language, code=v.code)
            missing_values.append(msg)
    warnings.extend(missing_values)
    
    # Existing Step 10: Statement for Key Roles
    missing_stmt = []
    key_roles = [VariableRole.DEPENDENT, VariableRole.INTERMEDIATE, VariableRole.MODERATOR]
    for v in variables:
        if v.role in key_roles and (not v.statement or v.statement.strip() == ''):
            msg = _get_message('missing_statement_key_role', language, code=v.code, role=v.role.value)
            missing_stmt.append(msg)
    warnings.extend(missing_stmt)
    
    # Existing Step 12: Basic Research Design (e.g., need independents for dependent)
    has_dependent = any(v.role == VariableRole.DEPENDENT for v in variables)
    has_independent = any(v.role == VariableRole.INDEPENDENT for v in variables)
    if has_dependent and not has_independent:
        warning_msg = _get_message('no_independents_for_dependent', language)
        warnings.append(warning_msg)
    
    # Existing Step 13: Unit Consistency in Hierarchy
    for parent_code, children in graph.items():
        parent = next((v for v in variables if v.code == parent_code), None)
        if parent and parent.unit:
            inconsistent_units = [c for c in children if next((vv for vv in variables if vv.code == c), None).unit != parent.unit]
            if inconsistent_units:
                warning_msg = _get_message('unit_mismatch_hierarchy', language, parent_code=parent_code, children=', '.join(inconsistent_units))
                warnings.append(warning_msg)
    
    # Existing Step 14: Unique Names
    names = [v.name for v in variables]
    if len(names) != len(set(names)):
        dups = [n for n in set(names) if names.count(n) > 1]
        warning_msg = _get_message('duplicate_names', language, names=', '.join(dups))
        warnings.append(warning_msg)
    
    # NEW: Step 15: Latent should not have values
    invalid_latent_values = []
    for v in variables:
        if v.variable_type == VariableType.LATENT and v.values and v.values.strip() != '':
            msg = _get_message('invalid_latent_values', language, code=v.code)
            invalid_latent_values.append(msg)
    errors.extend(invalid_latent_values)
    
    # Step 16: Indicators for Latent (children codes must follow naming convention: parent_code + digit)
    # Also, children of LATENT must be OBSERVED
    invalid_indicators = []
    for parent_code, children_codes in graph.items():
        parent = next((v for v in variables if v.code == parent_code), None)
        if parent and parent.variable_type == VariableType.LATENT:
            for child_code in children_codes:
                child = next((v for v in variables if v.code == child_code), None)
                if not child:
                    continue  # Shouldn't happen due to earlier checks
                if child.variable_type != VariableType.OBSERVED:
                    msg = _get_message('invalid_latent_indicator_type', language, child_code=child_code, parent_code=parent_code)
                    invalid_indicators.append(msg)
                suffix = child_code[len(parent_code):]
                if not suffix or not suffix.isdigit():
                    msg = _get_message('invalid_latent_indicator_naming', language, child_code=child_code, parent_code=parent_code)
                    invalid_indicators.append(msg)
                # Optional: Check for sequential uniqueness, but basic for now
    errors.extend(invalid_indicators)
    
    return {'errors': errors, 'warnings': warnings}

# ----------------------------------------------------- #
#### MERMAID UTILS ####
# ----------------------------------------------------- #
    
async def render_mermaid_to_image(graph_definition: str) -> bytes:
    html_template = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Mermaid Render</title>
        <style>
            body {{
                margin: 0;
                padding: 0;
                display: block;
            }}
            #mermaid-container {{
                width: fit-content;
                margin: 0;
                padding: 0;
                display: block;
            }}
            #mermaid-container svg {{
                display: block !important;
                visibility: visible !important;
                width: fit-content;
                height: auto;
                margin: 0;
                padding: 0;
            }}
        </style>
    </head>
    <body>
        <div id="mermaid-container"></div>

        <script type="module">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11.12.1/dist/mermaid.esm.min.mjs';
            
            // Initialize Mermaid with minimal padding and no max-width constraints
            mermaid.initialize({{
                startOnLoad: false,
                diagramPadding: 0,
                useMaxWidth: false
            }});
            
            // The graph definition is now dynamically inserted from Python
            const graphDefinition = `{graph_definition}`;

            const element = document.querySelector('#mermaid-container');
            const {{ svg }} = await mermaid.render('graphDiv', graphDefinition);
            element.innerHTML = svg;
            
            // Optional: Trim SVG viewBox to exact content bounds (removes any residual padding)
            const svgElement = element.querySelector('svg');
            if (svgElement) {{
                const bbox = svgElement.getBBox();
                svgElement.setAttribute('viewBox', `${{bbox.x}} ${{bbox.y}} ${{bbox.width}} ${{bbox.height}}`);
                // Set explicit width and height attributes based on bbox to ensure proper sizing
                svgElement.setAttribute('width', bbox.width);
                svgElement.setAttribute('height', bbox.height);
                // Remove any conflicting styles or attributes if present
                svgElement.removeAttribute('style');
                svgElement.style.display = 'block';
                svgElement.style.visibility = 'visible';
                svgElement.style.width = 'fit-content';
                svgElement.style.height = 'auto';
            }}
        </script>
    </body>
    </html>
    """
    try:
        async with async_playwright() as p:
            async with await p.chromium.launch(headless=True, args=["--no-sandbox"]) as browser:
                page = await browser.new_page()
                
                await page.set_content(html_template)
                
                # Wait for network idle to ensure the module import completes
                await page.wait_for_load_state('networkidle')
                
                mermaid_container_selector = "#mermaid-container"
                try:
                    # First, wait for the SVG to be attached (not necessarily visible yet)
                    await page.wait_for_selector(selector=f"{mermaid_container_selector} svg", timeout=15000, state="attached")
                    # Then, wait a bit for any post-render adjustments and check for errors
                    await page.wait_for_timeout(1000)
                    if await page.locator("#mermaid-error").count() > 0:  # Note: This selector assumes Mermaid adds an error div; adjust if needed
                        raise ValueError("Mermaid syntax error prevented rendering.")
                    
                    # Ensure visibility by evaluating a script to force it if needed
                    await page.evaluate("""
                        const svg = document.querySelector('#mermaid-container svg');
                        if (svg) {
                            svg.style.display = 'block';
                            svg.style.visibility = 'visible';
                            svg.style.opacity = '1';
                        }
                    """)
                    
                    # Now wait for it to be visible
                    await page.wait_for_selector(selector=f"{mermaid_container_selector} svg", timeout=5000, state="visible")
                    
                    mermaid_element = page.locator(mermaid_container_selector)

                    image_bytes = await mermaid_element.screenshot(omit_background=True)
                    return image_bytes
                except Exception as e:
                    # On failure, capture the page HTML for debugging
                    html_content = await page.content()
                    print(f"Debug HTML: {html_content[:1000]}...")  # Truncated for log
                    raise e
    except Exception as e:
        raise e

async def extract_mermaid(
    mermaid_code: str, 
    state: dict, 
    llm: Any,
    fix_code: bool = False,  # Controls fix_mermaid_syntax call
    fix_with_llm: bool = False  # Controls LLM fixing and retries
) -> tuple[bool, str, bytes, int, int]:
    
    if fix_code:
        mermaid_code = fix_mermaid_syntax(mermaid_code)
    
    render_input = 0
    render_output = 0
    success_render = False
    image_content = None
    
    # Try initial render
    try:
        image_content = await render_mermaid_to_image(mermaid_code)
        if image_content:
            success_render = True
            logger.info("Successfully rendered PNG graph")
    except Exception as e:
        logger.info(f"Initial render failed: {e}")
    
    # If fix_with_llm is False, return immediately without retries
    if not fix_with_llm:
        return success_render, mermaid_code, image_content, render_input, render_output
    
    # LLM fixing logic (only if fix_with_llm is True)
    for attempt in range(3):
        if not success_render:
            content = f"The following mermaid code is wrong, fix this mermaid syntax for me so that I can generate png file via POST 'https://kroki.io/mermaid/png/'\n```{mermaid_code}```"
            error, success, svg, input_tokens, output_tokens = await get_answer_with_schema(
                state['document_id'][:8], 
                llm, 
                f"Response in {state['language']}\n", 
                content, 
                Mermaid
            )
            if not success:
                raise error
            
            render_input += input_tokens
            render_output += output_tokens
            mermaid_code = svg.mermaid_code.strip()
            
            if fix_code:
                mermaid_code = fix_mermaid_syntax(mermaid_code)
            
            if "```" in mermaid_code:
                mermaid_code = mermaid_code.split("```")[1].split("mermaid")[-1]
            
            if "graph" in mermaid_code or "flowchart" in mermaid_code:
                try:
                    image_content = await render_mermaid_to_image(mermaid_code)
                    if image_content:
                        success_render = True
                        logger.info("Successfully rendered PNG graph")
                        break
                except Exception as e:
                    logger.info(f"LLM generated wrong format. Attempt {attempt + 1}/3 failed, retrying...")
            else:
                logger.info(f"LLM generated wrong format. Attempt {attempt + 1}/3 failed, retrying...")
        else:
            break
    
    return success_render, mermaid_code, image_content, render_input, render_output

def fix_mermaid_syntax(text: str) -> str:
    out_lines = []
    # Regex for node labels: node[...]
    label_re = re.compile(r'([A-Za-z0-9_]+)\[([^\]]*)\]')
    # NEW: Regex for link labels: -->|...|
    link_label_re = re.compile(r'(<-->|-->|-.->|==>|---)\s*\|(.*?)\|')

    lines = text.splitlines()
    if "subgraph" in lines[0]:
        lines.insert(0, "graph LR")
    elif "graph" not in lines[0] and "subgraph" in lines[1]:
        lines[0] = "graph LR"
    n = len(lines)

    # First pass: class fixes + label quoting
    i = 0
    while i < n:
        line = lines[i]

        # Fix single-percent comments to double-percent ---
        stripped = line.strip()
        if stripped.startswith('%') and not stripped.startswith('%%'):
            # Find original indentation to preserve it
            indent_len = len(line) - len(line.lstrip())
            indent = line[:indent_len]
            # Rebuild the line with the corrected comment syntax
            line = f"{indent}%%{stripped[1:]}"

        if line.strip().startswith('%%'):
            out_lines.append(line)
            i += 1
            continue

        # 1) class <name> fill:... -> classDef <name> fill:...
        m_fill = re.match(r'^(\s*)class\s+([A-Za-z_]\w*)(\s+fill:.*)$', line)
        if m_fill:
            indent, name, rest = m_fill.groups()
            out_lines.append(f"{indent}classDef {name}{rest}")
            i += 1
            continue

        # 2) class <nodes> <classname> -> normalize commas and add missing ones
        m_class = re.match(r'^(\s*)class\s+(.*)$', line)
        if m_class:
            indent, rest = m_class.groups()
            semicolon = ''
            if rest.endswith(';'):
                rest, semicolon = rest[:-1].rstrip(), ';'

            parts = rest.rsplit(None, 1)
            if len(parts) == 2:
                node_part, class_name = parts
                node_part = re.sub(r'\s*,\s*', ',', node_part.strip())
                node_part = re.sub(r'\s+', ',', node_part)
                node_part = re.sub(r',+', ',', node_part)
                node_part = node_part.strip(',')
                node_part = ",".join([x.strip() for x in re.split(r'[,\s]+', node_part) if x.strip()])
                if node_part and node_part != class_name:
                    out_lines.append(f"{indent}class {node_part} {class_name}{semicolon}")
                    i += 1
                    continue        
        # 3) Normalize and quote link labels: | H1 | -> |"H1"|
        def _fix_link_label(m):
            arrow = m.group(1)
            content = m.group(2).strip()
            # Strip existing quotes to prevent double-quoting
            if (content.startswith('"') and content.endswith('"')) or \
               (content.startswith("'") and content.endswith("'")):
                content = content[1:-1]
            # Escape any quotes inside the content itself
            content = content.replace('"', '\\"')
            # Rebuild in the strict format with no spaces around quotes
            return f'{arrow}|"{content}"|'

        # Apply this fix to the line
        line = link_label_re.sub(_fix_link_label, line)

        # 4) Quote labels inside [] (leave empty as [])
        def _quote_label(m):
            node = m.group(1)
            content = m.group(2).strip()
            if content == "":
                return f"{node}[]"
            if (content.startswith('"') and content.endswith('"')) or (content.startswith("'") and content.endswith("'")):
                return m.group(0)
            inner = content.replace('"', '\\"')
            return f'{node}["{inner}"]'
        new_line = label_re.sub(_quote_label, line)
        out_lines.append(new_line)
        i += 1

    # Second pass: merge broken edges across lines
    merged = []
    i = 0
    n = len(out_lines)
    arrow_tokens = ['-->', '-.->', '==>', '---', '*-->']

    while i < n:
        line = out_lines[i]
        stripped = line.rstrip()
        arrow_pos = None
        arrow_tok = None
        for tok in arrow_tokens:
            pos = stripped.find(tok)
            if pos != -1:
                arrow_pos = pos
                arrow_tok = tok
                break

        if arrow_pos is None:
            merged.append(line)
            i += 1
            continue

        tail = stripped[arrow_pos + len(arrow_tok):].strip()
        if tail == "":
            j = i + 1
            while j < n and out_lines[j].strip() == "":
                j += 1
            if j < n:
                target = out_lines[j].strip()
                merged.append(f"{stripped} {target}")
                i = j + 1
                continue
            else:
                merged.append(line)
                i += 1
                continue

        pipe_count = tail.count('|')
        if pipe_count >= 2:
            last_pipe = tail.rfind('|')
            after = tail[last_pipe + 1:].strip()
            if after:
                merged.append(line)
                i += 1
                continue
            else:
                j = i + 1
                while j < n and out_lines[j].strip() == "":
                    j += 1
                if j < n:
                    target = out_lines[j].strip()
                    merged.append(f"{stripped} {target}")
                    i = j + 1
                    continue
                else:
                    merged.append(line)
                    i += 1
                    continue
        if pipe_count == 1:
            j = i + 1
            while j < n and out_lines[j].strip() == "":
                j += 1
            if j < n:
                target = out_lines[j].strip()
                merged.append(f"{stripped}| {target}")
                i = j + 1
                continue
            else:
                merged.append(line)
                i += 1
                continue

        merged.append(line)
        i += 1

    return "\n".join([line.replace("'", '"').replace('[""]', '[ ]') for line in merged])

def normalize_name(name: str) -> str:
    """
    Normalizes a name for use as a Mermaid node ID: remove diacritics, replace spaces with underscores.
    """
    # Normalize to decompose diacritics
    nfkd = unicodedata.normalize('NFD', name)
    # Remove diacritic marks
    without_diac = ''.join(c for c in nfkd if unicodedata.category(c) != 'Mn')
    # Transliterate specific Vietnamese characters
    without_diac = without_diac.replace('Đ', 'D').replace('đ', 'd')
    # No lowercase
    # Replace spaces and other whitespace with underscores
    norm = re.sub(r'\s+', '_', without_diac)
    # Replace any remaining non-alnum with _
    norm = re.sub(r'[^a-zA-Z0-9_]', '_', norm)
    # Remove leading/trailing _ if any
    norm = norm.strip('_')
    return norm

def extract_inner(rest: str, inner_start: int) -> tuple[int, int]:
    """
    Extracts the content inside matching parentheses starting at inner_start.
    Returns (start_inner, end_inner) for slicing rest[start_inner:end_inner].
    """
    count = 1
    pos = inner_start + 1
    while pos < len(rest) and count > 0:
        if rest[pos] == '(':
            count += 1
        elif rest[pos] == ')':
            count -= 1
        pos += 1
    end_inner = pos - 1  # Exclusive end for slice (points to closing ')')
    return inner_start + 1, end_inner

def convert_sketch_to_mermaid(sketch_str: str, show_directions: bool = True, add_curve_config: bool = False) -> str:
    """
    Converts a sketch string (new syntax) to Mermaid diagram code (old syntax).
    
    Args:
        sketch_str (str): The sketch content as a multiline string.
        show_directions (bool): Whether to include direction symbols like (+), (-), (~) in edge labels. Defaults to True.
        add_curve_config (bool): Whether to add the flowchart curve configuration at the start. Defaults to False.
    
    Returns:
        str: The generated Mermaid code as a string.
    """
    def parse_sketch(sketch_str):
        lines = sketch_str.strip().split('\n')
        model_type = None
        variables = {}
        assumptions = []
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith('ModelType:'):
                parsed_model_type = line.split(':', 1)[1].strip()
                model_type = parsed_model_type if parsed_model_type in {
                    'Simple', 'Extended', 'Comprehensive'
                } else None
            elif line.startswith('Variables:'):
                i += 1
                while i < len(lines) and not lines[i].strip().startswith('Assumptions:'):
                    vline = lines[i].strip()
                    if vline:
                        # Updated regex to handle names with spaces, commas, parens: typ: name ("label")
                        match = re.match(r'^(\w+):\s*(.+?)\s*\(\s*"([^"]+)"\s*\)$', vline)
                        if match:
                            typ, raw_name, label = match.groups()
                            name = raw_name.strip()
                            variables[name] = {'type': typ, 'label': label}
                        else:
                            # Improved fallback
                            parts = vline.split(':', 1)
                            if len(parts) == 2:
                                typ = parts[0].strip()
                                name_label = parts[1].strip()
                                # Find the quoted label at the end
                                match_label = re.search(r'\(\s*"([^"]+)"\s*\)$', name_label)
                                if match_label:
                                    label = match_label.group(1)
                                    name = name_label[:match_label.start()].strip()
                                else:
                                    name = name_label
                                    label = name
                                variables[name] = {'type': typ, 'label': label}
                    i += 1
                continue
            elif line.startswith('Assumptions:'):
                i += 1
                while i < len(lines):
                    aline = lines[i].strip()
                    if aline:
                        assumptions.append(aline)
                    i += 1
                break
            i += 1
        
        # Create ID map for normalization
        id_map = {name: normalize_name(name) for name in variables}
        
        # Parse assumptions
        primary_paths = {}
        c_paths = []  # (c_num, ctrl_original, to_original, dir_)
        mod_info = {}  # mod_num: (mod_name_original, list of targets)
        mediations = {}  # m_num: {'chain': [nodes_original], 'hypo_refs': [h1,h2,h3]}
        
        var_names = list(variables.keys())
        
        for line in assumptions:
            line = line.strip()
            if line.startswith('H') and ':' in line:
                # Handle both primary H# and H#(moderate)
                parts = line.split(':', 1)
                if len(parts) == 2:
                    h_num_raw = parts[0].strip()
                    rest = parts[1].strip()
                    if '(moderate)' in h_num_raw:
                        # Moderator parsing: H#(moderate): (MOD -> H#; H#)
                        h_mod_num = h_num_raw.replace('(moderate)', '').strip()
                        if '(' in rest:
                            inner_start = rest.find('(')
                            start_inner, end_inner = extract_inner(rest, inner_start)
                            inner = rest[start_inner:end_inner].strip()
                            inner = inner.replace('→', '->')  # Normalize arrow
                            if '->' in inner:
                                parts_inner = inner.split('->', 1)
                                modn = parts_inner[0].strip()
                                tgtstr = parts_inner[1].strip()
                                # FIX: Split by semicolon, then extract only H# from each part
                                targets = [t.split('->')[-1].strip() if '->' in t else t.strip() 
                                           for t in tgtstr.split(';')]
                                mod_info[h_mod_num] = (modn, targets)
                    else:
                        # Primary path parsing: H#: (From, To) [dir] [ theory:Theory ]
                        if '(' in rest:
                            inner_start = rest.find('(')
                            start_inner, end_inner = extract_inner(rest, inner_start)
                            inner = rest[start_inner:end_inner].strip()
                            # Improved parsing using var names with flexible spaces
                            found = False
                            for fr_candidate in sorted(var_names, key=len, reverse=True):
                                fr_pat = re.escape(fr_candidate) + r'\s*,\s*'
                                match = re.match(fr_pat, inner)
                                if match:
                                    to_candidate = inner[match.end():].strip()
                                    if to_candidate in var_names:
                                        fr = fr_candidate
                                        to = to_candidate
                                        found = True
                                        break
                            if not found:
                                continue
                            # Find after: from end_inner to end (skip the closing ')')
                            after = rest[end_inner + 1:].strip()
                            dir_ = None
                            theory = None
                            aparts = re.findall(r'\[([^\]]+)\]', after)
                            for ap in aparts:
                                ap = ap.strip()
                                if ap in ['+', '-', '~']:
                                    dir_ = ap
                                elif ap.startswith(' theory:'):
                                    theory = ap.split(':', 1)[1].strip()
                            primary_paths[h_num_raw] = {'from': fr, 'to': to, 'dir': dir_, 'theory': theory}
            elif line.startswith('C') and ':' in line:
                # C#: (CTRL -> To) [dir]
                parts = line.split(':', 1)
                if len(parts) == 2:
                    c_num = parts[0].strip()
                    rest = parts[1].strip()
                    if '(' in rest:
                        inner_start = rest.find('(')
                        start_inner, end_inner = extract_inner(rest, inner_start)
                        inner = rest[start_inner:end_inner].strip()
                        inner = inner.replace('→', '->')  # Normalize arrow
                        # Improved parsing
                        found = False
                        ctrl_candidates = [name for name, info in variables.items() if info['type'] == 'CTRL']
                        if not ctrl_candidates:
                            ctrl_candidates = var_names
                        sep_pat = r'\s*->\s*' if '->' in inner else r'\s*,\s*'
                        for ctrl_candidate in sorted(ctrl_candidates, key=len, reverse=True):
                            ctrl_pat = re.escape(ctrl_candidate) + sep_pat
                            match = re.match(ctrl_pat, inner)
                            if match:
                                to_candidate = inner[match.end():].strip()
                                if to_candidate in var_names:
                                    ctrl = ctrl_candidate
                                    to = to_candidate
                                    found = True
                                    break
                        if not found:
                            continue
                        after = rest[end_inner + 1:].strip()
                        dir_ = None
                        aparts = re.findall(r'\[([^\]]+)\]', after)
                        for ap in aparts:
                            ap = ap.strip()
                            if ap in ['+', '-', '~']:
                                dir_ = ap
                        c_paths.append((c_num, ctrl, to, dir_))
            elif line.startswith('M') and ':' in line:
                # M#: (Chain: From -> MED -> To) [HypoRefs] [method: ...]
                parts = line.split(':', 1)
                if len(parts) == 2:
                    m_num = parts[0].strip()
                    rest = parts[1].strip()
                    chain = []
                    hypo_refs = []
                    if '(' in rest:
                        inner_start = rest.find('(')
                        start_inner, end_inner = extract_inner(rest, inner_start)
                        chain_str = rest[start_inner:end_inner].strip()
                        chain_str = chain_str.replace('→', '->')  # Normalize arrow
                        # No space removal
                        chain = [node.strip() for node in chain_str.split('->')]
                        after = rest[end_inner + 1:].strip()
                        # Extract hypo_refs from [H1; H2; H3]
                        hp_parts = re.findall(r'\[([^\]]+)\]', after)
                        for hp in hp_parts:
                            hp = hp.strip()
                            if ';' in hp:
                                refs = [r.strip() for r in hp.split(';')]
                                hypo_refs.extend(refs)
                            else:
                                hypo_refs.append(hp)
                        # Filter to valid H#
                        hypo_refs = [r for r in hypo_refs if r.startswith('H')]
                    mediations[m_num] = {'chain': chain, 'hypo_refs': hypo_refs}
        
        # Normalize paths to use IDs
        for h_num, p in primary_paths.items():
            p['from_id'] = id_map.get(p['from'], normalize_name(p['from']))
            p['to_id'] = id_map.get(p['to'], normalize_name(p['to']))
        
        # Normalize c_paths to use IDs
        c_paths_normalized = []
        for c_num, ctrl, to, dir_ in c_paths:
            ctrl_id = id_map.get(ctrl, normalize_name(ctrl))
            to_id = id_map.get(to, normalize_name(to))
            c_paths_normalized.append((c_num, ctrl_id, to_id, dir_))
        c_paths = c_paths_normalized
        
        # Normalize mod_info to use ID for mod_name
        mod_info_normalized = {}
        for mod_num, (modn, targets) in mod_info.items():
            modn_id = id_map.get(modn, normalize_name(modn))
            mod_info_normalized[mod_num] = (modn_id, targets)
        mod_info = mod_info_normalized
        
        return model_type, variables, primary_paths, c_paths, mod_info, mediations, id_map
    
    def build_mermaid(model_type, variables, primary_paths, c_paths, mod_info, mediations, id_map, show_directions, add_curve_config):
        # Map H to list of M's
        h_to_ms = defaultdict(list)
        for m_num, info in mediations.items():
            for h in info['hypo_refs']:
                h_to_ms[h].append(m_num)
        
        # Collect modded Hs
        modded_hs = set()
        for _, (_, targets) in mod_info.items():
            modded_hs.update(targets)
        
        # Node declarations for main (IV, MED, DV)
        main_decls = []
        class_groups = {
            'independent': [],
            'mediator': [],
            'dependent': [],
            'moderator': [],
            'secondary': [],
            'invisible': []
        }
        
        for name_original, info in variables.items():
            id_ = id_map[name_original]
            typ = info['type']
            label = info['label']
            if typ.startswith('MED'):
                decl = f'{id_}(["{label}"])'
            else:
                decl = f'{id_}["{label}"]'
            in_main = typ == 'IV' or typ.startswith('MED') or typ.startswith('DV')
            if in_main:
                main_decls.append(decl)
            # Class group for main types (using IDs)
            if typ == 'IV':
                class_groups['independent'].append(id_)
            elif typ.startswith('MED'):
                class_groups['mediator'].append(id_)
            elif typ.startswith('DV'):
                class_groups['dependent'].append(id_)
        
        # Invisible nodes inserted near their 'from' nodes
        modded_hs_list = sorted(list(modded_hs), key=lambda x: int(x[1:]))
        for h in modded_hs_list:
            fr_id = primary_paths[h]['from_id']
            inv_name = f'path{h}'
            decl = f'{inv_name}[ ]:::invisible'
            try:
                idx = next(i for i, d in enumerate(main_decls) if d.startswith(f'{fr_id}["') or d.startswith(f'{fr_id}(['))
                main_decls.insert(idx + 1, decl)
                class_groups['invisible'].append(inv_name)
            except StopIteration:
                # Fallback: append at end
                main_decls.append(decl)
                class_groups['invisible'].append(inv_name)
        
        # Outside nodes (MOD, CTRL)
        outside_decls = []
        for name_original, info in variables.items():
            typ = info['type']
            if typ.startswith('MOD') or typ == 'CTRL':
                id_ = id_map[name_original]
                label = info['label']
                decl = f'{id_}["{label}"]'
                outside_decls.append(decl)
                if typ.startswith('MOD'):
                    class_groups['moderator'].append(id_)
                elif typ == 'CTRL':
                    class_groups['secondary'].append(id_)
        
        # Edges
        edges = []
        
        # Primary paths
        for h_num, p in primary_paths.items():
            fr_id = p['from_id']
            to_id = p['to_id']
            dir_ = p['dir'] or '+'
            theory = p.get('theory', '')
            h_label_num = h_num.lstrip('H')  # Strip 'H' to avoid duplicate
            label = f"H{h_label_num}"
            # Add M only if this H is part of mediation and points to a mediator
            if h_num in h_to_ms and h_to_ms[h_num] and p['to'] in variables and variables[p['to']]['type'].startswith('MED'):
                ms = [m.lstrip('M') for m in h_to_ms[h_num]]  # e.g., ['1']
                m_str = ', '.join(ms)
                label += f"<br/>M{m_str}"
            if dir_ != '~' and show_directions:
                label += f" ({dir_})"
            if theory:
                label += f": {theory}"
            if h_num in modded_hs:
                inv = f'path{h_num}'
                edges.append(f'{fr_id} --> {inv}')
                edges.append(f'{inv} -->|"{label}"| {to_id}')
            else:
                arrow = '<-->' if dir_ == '~' else '-->'
                edges.append(f'{fr_id} {arrow}|"{label}"| {to_id}')
        
        # C paths
        for c_num, ctrl_id, to_id, dir_ in c_paths:
            c_label_num = c_num.lstrip('C')  # Strip 'C' to avoid duplicate
            label = f"C{c_label_num}"
            dirc = dir_ or '+'
            if dirc != '~' and show_directions:
                label += f" ({dirc})"
            arrow = '<-->' if dirc == '~' else '-->'
            edges.append(f'{ctrl_id} {arrow}|"{label}"| {to_id}')
        
        # MOD edges
        for mod_num, (mod_id, targets) in mod_info.items():
            mod_label_num = mod_num.lstrip('H')  # Strip 'H' to avoid duplicate (e.g., '5' for H5)
            for h in targets:
                h_num = h.lstrip('H')  # '3' for H3
                inv = f'path{h}'
                # mlabel = f"H{mod_label_num}(mod): moderates H{h_num}"
                mlabel = f"H{mod_label_num}"
                edges.append(f'{mod_id} -.->|"{mlabel}"| {inv}')
        
        # Build Mermaid
        mermaid_lines = ['graph LR']
        if add_curve_config:
            mermaid_lines.insert(0, '%%{init: {"flowchart": {"curve": "linear"}}}%%')
        
        # Subgraph main
        if main_decls:
            mermaid_lines.append('    subgraph main [" "]') # Main Path
            mermaid_lines.append('        direction LR')
            for decl in main_decls:
                mermaid_lines.append(f'        {decl}')
            mermaid_lines.append('    end')
        
        # Edges
        for edge in edges:
            mermaid_lines.append(f'    {edge}')
        
        # Outside decls (placed after edges to influence layout positioning)
        for decl in outside_decls:
            mermaid_lines.append(f'    {decl}')
        
        # Class defs
        class_defs = [
            'classDef independent fill:lightblue,stroke:darkblue,stroke-width:2px',
            'classDef mediator fill:lightyellow,stroke:orange,stroke-width:3px',
            'classDef dependent fill:lavender,stroke:purple,stroke-width:2px',
            'classDef moderator fill:lightsalmon,stroke:orangered,stroke-width:2px',
            'classDef secondary fill:lightgreen,stroke:darkgreen,stroke-width:1px',
            'classDef invisible height:0,width:0'
        ]
        for cdef in class_defs:
            mermaid_lines.append(f'    {cdef}')
        
        # Class applications (using IDs, no quotes needed)
        for cls_type, names in class_groups.items():
            if names:
                name_str = ','.join(names)
                mermaid_lines.append(f'    class {name_str} {cls_type}')

        # Add this line to make subgraph background transparent with no border
        mermaid_lines.append('    style main fill:none,stroke:none')

        return '\n'.join(mermaid_lines)
    
    logger.info(f"Sketch:\n{sketch_str}")
    parsed = parse_sketch(sketch_str)
    model_type, variables, primary_paths, c_paths, mod_info, mediations, id_map = parsed
    return build_mermaid(model_type, variables, primary_paths, c_paths, mod_info, mediations, id_map, show_directions, add_curve_config)

def process_text_with_sketch(text: str) -> str:
    """
    Processes input text to detect fenced code blocks (marked as ```sketch
    checks if the content matches a sketch structure, extracts it if it does, converts it to Mermaid code,
    and replaces the original block with a ```mermaid ... ``` block. Non-sketch code blocks are left unchanged.
    
    Args:
        text (str): The input text containing one or more fenced code blocks.
    
    Returns:
        str: The processed text with matching sketch blocks replaced by Mermaid blocks.
    """
    if isinstance(text, list):
        text_parts = []
        for part in text:
            if isinstance(part, str):
                text_parts.append(part)
            elif isinstance(part, dict) and "text" in part:
                text_parts.append(str(part["text"]))
            else:
                text_parts.append(str(part))
        text = "".join(text_parts)
    elif not isinstance(text, str):
        text = str(text) if text is not None else ""

    def is_sketch_content(content: str) -> bool:
        """Quick check if content looks like a sketch (has key sections)."""
        lines = [line.strip() for line in content.split('\n') if line.strip()]
        has_modeltype = any(re.match(r'^model\s*type\s*:', line, re.IGNORECASE) for line in lines)
        has_variables = any(line.startswith('Variables:') for line in lines)
        has_assumptions = any(line.startswith(('Assumptions:', 'Hypothesises:')) for line in lines)
        return has_modeltype and has_variables and has_assumptions

    def replace_match(match):
        full_block = match.group(0)
        lang = match.group(1) if match.group(1) else ''  # Optional language specifier (e.g., 'sketch', 'python')
        content = match.group(2).strip()
        
        if is_sketch_content(content):
            try:
                # Call your real converter here
                mermaid_code = convert_sketch_to_mermaid(content, show_directions=False, add_curve_config=True)
                return f'```mermaid\n{mermaid_code}\n```'
            except Exception as e:
                logger.info(f"Error converting sketch to Mermaid: {str(e)}")
                logger.info(f"Traceback: {traceback.format_exc()}")
                # Fallback error Mermaid diagram
                error_mermaid = '''
graph LR
    ErrorNode["Error in sketch syntax or drawing."]
    classDef error fill:#ffebee,stroke:#f44336,stroke-width:2px
    class ErrorNode error
'''
                return f'```mermaid\n{error_mermaid.strip()}\n```'
        else:
            # Not a sketch; return original unchanged
            return full_block
    
    # Regex to match any ```[lang]\ncontent\n``` (multiline, optional lang)
    pattern = r'```(?:(\w+))?\s*\n(.*?)\n```'
    processed_text = re.sub(pattern, replace_match, text, flags=re.DOTALL)
    return processed_text

def title_matching(query: str, titles: list[str], cut_off_score=85) -> str:
    titles_set = set(titles)
    if query in titles_set:
        return query
    else:
        best_match = rapidfuzz.process.extractOne(query, titles, scorer=rapidfuzz.fuzz.WRatio)
        if best_match and best_match[1] >= cut_off_score:
            return best_match[0]
        else:
            return "Original design"
        
# File pattern to description mapping
FILE_PATTERN_DESCRIPTIONS = {
    # Chart Analysis (overview_chart.py)
    "{col}_distribution_pie.png": "Pie chart showing the distribution of categories for nominal variable '{col}'",
    "{latent_code}_indicators_bar.png": "Grouped bar chart showing indicator response distributions for latent variable '{latent_code}'",
    "{col}_distribution_hist.png": "Histogram showing the distribution of continuous variable '{col}'",

    # Clustering Analysis (cluster.py)
    "elbow_plot_wss.png": "Elbow plot for K-means clustering showing Within-Cluster Sum of Squares (WSS) for different k values",
    "dendrogram.png": "Dendrogram for hierarchical clustering showing cluster relationships",
    "elbow_plot_distances.png": "Elbow plot showing linkage distances for hierarchical clustering",
    "elbow_plot_bic.png": "Elbow plot for Gaussian Mixture Model showing Bayesian Information Criterion (BIC) for different k values",
    "cluster_characteristics.csv": "Table of cluster characteristics showing mean values and counts for each cluster",
    "scatter_plot.png": "2D scatter plot of clusters using PCA dimensionality reduction",

    # Regression Analysis (regression.py)
    "residual_plot.png": "Scatter plot of residuals vs predicted values for regression diagnostics",
    "qq_plot.png": "Q-Q plot for testing normality of residuals",
    "residual_histogram.png": "Histogram showing the distribution of regression residuals",
    "actual_vs_predicted.png": "Scatter plot comparing actual vs predicted values with perfect prediction line",
    "coefficients_table.csv": "Table of regression coefficients with standard errors, t-statistics, and p-values",
    "model_summary.csv": "Summary statistics for the regression model (R², Adjusted R², RMSE, MAE, Durbin-Watson)",
    "f_test_table.csv": "ANOVA table showing F-test results for overall model significance",
    "vif_table.csv": "Variance Inflation Factor (VIF) values for detecting multicollinearity among predictors",

    # Logistic Regression (regression.py)
    "classification_report.csv": "Classification performance metrics (precision, recall, F1-score) for each class",
    "roc_curve.png": "Receiver Operating Characteristic (ROC) curve showing model's discriminative ability",
    "confusion_matrix.png": "Heatmap of confusion matrix showing predicted vs actual classifications",
    "calibration_plot.png": "Calibration plot (reliability curve) showing predicted probability vs observed frequency",

    # Reliability Analysis (efa.py: single_factor_scale_reliability_testing, run_reliability_analysis)
    # NOTE: reliability_analysis_{timestamp}.csv (CFA) and reliability_validity.csv (PLS-SEM)
    # must be tried BEFORE the generic reliability_{factor_key}.csv pattern
    "reliability_analysis_{timestamp}.csv": "Construct reliability statistics (Composite Reliability, AVE) from CFA",
    "reliability_validity.csv": "Construct reliability and validity statistics (Cronbach's Alpha, Composite Reliability, AVE)",
    "reliability_{factor_key}.csv": "Cronbach's Alpha reliability statistics for scale/factor '{factor_key}'",
    "item_total_{factor_key}.csv": "Item-total statistics for factor '{factor_key}' (corrected item-total correlation, Cronbach's Alpha if item deleted)",
    "descriptive_statistics_all_variables.csv": "Descriptive statistics table (mean, SD, min, max) for all variables",

    # EFA (efa.py: run_efa_analysis)
    "kmo_bartlett_results.csv": "Kaiser-Meyer-Olkin (KMO) and Bartlett's test results for sampling adequacy",
    "eigenvalues.csv": "Eigenvalues table for determining number of factors to retain",
    "scree_plot.png": "Scree plot visualizing eigenvalues for factor extraction decision",
    "rotated_component_matrix.csv": "Rotated factor loadings matrix showing variable-factor relationships",
    "full_variance_table.csv": "Total variance explained table showing percentage of variance explained by each factor",

    # ANOVA & T-Test (efa.py: run_anova_ttest_analysis)
    "desc_stats_{demo_col}.csv": "Descriptive statistics grouped by demographic variable '{demo_col}'",
    "ttest_{demo_col}.csv": "Independent samples t-test results comparing groups of '{demo_col}'",
    "anova_{demo_col}.csv": "One-way ANOVA results comparing groups of '{demo_col}'",
    "summary_anova_ttest.csv": "Summary table of all ANOVA and t-test results across demographic variables",

    # CFA (efa.py: run_cfa_analysis - filenames carry a 2-digit run id)
    "correlation_matrix_{timestamp}.csv": "Correlation matrix of observed variables for CFA",
    "kmo_bartlett_results_{timestamp}.csv": "Kaiser-Meyer-Olkin (KMO) and Bartlett's test results for CFA sampling adequacy",
    "model_fit_indices_{timestamp}.csv": "Model fit indices (CFI, TLI, RMSEA, SRMR, Chi-square) for CFA model",
    "modification_indices_{timestamp}.csv": "Modification indices suggesting potential CFA model improvements",
    "factor_loadings_{timestamp}.csv": "Standardized factor loadings from CFA model",
    "cfa_path_diagram_{timestamp}.png": "Visual diagram of CFA measurement model structure",
    "cfa_path_diagram.png": "Visual diagram of CFA measurement model structure",

    # CB-SEM (efa.py: run_cb_sem_analysis)
    "sem_path_diagram.png": "Path diagram showing structural relationships in the SEM model",
    "intra_construct_correlation_matrix.csv": "Intra-construct correlation matrix for SEM measurement model assessment",
    "sem_parameter_estimates.csv": "Table of estimated SEM parameters (loadings, path coefficients) with significance tests",
    "modification_indices.csv": "Modification indices for SEM model improvement suggestions",
    "loadings.csv": "Outer model loadings showing indicator-construct relationships",
    "weights.csv": "Outer model weights showing indicator contributions to constructs",
    "htmt_matrix.csv": "Heterotrait-Monotrait (HTMT) ratio matrix for discriminant validity assessment",
    "r_squared.csv": "R-squared values showing explained variance for endogenous constructs",
    "structural_path_coefficients.csv": "Inner model path coefficients showing structural relationships",

    # PLS-SEM (efa.py: run_pls_sem_analysis)
    "correlation_matrix.csv": "Correlation matrix of observed variables",
    "path_coefficients.csv": "Path coefficients showing structural relationships between constructs",
    "model_fit_indices.csv": "Model fit indices for the structural equation model",
    "effect_sizes.csv": "Effect sizes (f²) for structural model relationships",
    "bootstrap_path_results.csv": "Bootstrap results for significance testing of path coefficients",

    # GSCA (efa.py: run_gsca_analysis)
    "gsca_results_summary.txt": "Summary report of GSCA (Generalized Structured Component Analysis) results",
    "model_specification.txt": "Model specification detailing constructs, indicators, and structural paths",
    "indicator_weights.csv": "Indicator weights for GSCA constructs",
    "r_squared_values.csv": "R-squared values for endogenous constructs in GSCA model",

    # Panel Data Analysis (panel_model_selection.py, advanced_panel_analysis.py)
    "model_comparison.csv": "Comparison table of different panel models (Pooled OLS, Fixed Effects, Random Effects)",
    "final_model_summary.txt": "Comprehensive summary report of the final selected panel model",
    "diagnostic_plots.png": "Panel of diagnostic plots (residuals, Q-Q plot, histogram, VIF)",
    "model_selection_plots.png": "Visual comparison of model selection criteria",
    "panel_analysis_report.txt": "Detailed panel data analysis report with model specifications",
    "coefficient_results.csv": "Table of estimated coefficients for the selected panel data model",
    "iv_diagnostics.txt": "Instrumental Variables (IV) regression diagnostics including weak instrument tests",
    "gmm_diagnostics.txt": "Generalized Method of Moments (GMM) diagnostics (Hansen test, AR tests)",
    "summary_statistics.txt": "Summary statistics for panel data including entity and time information",
    "advanced_panel_report_{analysis_type}.txt": "Comprehensive report for advanced panel analysis ({analysis_type})",
    "error_log.txt": "Error log documenting issues encountered during panel analysis",

    # Data Preprocessing (preprocessing.py)
    "preprocessing_summary_report.csv": "Summary report of preprocessing results for all variables",
    "preprocessing_{var_code}_diagnostics.png": "Diagnostic plots panel for variable '{var_code}' (time series, histogram, box plot, Q-Q plot)",

    # Stationarity Assessment (stationarity_assessment.py)
    "{var_code}_original_timeseries.png": "Time series plot of original (undifferenced) series for '{var_code}'",
    "{var_code}_original_acf.png": "ACF plot of original series for '{var_code}'",
    "{var_code}_original_pacf.png": "PACF plot of original series for '{var_code}'",
    "{var_code}_d{d}_D{D}_timeseries.png": "Time series plot after d={d} and D={D} differencing for '{var_code}'",
    "{var_code}_d{d}_D{D}_acf.png": "ACF plot after d={d} and D={D} differencing for '{var_code}'",
    "{var_code}_d{d}_D{D}_pacf.png": "PACF plot after d={d} and D={D} differencing for '{var_code}'",
    "stationarity_summary_report.csv": "Summary of stationarity tests and differencing recommendations for all variables",

    # Model Structure Identification (model_structure_identification.py)
    "{var_code}_stationarized_acf_pacf.png": "Combined ACF and PACF plots for stationarized series of variable '{var_code}'",
    "{var_code}_arima_ic_table.csv": "Information criteria (AIC, BIC, HQIC) table for ARIMA model selection for '{var_code}'",
    "{var_code}_exog_vif_table.csv": "VIF table for exogenous variables in model for '{var_code}'",
    "{var_code}_ets_ic_table.csv": "Information criteria comparison table for ETS model selection for '{var_code}'",
    "var_lag_selection_summary.txt": "VAR lag order selection summary with information criteria",
    "johansen_test_plot.png": "Johansen cointegration test results visualization",
    "granger_lag_selection_summary.txt": "Lag order selection summary for Granger causality tests",

    # Time Series Model Estimation & Diagnostics (model_estimation_and_diagnostics.py)
    # NOTE: the generic {model_prefix}_{clean_title}.csv pattern must stay LAST -
    # it matches almost any csv, so every specific pattern must be tried first.
    "{model_prefix}_model_summary.txt": "Complete model summary with parameter estimates and diagnostics for {model_prefix} model",
    "{model_prefix}_residuals_overview_{col}.png": "Residual diagnostics overview for variable '{col}' in {model_prefix} model",
    "{model_prefix}_residuals_overview.png": "Residual diagnostics overview for {model_prefix} model",
    "{model_prefix}_{col_name}_ljung_box_test.csv": "Ljung-Box test results for '{col_name}' residuals in {model_prefix} model",
    "{model_prefix}_model_stability_roots.png": "Stability roots plot for {model_prefix} VAR/VECM model",
    "{model_prefix}_impulse_response_functions.png": "Impulse Response Functions (IRF) showing dynamic effects between variables",
    "{model_prefix}_fevd.png": "Forecast Error Variance Decomposition (FEVD) plot",
    "{model_prefix}_granger_causality.csv": "Granger causality test results showing causal relationships between variables",
    "{model_prefix}_{clean_title}.csv": "Model output table '{clean_title}' for {model_prefix} model",
}

# Regexes for the {placeholder} variables inside FILE_PATTERN_DESCRIPTIONS keys.
# Longer/more specific placeholders are fine to list in any order - no placeholder
# is a substring of another once braces are included.
_FILE_PATTERN_VAR_REGEXES = {
    '{timestamp}': r'(?P<timestamp>\d+)',
    '{var_code}': r'(?P<var_code>.+?)',
    '{col}': r'(?P<col>.+?)',
    '{col_name}': r'(?P<col_name>.+?)',
    '{model_prefix}': r'(?P<model_prefix>[A-Za-z0-9]+)',
    '{latent_code}': r'(?P<latent_code>.+?)',
    '{clean_title}': r'(?P<clean_title>.+?)',
    '{analysis_type}': r'(?P<analysis_type>[A-Za-z_]+)',
    '{factor_key}': r'(?P<factor_key>.+?)',
    '{demo_col}': r'(?P<demo_col>.+?)',
    '{d}': r'(?P<d>\d+)',
    '{D}': r'(?P<D>\d+)',
}


def _compile_file_patterns() -> List[tuple]:
    """Precompile every pattern in FILE_PATTERN_DESCRIPTIONS once at import time."""
    compiled = []
    for pattern, description in FILE_PATTERN_DESCRIPTIONS.items():
        regex_pattern = pattern
        for var_placeholder, var_regex in _FILE_PATTERN_VAR_REGEXES.items():
            regex_pattern = regex_pattern.replace(var_placeholder, var_regex)
        compiled.append((re.compile('^' + regex_pattern + '$'), description))
    return compiled


_COMPILED_FILE_PATTERNS = _compile_file_patterns()


def get_file_description(filename: str, reflect: bool = False) -> Dict[str, str]:
    """
    Detect the file pattern and return its description with extracted variables.

    Args:
        filename: The actual filename (e.g., "VAR_model_summary.txt", "reliability_TR.csv"),
                  with or without a leading output directory (e.g., "42_efa_analysis/scree_plot.png")
        reflect: If True, adds "Reanalyze: " prefix to the description to indicate this is a reflection/reanalysis

    Returns:
        Dictionary containing:
        - 'description': Description of what the file contains (or "Unknown file pattern: {filename}" if no match)
        - 'execution_type': Type of execution ('normal' for initial analysis, 'reflection' for reanalysis/reflection)

        Always returns a dictionary with default values if no pattern matches.
    """

    # Extract just the filename without directory path
    filename_only = filename.split('/')[-1].split('\\')[-1]

    # Try each precompiled pattern in declaration order (specific before generic)
    for compiled_regex, description in _COMPILED_FILE_PATTERNS:
        match = compiled_regex.match(filename_only)
        if match:
            # Extract all captured variables
            variables = match.groupdict()

            # Fill in the description with actual values
            filled_description = description
            for var_name, var_value in variables.items():
                filled_description = filled_description.replace(f"'{{{var_name}}}'", f"'{var_value}'")
                filled_description = filled_description.replace(f"{{{var_name}}}", var_value)

            # Add "Reanalyze: " prefix if this is a reflection/reanalysis
            if reflect:
                filled_description = f"Reanalyze: {filled_description}"

            return {
                'description': filled_description,
                'execution_type': 'reflection' if reflect else 'normal',
                # 'pattern': pattern,
                # 'variables': variables,
                # 'file_type': _get_file_type(filename_only),
                # 'analysis_category': _get_analysis_category(pattern)
            }

    # No pattern matched - log and return default
    logger.info(f"No matching pattern found for filename: {filename_only}")

    default_description = f"Unknown file pattern: {filename_only}"
    if reflect:
        default_description = f"Reanalyze: {default_description}"

    return {
        'description': default_description,
        'execution_type': 'reflection' if reflect else 'normal',
        # 'pattern': None,
        # 'variables': {},
        # 'file_type': _get_file_type(filename_only),
        # 'analysis_category': 'Unknown'
    }


def _get_file_type(filename: str) -> str:
    """Determine the file type based on extension."""
    if filename.endswith('.png'):
        return 'visualization'
    elif filename.endswith('.csv'):
        return 'table'
    elif filename.endswith('.txt'):
        return 'report'
    else:
        return 'unknown'


def _get_analysis_category(pattern: str) -> str:
    """Categorize the file based on its pattern."""
    if 'residual' in pattern or 'qq_plot' in pattern or 'histogram' in pattern:
        return 'Model Diagnostics'
    elif 'reliability' in pattern or 'cronbach' in pattern:
        return 'Reliability Analysis'
    elif 'factor' in pattern or 'eigenvalue' in pattern or 'loading' in pattern:
        return 'Factor Analysis'
    elif 'cluster' in pattern or 'elbow' in pattern or 'dendrogram' in pattern:
        return 'Cluster Analysis'
    elif 'forecast' in pattern or 'irf' in pattern or 'fevd' in pattern:
        return 'Time Series Forecasting'
    elif 'stationarity' in pattern or 'stationarized' in pattern:
        return 'Stationarity Analysis'
    elif 'panel' in pattern or 'iv_' in pattern or 'gmm_' in pattern:
        return 'Panel Data Analysis'
    elif 'pls_' in pattern or 'sem_' in pattern or 'cfa' in pattern:
        return 'Structural Equation Modeling'
    elif 'coefficients' in pattern or 'model_summary' in pattern:
        return 'Regression Analysis'
    elif 'preprocessing' in pattern:
        return 'Data Preprocessing'
    elif 'distribution' in pattern or 'chart' in pattern:
        return 'Descriptive Statistics'
    else:
        return 'General Analysis'


async def get_uncite_survey(_id, llm, surveys, papers) -> tuple[list, int, int]:
    tasks = [_get_uncite_survey(_id, llm, survey, papers) for survey in surveys]
    results = await asyncio.gather(*tasks)
    total_in = 0
    total_out = 0
    for i, result in enumerate(results):
        surveys[i].source = result[0]
        total_in += result[1]
        total_out += result[2]
    return surveys, total_in, total_out


async def _get_uncite_survey(_id, llm, survey, papers) -> tuple[str, int, int]:
    if survey.source:
        return survey.source, 0, 0
    system_prompt = """You're a researcher speacialize in citing research paper for social science
    You will be given a Survey variable including variable name, type, and survey question, and a list of summarize of reference paper
    Your task is choosing the correct reference paper for the given survey variable
    Return the index order of the reference paper only
    For example:
    - Survey Variable:
    ```
    "variable_name": "Chuyển đổi số doanh nghiệp",
    "variable_code": "DTS",
    "variable_type": "Độc lập / Điều tiết",
    "measurement_type": "Tiềm ẩn",
    "questions": [
        {
        "indicator_name": "Ứng dụng công nghệ số",
        "question": "Doanh nghiệp của bạn ứng dụng công nghệ số (AI, blockchain, IoT, v.v.) vào hoạt động kinh doanh?",
        "question_code": "DTS1",
        "answer_content": "1. Hoàn toàn không đồng ý; 2. Không đồng ý; 3. Trung lập; 4. Đồng ý; 5. Hoàn toàn đồng ý",
        "scale_type": "Likert"
        },
        {
        "indicator_name": "Số hóa quy trình",
        "question": "Các quy trình vận hành trong doanh nghiệp đã được số hóa?",
        "question_code": "DTS2",
        "answer_content": "1. Hoàn toàn không đồng ý; 2. Không đồng ý; 3. Trung lập; 4. Đồng ý; 5. Hoàn toàn đồng ý",
        "scale_type": "Likert"
        },
        {
        "indicator_name": "Đầu tư chuyển đổi số",
        "question": "Doanh nghiệp của bạn đầu tư mạnh vào chuyển đổi số?",
        "question_code": "DTS3",
        "answer_content": "1. Hoàn toàn không đồng ý; 2. Không đồng ý; 3. Trung lập; 4. Đồng ý; 5. Hoàn toàn đồng ý",
        "scale_type": "Likert"
        },
        {
        "indicator_name": "Nhân sự số",
        "question": "Doanh nghiệp của bạn có đội ngũ chuyên trách về chuyển đổi số?",
        "question_code": "DTS4",
        "answer_content": "1. Hoàn toàn không đồng ý; 2. Không đồng ý; 3. Trung lập; 4. Đồng ý; 5. Hoàn toàn đồng ý",
        "scale_type": "Likert"
        }
    ],
    "source": ""
    ```
    - Reference papers:
    1. **Nghiên cứu thương mại điện tử trong bối cảnh chuyển đổi số: Trường hợp tại thành phố Đà Nẵng**
Mục tiêu của nghiên cứu này nhằm phân tích, kiểm định mô hình, xác các nhân tố tác động đến thương mại điện tử trên địa bàn thành phố Đà Nẵng
Thu thập dữ liệu bằng hình thức trực tuyến từ 252 mẫu tại cứu, cách chọn thuận tiện. 
Số được xử lý phần mềm SPSS 22. Nghiên sử dụng phương pháp tính và lượng, thống kê tả, thang đo độ tin cậy hệ số Cronbach's Alpha, tích khám phá (EFA), tương quan (pearson), hồi quy tính. 
Kết quả cho thấy có 5 chiều Nẵng, bao gồm: Công nghệ; Nhận hữu ích; Niềm tin; Ảnh hưởng xã hội; rủi ro. 
Từ đó đề xuất hàm ý chính sách để nhà hoạch định, quản doanh nghiệp tham khảo đưa chiến lược phát triển tạo lực thúc đẩy hoạt sản xuất, dịch vụ kinh mại.
    2. **Quy mô doanh nghiệp ảnh hưởng như thế nào đến lòng tin, thái độ và ý định mua hàng trực tuyến của khách hàng Việt Nam?**
Nghiên cứu này nhằm khám phá vai trò tiền đề của quy mô doanh nghiệp trực tuyến đối với lòng tin tuyến, thái độ và ý định mua hàng khách tại Việt Nam. 
Thông qua khảo sát 918 các thành phố lớn như Hà Nội, Đà Nẵng Hồ Chí Minh, nghiên đã sử dụng hình cấu trúc tính (PLS-SEM) để kiểm giả thuyết cứu. 
Kết quả chỉ ra rằng có tác động tích cực đến người tiêu dùng. Ngoài ra, dùng ảnh hưởng đáng kể trong thương mại điện tử. cũng một số hàm cho kinh gia tăng từ trang tử
    3. **Nghiên cứu về hành vi công dân của khách hàng trong môi trường thương mại điện tử: vai trò trung gian của sự cam kết mối quan hệ và niềm tin**
Hành vi công dân của khách hàng đóng vai trò quan trọng trong việc duy trì và phát triển mối hệ giữa doanh nghiệp. Tuy nhiên, những nghiên cứu về hành còn hạn chế. 
Dựa trên thuyết đáp ứng kích thích lý sự cam kết - niềm tin, này đánh giá các yếu tố ảnh hưởng tới người tiêu dùng môi trường thương mại điện tử thông qua trung gian tin. 
Kỹ thuật bình phương tối thiểu từng phần (PLS-SEM) được sử dụng để phân tích dữ liệu thu thập từ mẫu gồm 287 đã mua tử. 
Kết quả cho thấy đặc quyền cá nhân hóa cực kết. Đồng thời, chất lượng tin tính năng an toàn có Vai với cũng khẳng định này. gợi ra một số hàm ý nghiệp kinh nhằm thúc đẩy dùng.
    -> Return 1 (integer)
    """
    content = f"""- Given Survey Variable:
    {survey}
    - Reference papers:
    {[f"{i + 1}. **{paper["title"]}**\n{paper["key_points"]}" for i, paper in enumerate(papers)]}
    """
    _, success, source, tokens_in, tokens_out = await get_answer_with_schema(
        _id=_id,
        llm=llm,
        system_prompt=system_prompt,
        content=content,
        structure=Source
    )
    if not success:
        return papers[0]["title"], 0, 0
    if source.source_index < 1:
        source.source_index = 1
    elif source.source_index > len(papers):
        source.source_index = len(papers)
    return "[" + papers[source.source_index - 1]["title"] + "]", tokens_in, tokens_out

