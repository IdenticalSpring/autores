"""Event `parsed_value`: parse survey/variables thành `parsed_variables` độc lập.

CONTRACT (WriteSectionEvent `parsed_value`, topic write_section_request):

Request
    event_type        str   = "parsed_value"
    user_id           str   bắt buộc
    document_id       str   bắt buộc (echo lại trong response)
    session_id        str   bắt buộc
    model_id          str   bắt buộc
    language          str   mặc định "Vietnamese" — ngôn ngữ của name/statement đầu ra
    survey_questions  list[dict]  bắt buộc, không rỗng. Mỗi phần tử:
        variable_name, variable_code, variable_type (vai trò: Độc lập/Phụ thuộc/...),
        measurement_type (Latent/Observable hoặc Tiềm ẩn/Quan sát),
        questions (hoặc question) = nội dung câu hỏi, answer_content (tuỳ chọn)
    variables         list[dict]  tuỳ chọn, mặc định []

Response (write_section_response)
    event_type, user_id, document_id, session_id (echo)
    parsed_variables  list[dict]  schema Variable (name, code, statement, variable_type,
                                  role, parent_code, values, scale, unit)
    input_tokens, output_tokens, embed_tokens=0, web_search_call=0, embed_model
    error_code=null khi thành công; 603 khi thiếu/sai tham số; 639 khi lỗi xử lý.
"""

import logging

from data_analysis.src.modules.utils import manually_parse_survey_to_variables, parse_raw_variables
from exception_type import AIERROR
from get_llm_response import get_llm

logger = logging.getLogger(__name__)

DEFAULT_LANGUAGE = "Vietnamese"


def validate_parsed_value_input(survey_questions, variables) -> None:
    """Kiểm tra cấu trúc đầu vào. Ném ValueError (listener map sang 603)."""
    if not isinstance(survey_questions, list) or not survey_questions:
        raise ValueError("survey_questions must be a non-empty list")
    for idx, question in enumerate(survey_questions):
        if not isinstance(question, dict):
            raise ValueError(f"survey_questions[{idx}] must be a dictionary")
        if not (question.get("variable_code") or question.get("variable_name")):
            raise ValueError(f"survey_questions[{idx}] requires variable_code or variable_name")
        if not (question.get("questions") or question.get("question")):
            raise ValueError(f"survey_questions[{idx}] requires questions")
    if not isinstance(variables, list) or any(not isinstance(v, dict) for v in variables):
        raise ValueError("variables must be a list of dictionaries")


async def parse_values(
    _id: str,
    model_id: str,
    llm_key: str,
    max_tokens: int,
    language: str,
    variables: list[dict],
    survey_questions: list[dict],
) -> tuple[list[dict], int, int]:
    """Trả (parsed_variables, input_tokens, output_tokens). Ném AIERROR khi LLM thất bại."""
    pre_parsed = manually_parse_survey_to_variables(
        variables=variables, survey_questions=survey_questions
    )
    llm = get_llm(model_id, llm_key, max_tokens)
    error, success, parsed_variables, input_tokens, output_tokens = await parse_raw_variables(
        _id=_id,
        variables=variables,
        survey_questions=survey_questions,
        llm=llm,
        language=language,
        pre_parsed_variables=pre_parsed,
    )
    if not success:
        if isinstance(error, AIERROR):
            raise error
        raise AIERROR(500, f"parsed_value failed: {error}")
    logger.info(f"[{_id}] parsed_value: {len(parsed_variables)} variables")
    return parsed_variables, input_tokens, output_tokens
