"""Event `edit_report`: sửa nội dung một section/đoạn của báo cáo theo chỉ dẫn.

CONTRACT (WriteEvent `edit_report`, topic write_section_request):

Request
    event_type    str = "edit_report"
    user_id       str  bắt buộc
    document_id   str  bắt buộc (echo lại trong response)
    session_id    str  bắt buộc
    model_id      str  bắt buộc
    content       str  bắt buộc, không rỗng — nội dung gốc cần sửa (markdown)
    instruction   str  bắt buộc, không rỗng — yêu cầu chỉnh sửa của người dùng
    section_name  str  tuỳ chọn, mặc định "" — tên section (chỉ để làm ngữ cảnh, echo lại)
    context       str  tuỳ chọn, mặc định "" — văn bản lân cận để giữ mạch văn
    language      str  tuỳ chọn, mặc định "Vietnamese"

Response (write_section_response)
    event_type, user_id, document_id, session_id, section_name (echo)
    content        str  nội dung đã sửa (markdown, giữ nguyên ảnh/trích dẫn/định dạng gốc)
    input_tokens, output_tokens, embed_tokens=0, web_search_call=0, embed_model
    error_code=null khi thành công; 603 thiếu/sai tham số; 643 lỗi xử lý;
    mã LLM (401/403/429/5xx) khi nhà cung cấp lỗi.
"""

import logging

from pydantic import BaseModel, Field

from exception_type import AIERROR
from get_llm_response import get_answer_with_schema, get_llm

logger = logging.getLogger(__name__)

DEFAULT_LANGUAGE = "Vietnamese"

SYSTEM_PROMPT = (
    "You are an expert academic editor. Rewrite the given report content strictly following the "
    "user's instruction. Preserve markdown structure, headings, images, tables, formulas and "
    "citation formats unless the instruction explicitly asks to change them. Do not invent facts, "
    "references or data that are not in the original. Write in {language}. "
    "Return only the edited content."
)


class EditedContent(BaseModel):
    content: str = Field(..., description="The edited content in markdown.")


def validate_edit_report_input(content, instruction, section_name, context) -> None:
    """Ném ValueError (listener map sang 603)."""
    for name, value in (("content", content), ("instruction", instruction)):
        if not isinstance(value, str):
            raise ValueError(f"{name} must be a string")
        if not value.strip():
            raise ValueError(f"{name} cannot be empty")
    for name, value in (("section_name", section_name), ("context", context)):
        if not isinstance(value, str):
            raise ValueError(f"{name} must be a string")


def build_user_prompt(content: str, instruction: str, section_name: str, context: str) -> str:
    parts = []
    if section_name:
        parts.append(f"Section: {section_name}")
    if context:
        parts.append(f"Surrounding context (do not rewrite):\n{context}")
    parts.append(f"Instruction:\n{instruction}")
    parts.append(f"Original content:\n{content}")
    return "\n\n".join(parts)


async def edit_report(
    _id: str,
    model_id: str,
    llm_key: str,
    max_tokens: int,
    language: str,
    content: str,
    instruction: str,
    section_name: str = "",
    context: str = "",
) -> tuple[str, int, int]:
    """Trả (edited_content, input_tokens, output_tokens). Ném AIERROR khi thất bại."""
    llm = get_llm(model_id, llm_key, max_tokens)
    error, success, edited, input_tokens, output_tokens = await get_answer_with_schema(
        _id,
        llm,
        SYSTEM_PROMPT.format(language=language),
        build_user_prompt(content, instruction, section_name, context),
        EditedContent,
    )
    if not success:
        if isinstance(error, AIERROR):
            raise error
        raise AIERROR(500, f"edit_report failed: {error}")
    if not edited.content.strip():
        raise AIERROR(500, "edit_report returned empty content")
    logger.info(f"[{_id}] edit_report done ({input_tokens}/{output_tokens} tokens)")
    return edited.content, input_tokens, output_tokens
