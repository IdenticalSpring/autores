from pydantic import BaseModel, Field, ConfigDict
from enum import Enum


class SubHeading(BaseModel):
    detail_description: str = Field(None, description="Detail description of the current Subheading")
    subheading: str = Field(None, description="Subheading label")
    subheading_word_count: str = Field(None, description="Subheading word limit, for example: '100-200'")


class SubHeadingInfo(BaseModel):
    detail_description: str = Field(None, description="Detail description of the current subheading")
    subheading_word_count: str = Field(None, description="Subheading word limit, for example: '100-200'")


class Heading(BaseModel):
    heading: str = Field(None, description="Heading label")
    word_count: str = Field(None, description="Number of word")
    overview: str = Field(None, description="Overview of the overall section")
    subheadings: list[SubHeading] | None = Field(None, description="List of subheadings")

    def has_subheadings(self) -> bool:
        """Check if this heading has subheadings"""
        return self.subheadings is not None and len(self.subheadings) > 0


class Outline(BaseModel):
    title: str = Field(None, description="Title of the research work")
    outline: list[Heading]


class HeadingChat(BaseModel):
    heading: str = Field(None, description="Heading label")
    word_count: str = Field(None, description="Number of word")
    subheadings: list[str] | None = Field(None, description="List of subheading")


class OutlineChat(BaseModel):
    outline: list[HeadingChat]


class HeadingDescription(BaseModel):
    overview: str = Field(None, description="Overview of the overall section")


class SeminarHeading(BaseModel):
    heading: str = Field(None, description="Heading label")
    word_count: str = Field(None, description="Number of word")
    description: str = Field(None, description="Overview of the overall section")


class SeminarOutline(BaseModel):
    name: str = Field(None, description="Title of the research work")
    outline: list[SeminarHeading]


class RemainingRefs(BaseModel):
    remaining_refs: list[str] = Field(description="List of relevance research papers")


class RefsUsage(BaseModel):
    title: str = Field(description="The reference search paper's title")
    usage: str = Field(
        description="Detail description on how to use it to write the subsection"
    )
    key_points_used: list[int] = Field(
        description="List of index of key points used in order to write according to the usage"
    )


class SectionRefs(BaseModel):
    section_refs: list[str] = Field(
        description="The references research papers for every sections."
    )


class SubsectionRefs(BaseModel):
    subsection_refs: list[RefsUsage] = Field(
        description="The references research papers for every subsections."
    )


class UpdatedHeading(BaseModel):
    heading: str = Field(description="Heading label")
    word_count: str = Field(None, description="Number of word")
    subheadings: list[str] | None = Field(None, description="List of subheadings for heading")


class UpdatedSubheading(BaseModel):
    subheadings: list[str] = Field(description="List of subheadings for heading")


class UpdatedOutline(BaseModel):
    updated_outline: list[UpdatedHeading] = Field(None, description="List of the updated heading")


class ReorderOutline(BaseModel):
    """Kết quả sắp xếp lại heading/subheading.

    `reorder_outline` là **Optional** vì model (đặc biệt DeepSeek đi plain-JSON) có
    thể trả `{}` hoặc bỏ field. Caller phải coi `None` là "không có kết quả" để
    retry/fallback — TUYỆT ĐỐI không gọi `len()` trực tiếp (từng gây TypeError).
    """

    model_config = ConfigDict(extra="allow")

    reorder_outline: list[int] | None = Field(
        None,
        description="Danh sách thứ tự (index 1-based) cho từng heading/subheading",
    )


class UserRefsUsage(BaseModel):
    heading: str = Field(description="Heading of the section that use this reference")
    usage: str = Field(
        description="Brief description on how to use it to write the section"
    )


class UserPaper(BaseModel):
    title: str = Field(description="Title of the provided research papers")
    usage: list[int] = Field(description="List of heading numbering of the section that use this as reference")


class UserPaperV2(BaseModel):
    usage: list[int] = Field(description="List of heading numbering of the section that use this as reference")


class SubsectionAssignment(BaseModel):
    subsection_index: int = Field(..., description="The numeric index of the subsection (1-based)")
    paper_indices: list[int] = Field(..., description="List of numeric indices of papers assigned to this subsection")


class SubsectionMapping(BaseModel):
    assignments: list[SubsectionAssignment]


class OutlinePercent(BaseModel):
    headings_percent: list[float] = Field(description="List of headings percentage")


class SearchQuery(BaseModel):
    search_query: str


class ReferenceUsageType(str, Enum):
    BACKGROUND_CONTEXT = "Cung cấp bối cảnh và nhận thức tình hình hiện tại"
    LITERATURE_GAP = "Xác định khoảng trống nghiên cứu hoặc phê bình các công trình trước"
    MODEL_INHERITANCE = "Kế thừa hoặc điều chỉnh mô hình nghiên cứu/lý thuyết"
    HYPOTHESIS_INHERITANCE = "Kế thừa, hỗ trợ hoặc phát triển giả thuyết nghiên cứu"
    METHODOLOGY_JUSTIFICATION = "Biện minh cho việc lựa chọn phương pháp luận"
    DATA_SOURCE = "Sử dụng làm nguồn dữ liệu hoặc minh họa cho bộ dữ liệu"
    RESULT_COMPARISON = "So sánh, đối chiếu hoặc củng cố kết quả nghiên cứu"
    PRACTICAL_EXAMPLE = "Minh họa bằng một ví dụ hoặc case study thực tiễn"


class ReferenceUsagePlan(BaseModel):
    paper_index: int = Field(description="The index of the research paper this plan refers to.")
    usage_type: str = Field(description="The primary category of how this reference is being used.")
    usage_description: str = Field(description="A detailed explanation of the usage, elaborating on the chosen type.")
    key_points_used: list[int] = Field(description="A list of the integer indices of the key points (1-18) relevant to the planned usage.")


class BatchReferenceUsagePlan(BaseModel):
    usage_plans: list[ReferenceUsagePlan] = Field(description="A list of usage plans, one for each research paper provided in the prompt.")


class IntegrationParagraph(BaseModel):
    integration_paragraph: str = Field(description="A ready-to-use academic paragraph integrating the reference paper into the subsection.")