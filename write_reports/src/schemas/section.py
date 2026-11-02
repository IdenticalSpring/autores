from pydantic import BaseModel, Field
from typing import Optional

class RefsUsage(BaseModel):
    title: str = Field(description="The reference search paper's title")
    usage: str = Field(
        description="Detail description on how to use it to write the subsection"
    )
    ref_chunk: str = Field(description="Chunk id for reference text")
    summary: str = Field(description="Key points of the research paper")


class SubSectionDescription(BaseModel):
    detail_description: str = Field(
        description="Brief overview of the main topics and concepts to be covered in this subsection.",
    )
    subheading: str = Field(
        description="Name for this subsection of the report.",
    )
    subheading_word_count: str = Field(
        description="Limit word range"
    )
    refs: list[RefsUsage] = Field(default_factory=list)


class SectionDescription(BaseModel):
    heading: str = Field(
        description="Name for this section of the report.",
    )
    overview: str = Field(
        description="Brief overview of the main topics and concepts to be covered in this section.",
    )
    word_count: str = Field(
        description="Limit word range"
    )
    subsections: list[SubSectionDescription] | None = Field(
        None,
        description="Optional list of subsections. If None, write entire section without subsections."
    )
    section_refs: list[RefsUsage] | None = Field(
        None,
        description="References for heading-level writing (used when subsections is None)"
    )

    def has_subsections(self) -> bool:
        """Check if this section has subsections"""
        return self.subsections is not None and len(self.subsections) > 0


class SectionContent(BaseModel):
    content: str = Field(
        description="The content of the section."
    )
    ref: list[str] = Field(
        description="List of references."
    )


class SectionContentWithTokenCount(BaseModel):
    content: str = Field(
        description="The content of the section."
    )
    ref: list[str] = Field(
        description="List of references."
    )
    web_search_call: int
    input_tokens: int
    output_tokens: int
    embed_tokens: int


class Grader(BaseModel):
    grade: bool = Field(
        description="Evaluation result indicating whether the response meets requirements ('True') or needs revision ('False')."
    )
    comment: str = Field(
        description="Comment if any error.",
    )


class ExecutionPlan(BaseModel):
    plan: list[list[int]] = Field(..., description="The full execution plan for the section (index), consisting of a series of sequential batches.")


class DataSection(BaseModel):
    need_data: bool


class LitSection(BaseModel):
    lit_review: int = Field(description="The order of literature review section")


class MethodSection(BaseModel):
    propose_method: int = Field(description="The order of research methodology section")


class ResultSection(BaseModel):
    result: int = Field(description="The order of research methodology section")


class MethodSubSection(BaseModel):
    new_contribution: bool = Field(
        description="Set to `True` if the subsection describes the author's original proposed work for THIS specific study. This includes the proposed research model, the conceptual framework constructed for this research, the specific hypotheses to be tested, the research design, and the chosen variables and measurement scales. Set to `False` if the subsection is a review of existing, established knowledge, such as summarizing general theories, defining concepts, or discussing findings from prior literature."
    )


class ChosenLogs(BaseModel):
    chosen_logs: list[int] = Field(description="List of order of chosen logs")


class ChosenBlocks(BaseModel):
    chosen_blocks: list[int] = Field(description="List of indices of the chosen content blocks")
    reasoning: str = Field(description="Brief explanation of why these blocks are relevant to the subsection")


class ChunkPlanWithRefsUsage(BaseModel):
    chunk_description: str = Field(description="Description of what to write in this chunk")
    chunk_word_count: str = Field(description="Word count range for the chunk")
    refs: list[RefsUsage] = Field(description="List of references papers")


class ChunkPlan(BaseModel):
    chunk_description: str = Field(description="Description of what to write in this chunk")
    chunk_word_count: str = Field(description="Word count range for the chunk")
    refs: list[str] = Field(description="List of references papers")


class ChunkPlans(BaseModel):
    chunks: list[ChunkPlan] = Field(description="Chunks to write for subsection")


class ChunksPlanWithRefsUsage(BaseModel):
    chunks: list[ChunkPlanWithRefsUsage] = Field(description="Chunks to write for subsection")


class ChunkRefAssignment(BaseModel):
    chunk_index: int = Field(description="Chunk index, starting from 0")
    refs: list[str] = Field(description="List of references papers")


class UserInfo(BaseModel):
    field: str
    domain: str


class DraftItem(BaseModel):
    type: str = Field(
        description="Type of item: 'comment' for text content, 'table' for tables, 'image' for images/figures"
    )
    content: str = Field(
        description="Description of what should go here - either text outline or placeholder description"
    )


class DraftOutline(BaseModel):
    items: list[DraftItem] = Field(
        description="Ordered list of items (comments, tables, images) that structure the section"
    )
    ref: list[str] = Field(
        description="List of reference paper titles to cite"
    )


class ParagraphPlacement(BaseModel):
    integration_idx: int = Field(description="The index of the integration paragraph to place.")
    insert_after_block_idx: int = Field(description="The index of the main text block to insert AFTER. Use -1 to insert at the very beginning.")
    reasoning: str = Field(description="Short reasoning why this fits here (e.g. 'Matches the discussion on methodology').")


class PlacementPlan(BaseModel):
    placements: list[ParagraphPlacement]


class AnalysisOutput(BaseModel):
    needs_search: bool = Field(description="True if description requires dynamic data (stats, news). False if static theory.")
    reasoning: str = Field(description="Explanation of why search is needed or not based on keywords.")
    search_purpose: str = Field(description="Search purpose")
    search_case: str = Field(description="Specific search cases")


class LogContentBlock(BaseModel):
    index: int = Field(description="Index position in the original log")
    type: str = Field(description="Type: 'text' for normal text, 'file' for file reference")
    content: str = Field(description="Text content if type='text', or empty string if type='file'")
    file: str = Field(default="", description="File key (e.g., 'a.txt', 'b.csv', 'c.png') if type='file', empty otherwise")


class FusionWritingItem(BaseModel):
    index: int = Field(description="Index matching the original LogContentBlock")
    skip: bool = Field(description="True to skip this block, False to include it")
    comment: str = Field(default="", description="For text blocks: the fused/written content based on subsection purpose and research goals. For file blocks: empty string (no comment needed)")


class FusionWritingMapping(BaseModel):
    items: list[FusionWritingItem] = Field(description="Mapping for each content block, maintaining original order")


class SubsectionLogAssignment(BaseModel):
    subsection_index: int = Field(description="Index of the subsection (0-based)")
    chosen_logs: Optional[list[int]] = Field(default=None, description="List of log indices to use for this subsection")
    reasoning: str = Field(description="Brief explanation of why these logs are relevant to this subsection")


class SectionLogAssignments(BaseModel):
    assignments: list[SubsectionLogAssignment] = Field(description="Log assignments for all subsections in the section")