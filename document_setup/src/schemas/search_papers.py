from pydantic import BaseModel, Field


class SearchQueries(BaseModel):
    user_query: str = Field(None, description="Input user query")
    search_queries: list[str] = Field(None, description="Query that is optimized web search.")


class PaperAbstract(BaseModel):
    abstract: str = Field(None, description="Paper abstract")


class PublicationInfo(BaseModel):
    authors: list[str] = Field(["Authors"], description="List of authors")
    title: str = Field("Unknown", description="Publication title")
    year: str = Field(None, description="Year of publication")
    journal: str = Field("Unknown", description="Journal name")
    publisher: str = Field("Unknown", description="Publisher name")
    language: str = Field("Unknown", description="Primary language use in the paper (example: en, vi, vn, ...)")
    status: str = Field("Unknown", description="Either 'Preprint' or 'Peer-reviewed'")
    type: str = Field("Unknown", description="Thesis, Dissertation, Journal, Article, Conference paper, ...")
    

class KeyPoints(BaseModel):
    main_topic: str = Field(
        ...,
        description="The paper's exact title and core focus."
    )
    background: str = Field(
        ...,
        description="Socio-economic background, year(s), key events/policies, important statistics or figures mentioned."
    )
    importance: str = Field(
        ...,
        description="2–3 clear reasons (academic + practical) why the topic deserves research attention. Format as bullet points, each on a separate line."
    )
    research_gap: str = Field(
        ...,
        description="Explicitly state and categorize the gaps identified (data, model, theoretical, contextual, methodological)."
    )
    problem: str = Field(
        ...,
        description="Describe the real-world practical problem, current limitations, and bottlenecks the paper addresses."
    )
    objectives: str = Field(
        ...,
        description="Rewrite general and specific objectives as full, citable objective sentences (e.g., 'To examine…', 'To propose…'). Format as bullet points, each on a separate line."
    )
    scope: str = Field(
        ...,
        description="State spatial, temporal, and subject/object boundaries of the study."
    )
    methodology: str = Field(
        ...,
        description="Specify qualitative/quantitative/mixed approach, models, techniques, or indices in 2–4 concise, citation-friendly points. Format as bullet points, each on a separate line."
    )
    data: str = Field(
        ...,
        description="List all data sources and data types (time-series, panel, survey, official statistics, etc.). Format as bullet points, each on a separate line."
    )
    analysis: str = Field(
        ...,
        description="Identify dependent/independent variables, mediating/moderating variables, scales, key indicators, and overall analytical approach."
    )
    results: str = Field(
        ...,
        description="Summarize the most important empirical findings with numbers, coefficients, significance levels, and interpretation."
    )
    main_conclusions: str = Field(
        ...,
        description="List the 2–4 core conclusions of the author(s). Format as bullet points, each on a separate line."
    )
    contributions: str = Field(
        ...,
        description="Clearly separate theoretical/academic and practical/policy contributions; note relevance to your own dissertation."
    )
    future_directions: str = Field(
        ...,
        description="Summarize all suggested future research directions and acknowledged limitations."
    )
    hypotheses: list[str] = Field(
        None,
        description="""
        List all hypotheses exactly as stated (or rephrased for clarity if needed). 
        For each: [Hypothesis X: "exact or close quote"] + brief explanation if provided in the paper.
        """
    )
    model_design: str = Field(
        None,
        description="""
        Summarize the conceptual framework, theoretical model, or analytical/empirical model proposed or used.
        """
    )
    variables: list[str] = Field(
        None,
        description="""
        For each key variable, provide in this format:
        [Name (role: dependent/independent/mediator/moderator/control): description – measurement (observable/latent) – scale/source].
        Example:
        ```
        [
        Quản lý chuỗi cung ứng xanh (độc lập): Đo lường dựa trên các hành vi và thực hành môi trường trong quản lý chuỗi cung ứng,
        Chia sẻ kiến thức xanh (trung gian): Đo lường qua các hoạt động chia sẻ thông tin và kỹ năng về các thực hành bền vững,
        Đổi mới xanh (trung gian): Đo lường qua các sáng kiến đổi mới trong sản phẩm, quy trình nhằm giảm thiểu tác động môi trường,
        ]
        """
    )
    measurement_items: list[str] = Field(
        None,
        description="""
        List all survey/measurement items exactly as they appear (or summarized if too long).
        For each item: measures → Variable name | Item text | Scale | Source (author-developed / adapted from Author, Year).
        """
    )


class PaperTitle(BaseModel):
    title: str = Field(None, description="Paper's title in user's language")


class JournalInfo(BaseModel):
    authors: list[str] = Field(None, description="List of authors")
    title: str = Field(None, description="Publication title")
    year: int = Field(None, description="Year of publication")
    journal: str = Field(None, description="Journal name")
    pages: str = Field(None, description="Journal pages")
    issue: str = Field(None, description="Journal issue")
    volume: str = Field(None, description="Journal volume")


class JournalInfos(BaseModel):
    journal_infos: list[JournalInfo]