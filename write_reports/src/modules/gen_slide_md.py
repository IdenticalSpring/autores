import os
from langgraph.graph import StateGraph, START, END
from langgraph.types import Checkpointer, Send
import logging
import operator
import re
import httpx
from bs4 import BeautifulSoup
from typing import Annotated, Dict, Any, List, Tuple, Optional
from typing_extensions import TypedDict
from pydantic import BaseModel
from pptx import Presentation
from copy import deepcopy
import io
from write_reports.src.configs.app import settings
from utils import get_s3_client
from exception_type import AIERROR

from write_reports.src.modules.gen_slide_prompt import (
    SUMMARIZER_SYSTEM_PROMPT,
    WRITER_SYSTEM_PROMPT,
    CONTENT_WRITER_SYSTEM_PROMPT
)
from write_reports.src.schemas.slide import ChunkContent, SummaryChunk

from get_llm_response import get_llm, get_answer_with_schema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)
    

class StructuredSlides(BaseModel):
    title: str
    agenda: str  
    content: str
    thanks: str


class ContentOnlySlides(BaseModel):
    """Schema for manual mode - only content slides"""
    content: str


class SlideReplacement(BaseModel):
    """Pydantic model for a single slide's layout key and content_map."""
    layout_key: str
    content_map: Optional[Dict[str, str]] = None  # Changed here


class SlideContents(BaseModel):
    """Structured output for the list of slide contents: List[Tuple[str, Dict[str, str]]]"""
    slides: List[SlideReplacement]


class State(TypedDict):
    # Input
    document_id: str
    raw_report: str
    language: str
    llm_key: str
    model_id: str
    user_id: str
    max_chunk_size: int  # Configuration for chunk size limit
    user_preference: str  # Maximum number of slides (e.g., "5", "10")
    manual_mode: bool  # Flag for manual vs automatic mode
    use_direct_markdown_parsing: bool  # New flag for direct markdown parsing (no LLM)
    max_tokens: int

    # Processed content
    raw_html: str
    outlines: str
    chunks: list[ChunkContent]
    summarized_chunks: Annotated[list[str], operator.add]
    final_slides: str
    structured_slides: dict  # New field for structured output
    extracted_title: str  # For manual mode - extracted from h1
    pptx_url: str
    
    # LLM and token tracking
    total_input_tokens: Annotated[int, operator.add]
    total_output_tokens: Annotated[int, operator.add]


class WorkerSummarize(TypedDict):
    document_id: str
    model_id: str
    llm_key: str
    outlines: str
    chunk_content: str
    chunk_index: int
    section_title: str
    language: str
    max_tokens: int

# --- Slide Layout Configuration ---#


slide_layouts = {
    "opening_title": {
        "index_slide": 0,
        "placeholders": ["TITLE"]
    },
    "agenda": {
        "index_slide": 1,
        "placeholders": ["AGENDA", "LIST SECTIONS"]
    },
    "section_title": {
        "index_slide": 2,
        "placeholders": ["SECTION TITLE"]
    },
    "subsection": {
        "index_slide": 3,
        "placeholders": ["SUBSECTION", "SUBSECTION BODY"]
    },
    "thank_you": {
        "index_slide": 4,
        "placeholders": ["THANK YOU"]
    }
}


def preprocess_markdown(markdown: str) -> str:
    """
    Preprocess the Markdown input to handle duplicate thank-you slides.
    If the last two sections (split by ---) both contain 'thank you' or 'cảm ơn' (case-insensitive),
    remove the near-last section and return the updated Markdown.
    
    Args:
        markdown (str): Raw Markdown string.
    
    Returns:
        str: Preprocessed Markdown string.
    """
    # Split by --- and strip empty parts
    sections = [s.strip() for s in markdown.split('---') if s.strip()]
    
    if len(sections) < 2:
        return markdown  # No change if fewer than 2 sections
    
    last_section = sections[-1].lower()
    near_last_section = sections[-2].lower()
    
    # Check if both contain 'thank you' or 'cảm ơn'
    thank_you_keywords = ['thank you', 'cảm ơn']

    def contains_thank_you(text: str) -> bool:
        return any(keyword in text for keyword in thank_you_keywords)
    
    if contains_thank_you(near_last_section) and contains_thank_you(last_section):
        # Remove the near-last section, keep the last
        preprocessed_sections = sections[:-2] + [sections[-1]]
        # Reconstruct Markdown
        return '---\n'.join(preprocessed_sections)
    
    return markdown  # No change


def parse_markdown_with_titles(content: str) -> List[Dict[str, Any]]:
    """
    Parse Markdown content into elements (h1, h2, h3, bullets, text, quotes, code starts/ends).
    Updated to distinguish H1 (#), H2 (##), H3 (###), and handle basic code block detection.
    Returns a list of dicts: {'type': str, 'text': str, ...}
    """
    lines = content.split('\n')
    elements = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue
        if line.startswith('# '):
            elements.append({'type': 'h1', 'text': line[2:].strip()})
            i += 1
        elif line.startswith('## '):
            elements.append({'type': 'h2', 'text': line[3:].strip()})
            i += 1
        elif line.startswith('### '):
            elements.append({'type': 'h3', 'text': line[4:].strip()})
            i += 1
        elif line.startswith('> '):
            elements.append({'type': 'quote', 'text': line[2:].strip()})
            i += 1
        elif line.startswith('```'):
            # Collect code block
            code_text = [line]
            i += 1
            while i < len(lines) and not lines[i].strip().startswith('```'):
                code_text.append(lines[i])
                i += 1
            if i < len(lines):
                code_text.append(lines[i])  # Include closing ```
                i += 1
            elements.append({'type': 'code', 'text': '\n'.join(code_text)})
        elif line.startswith('- ') or line.startswith('* ') or line.startswith('+ ') or line[0].isdigit():
            elements.append({'type': 'bullet', 'text': line, 'isBullet': True})
            i += 1
        else:
            elements.append({'type': 'text', 'text': line})
            i += 1
    return elements


def get_clean_bullet_text(text: str) -> str:
    """
    Clean bullet text: remove leading spaces, bullet markers, numbers, and trim.
    Matches TypeScript getCleanBulletText logic.
    """
    # Find first non-space character
    leading_spaces = next((i for i, char in enumerate(text) if not char.isspace()), 0)
    content_part = text[leading_spaces:]
    # Remove bullet points or numbered bullets
    content_part = re.sub(r'^[-*+]\s+', '', content_part, count=1)
    content_part = re.sub(r'^(\d+\.)\s+', '', content_part, count=1)
    return content_part.strip()


def parse_agenda_content(agenda: str) -> List[str]:
    """
    Parse agenda Markdown into list of items.
    Matches TypeScript parseAgendaContent (assumed simple split).
    """
    return [line.strip() for line in agenda.split('\n') if line.strip()]


def detect_heading_structure(content_subsections: List[str]) -> str:
    """
    Detect whether the markdown uses ## for sections or # for sections.
    Returns 'h2_structure' if ## is used for sections, 'h1_structure' if # is used.
    
    Logic: If we find ## headings in content sections (after title/agenda), it's h2_structure.
    Otherwise, if we find # headings, it's h1_structure.
    """
    for subsection in content_subsections:
        lines = subsection.split('\n')
        for line in lines:
            line = line.strip()
            if line.startswith('## '):
                return 'h2_structure'
            elif line.startswith('# '):
                return 'h1_structure'
    return 'h2_structure'  # default


def build_slide_contents_from_markdown(
    markdown: str,
    slide_layouts: Dict[str, Dict[str, Any]],
    document_id: str = "",
) -> List[Tuple[str, Dict[str, str]]]:
    """
    Parse the raw Markdown string (structured with --- separators) into a slide_contents list
    suitable for passing to create_presentation. This simplifies the conversion by directly
    building the list of (layout_key, content_map) tuples, reusing existing parsers where possible.

    Structure:
    ---
    # [title content]
    ---
    ## Agenda OR list
    [list items]
    ---
    CASE 1 (h2_structure):
    ## [Section]
    ### [Subheading 1]
    [content: bullets, text, etc.]
    ### [Subheading 2]
    [content...]
    ---
    
    CASE 2 (h1_structure):
    # [Section]
    ---
    ## [Subheading 1]
    [content: bullets, text, etc.]
    ---
    ## [Subheading 2]
    [content...]
    ---
    
    [thanks content]
    ---
    
    Updated parsing logic:
    - Detects whether content uses ## (h2_structure) or # (h1_structure) for sections
    - For h2_structure: ## = section_title, ### = subsection
    - For h1_structure: # = section_title, ## = subsection
    - Handles both patterns dynamically

    Args:
        markdown (str): Raw Markdown string.
        slide_layouts (Dict): Layout config (used for validation).

    Returns:
        List[Tuple[str, Dict[str, str]]]: The slide_contents list.
    """
    slide_contents = []

    # Top-level split by --- (ignore empty parts)
    top_sections = [s.strip() for s in markdown.split('---') if s.strip()]

    title = ""
    agenda_raw = ""
    content_raw = ""
    thanks = ""

    if len(top_sections) > 0:
        title = top_sections[0]
    if len(top_sections) > 1:
        agenda_raw = top_sections[1]
    if len(top_sections) > 2:
        thanks = top_sections[-1]
        if len(top_sections) > 3:
            content_raw = '---\n'.join(top_sections[2:-1])
        elif len(top_sections) == 3:
            content_raw = top_sections[2]

    # Title slide
    if title:
        cleaned_title = re.sub(r'^#{1,6}\s*', '', title, count=1).strip()
        slide_contents.append(("opening_title", {"TITLE": cleaned_title}))

    # Agenda slide
    if agenda_raw:
        agenda_items = parse_agenda_content(agenda_raw)
        agenda_title = "Agenda"
        list_items = agenda_items
        if agenda_items:
            potential_title = agenda_items[0]
            if not (
                potential_title.startswith('-') or potential_title.startswith('*') or potential_title.startswith('+') or potential_title[0].isdigit()
            ):
                # Clean potential header markers from title (e.g., remove "## " or "# ")
                potential_title = re.sub(r'^#{1,6}\s*', '', potential_title).strip()
                agenda_title = potential_title
                list_items = agenda_items[1:]
        agenda_text = '\n'.join(get_clean_bullet_text(item) for item in list_items).strip()
        if agenda_text or agenda_title != "Agenda":
            slide_contents.append(("agenda", {"AGENDA": agenda_title, "LIST SECTIONS": agenda_text or ""}))

    # Content sections (split by --- for sub-sections)
    if content_raw:
        content_subsections = [cs.strip() for cs in content_raw.split('---') if cs.strip()]
        
        # Detect structure
        structure = detect_heading_structure(content_subsections)
        
        if structure == 'h1_structure':
            # Case: # for sections, ## for subsections
            for subsection in content_subsections:
                elements = parse_markdown_with_titles(subsection)
                if not elements:
                    continue

                i = 0
                sec_title = None
                
                # Add section_title if first element is h1 (#)
                if i < len(elements) and elements[i]['type'] == 'h1':
                    sec_title = elements[i]['text']
                    slide_contents.append(("section_title", {"SECTION TITLE": sec_title}))
                    i += 1

                # Process remaining elements: group under h2 (##) for subsection slides
                body_parts = []
                h2_count = 0
                while i < len(elements):
                    if elements[i]['type'] == 'h2':
                        sub_title = elements[i]['text']
                        sub_body_parts = []
                        h2_count += 1
                        # Prepend collected body_parts if this is the first h2
                        if h2_count == 1 and body_parts:
                            sub_body_parts.extend(body_parts)
                            body_parts = []
                        i += 1
                        # Collect content until next h2 or end
                        while i < len(elements) and elements[i]['type'] != 'h2':
                            el = elements[i]
                            if el['type'] == 'bullet':
                                clean = get_clean_bullet_text(el['text'])
                                sub_body_parts.append(f"- {clean}")
                            elif el['type'] in ['text', 'quote']:
                                sub_body_parts.append(el['text'])
                            elif el['type'] == 'code':
                                sub_body_parts.append(el['text'])
                            else:
                                sub_body_parts.append(el.get('text', str(el)))
                            i += 1
                        sub_body_text = '\n'.join(sub_body_parts).strip()
                        slide_contents.append(("subsection", {"SUBSECTION": sub_title, "SUBSECTION BODY": sub_body_text}))
                    else:
                        # Collect for potential default subsection (pre-h2 text)
                        el = elements[i]
                        if el['type'] == 'bullet':
                            clean = get_clean_bullet_text(el['text'])
                            body_parts.append(f"- {clean}")
                        elif el['type'] in ['text', 'quote']:
                            body_parts.append(el['text'])
                        elif el['type'] == 'code':
                            body_parts.append(el['text'])
                        else:
                            body_parts.append(el.get('text', str(el)))
                        i += 1

                # If no h2 but collected body, default to subsection using section title
                if h2_count == 0 and body_parts:
                    body_text = '\n'.join(body_parts).strip()
                    default_title = sec_title if sec_title else "Content"
                    slide_contents.append(("subsection", {"SUBSECTION": default_title, "SUBSECTION BODY": body_text}))
        else:
            # Case: ## for sections, ### for subsections (original logic)
            for subsection in content_subsections:
                elements = parse_markdown_with_titles(subsection)
                if not elements:
                    continue

                i = 0
                sec_title = None
                
                # Add section_title if first element is h2 (##)
                if i < len(elements) and elements[i]['type'] == 'h2':
                    sec_title = elements[i]['text']
                    slide_contents.append(("section_title", {"SECTION TITLE": sec_title}))
                    i += 1

                # Process remaining elements: group under h3 (###) for subsection slides
                body_parts = []
                h3_count = 0
                while i < len(elements):
                    if elements[i]['type'] == 'h3':
                        sub_title = elements[i]['text']
                        sub_body_parts = []
                        h3_count += 1
                        # Prepend collected body_parts if this is the first h3
                        if h3_count == 1 and body_parts:
                            sub_body_parts.extend(body_parts)
                            body_parts = []
                        i += 1
                        # Collect content until next h3 or end
                        while i < len(elements) and elements[i]['type'] != 'h3':
                            el = elements[i]
                            if el['type'] == 'bullet':
                                clean = get_clean_bullet_text(el['text'])
                                sub_body_parts.append(f"- {clean}")
                            elif el['type'] in ['text', 'quote']:
                                sub_body_parts.append(el['text'])
                            elif el['type'] == 'code':
                                sub_body_parts.append(el['text'])
                            else:
                                sub_body_parts.append(el.get('text', str(el)))
                            i += 1
                        sub_body_text = '\n'.join(sub_body_parts).strip()
                        slide_contents.append(("subsection", {"SUBSECTION": sub_title, "SUBSECTION BODY": sub_body_text}))
                    else:
                        # Collect for potential default subsection (pre-h3 text)
                        el = elements[i]
                        if el['type'] == 'bullet':
                            clean = get_clean_bullet_text(el['text'])
                            body_parts.append(f"- {clean}")
                        elif el['type'] in ['text', 'quote']:
                            body_parts.append(el['text'])
                        elif el['type'] == 'code':
                            body_parts.append(el['text'])
                        else:
                            body_parts.append(el.get('text', str(el)))
                        i += 1

                # If no h3 but collected body, default to subsection using section title
                if h3_count == 0 and body_parts:
                    body_text = '\n'.join(body_parts).strip()
                    default_title = sec_title if sec_title else "Content"
                    slide_contents.append(("subsection", {"SUBSECTION": default_title, "SUBSECTION BODY": body_text}))

    # Thanks slide
    if thanks:
        cleaned_thanks = re.sub(r'^[-–—]{1,3}\s*', '', thanks, count=1).strip()
        cleaned_thanks = re.sub(r'^#{1,6}\s*', '', cleaned_thanks, count=1).strip()
        cleaned_thanks = re.sub(r'^\*\s*', '', cleaned_thanks, count=1).strip()
        slide_contents.append(("thank_you", {"THANK YOU": cleaned_thanks}))

    # Validate layouts
    for layout_key, _ in slide_contents:
        if layout_key not in slide_layouts:
            logger.warning(f"Layout '{layout_key}' not in slide_layouts; skipping related slides.")

    logger.info(f"[{_doc_tag(document_id)}] Built slide_contents with {len(slide_contents)} entries from Markdown using {structure}.")
    return slide_contents


def _doc_tag(document_id: str) -> str:
    """Tag ngắn để log, không phụ thuộc biến `state` (trước đây gây NameError)."""
    return (document_id or "unknown")[:8]


def create_presentation_from_markdown(
    template_path: str,
    markdown: str,
    slide_layouts: Dict[str, Dict[str, Any]],
    output_path: Optional[str] = None,
    document_id: str = "",
) -> Optional[bytes]:
    """
    Simplified Markdown-to-PPTX converter. Parses the structured Markdown into slide_contents,
    then delegates to create_presentation for slide building and template handling.

    Args:
        template_path (str): Path to the template PPTX file.
        markdown (str): Raw Markdown string with --- separators.
        slide_layouts (Dict): Layout configuration.
        output_path (str, optional): Path to save PPTX file. If None, returns bytes.

    Returns:
        Optional[bytes]: PPTX bytes if output_path is None, else None.
    """
    # Preprocess Markdown to handle duplicate thank-you slides
    markdown = preprocess_markdown(markdown)
    
    slide_contents = build_slide_contents_from_markdown(markdown, slide_layouts, document_id)
    logger.info(f"[{_doc_tag(document_id)}] slide_contents built: {len(slide_contents)} slide(s)")
    return create_presentation(template_path, slide_layouts, slide_contents, output_path, document_id)


def create_presentation(
    template_path: str,
    slide_layouts: Dict[str, Dict[str, any]],
    slide_contents: List[Tuple[str, Dict[str, str]]],
    output_path: Optional[str] = None,
    document_id: str = "",
) -> Optional[bytes]:
    """
    Creates a new PPTX presentation from a template by adding slides based on the provided layout keys
    and replacing placeholders with custom content.

    Args:
        template_path (str): Path to the template PPTX file.
        slide_layouts (Dict[str, Dict[str, any]]): Dictionary mapping layout keys (e.g., 'opening_title')
            to {'index_slide': int, 'placeholders': List[str]} where placeholders lists the placeholders.
        slide_contents (List[Tuple[str, Dict[str, str]]]): List of (layout_key, content_map) tuples.
            Each content_map dict maps placeholders to replacement text (e.g., {'TITLE': 'My Title'}).
            Same layout_key can appear multiple times for repeated slides.
        output_path (str): Path to save the new PPTX file.

    Returns:
        Optional[bytes]: PPTX bytes if output_path is None, else None.

    Raises:
        FileNotFoundError: If template_path does not exist.
        ValueError: If a layout_key in slide_contents is not found in slide_layouts.
        RuntimeError: If template loading fails.
    """
    # Load the template presentation
    try:
        prs = Presentation(template_path)
    except Exception as e:
        raise RuntimeError(f"Error loading template {template_path}: {e}")

    # Record original slides by layout key
    original_slides = {}
    for key, layout_info in slide_layouts.items():
        slide_index = layout_info["index_slide"]
        if slide_index < len(prs.slides):
            original_slides[key] = prs.slides[slide_index]
        else:
            logger.info(f"[{_doc_tag(document_id)}] Warning: No slide found at index {slide_index} for layout '{key}'.")

    len_original = len(prs.slides)

    # Add new slides based on slide_contents
    for layout_key, content_map in slide_contents:
        if layout_key not in slide_layouts:
            raise ValueError(f"Layout key '{layout_key}' not found in slide_layouts.")
        if layout_key not in original_slides:
            logger.info(f"[{_doc_tag(document_id)}] Warning: No original slide for '{layout_key}', skipping.")
            continue

        source_slide = original_slides[layout_key]
        # Add new slide with the same layout
        new_slide = prs.slides.add_slide(source_slide.slide_layout)

        # Clear default shapes in the new slide
        for shape in new_slide.shapes:
            new_slide.shapes.element.remove(shape.element)

        # Copy shapes from source to new slide using deepcopy of XML elements
        # This preserves original content, including text and formatting
        for shape in source_slide.shapes:
            el = shape.element
            new_el = deepcopy(el)
            new_slide.shapes._spTree.insert_element_before(new_el, 'p:extLst')

        # logger.info(f"[{_doc_tag(document_id)}] Added slide {len(prs.slides)} with layout '{layout_key}' (original content preserved)...")

        # Apply custom content_map if any
        if content_map:
            for shape in new_slide.shapes:
                if not shape.has_text_frame:
                    continue
                for paragraph in shape.text_frame.paragraphs:
                    for run in paragraph.runs:
                        # Trim for robustness (handles extra whitespace)
                        trimmed_text = run.text.strip()
                        for placeholder, new_text in content_map.items():
                            if trimmed_text == placeholder:
                                run.text = new_text  # Direct overwrite for exact match
                                # logger.info(f"[{_doc_tag(document_id)}]   - Replaced exact '{placeholder}' with '{new_text}' in slide {len(prs.slides)}")
                                break  # Optional: Stop after first match per run (assuming one placeholder per run)

    # Remove the original slides (now at indices 0 to len_original-1)
    for i in range(len_original - 1, -1, -1):
        rId = prs.slides._sldIdLst[i].rId
        prs.part.drop_rel(rId)
        del prs.slides._sldIdLst[i]
    
    if output_path:
        # Save to physical file
        prs.save(output_path)
        logger.info(f"[{_doc_tag(document_id)}] \nPresentation rebuilt successfully and saved as {output_path}!")
        return None
    else:
        # Generate in-memory bytes buffer
        buffer = io.BytesIO()
        prs.save(buffer)
        pptx_bytes = buffer.getvalue()
        buffer.close()

        logger.info(f"[{_doc_tag(document_id)}] \nPresentation rebuilt successfully ({len(pptx_bytes)} bytes generated in memory)!")
        return pptx_bytes
    

# System prompt for LLM: Guide with simple rules on structure
EXPORT_SYSTEM_PROMPT = """You are an expert at turning markdown presentation content into a clean PowerPoint slide structure. Follow these layout definitions and purposes:

1. **opening_title**: The first slide. Shows the main title. Purpose: Grab attention. Replace "TITLE" with a short, bold presentation title.
2. **agenda**: The second slide. Lists what’s coming. Purpose: Set expectations. Replace "AGENDA" with "Agenda" header, and "LIST SECTIONS" with bullet points of main sections.
3. **section_title**: A main section slide (use one per major topic). Purpose: Introduce a big idea. Replace "SECTION TITLE" with the section name.
4. **subheading**: Follows a section_title for details. Purpose: Add key points under the section. Replace "SUBHEADING" with a short subsection title, and "SUBHEADING BODY".
5. **thank_you**: The last slide. Purpose: Wrap up. Replace "THANK YOU" with a simple message like "Thank you! Questions?".

The full deck order: opening_title → agenda → (section_title + subheadings) repeated for each section in agenda → thank_you.

Now, parse the provided final_slides (markdown with --- separators). Map content like this:
- Title section → opening_title.
- Agenda section → agenda (pull section list from content).
- Content section → Break into section_title + subheading(s) using ## for sections and ### for subs.
- Thanks section → thank_you.

Slide formatting rules:
- Keep everything slide-friendly: short bullet points, no long paragraphs.
- If a section or subheading has too much content, split it into multiple slides instead of shortening it excessively.
- Each slide should stay focused on one main idea.
- Avoid creating new or custom layouts — use only these layout_keys exactly.

Output must be a valid list like:
[
  {"layout_key": "opening_title", "content_map": {"TITLE": "Your Title Here"}},
  {"layout_key": "agenda", "content_map": {"AGENDA": "Agenda", "LIST SECTIONS": "- Bullet 1
  - Bullet 2"}},
  ...
]
"""
    

async def fetch_html_content(state: State) -> Dict[str, Any]:
    # await send_health_check(state["document_id"], AIStatus.PROCESSING)
    raw_report = (state.get("raw_report") or "").strip()
    # BE có thể gửi URL (http/https) HOẶC nội dung HTML trực tiếp.
    # Trước đây luôn coi là URL ⇒ payload HTML bị httpx báo
    # "Request URL is missing an 'http://'..." và trả error 634.
    if raw_report.startswith("<") or "<html" in raw_report[:200].lower():
        logger.info(f"[{state['document_id'][:8]}] raw_report là HTML trực tiếp ({len(raw_report)} chars), bỏ qua HTTP fetch")
        return {
            "raw_html": raw_report,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
        }
    if not raw_report.startswith(("http://", "https://")):
        raise AIERROR(
            634,
            f"raw_report phải là URL http(s) hoặc HTML trực tiếp, nhận được: {raw_report[:80]!r}",
        )
    try:
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        transport = httpx.AsyncHTTPTransport(retries=3) 
        async with httpx.AsyncClient(limits=limits, transport=transport, timeout=(3.0, 5.0)) as client:
            response = await client.get(state["raw_report"])
            response.raise_for_status()
            raw_html = response.text

        logger.info(f"[{state["document_id"][:8]}] Successfully fetched HTML content from {state['raw_report']}")
        return {
            "raw_html": raw_html,
            "total_input_tokens": 0,
            "total_output_tokens": 0
        }

    except Exception as e:
        logger.error(f"[{state["document_id"][:8]}] Error fetching HTML content: {str(e)}")
        raise


async def extract_outlines_and_chunk_content(state: State) -> Dict[str, Any]:
    """Extract outlines from headings and chunk content for processing."""
    # await send_health_check(state["document_id"], AIStatus.PROCESSING)
    soup = BeautifulSoup(state["raw_html"], 'html.parser')
    
    # Extract title from h1 for manual mode
    extracted_title = ""
    if state.get("manual_mode", False):
        h1_tag = soup.find('h1')
        if h1_tag:
            extracted_title = h1_tag.get_text().strip()
            logger.info(f"[{state["document_id"][:8]}] Extracted title for manual mode: {extracted_title}")
        else:
            # Fallback to title tag or first heading
            title_tag = soup.find('title')
            if title_tag:
                extracted_title = title_tag.get_text().strip()
            else:
                extracted_title = "Presentation Title"
    
    # Extract outlines from all headings
    headings = soup.find_all(['h1', 'h2', 'h3', 'h4'])
    outlines_parts = []
    
    for heading in headings:
        level = heading.name
        text = heading.get_text().strip()
        indent = "  " * (int(level[1]) - 1)  # Create indentation based on heading level
        outlines_parts.append(f"{indent}- {text}")
    
    outlines = "\n".join(outlines_parts)
    
    # Split content by h2 tags first
    h2_sections = soup.find_all('h2')
    chunks = []
    max_chunk_size = state.get("max_chunk_size", 20000)
    
    # If no h2 sections found, fallback to other headings or create single chunk
    if not h2_sections:
        logger.info(f"[{state["document_id"][:8]}] No h2 sections found, looking for other headings or creating single chunk")
        h3_sections = soup.find_all('h3')
        if h3_sections:
            sections_to_process = h3_sections
            heading_level = "h3"
        else:
            # Create a single chunk from all content
            body_text = soup.get_text()
            chunks.append({
                "content": body_text.strip(),
                "chunk_index": 0,
                "heading_level": "body",
                "section_title": "Main Content"
            })
            return {
                "outlines": outlines, 
                "chunks": chunks, 
                "extracted_title": extracted_title
            }
    else:
        sections_to_process = h2_sections
        heading_level = "h2"
    
    for section in sections_to_process:
        section_title = section.get_text().strip()
        
        # Get content from current heading until next heading of same level (or end)
        section_content = []
        section_content.append(str(section))
        
        # Collect all content until next heading of same level
        for sibling in section.next_siblings:
            if sibling.name == section.name:  # Same heading level
                break
            section_content.append(str(sibling))
        
        section_html = ''.join(section_content)
        section_text = BeautifulSoup(section_html, 'html.parser').get_text()
        
        # Ensure minimum content length to avoid very short chunks
        if len(section_text.strip()) < 100:
            logger.info(f"[{state["document_id"][:8]}] Section '{section_title}' has very little content ({len(section_text)} chars)")
            continue
        
        # If section is too large, split by next heading level
        if len(section_text) > max_chunk_size:
            section_soup = BeautifulSoup(section_html, 'html.parser')
            next_level_tag = f"h{int(section.name[1]) + 1}"  # h2 -> h3, h3 -> h4
            sub_sections = section_soup.find_all(next_level_tag)
            
            if sub_sections:
                # Split by sub-headings
                for sub_section in sub_sections:
                    sub_title = sub_section.get_text().strip()
                    sub_content = []
                    sub_content.append(str(sub_section))
                    
                    for sibling in sub_section.next_siblings:
                        if sibling.name == next_level_tag:
                            break
                        sub_content.append(str(sibling))
                    
                    sub_html = ''.join(sub_content)
                    sub_text = BeautifulSoup(sub_html, 'html.parser').get_text()
                    
                    if len(sub_text.strip()) >= 100:  # Only add if substantial content
                        chunks.append({
                            "content": sub_text.strip(),
                            "chunk_index": len(chunks),
                            "heading_level": next_level_tag,
                            "section_title": f"{section_title} - {sub_title}"
                        })
            else:
                # Cannot split further, keep as is but log warning
                logger.info(f"[{state["document_id"][:8]}] Large section '{section_title}' cannot be split further ({len(section_text)} chars)")
                chunks.append({
                    "content": section_text.strip(),
                    "chunk_index": len(chunks),
                    "heading_level": heading_level,
                    "section_title": section_title
                })
        else:
            chunks.append({
                "content": section_text.strip(),
                "chunk_index": len(chunks),
                "heading_level": heading_level,
                "section_title": section_title
            })
    
    # Ensure we have at least some chunks
    if not chunks:
        logger.error(f"[{state["document_id"][:8]}] No chunks were created, creating fallback chunk from body text")
        body_text = soup.get_text()
        chunks.append({
            "content": body_text.strip(),
            "chunk_index": 0,
            "heading_level": "body",
            "section_title": "Document Content"
        })
    
    logger.info(f"[{state["document_id"][:8]}] Extracted {len(chunks)} chunks from HTML content")
    logger.info(f"[{state["document_id"][:8]}] Generated outlines with {len(outlines_parts)} headings")
    
    # Log chunk details for debugging
    for chunk in chunks:
        logger.info(f"[{state["document_id"][:8]}] Chunk {chunk['chunk_index']}: '{chunk['section_title']}' ({len(chunk['content'])} chars)")
    
    return {
        "outlines": outlines,
        "chunks": chunks,
        "extracted_title": extracted_title
    }


def prepare_summarization(state: State) -> list[Send]:
    """Prepare to send each chunk to the summarize node for parallel processing."""
    return [
        Send(
            "summarize_chunk",
            {
                "document_id": state["document_id"],
                "chunk_content": chunk["content"],
                "chunk_index": chunk["chunk_index"],
                "section_title": chunk["section_title"],
                "outlines": state["outlines"],
                "llm_key": state["llm_key"],
                "model_id": state["model_id"],
                "language": state["language"],
                "max_tokens": state["max_tokens"],
            }
        )
        for chunk in state["chunks"]
    ]


async def summarize_chunk(state: WorkerSummarize) -> Dict[str, Any]:
    """Summarize a single chunk with context from outlines."""
    logger.info(f"[{state['document_id'][:8]}] Summarizing chunk {state['chunk_index']}: {state['section_title']}")
    llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
    content = f"""
    Document Outlines:
    {state['outlines']}
    
    Section Title: {state['section_title']}
    
    Chunk Content to Summarize:
    {state['chunk_content']}
    
    User's Language: {state['language']}
    
    Please summarize this chunk in the context of the overall document structure shown in the outlines.
    Focus on key points that would be valuable for creating presentation slides.
    Maintain important details while making content slide-friendly.
    Keep the section's main concepts and structure intact.
    If the content contains valuable information, preserve it in a concise but comprehensive manner.
    """
    error, success, summary_result, input_tokens, output_tokens = await get_answer_with_schema( 
        state["document_id"][:8],
        llm, 
        SUMMARIZER_SYSTEM_PROMPT, 
        content, 
        SummaryChunk
    )
    
    if not success:
        if error.status_code in [401, 403, 429, 500]:
            raise error
        summary_text = f"Section: {state['section_title']}\n{state['chunk_content'][:500]}..."
    else:
        # Ensure summary includes section context
        summary_text = f"Section: {state['section_title']}\n{summary_result.summary}"
    
    logger.info(f"[{state["document_id"][:8]}] Finished summarizing chunk {state['chunk_index']}: {len(summary_text)} characters")
    
    return {
        "summarized_chunks": [summary_text],
        "total_input_tokens": input_tokens,
        "total_output_tokens": output_tokens
    }


async def consolidate_and_write_slides(state: State) -> Dict[str, Any]:
    # await send_health_check(state["document_id"], AIStatus.PROCESSING)
    """Consolidate all summarized chunks and generate slides based on mode."""
    is_manual_mode = state.get("manual_mode", False)
    mode_text = "manual" if is_manual_mode else "automatic"
    logger.info(f"[{state["document_id"][:8]}] Consolidating summaries and generating slides in {mode_text} mode")
    
    # Combine all summarized content
    final_content = "\n\n".join(state["summarized_chunks"])
    
    # Count sections for validation
    section_count = len([chunk for chunk in final_content.split("\n\n") if chunk.strip().startswith("Section:")])
    logger.info(f"[{state["document_id"][:8]}] Processing {section_count} sections for slide generation")
    
    # Extract max slides from user_preference (assume it's a number string)
    max_slides = state["user_preference"]
    llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
    if is_manual_mode:
        # Manual mode: Generate only content slides
        slide_generation_content = f"""
        Document Outlines:
        ```
        {state['outlines']}
        ```
        
        Summarized Content:
        ```
        {final_content}
        ```
        
        User's Language: {state['language']}
        User's Preference: {f"Maximum {max_slides} content slides" if max_slides else "Generate comprehensive content slides"}
        
        Please create ONLY the content slides based on the above summaries.
        Do not create title, agenda, or thank you slides - these will be handled separately.
        Focus on presenting the detailed information from each section effectively.
        {"Limit to maximum " + str(max_slides) + " content slides." if max_slides else ""}
        """
        
        error, success, slides_result, input_tokens, output_tokens = await get_answer_with_schema( 
            state["document_id"][:8],
            llm, 
            CONTENT_WRITER_SYSTEM_PROMPT, 
            slide_generation_content, 
            ContentOnlySlides
        )
        if not success:
            raise error
        content_slides = slides_result.content
         
        # For manual mode, return only content - other parts will be added in post-processing
        structured_slides = {
            "title": "",  # Will be filled in post-processing
            "agenda": "",  # Will be filled in post-processing  
            "content": content_slides,
            "thanks": ""  # Will be filled in post-processing
        }
        
    else:
        # Automatic mode: Generate all slides as before
        slide_generation_content = f"""
        Document Outlines:
        ```
        {state['outlines']}
        ```
        
        Summarized Content:
        ```
        {final_content}
        ```
        
        User's Language: {state['language']}
        User's Preference: {f"Maximum {max_slides} content slides" if max_slides else "Generate comprehensive slides"}
        
        Please create comprehensive presentation slides based on the above content and outlines.
        The presentation should include:
        1. A title slide
        2. An agenda slide  
        3. Content slides {f"(maximum {max_slides} slides)" if max_slides else ""}
        4. A thank you/conclusion slide
        
        Ensure all sections from the summarized content are covered in the slides.
        """
        
        error, success, slides_result, input_tokens, output_tokens = await get_answer_with_schema( 
            state["document_id"][:8],
            llm, 
            WRITER_SYSTEM_PROMPT, 
            slide_generation_content, 
            StructuredSlides
        )
        if not success:
            raise error
        structured_slides = {
            "title": slides_result.title,
            "agenda": slides_result.agenda,
            "content": slides_result.content,
            "thanks": slides_result.thanks
        }
    
    logger.info(f"[{state["document_id"][:8]}] Input Token Slides: {input_tokens}; Output Token Slides: {output_tokens}")
    
    # Create final slides string (backward compatibility) - will be updated in post-processing for manual mode
    final_slides = f"""---
{structured_slides['title']}
---
{structured_slides['agenda']}
---
{structured_slides['content']}
---
{structured_slides['thanks']}
---"""
    
    logger.info(f"[{state["document_id"][:8]}] Successfully generated slides in {mode_text} mode")
    
    return {
        "final_slides": final_slides,
        "structured_slides": structured_slides,
        "total_input_tokens": input_tokens,
        "total_output_tokens": output_tokens
    }


def extract_headings_from_content(content: str) -> list[str]:
    """Extract headings from content slides for agenda generation."""
    headings = []
    
    lines = content.split('\n')
    for line in lines:
        line = line.strip()
        if line.startswith('## '):
            # Main heading
            heading = line[3:].strip()
            if heading:
                headings.append({
                    'type': 'main',
                    'title': heading,
                    'subheadings': []
                })
        elif line.startswith('### '):
            # Subheading
            subheading = line[4:].strip()
            if subheading and headings:
                headings[-1]['subheadings'].append(subheading)
    
    return headings


async def post_process_structured_slides(state: State) -> Dict[str, Any]:
    """Post-process structured slides based on mode."""
    # await send_health_check(state["document_id"], AIStatus.PROCESSING)
    is_manual_mode = state.get("manual_mode", False)
    
    structured_slides = state["structured_slides"].copy()
    
    if is_manual_mode:
        # Manual mode: Generate title, agenda, and thanks manually
        
        # 1. Title slide - use extracted title or fallback
        title = state.get("extracted_title", "").strip()
        if not title:
            title = "Presentation Title"
        structured_slides["title"] = f"# {title}"
        
        # 2. Generate agenda from content headings
        content = structured_slides.get("content", "")
        headings_structure = extract_headings_from_content(content)
        
        # Extract unique main headings and sort them
        unique_headings = []
        seen_headings = set()

        for heading in headings_structure:
            heading_title = heading['title'].strip()
            if heading_title and heading_title not in seen_headings:
                seen_headings.add(heading_title)
                unique_headings.append(heading_title)
        
        # Sort the headings alphabetically (or keep original order by removing sort())
        unique_headings.sort()
        
        agenda_parts = ["# Agenda\n"]
        for heading_title in unique_headings:
            agenda_parts.append(f"* {heading_title}")

        if len(agenda_parts) == 1:  # Only header, add fallback
            agenda_parts.append("* Main Content")
            
        structured_slides["agenda"] = "\n".join(agenda_parts)
        
        # 3. Thanks slide - fixed content
        thanks_text = "# Thank You!\n\n## Questions & Answers"
        structured_slides["thanks"] = thanks_text
        
    else:
        # Automatic mode: Clean existing content
        def clean_and_format_section(content: str, is_last_section: bool = False) -> str:
            """Clean a section and ensure it has proper '---' formatting."""
            if not content:
                return ""
            
            # Remove any existing '---' at the beginning and end
            content = content.strip()
            while content.startswith("---"):
                content = content[3:].strip()
            while content.endswith("---"):
                content = content[:-3].strip()
            
            # Add single '---' prefix
            formatted_content = f"---\n{content}"
            
            return formatted_content
        
        # Process each section
        sections = ["title", "agenda", "content", "thanks"]
        
        for i, section in enumerate(sections):
            is_last = (i == len(sections) - 1)
            structured_slides[section] = clean_and_format_section(
                structured_slides.get(section, ""), 
                is_last_section=is_last
            )
    
    # Create final slides string with proper formatting
    def ensure_section_separator(content: str, is_first: bool = False) -> str:
        """Ensure each section starts with exactly one '---' separator."""
        if not content:
            return ""
        
        content = content.strip()
        
        # Remove existing '---' at start
        while content.startswith("---"):
            content = content[3:].strip()
        
        # Add single '---' prefix
        return f"---\n{content}"
    
    final_slides_parts = []
    sections = ["title", "agenda", "content", "thanks"]
    
    for i, section in enumerate(sections):
        section_content = structured_slides.get(section, "")
        if section_content:
            formatted_section = ensure_section_separator(section_content, is_first=(i == 0))
            final_slides_parts.append(formatted_section)
    
    final_slides = "\n".join(final_slides_parts)
    
    # Log formatting validation
    logger.info(f"[{state["document_id"][:8]}] Sections processed: " + ", ".join([s for s in sections if structured_slides[s]]))
    
    if is_manual_mode:
        headings_count = len(extract_headings_from_content(structured_slides.get("content", "")))
        logger.info(f"[{state["document_id"][:8]}] Manual mode: Extracted {headings_count} content headings for agenda")
    
    return {
        "structured_slides": structured_slides,
        "final_slides": final_slides,
        "total_input_tokens": state["total_input_tokens"],
        "total_output_tokens": state["total_output_tokens"],
    }


async def export_presentation(state: State) -> Dict[str, Any]:
    """Use LLM to parse final_slides into slide_contents format and export to PPTX."""
    final_slides = state["final_slides"]
    document_id = state["document_id"]
    is_manual_mode = state.get("manual_mode", False)
    llm = get_llm(state["model_id"], state["llm_key"], state["max_tokens"])
    
    # From .../src/modules, go up one level to .../src, then into the 'template' folder
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.join(current_script_dir, '..', 'template', 'template.pptx')
    template_path = os.path.normpath(template_path)  # Normalize to remove any relative artifacts like '..'
        
    # Step 1: Use LLM to generate slide_contents from final_slides
    logger.info(f"[{state["document_id"][:8]}] Generating slide contents via LLM for document {document_id} in {'manual' if is_manual_mode else 'automatic'} mode")
    logger.info(f"[{state["document_id"][:8]}] Final Slides: \n{final_slides}")
    
    pptx_bytes = None
    custom_parsing = True
    input_tokens = 0
    output_tokens = 0
    
    if custom_parsing:
        try:
            pptx_bytes = create_presentation_from_markdown(template_path, final_slides, slide_layouts, document_id=state["document_id"])
        except Exception as e:
            logger.error(f"[{state["document_id"][:8]}] PPTX generation failed: {e}")
            raise
    else:
        # User prompt with final_slides and slide_layouts info
        export_prompt = f"""Slides Content:
        ```
        {final_slides}
        ```
        My Language: {state['language']}.
        Generate the sequence of slides following the rules."""

        error, success, slide_contents_result, input_tokens, output_tokens = await get_answer_with_schema(
            state["document_id"][:8],
            llm,
            EXPORT_SYSTEM_PROMPT,
            export_prompt,
            SlideContents
        )
        
        if not success:
            logger.error(f"[{state["document_id"][:8]}] LLM failed to generate slide contents: {error}")
            raise error
        
        # Convert structured result to List[Tuple[str, Dict[str, str]]]
        slide_contents_list: List[Tuple[str, Dict[str, str]]] = [
            (slide.layout_key, slide.content_map or {})  # Handle None as {}
            for slide in slide_contents_result.slides
        ]
        
        logger.info(f"[{state["document_id"][:8]}] LLM generated {len(slide_contents_list)} slides for export")
        
        # Step 3: Create presentation in memory
        try:
            pptx_bytes = create_presentation(
                document_id=state["document_id"],
                template_path=template_path,
                slide_layouts=slide_layouts,  # Your global dict
                slide_contents=slide_contents_list,
                output_path=None  # Generate in-memory bytes
            )
            logger.info(f"[{state["document_id"][:8]}] Generated PPTX in memory ({len(pptx_bytes)} bytes)")
        except Exception as e:
            logger.error(f"[{state["document_id"][:8]}] PPTX generation failed: {e}")
            raise
    
    # Step 4: Upload to S3 (assuming user_id is available; add to State if needed)
    # Note: If user_id is not in state, pass it as a parameter or retrieve from context
    user_id = state.get("user_id")
    if not user_id:
        raise ValueError("user_id is required for S3 upload but not provided in state")
    
    s3_client = get_s3_client()
    s3_key = f"{user_id}/article/{document_id}/presentation.pptx"
    s3_client.put_object(
        Bucket="users",
        Key=s3_key,
        Body=pptx_bytes,
        ContentType="application/vnd.openxmlformats-officedocument.presentationml.presentation"
    )
    
    # Generate public URL
    public_url = f"{settings.MINIO_DOMAIN}/users/{user_id}/article/{document_id}/presentation.pptx"
    
    logger.info(f"[{state["document_id"][:8]}] Uploaded PPTX to S3: {public_url}")
    
    return {
        "pptx_url": public_url,  # Updated key for the slide link
        "total_input_tokens": input_tokens,
        "total_output_tokens": output_tokens
    }
    

async def get_graph(checkpointer: Checkpointer):
    # Build the state graph
    writer_builder = StateGraph(State)

    # Add nodes
    writer_builder.add_node("fetch_html_content", fetch_html_content)
    writer_builder.add_node("extract_outlines_and_chunk_content", extract_outlines_and_chunk_content)
    writer_builder.add_node("summarize_chunk", summarize_chunk)
    writer_builder.add_node("consolidate_and_write_slides", consolidate_and_write_slides)
    writer_builder.add_node("post_process_structured_slides", post_process_structured_slides)
    writer_builder.add_node("export_presentation", export_presentation)

    # Add edges
    writer_builder.add_edge(START, "fetch_html_content")
    writer_builder.add_edge("fetch_html_content", "extract_outlines_and_chunk_content")
    writer_builder.add_conditional_edges("extract_outlines_and_chunk_content", prepare_summarization, ["summarize_chunk"])
    writer_builder.add_edge("summarize_chunk", "consolidate_and_write_slides")
    writer_builder.add_edge("consolidate_and_write_slides", "post_process_structured_slides")
    writer_builder.add_edge("post_process_structured_slides", "export_presentation")
    writer_builder.add_edge("export_presentation", END)

    # Compile the graph
    return writer_builder.compile(checkpointer=checkpointer)
