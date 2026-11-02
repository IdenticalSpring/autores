import asyncio
from langchain_core.documents import Document
from langchain_core.documents import Document
from langchain_qdrant import QdrantVectorStore
from langchain_text_splitters import RecursiveCharacterTextSplitter
from utils import get_minio_client, get_embeddings
import logging
import tiktoken
from pymongo import AsyncMongoClient

from ai_chatbot.src.configs.app import settings
from ai_chatbot.src.schemas.in_doc import (
    SearchQueries, 
    ModifiedParagraphs, 
    ParagraphsSuggestions,
    NeedEnhanceParagraphs,
    ParagraphsComment,
    EnhancedParagraphs,
    LitSection,
    LitSectionComment,
)

from get_llm_response import get_llm, get_answer_with_schema
from utils import get_minio_client, markdownify_keep_images, hash_blake3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

ENHANCE_DOC_KEY = "enhancement:{doc_id}:{paragraph_id}"


class AIInDoc:

    def __init__(
        self, 
        mongo_client: AsyncMongoClient, 
        model_id: str, 
        llm_key: str, 
        db_key: str,
        document_id: str, 
        language: str,
        max_tokens: int,
    ):
        self.llm_key = llm_key
        self.llm = get_llm(model_id, self.llm_key, max_tokens)
        self.model_id = model_id
        self.language = language
        self.document_id = document_id
        self.embed_tokens = 0
        self.input_tokens = 0
        self.output_tokens = 0
        self.mongo_client = mongo_client
        self.minioClient = get_minio_client()
        self.embeddings = get_embeddings(db_key)
        self.minioBucket = "ENHANCE"
        self.process_single = True

    @property
    def id(self):
        return self.document_id[:8]

    def __str__(self):
        return self.id

    async def _generate_multiple_retrieval_queries_for_references(
        self, 
        search_queries_prompt: str,
    ) -> SearchQueries:
        """
        Generates multiple distinct search queries for the reference paper database.
        """
        system_prompt = "You are an AI assistant that generates a set of distinct search queries for a database of academic reference papers."
        error, success, search_queries, input_tokens, output_tokens = await get_answer_with_schema(
            self.id,
            self.llm,
            system_prompt,
            search_queries_prompt,
            SearchQueries
        )
        if not success:
            if error.status_code in [401, 403, 429, 500]:
                raise error
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        return search_queries if success else SearchQueries(search_queries=[]) 

    async def _retrieve_relevant_snippets_from_references(
        self,
        paragraphs: str,
        search_queries_prompt: str,
        k_per_query: int = 2,
    ) -> str:
        search_queries = await self._generate_multiple_retrieval_queries_for_references(
            search_queries_prompt
        )
        if not search_queries.search_queries:
            logger.error(f"[{self}] Fail to generate query for querying vector database")
        search_queries.search_queries.append(paragraphs)
        all_retrieved_docs_content: dict[str, Document] = {}
        try:
            encoding = tiktoken.encoding_for_model(settings.EMBEDDING_MODEL)
        except Exception:
            encoding = tiktoken.get_encoding("cl100k_base")
        for query in search_queries.search_queries:
            self.embed_tokens += len(encoding.encode(query))
            retrieved_docs_for_query = await self.vector_store.asimilarity_search(query=query, k=k_per_query)
            for doc in retrieved_docs_for_query:
                if doc.page_content not in all_retrieved_docs_content:
                    all_retrieved_docs_content[doc.page_content] = doc

        unique_docs = list(all_retrieved_docs_content.values())
        
        return "\n\n".join(
            [
                f"Reference Paper Snippet {i + 1}:\n{doc.page_content} - from {doc.metadata["source"]}" for i, doc in enumerate(unique_docs)
            ]
        )

    async def process_paragraph(
        self,
        paragraph_context: str,
        paragraphs: str,
        action: str,
        proposal: dict = None,
        research_note: dict = {},
        user_content: str = "",
    ) -> str:
        if len(self.document_id):
            try:
                self.vector_store = QdrantVectorStore.from_existing_collection(
                    url=settings.QDRANT_URL,
                    collection_name=self.document_id,
                    embedding=self.embeddings
                )
            except Exception:
                self.vector_store = None
        system_prompt = "You are an expert academic writing assistant. Your task is to help improve and modify report content based on the provided context. Be thorough and integrate information effectively."
        common_instructions = "Maintain the original core message and tone of the paragraph unless the goal implies otherwise (e.g., making it more impactful for 'better'). Use the same references format"
        
        action_clean = str(action or "").lower().strip()
        action_map = {
            "expand": "longer",
            "longer": "longer",
            "shorten": "shorter",
            "shorter": "shorter",
            "rewrite": "better",
            "better": "better",
            "fix_grammar": "fix_grammar",
            "make_academic": "make_academic",
            "suggestions": "suggestions",
        }
        internal_action = action_map.get(action_clean)
        if not internal_action:
            raise ValueError(f"Invalid action: '{action}'. Expected one of {list(action_map.keys())}")

        proposal = proposal or {}
        research_gap = proposal.get("research_gap", "")
        web_search = proposal.get("web_search", "")
        paragraphs = paragraphs or ""
        paragraph_context_md = markdownify_keep_images(paragraph_context) if paragraph_context else ""

        if internal_action in ["longer", "better", "fix_grammar", "make_academic", "suggestions"]:
            search_queries_prompt = f"""
            Based on the user's paragraphs, the intended action, the stated research gap, and the proposed method,
            generate exactly 3 concise and distinct search queries (each ideally 3-7 keywords/phrases).
            Each query should aim to find relevant but potentially different facets of information (e.g., background theory,
            comparable methods, supporting data, limitations of prior art) from the reference papers.
            If you cannot generate distinct queries, you can provide fewer, but strive for diversity.
            User's Paragraphs:
            {paragraphs}
            Intended Action on the original paragraphs: To make it '{internal_action}'.
            Research Gap Being Addressed: {research_gap}
            Web search result on Research Gap: {web_search}
            Generate 3 distinct search queries
            """
            retrieved_reference_snippets = await self._retrieve_relevant_snippets_from_references(
                paragraphs,
                search_queries_prompt
            ) if self.vector_store else ""
            extra_snippets = f"Additionally, consider these relevant snippets from foundational Reference Papers\n{retrieved_reference_snippets}" if retrieved_reference_snippets else ""
            
            content = f"""Consider the following research context derived from the overall proposal:
                Research Gap Being Addressed:
                {research_gap}
                Also consider this up-to-date knowledge from a Web Search Report:
                {web_search}
                {extra_snippets}
                Context of the original paragraphs
                {paragraph_context_md}
                Original Paragraph to be modified
                {paragraphs}"""
            
            instruction_prefix = "Based on all the provided information (original paragraphs, overall research context, web search report, and any retrieved reference paper snippets) "
            if internal_action == "longer":
                instruction = f"{instruction_prefix}please make the original paragraph substantively longer. Use critical thinking, looking at another perspective to expand on its ideas, add relevant details, examples, or explanations. Integrate insights from the web search report, reference snippets, and overall research proposal context to enrich the content. {common_instructions}"
            elif internal_action == "fix_grammar":
                instruction = f"{instruction_prefix}please carefully fix all grammatical errors, typos, punctuation, sentence structures, and awkward phrasing in the original paragraph while preserving its core meaning and academic tone. {common_instructions}"
            elif internal_action == "make_academic":
                instruction = f"{instruction_prefix}please rewrite the original paragraph using formal, precise academic vocabulary, scholarly phrasing, and an authoritative tone suitable for high-impact journals. {common_instructions}"
            elif internal_action == "better":
                instruction = f"{instruction_prefix}please improve the original paragraph. Use critical thinking, looking at another perspective to enhance its clarity, flow, engagement, argumentation, and impact. Use insights from the web search report, reference snippets, and overall research proposal context to strengthen it. {common_instructions}"
            elif internal_action == "suggestions":
                instruction = f"{instruction_prefix}please provide exactly 3 actionable and specific suggestions to make the original paragraph better. Suggestions should consider how to better align the paragraph with the research gap, proposed method, and findings from the web search report and reference papers. {common_instructions}"
            content += "\nTask:\n" + instruction
        elif internal_action == "shorter":
            content = f"""Based on the original paragraphs, please make the original paragraphs shorter and more concise. 
            Retain the most crucial information and core message. Aim for clarity. 
            {common_instructions}
            Context of the original paragraphs
            {paragraph_context_md}
            Original Paragraph to be modified
            {paragraphs}"""
        else:
            research_note = research_note or {}
            orig_content = research_note.get("original_content", paragraphs)
            rev_goal = research_note.get("revision_goal", internal_action)
            extra_context = f"Consider these user's context:\n{user_content}\n" if user_content else ""
            content = f"""{extra_context}
                Modify the original content
                {orig_content}
                The revision goal
                {rev_goal}
                Based on revision goal, please improve the original content. {common_instructions}"""

        content += f"\nGenerate response in {self.language}"
        if internal_action in ["longer", "better", "shorter", "fix_grammar", "make_academic"]:
            error, success, result, input_tokens, output_tokens = await get_answer_with_schema(
                self.id,
                self.llm,
                system_prompt,
                content,
                ModifiedParagraphs
            )
            if not success:
                raise error
            response = [result.new_paragraphs]
        elif internal_action == "suggestions":
            error, success, result, input_tokens, output_tokens = await get_answer_with_schema(
                self.id,
                self.llm,
                system_prompt,
                content,
                ParagraphsSuggestions
            )
            if not success:
                raise error
            response = result.suggestions
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        return response
   
    async def _review(
        self, 
        section: str,
        section_context: str,
    ) -> tuple[dict, int, int]:
        db = self.mongo_client["ai_in_doc"]
        collection = db["enhance"]
        if section:
            await collection.update_one(
                {"_id": self.document_id},
                {
                    "$addToSet": {
                        "documents": {
                            "id": hash_blake3(section_context),
                            "content": section_context
                        }
                    }
                },
                upsert=True
            )
            identifier_prompt = """
                You are an essay grader for college students to help them improving their report.
                You will be given a section and the section's context of the report, your task is to evalute the overall quality of the following essay out of 10.
                Considering grammar, coherence, organization, and relevance.
                Be strict and harsh as it help the user to get better
                Return a JSON object with `score` key
                """
            total_input_tokens = 0
            total_output_tokens = 0
            report_content = f"Section:\n{section}\nSection's context:\n{section_context}"
            error, success, result, input_tokens, output_tokens = await get_answer_with_schema(
                self.id,
                self.llm,
                identifier_prompt,
                report_content,
                NeedEnhanceParagraphs
            )
            if not success:
                if error.status_code in [401, 403, 429, 500]:
                    raise error
            if not success:
                result = NeedEnhanceParagraphs(score=4)
                logger.error(f"[{self}] Fail to check if the paragraph need enhancement, set to false")
            
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            if result.score < 5:
                logger.info(f"[{self}] Comment")
                if len(self.document_id) and self.vector_store:
                    db = self.mongo_client["ai_in_doc"]
                    self.collection = db[self.document_id]
                    search_queries_prompt = f"""
                    Based on the user's paragraphs, the section that the paragraphs belong to,
                    generate exactly 3 concise and distinct search queries (each ideally 3-7 keywords/phrases).
                    Each query should aim to find relevant but potentially different facets of information (e.g., background theory,
                    comparable methods, supporting data, limitations of prior art) from the reference papers.
                    If you cannot generate distinct queries, you can provide fewer, but strive for diversity.
                    User's Paragraphs:
                    {section}
                    The paragraphs belong to the following section:
                    {section_context}
                    Generate 3 distinct search queries
                    """
                    retrieved_reference_snippets = await self._retrieve_relevant_snippets_from_references(
                        section,
                        search_queries_prompt
                    )
                    extra_context = f"Consider these relevant snippets from foundational Reference Papers: \n{retrieved_reference_snippets}"
                else:
                    extra_context = ""
                writer_prompt = "You are an expert academic writing assistant. Your task is to help improve and modify report content based on the provided context. Be thorough and integrate information effectively."
                writer_content = f"""
                {extra_context}
                Consider the following research context:
                User's Paragraphs:
                {section}
                The paragraphs belong to the following section:
                {section_context}
                Based on all the provided information (original paragraphs, section)
                please provide exactly 1 comment (can have multiple parts) on how to make the original paragraphs better. Comment should consider how to better align the paragraphs with the section.
                Comment should also address any grammar error in the paragraphs.
                Maintain the original core message and tone of the paragraphs.
                Return in JSON object with `comment` keys
                Response in {self.language}
                """
                error, success, result, input_tokens, output_tokens = await get_answer_with_schema(
                    self.id,
                    self.llm,
                    writer_prompt,
                    writer_content,
                    ParagraphsComment
                )
                if not success:
                    raise error
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens
                paragraph_id = str(hash(section))
                final_paragraphs = {
                    "paragraphs": section,
                    "comment": result.comment,
                    "paragraph_id": paragraph_id,
                }
                    
                if len(self.document_id) and self.vector_store:
                    await self.collection.insert_one({"_id": paragraph_id, "content": retrieved_reference_snippets})
                return final_paragraphs, total_input_tokens, total_output_tokens
            else:
                return {}, total_input_tokens, total_output_tokens
        else:
            try:
                documents_cache = await collection.find_one({"_id": self.document_id})
                documents = [document_cache["content"] for document_cache in documents_cache["documents"]]
                content = f"""Report:
                {[f"Section {i + 1}:\n{document[:500]}...\n---\n" for i, document in enumerate(documents)]}
                Return from 0 to {len(documents)}, base on the order that the literature review section appear in the outline
                If the outline does not contain the literature review section, return 0
                """
                prompt = """Identify Literature Review section
                You are an expert academic research assistant. Your task is to analyze a research report, to identify the "Literature Review" section
                The section can have different name, for example: "Literature Review", "Related Work", "Tổng quan tài liệu", "Tổng quan nghiên cứu", ...
                Return the order that it appear in the report, if you can't find it, return 0, as there will be those that does not have the Literature Review section
                For example:
                - Report: ["Introduction...", "Literature Review...", "Methodology...", "Discussion and Result...", "Conclusion..."]
                - Output: 2

                - Report: ["Tóm tắt...", "Phần mở đầu...", "Tổng quan tài liệu...", "Phương pháp nghiên cứu...", "Kết quả và Kiến nghị...", "Tổng kết..."]
                - Output: 3

                - Report: ["Mở đầu...", "Thiết kế nghiên cứu...", "Quy trình thu thập và Phân tích dữ liệu...", "Kế hoạch triển khai và tiến độ...", "Kết cấu bài viết..."]
                - Output: 0
                """
                error, success, lit_review, input_tokens, output_tokens = await get_answer_with_schema(self.id, self.llm, prompt, content, LitSection)
                if not success:
                    return {}, 0, 0
                else:
                    if not lit_review.lit_review or lit_review.lit_review > len(documents):
                        return {}, 0, 0
                    else:
                        writer_prompt = """You are an expert academic writing assistant. 
                        Your task is to help check if the Literature Review section of a report new more references. 
                        If it need more references, explain what is missing and what and how to improve
                        Be thorough and integrate information effectively.
                        """
                        content = f"""Report:
                        {[f"Section {i + 1}:\n{document}\n---\n" for i, document in enumerate(documents)]}
                        Literature Review section:
                        {documents[lit_review.lit_review - 1]}
                        Response in {self.language}
                        """
                        error, success, lit_review_comment, review_input_tokens, review_output_tokens = await get_answer_with_schema(
                            self.id, 
                            self.llm, 
                            writer_prompt, 
                            content, 
                            LitSectionComment
                        )
                        if not success:
                            return {}, input_tokens, output_tokens
                        else:
                            if not lit_review_comment.need_enhance:
                                return {}, input_tokens + review_input_tokens, output_tokens + review_output_tokens
                            else:
                                final_paragraphs = {
                                    "paragraphs": documents[lit_review.lit_review - 1],
                                    "comment": lit_review_comment.comment,
                                    "paragraph_id": documents_cache["documents"][lit_review.lit_review - 1]["id"],
                                }
                                return final_paragraphs, input_tokens + review_input_tokens, output_tokens + review_output_tokens
            except Exception:
                return {}, 0, 0
            finally:
                try:
                    await collection.delete_one({"_id": self.document_id})
                except Exception:
                    pass

    async def review(self, report: str, overlap_char: int) -> list[dict]:
        sections = self.get_text_chunks(report[:-overlap_char]) if overlap_char else self.get_text_chunks(report)
        logger.info(f"[{self}] Review {len(sections)} sections")
        tasks = [
            self._review(
                section,
                report,
            ) 
            for section in sections
        ]
        review_sections = await asyncio.gather(*tasks)
        results = [result[0] for result in review_sections]
        input_tokens = [result[1] for result in review_sections]
        output_tokens = [result[2] for result in review_sections]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        return [result for result in results if len(result)]

    async def update_one(
        self,
        suggested_paragraph: dict[str, str],
        section: str,
    ) -> dict:
        # if self.process_single:
        #     await send_health_check(self.document_id, AIStatus.PROCESSING)
        try:
            db = self.mongo_client["ai_in_doc"]
            self.collection = db[self.document_id]
            result = await self.collection.find_one({"_id": str(hash(suggested_paragraph["paragraphs"]))})
            if result:
                extra_prompt = f"""
                Consider these relevant snippets from foundational Reference Papers:
                {result["content"]}\n
                """
            else:
                extra_prompt = ""   
        except Exception as e:
            logger.error(f"[{self}] {e}")
            extra_prompt = ""      
        writer_prompt = "You are an expert academic writing assistant. Your task is to help improve and modify report content based on the provided context. Be thorough and integrate information effectively."
        writer_content = f"""
            {extra_prompt}Consider the following research context:
            User's Paragraphs:
            {suggested_paragraph["paragraphs"]} 
            The paragraphs belong to the following section:
            {section}
            Improve the User's Paragraphs with {suggested_paragraph["comment"]}
            """
        error, success, result, input_tokens, output_tokens = await get_answer_with_schema(
            self.id, 
            self.llm,
            writer_prompt,
            writer_content,
            EnhancedParagraphs
        )
        if not success:
            raise error
        else:
            output = {
                "paragraphs": suggested_paragraph["paragraphs"],
                "new_paragraphs": result.new_content,
                "paragraph_id": suggested_paragraph["paragraph_id"],
            }
            return output, input_tokens, output_tokens

    async def update_many(self, suggested_paragraphs: list[dict[str, str]], section: str,) -> list[dict]:
        self.process_single = False
        tasks = [
            self.update_one(
                suggested_paragraph, 
                section
            ) 
            for suggested_paragraph in suggested_paragraphs
        ]
        update_sections = await asyncio.gather(*tasks)
        results = [result[0] for result in update_sections]
        input_tokens = [result[1] for result in update_sections]
        output_tokens = [result[2] for result in update_sections]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        return results
    
    def get_text_chunks(self, content: str) -> list[str]:
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=500,
            chunk_overlap=0,
            length_function=lambda text: len(text.split()),
            separators=["\n\n", "\n", "."],
        )
        chunks = text_splitter.split_text(content)
        last_part = ""
        for i, chunk in enumerate(chunks):
            chunk = last_part + " " + chunk
            if chunk.strip()[-1] != "." or not chunk.strip().endswith("\n"):
                last_part = chunk.split(".")[-1]
                chunk = chunk.replace(last_part, "".strip())
            chunks[i] = chunk
        return chunks