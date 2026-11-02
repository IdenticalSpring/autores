import asyncio
from bs4 import BeautifulSoup
from bson import ObjectId
from pymongo import AsyncMongoClient, ASCENDING

from get_llm_response import get_llm, get_answer_with_schema

from write_reports.src.schemas.translate import TranslatedChunk

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Translator:
    def __init__(self, mongo_client: AsyncMongoClient, document_id: str, model_id: str, llm_key: str, max_tokens: int, language: str):
        self.model_id = model_id
        self.document_id = document_id
        self.llm = get_llm(model_id, llm_key, max_tokens)
        self.language = language
        self.admin_db = mongo_client["admin"]

    @property
    def id(self):
        return self.document_id[:8]

    def __str__(self):
        return self.id

    async def get_document_content(self) -> list[str]:
        logger.info(f"[{self}] Get content for translate")
        content_collection = self.admin_db["outlines"]
        user_report_caches = content_collection.find({"documentId": self.document_id}).sort("index", ASCENDING)
        user_report_cache: list[str] = []
        async for doc in user_report_caches:
            if doc["content"]:
                user_report_cache.append(doc["content"])
            else:
                if doc["contentArr"]:
                    section_content = ""
                    for subsection in doc["contentArr"]:
                        section_content += subsection["text"]
                    user_report_cache.append(section_content)
                else:
                    break
        user_report_cache = user_report_cache[1:]
        return user_report_cache

    async def get_outline_content(self) -> list[str]:
        logger.info(f"[{self}] Get content for translate")
        content_collection = self.admin_db["document_configurations"]
        user_report_caches = await content_collection.find_one({"documentId": self.document_id})
        outline = user_report_caches["processedOutline"]["outline"]
        user_report_cache: list[str] = []
        for section in outline:
            user_report_cache.append(f"<h1>{section.get('heading') or section.get('title') or ''}</h1><p>{section.get('overview', '')}</p>")
            section_text = ""
            # Only process subsections if they exist (not None for section-only content)
            if section.get("subheadings") is not None:
                for subsection in section["subheadings"]:
                    section_text += f"<h2>{subsection["subheading"]}</h2><p>{subsection["detail_description"]}</p>"
            user_report_cache.append(section_text)
        return user_report_cache

    async def get_title(self) -> str:
        logger.info(f"[{self}] Get title for translate")
        articles_collection = self.admin_db["articles"]
        article = await articles_collection.find_one({"_id": ObjectId(self.document_id)})
        if not article:
            return f"Error: Article with document_id '{self.document_id}' not found."

        title_id = article["title"]
        proposal_collection = self.admin_db["proposal_titles"]
        proposal = await proposal_collection.find_one({"_id": ObjectId(title_id)})
        return proposal["title"]

    async def _translate(self, text: str) -> tuple[str, int, int]:
        """Translate a single text node with LLM"""
        if not text.strip():
            return text, 0, 0
        prompt = f"Translate the following text into {self.language}, keep it formal and do not use any casual words:\n\n{text}"
        error, success, translated, input_tokens, output_tokens = await get_answer_with_schema(self.id, self.llm, "", prompt, TranslatedChunk)
        if not success:
            logger.info(f"[{self}] {error}")
            return "Error translation", 0, 0
        return translated.translated_text.strip(), input_tokens, output_tokens
    
    async def translate_soup(self, soup: BeautifulSoup) -> tuple[str, int, int]:
        nodes = soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li"])
        tasks = [self._translate(node.get_text(strip=True)) for node in nodes]
        total_input_tokens = 0
        total_output_tokens = 0
        results = await asyncio.gather(*tasks)
        for node, (translated, input_tokens, output_tokens) in zip(nodes, results):
            node.string = translated
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
        return str(soup), total_input_tokens, total_output_tokens
        
    async def translate(self, is_outline: bool):
        title = await self.get_title()
        if not is_outline:
            user_report_cache = await self.get_document_content()
        else:
            user_report_cache = await self.get_outline_content()
        user_report_cache.insert(0, f"<h1>{title}</h1>")

        total_input_tokens = 0
        total_output_tokens = 0
        translated_chunks: list[str] = []
        tasks = [self.translate_soup(BeautifulSoup(chunk, "html.parser")) for chunk in user_report_cache]
        results = await asyncio.gather(*tasks)
        for translated, input_tokens, output_tokens in results:
            translated_chunks.append(translated)
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens

        translated_texts = "\n".join(translated_chunks)
        documents_collection = self.admin_db["document_configurations"]
        if not is_outline:
            await documents_collection.update_one(
                {"documentId": self.document_id},
                {
                    "$set": {
                        "translatedContent": translated_texts,
                    }
                },
                upsert=True
            )
        else:
            await documents_collection.update_one(
                {"documentId": self.document_id},
                {
                    "$set": {
                        "translatedOutline": translated_texts,
                    }
                },
                upsert=True
            )
        return total_input_tokens, total_output_tokens