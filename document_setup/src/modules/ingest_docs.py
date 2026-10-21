import asyncio
import docx
import json
from langchain_community.document_loaders import PyPDFLoader
from langchain_core.documents import Document
from langchain_qdrant import QdrantVectorStore
from utils import get_embeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
import logging
import os
import pandas as pd
from pathlib import Path
from pymongo import AsyncMongoClient
import tiktoken
import time

from document_setup.src.configs.app import settings
from document_setup.src.modules.minio_path import parse_minio_path
from document_setup.src.modules.knowledge_prompt_bank import (
    GET_ABSTRACT_PROMPT, 
    KEY_POINTS_PROMPT, 
    GET_PUBLICATION_INFO_PROMPT,
    TRANSLATE_PROMPT_KEY_POINTS,
)
from document_setup.src.schemas.search_papers import (
    KeyPoints, 
    PaperAbstract, 
    PublicationInfo,
    PaperTitle,
)

from exception_type import AIERROR
from get_llm_response import get_llm, get_answer_with_schema
from utils import get_minio_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)
 

class VectorDBProcessor():
    def __init__(
        self, 
        llm_key: str, 
        db_key: str, 
        max_tokens: int,
        language: str = "",
        document_id: str = None, 
        model_id: str = "gpt-4.1-mini",
    ):
        # self.admin_user_guide = admin_user_guide
        self.collection_admin = "admin"
        self.language = language
        self.chain = None
        self.model_id = model_id
        self.llm = get_llm(self.model_id, llm_key, max_tokens, 1.0)
        self.minioClient = get_minio_client()
        self.collection_name = document_id if document_id else None
        self.embeddings = get_embeddings(db_key)
        self.db_collection = None
        self.input_tokens = 0
        self.output_tokens = 0
        self.document_id = document_id if document_id else self.collection_admin

    @property
    def id(self):
        return self.document_id[:8]

    def __str__(self):
        return self.id

    async def count_tokens_async(self, text: str) -> int:
        def count_tokens_sync(text: str) -> int:
            try:
                encoding = tiktoken.encoding_for_model(settings.EMBEDDING_MODEL)
            except Exception:
                encoding = tiktoken.get_encoding("cl100k_base")
            return len(encoding.encode(text))
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, count_tokens_sync, text)

    async def calculate_total_tokens_async(self, documents: list[Document]) -> int:
        tasks = [self.count_tokens_async(doc.page_content) for doc in documents]
        token_counts = await asyncio.gather(*tasks)
        return sum(token_counts)

    async def batch_ingestion(
        self, 
        chunked_documents: list[Document], 
        collection_name: str,
    ) -> list[list[str]]:
        uuids: list[list[str]] = []
        try:
            store = QdrantVectorStore.from_existing_collection(
                url=settings.QDRANT_URL,
                collection_name=collection_name,
                embedding=self.embeddings
            )
        except Exception:
            store = QdrantVectorStore.from_documents(
                documents=[],
                embedding=self.embeddings,
                url=settings.QDRANT_URL,
                collection_name=collection_name
            )
        batch_size = 20
        for i in range(0, len(chunked_documents), batch_size):
            batch = chunked_documents[i:i + batch_size]
            uuid = await store.aadd_documents(batch)
            uuids.append(uuid)
        return uuids

    async def ingest_admin_docs(self, docs: list[dict], mongo_client: AsyncMongoClient) -> tuple[list[dict], int, int, int, str]:
        tasks = [self._process_single_paper(doc, mongo_client) for doc in docs if "url" in doc]
        papers = await asyncio.gather(*tasks)
        results = [result[0] for result in papers]
        input_tokens = [result[1] for result in papers]
        output_tokens = [result[2] for result in papers]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        return (
            [{k: v for k, v in res.items() if k not in ["full_text", "key_points"]} for res in results if res], 
            self.input_tokens, 
            self.output_tokens, 
            self.embeddings.model
        )
    async def get_key_points_user_docs(self, mongo_client: AsyncMongoClient, docs: list[dict]) -> tuple[list[dict], int, int]:
        db = mongo_client["user_documents"]
        self.db_collection = db["reports_refs"]
        cached_data = await self.db_collection.find_one({"_id": self.document_id})
        logger.info(f"[{self}] Get key points user references documents")
        processed_docs = [doc for doc in docs if "url" in doc]
        if cached_data and "documents" in cached_data:
            documents = cached_data["documents"]
        else:
            tasks = [self._process_single_paper(doc, mongo_client) for doc in processed_docs]
            results = await asyncio.gather(*tasks)
            documents = [result[0] for result in results]
            input_tokens = [result[1] for result in results]
            output_tokens = [result[2] for result in results]
            self.input_tokens += sum(input_tokens)
            self.output_tokens += sum(output_tokens)
            await self.db_collection.update_one(
                {"_id": self.document_id},
                {"$push": {"documents": {"$each": documents}}},
                upsert=True
            )
        for res, doc in zip(documents, processed_docs):
            res["title"] = doc["title"]
            if "full_text" in res:
                del res["full_text"]
            res["source"] = doc["source"]
            if "key_points" in res:
                del res["key_points"]
            if "_id" in res:
                del res["_id"]
            res["url"] = doc["url"]
            res["authors"] = doc.get("authors", "")
            res["year"] = doc.get("year", "")
            res["journal"] = doc.get("journal", "")
            res["volume"] = doc.get("volume", "")
            res["issue"] = doc.get("issue", "")
            res["pages"] = doc.get("pages", "")
            res["q"] = doc.get("q", "")
            res["citation"] = doc.get("citation", 0)
            res["language"] = doc.get("language", "")
            res["type"] = doc.get("type", "")
            res["status"] = doc.get("status", "")
            res["ref_id"] = doc.get("ref_id", "")

        return documents, self.input_tokens, self.output_tokens
    
    async def update_user_docs(self, mongo_client: AsyncMongoClient, docs: list[dict]) -> int:
        db = mongo_client["user_documents"]
        self.db_collection = db["reports_refs"]
        logger.info(f"[{self}] Get key points user update references documents")
        processed_docs = [doc for doc in docs if "url" in doc]
        tasks = [self._process_single_paper(doc, mongo_client) for doc in processed_docs]
        results = await asyncio.gather(*tasks)
        documents = [result[0] for result in results]
        input_tokens = [result[1] for result in results]
        output_tokens = [result[2] for result in results]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        logger.info(f"[{self}] Start ingest user update references documents")
        await self.db_collection.update_one(
            {"_id": self.document_id},
            {"$push": {"documents": {"$each": documents}}},
            upsert=True
        )
        embed_tokens, _ = await self._ingest_documents(documents, self.collection_name)
        return embed_tokens

    async def _load_file_content(self, url: str) -> str:
        try:
            input_tokens, output_tokens = 0, 0
            object_name = url.split("users/")[-1]    
            file_path = f"{settings.TMP_FOLDER}/{object_name.split("/")[-1]}"
            os.makedirs(settings.TMP_FOLDER, exist_ok=True)
            self.minioClient.fget_object("users", object_name, file_path)
            path = Path(file_path)
            extension = path.suffix.lower()
            if extension == ".pdf":
                logger.info(f"[{self}] Reading pdf file: {path.name}")
                loader = PyPDFLoader(file_path)
                doc = await loader.aload() 
                content = '\n\n'.join([page.page_content for page in doc])
            elif extension in [".txt", ".md"]:
                logger.info(f"[{self}] Reading text file: {path.name}")
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
            elif extension in [".docx"]:
                content = await self._get_text_from_word(path)
            elif extension in [".csv", ".xlsx", ".xls"]:
                content = await self._get_text_from_tabular(path)
            else:
                logger.info(f"[{self}] Unsupported file type: {extension} for file {path.name}")
                content = f"Error: Unsupported file type '{extension}' for file {path.name}."
            return content, input_tokens, output_tokens
        except Exception as e:
            logger.info(f"[{self}] {e}")
            raise AIERROR(601, f"Invalid file url: {url}")
        finally:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.info(f"[{self}] Successfully cleaned up temporary file: {file_path}")
                except OSError as e:
                    logger.error(f"Error removing temporary file {file_path}: {e}")

    async def load_datas_chat(self, user_id: str, task_name: str):
        prefix = f"{user_id}/{task_name}/{self.document_id}/datas/"
        full_text_tasks = [
            self._load_file_content(obj.object_name) 
            for obj in self.minioClient.list_objects("users", prefix=prefix, recursive=True)
        ]
        logger.info(f"[{self}] Find {len(full_text_tasks)} data files")
        files_content = await asyncio.gather(*full_text_tasks)
        full_texts = [result[0] for result in files_content]
        input_tokens = [result[1] for result in files_content]
        output_tokens = [result[2] for result in files_content]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        input_context = ("\nInput data:\n" + "\n\n".join(full_texts)) if full_texts else ""
        return input_context, self.input_tokens, self.output_tokens
            
    async def load_refs_chat(self, user_id: str):
        prefix = f"{user_id}/document-request/{self.document_id}/refs/"
        full_text_tasks = [
            self._load_file_content(obj.object_name) 
            for obj in self.minioClient.list_objects("users", prefix=prefix, recursive=True)
        ]
        files_content = await asyncio.gather(*full_text_tasks)
        full_texts = [result[0] for result in files_content]
        input_tokens = [result[1] for result in files_content]
        output_tokens = [result[2] for result in files_content]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        tasks = [
            self.publication_info(full_text, True) 
            for full_text in full_texts
        ]
        results = await asyncio.gather(*tasks)
        docs = [result[0] for result in results]
        input_tokens = [result[1] for result in results]
        output_tokens = [result[2] for result in results]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        tasks = [self.get_key_points(full_text) for full_text in full_texts]
        results = await asyncio.gather(*tasks)
        key_points = [result[0] for result in results]
        input_tokens = [result[1] for result in results]
        output_tokens = [result[2] for result in results]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        for doc, key_point in zip(docs, key_points):
            doc["key_points"] = key_point
        embed_tokens, _ = await self._ingest_documents(docs, self.collection_name)
        return docs, self.input_tokens, self.output_tokens, embed_tokens

    async def embed_user_docs(self, mongo_client: AsyncMongoClient, docs: list[dict]):
        db = mongo_client["user_documents"]
        self.db_collection = db["reports_refs"]
        cached_data = await self.db_collection.find_one({"_id": self.document_id})
        processed_docs = [doc for doc in docs if "url" in doc]
        if cached_data and "documents" in cached_data:
            documents = cached_data["documents"]
        else:
            tasks = [self._process_single_paper(doc, mongo_client) for doc in processed_docs]
            results = await asyncio.gather(*tasks)
            documents = [result[0] for result in results]
            input_tokens = [result[1] for result in results]
            output_tokens = [result[2] for result in results]
            self.input_tokens += sum(input_tokens)
            self.output_tokens += sum(output_tokens)
            await self.db_collection.update_one(
                {"_id": self.document_id},
                {"$push": {"documents": {"$each": documents}}},
                upsert=True
            )
        logger.info(f"[{self}] Embed user references documents")
        embed_tokens, _ = await self._ingest_documents(documents, self.collection_name)
        return embed_tokens, settings.EMBEDDING_MODEL

    # async def ingest_user_key_points(self, full_text: str, title: str):
    #     key_points, input_tokens, output_tokens = await self.get_key_points(full_text)
    #     doc = {
    #         # "full_text": full_text,
    #         "key_points": key_points,
    #         "title": title
    #     }
    #     return await self._ingest_documents([doc], self.collection_name + "_user"), input_tokens, output_tokens

    async def get_key_points(self, full_text: str) -> tuple[KeyPoints, int, int]:
        """
        Extracts key points from the full text of a research paper using the LLM.

        Args:
            full_text (str): The full text content of the research paper.

        Returns:
            KeyPoints: A Pydantic model instance containing the extracted key points
                       (e.g., research questions, methodology, results).
        """
        content = f"Research paper: {full_text}\nResponse and **translate** it the following language: {self.language}\nReturn just the JSON object"
        total_input_tokens, total_output_tokens = 0, 0
        error, success, result, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm, 
            KEY_POINTS_PROMPT, 
            content, 
            KeyPoints
        )
        if not success:
            raise error
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens
        content = f"""
        Research paper's summary:
        {result}
        User's language:
        {self.language}
        """
        error, success, result, input_tokens, output_tokens = await get_answer_with_schema(
            self.id, 
            self.llm, 
            TRANSLATE_PROMPT_KEY_POINTS, 
            content, 
            KeyPoints
        )
        if not success:
            raise error
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens
        # logger.info(f"Input tokens: {total_input_tokens}")
        # logger.info(f"Output tokens: {total_output_tokens}")
        key_points_dict = result.model_dump()
        formatted_key_points = {
            k: (("</br>" + "</br>".join(v.split("\n")) if len(v.split("\n")) > 1 else v) if isinstance(v, str) else v)
            for k, v in key_points_dict.items()
        }
        return formatted_key_points, total_input_tokens, total_output_tokens
    
    async def get_abstract(self, full_text: str) -> tuple[PaperAbstract, int, int]:
        content = f"Research paper: {full_text}\nReturn just the JSON object"
        error, success, result, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm, 
            GET_ABSTRACT_PROMPT, 
            content, 
            PaperAbstract
        )
        if not success:
            raise error
        else:
            return result, input_tokens, output_tokens
    
    async def publication_info(
        self, 
        full_text: str, 
        user: bool = False, 
        ref_id: str = "", 
        outline_id: str = ""
    ) -> tuple[PublicationInfo | dict, int, int]:
        content = f"Research paper: {full_text[:5000] if len(full_text) > 5000 else full_text}\nReturn just the JSON object"
        error, success, result, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm, 
            GET_PUBLICATION_INFO_PROMPT, 
            content, 
            PublicationInfo
        )
        if not success:
            raise error
        if result.authors is None:
            result.authors = ["Authors"]
        if isinstance(result.year, str):
            result.year = int(result.year) if result.year.isdigit() else None
        if user:
            result_dict = result.model_dump()
            title_prompt = f"Translate the following research paper's title into the {self.language}"
            _, success, title, title_input_tokens, title_output_tokens = await get_answer_with_schema(
                self.id, 
                self.llm, 
                title_prompt, 
                result.title, 
                PaperTitle
            )
            if not success:
                title = PaperTitle(title="Unknown")
            abstract, abs_input_tokens, abs_output_tokens = await self.get_abstract(full_text)                
            result_dict["user_language_title"] = title.title
            result_dict["abstract"] = abstract.abstract
            result_dict["issn"] = "Unknown"
            result_dict["volume"] = "Unknown"
            result_dict["issue"] = "Unknown"
            result_dict["q"] = "Unknown"
            result_dict["citation"] = 0
            result_dict["ref_id"] = self.document_id if not ref_id else ref_id
            result_dict["outline_id"] = outline_id
            return result_dict, input_tokens + title_input_tokens + abs_input_tokens, output_tokens + title_output_tokens + abs_output_tokens
        else:
            return result, input_tokens, output_tokens
        
    async def _get_publication_info(self, document: dict, user: bool) -> tuple[list[dict], int, int]:
        full_text, _ = await self.get_full_text_async(document["title"], document["url"])
        return await self.publication_info(full_text, user, document["ref_id"], document["outline_id"])
        
    async def get_publication_info(self, documents: list[dict]) -> tuple[list[dict], int, int]:
        tasks = [self._get_publication_info(doc, True) for doc in documents if "url" in doc and doc["source"] == "USER"]
        results = await asyncio.gather(*tasks)
        infos = [result[0] for result in results]
        input_tokens = [result[1] for result in results]
        output_tokens = [result[2] for result in results]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        return infos, self.input_tokens, self.output_tokens
    
    async def get_full_text_async(self, title: str, pdf_path: str) -> tuple[str, str]:
        try:
            if "users/" in pdf_path:
                minio_bucket = "users"
                object_name = pdf_path.split("users/")[-1]
                file_path = f"{settings.TMP_FOLDER}/{object_name.split("/")[-1]}"
            else:
                minio_bucket = pdf_path.split('/')[0]
                object_name = pdf_path.replace(minio_bucket, "")
                file_path = f"{settings.TMP_FOLDER}/{object_name}"
            os.makedirs(settings.TMP_FOLDER, exist_ok=True)
            self.minioClient.fget_object(minio_bucket, object_name, file_path)
            path = Path(file_path)
            extension = path.suffix.lower()
            if extension == ".pdf":
                logger.info(f"[{self}] Reading pdf file: {path.name}")
                loader = PyPDFLoader(file_path)
                doc = await loader.aload() 
                content = '\n\n'.join([page.page_content for page in doc])
                logger.info(f"[{self}] {pdf_path} - {len(doc)} pages - {len(content.split(" "))} words")
                return content, object_name
            if extension in [".txt", ".md"]:
                logger.info(f"[{self}] Reading text file: {path.name}")
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                return content, object_name
            elif extension in [".docx"]:
                content = await self._get_text_from_word(path)
                return content, object_name
            else:
                raise AIERROR(602, f"Invalid file type for {title}")
        except Exception as e:
            logger.info(f"[{self}] {e}")
            raise AIERROR(601, f"Invalid file url: {pdf_path}")
        finally:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.info(f"[{self}] Successfully cleaned up temporary file: {file_path}")
                except OSError as e:
                    logger.error(f"[{self}] Error removing temporary file {file_path}: {e}")
            
    async def _process_single_paper(self, document: dict, mongo_client: AsyncMongoClient) -> tuple[dict, int, int]:
        """
        Asynchronously processes a single research paper document.

        This involves fetching its full text (if it's a PDF specified by 'url')
        and then extracting key points from the text.

        Args:
            document (dict): A dictionary representing the paper, expected to have
                             at least "url" (path to PDF), "title", and "authors".

        Returns:
            dict: A dictionary containing the "title", "authors", and "key_points"
                  (as a model dump) of the processed paper. Returns an empty
                  dictionary if the paper cannot be read or processed.
        """
        total_input_tokens = 0
        total_output_tokens = 0
        if document["source"] == "ADMIN":
            db = mongo_client["admin"]
            collection_docs = db["processed_docs"]
            doc_record = await collection_docs.find_one({"documentId": document.get("document_id", document["ref_id"])})
            if doc_record:
                return {
                    "source": document["source"],
                    "title": document["title"],
                    "url": document["url"],
                    "full_text": doc_record["full_text"],
                    "key_points": doc_record["key_points"],
                    "authors": doc_record.get("authors", "Author"),
                    "year": doc_record.get("year", "Unknown"),
                    "journal": doc_record.get("journal", "Unknown"),
                    "volume": doc_record.get("volume", "Unknown"),
                    "issue": doc_record.get("issue", "Unknown"),
                    "pages": doc_record.get("pages", "Unknown"),
                    "q": doc_record.get("q", "Unknown"),
                    "citation": doc_record.get("citation", 0),
                    "language": doc_record.get("language", "Unknown"),
                    "type": doc_record.get("type", ""),
                    "status": doc_record.get("status", ""),
                }, total_input_tokens, total_output_tokens
        start_time = time.time()
        full_text, file_id = await self.get_full_text_async(document["title"], document["url"])
        key_points, input_tokens, output_tokens = await self.get_key_points(full_text) if "key_points" not in document else document["key_points"]
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens
        logger.info(f"[{self}] {file_id}")
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens
        end_time = time.time()
        logger.info(f'[{self}] read_paper time: {end_time - start_time}')  
        if document["source"] == "ADMIN":
            info, input_tokens, output_tokens = await self.publication_info(full_text)
            abstract, input_tokens, output_tokens = await self.get_abstract(full_text)
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            await collection_docs.insert_one(
                {
                    "documentId": document.get("document_id", document["ref_id"]),
                    "isDeleted": False,
                    "createdBy": None,
                    "updatedBy": None,
                    "createdAt": int(time.time()),
                    "updatedAt": int(time.time()),
                    "title": document["title"],
                    "full_text": full_text,
                    "key_points": key_points,
                    "abs": abstract.abstract,
                    "authors": document["authors"] if ("authors" in document and document["authors"] not in [["Author"], [""]]) else info.authors,
                    "year": document["year"] if ("year" in document and document["year"] not in ["Unknown", ""]) else info.year,
                    "journal": document["journal"] if ("journal" in document and document["journal"] not in ["Unknown", ""]) else info.journal,
                    "volume": document.get("volume", "Unknown"),
                    "issue": document.get("issue", "Unknown"),
                    "pages": document.get("pages", "Unknown"),
                    "q": document.get("q", "Unknown"),
                    "publisher": document["publisher"] if ("publisher" in document and document["publisher"] not in ["Unknown", ""]) else info.publisher,
                    "citation": document.get("citation", 0),
                    "language": document.get("language", info.language),
                    "type": document.get("type", info.type),
                    "status": document.get("status", info.status),
                }
            )
            return {
                "source": document["source"],
                "title": document["title"],
                "url": document["url"],
                "full_text": full_text,
                "key_points": key_points,
                "abs": abstract.abstract,
                "document_id": document.get("document_id", document["ref_id"]),
                "authors": document["authors"] if ("authors" in document and document["authors"] not in [["Author"], [""]]) else info.authors,
                "year": document["year"] if ("year" in document and document["year"] not in ["Unknown", ""]) else info.year,
                "journal": document["journal"] if ("journal" in document and document["journal"] not in ["Unknown", ""]) else info.journal,
                "volume": document.get("volume", "Unknown"),
                "issue": document.get("issue", "Unknown"),
                "pages": document.get("pages", "Unknown"),
                "q": document.get("q", "Unknown"),
                "publisher": document["publisher"] if ("publisher" in document and document["publisher"] not in ["Unknown", ""]) else info.publisher,
                "citation": document.get("citation", 0),
                "language": document.get("language", info.language),
                "type": document.get("type", info.type),
                "status": document.get("status", info.status),
            }, total_input_tokens, total_output_tokens
        elif document["source"] == "USER":
            return {
                "source": document["source"],
                "title": document["title"],
                "url": document["url"],
                "authors": document["authors"],
                "year": document["year"],
                "journal": document["journal"],
                "full_text": full_text,
                "key_points": key_points,
                "volume": "Unknown",
                "issue": "Unknown",
                "pages": "Unknown",
                "q": document.get("q", "Unknown"),
                "publisher": document.get("publisher", "Unknown"),
                "citation": document.get("citation", 0),
                "language": document.get("language", "Unknown"),
                "type": document.get("type", "Unknown"),
                "status": document.get("status", "Unknown"),
            }, total_input_tokens, total_output_tokens
        else:
            info, input_tokens, output_tokens = await self.publication_info(full_text)
            return {
                "source": document["source"],
                "title": document["title"],
                "url": document["url"],
                "full_text": full_text,
                "key_points": key_points,
                "authors": document["authors"] if ("authors" in document and document["authors"] not in [["Author"], [""]]) else info.authors,
                "year": document["year"] if ("year" in document and document["year"] not in ["Unknown", ""]) else info.year,
                "journal": document["journal"] if ("journal" in document and document["journal"] not in ["Unknown", ""]) else info.journal,
                "volume": document.get("volume", "Unknown"),
                "issue": document.get("issue", "Unknown"),
                "pages": document.get("pages", "Unknown"),
                "publisher": document["publisher"] if ("publisher" in document and document["publisher"] not in ["Unknown", ""]) else info.publisher,
                "q": document.get("q", "Unknown"),
                "citation": document.get("citation", 0),
                "language": document.get("language", info.language),
                "type": document.get("type", info.type),
                "status": document.get("status", info.status),
            }, total_input_tokens, total_output_tokens

    async def _ingest_documents(self, documents: list[dict], collection: str) -> tuple[int, list[str]]:
        docs: list[Document] = []
        
        for doc in documents:
            if "full_text" in doc:
                docs.append(
                    Document(
                        page_content=doc["full_text"], 
                        metadata={"source": doc["title"]}
                    ) 
                )
            if "key_points" in doc:
                docs.append(
                    Document(
                        page_content=json.dumps(doc["key_points"]), 
                        metadata={"source": f"{doc["title"]}_keypoints"}
                    ) 
                )
        chunked_documents = self.get_text_chunks(docs)
        if not chunked_documents:
            logger.info(f"[{self}] No chunks to ingest.")
            return 0, []
        uuids_batches = await self.batch_ingestion(chunked_documents, collection)
        return (
            await self.calculate_total_tokens_async(chunked_documents), 
            [uuid for batch in uuids_batches for uuid in batch]
        )
        
    def get_text_chunks(self, docs: list[Document]) -> list[Document]:
        try:
            text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
                model_name=settings.EMBEDDING_MODEL,
                chunk_size=1000,
                chunk_overlap=100,
            )
        except Exception:
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=1000,
                chunk_overlap=100,
            )
        return text_splitter.split_documents(docs)

    # async def update_user_report(self, uuids: list[str], new_content: str, title: str):
    #     self.delete_documents(uuids, self.collection_name + "_user")
    #     return await self.ingest_user_key_points(new_content, title)
    
    # async def delete_documents(self, uuids: list[str]):
        # store = QdrantVectorStore.from_existing_collection(
        #         url=settings.QDRANT_URL,
        #         collection_name=collection,
        #         embedding=self.embeddings
        #     )
        # await self.admin_user_guide.adelete(ids=uuids)
    
    # async def _get_text_from_image(self, file_path: Path) -> tuple[str, int, int]:
    #     """
    #     Encodes an image and uses the multimodal LLM to extract its content as text.
    #     """
    #     try:
    #         logger.info(f"Processing image file with LLM: {file_path.name}")
    #         with open(file_path, "rb") as image_file:
    #             image_bytes = image_file.read()

    #         base64_image = base64.b64encode(image_bytes).decode("utf-8")
    #         extension = file_path.suffix.lower().lstrip('.')
    #         mime_type = f"image/{'jpeg' if extension == 'jpg' else extension}"

    #         prompt = {
    #             "role": "user",
    #             "content": [
    #                 {
    #                     "type": "text",
    #                     "text": (
    #                         "You are an expert data extractor. "
    #                         "Analyze this image and extract all text and relevant information. "
    #                         "If it contains text, transcribe the text exactly as you see it. "
    #                         "If it is a chart or graph, describe its key findings, data points, and labels."
    #                     ),
    #                 },
    #                 {
    #                     "type": "image",
    #                     "source_type": "base64",
    #                     "data": base64_image,
    #                     "mime_type": mime_type,
    #                 },
    #             ]
    #         }

    #         response = await self.llm.ainvoke(prompt)
    #         input_tokens = response.usage_metadata.get("input_tokens", 0)
    #         output_tokens = response.usage_metadata.get("output_tokens", 0)
    #         return response.content, input_tokens, output_tokens
    #     except:
    #         return "", 0, 0
    async def _get_text_from_tabular(self, file_path: Path) -> str:
        """
        Reads a tabular file and converts it to string format.
        """
        logger.info(f"[{self}] Processing tabular file: {file_path.name}")
        if file_path.suffix.lower() == ".csv":
            df = pd.read_csv(file_path)
        elif file_path.suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(file_path)
        else:
            raise AIERROR(602, f"Invalid file type: {file_path.suffix}")
        return df.to_string(index=False, header=False)
    
    async def _get_text_from_word(self, file_path: Path) -> str:
        logger.info(f"[{self}] Processing word file: {file_path.name}")
        if file_path.suffix.lower() == ".docx":
            doc = docx.Document(file_path)
            content = []
            for docpara in doc.paragraphs:
                content.append(docpara.text)
            for table in doc.tables:
                for row in table.rows:
                    for cell in row.cells:
                        for paragraph in cell.paragraphs:
                            content.append(paragraph.text)
            return "\n\n".join(content)
        else:
            return f"Unsupported word format: {file_path.suffix}"

    async def _ingest_user_guide_docs(self, url: str) -> str:
        # url là "bucket/key" (không phải URL đầy đủ); key phải bỏ dấu '/' đầu
        minio_bucket, object_name = parse_minio_path(url)
        file_path = os.path.join(settings.TMP_FOLDER, object_name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        self.minioClient.fget_object(minio_bucket, object_name, file_path)
        path = Path(file_path)
        extension = path.suffix.lower()
        if extension == ".pdf":
            logger.info(f"[{self}] Reading pdf file: {path.name}")
            loader = PyPDFLoader(file_path)
            doc = await loader.aload()
            content = '\n\n'.join([page.page_content for page in doc])
        elif extension in [".txt", ".md"]:
            logger.info(f"[{self}] Reading text file: {path.name}")
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
        elif extension in [".docx"]:
            content = await self._get_text_from_word(path)
        elif extension in [".csv", ".xlsx", ".xls"]:
            content = await self._get_text_from_tabular(path)
        else:
            logger.error(f"Unsupported file type: {extension} for file {path.name}")
            content = f"Error: Unsupported file type '{extension}' for file {path.name}."
            return {
                "url": url,
                "embed_tokens": 0,
                "uuids": []
            }
        os.remove(file_path)
        user_guide = [
            {
                "full_text": content,
                "title": "user_guide",
            }
        ]
        embed_tokens, uuids = await self._ingest_documents(user_guide, f"{self.collection_admin}_user_guide")
        final_dict = {
            "url": url,
            "embed_tokens": embed_tokens,
            "uuids": uuids
        }
        return final_dict
    
    async def ingest_user_guide_docs(self, urls: list[str]) -> list[str]:
        tasks = [
            self._ingest_user_guide_docs(url) 
            for url in urls
        ]
        results = await asyncio.gather(*tasks)
        total_embed = sum([result["embed_tokens"] for result in results])
        return results, total_embed, settings.EMBEDDING_MODEL
