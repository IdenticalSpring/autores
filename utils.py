from aiokafka.admin import NewPartitions, NewTopic
from aiokafka.admin.client import AIOKafkaAdminClient
import blake3
import boto3
from botocore.client import Config
import httpx
import json
from langgraph.checkpoint.redis.aio import AsyncRedisSaver
# from langgraph.checkpoint.redis.ashallow import AsyncShallowRedisSaver
from langgraph.checkpoint.mongodb import AsyncMongoDBSaver
from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import QdrantVectorStore
from markdownify import MarkdownConverter
from minio import Minio
from pymongo import AsyncMongoClient
from redis.asyncio import Redis
import orjson
import os
import re
from pydantic import BaseModel
from typing import Any
import logging

from exception_type import AIERROR
from write_reports.src.configs.app import settings
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


class KeepImageMarkdownConverter(MarkdownConverter):
    def convert_img(self, el, text, *args, **kwargs):
        return str(el)


def markdownify_keep_images(html):
    return KeepImageMarkdownConverter().convert(html)


def hash_blake3(chunk: str) -> str:
    hasher = blake3.blake3()
    hasher.update(chunk.encode("utf-8"))
    return hasher.hexdigest()


def _clean_text(clean_text: str) -> str:
    """Original text cleaning function - kept for backward compatibility."""
    chars_to_remove = "''"""
    translation_table = str.maketrans('', '', chars_to_remove)
    clean_text = clean_text.translate(translation_table)
    clean_text = re.sub(r'\s+([,.])', r'\1', clean_text)
    clean_text = re.sub(r'[,\.\s]+(\.)', r'\1', clean_text)
    clean_text = re.sub(r'(,)(\s*[,])+', r'\1', clean_text)
    clean_text = re.sub(r'\s+([,.])', r'\1', clean_text)
    return clean_text


def _clean_text_enhanced(clean_text: str) -> str:
    """
    Enhanced text cleaning with statistical marker removal.

    This function extends the original _clean_text() with additional cleaning
    for statistical markers, diagnostic text, and other technical metadata
    that should not appear in final academic reports.

    Args:
        clean_text: Text to clean

    Returns:
        Cleaned text with markers and excessive whitespace removed
    """
    # Original cleaning - smart quotes
    chars_to_remove = "''"""
    translation_table = str.maketrans('', '', chars_to_remove)
    clean_text = clean_text.translate(translation_table)

    # Remove statistical markers
    statistical_patterns = [
        r': ``.*?``',  # Markers like ': ``value``'
        r'Cronbach\'?s?\s+Alpha\s+if\s+Item\s+Deleted',
        r'Scale\s+Mean\s+if\s+Item\s+Deleted',
        r'Scale\s+Variance\s+if\s+Item\s+Deleted',
        r'Corrected\s+Item-Total\s+Correlation',
        r'Squared\s+Multiple\s+Correlation',
    ]

    for pattern in statistical_patterns:
        clean_text = re.sub(pattern, '', clean_text, flags=re.IGNORECASE)

    # Original punctuation/spacing cleanup
    clean_text = re.sub(r'\s+([,.])', r'\1', clean_text)
    clean_text = re.sub(r'[,\.\s]+(\.)', r'\1', clean_text)
    clean_text = re.sub(r'(,)(\s*[,])+', r'\1', clean_text)
    clean_text = re.sub(r'\s+([,.])', r'\1', clean_text)

    # Normalize excessive whitespace
    clean_text = re.sub(r'\n{3,}', '\n\n', clean_text)
    clean_text = re.sub(r' {2,}', ' ', clean_text)

    return clean_text


def serializer(obj: Any):
    if obj is None or isinstance(obj, bytes):
        return obj
    if isinstance(obj, str):
        return obj.encode()
    if isinstance(obj, BaseModel):
        return obj.model_dump_json().encode()
    return orjson.dumps(obj)


async def set_num_partitions(
    bootstrap_servers: str,
    topics: list[str],
    n_partitions: int,
):
    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
    )
    await admin_client.start()
    try:
        existing_topics = await admin_client.list_topics()
        for topic in topics:
            if topic in existing_topics:
                metadata = await admin_client.describe_topics([topic])
                current_partition_count = len(metadata[0]["partitions"])
                if current_partition_count < n_partitions:
                    await admin_client.create_partitions(
                        {
                            topic: NewPartitions(n_partitions)
                        }
                    )
            else:
                await admin_client.create_topics(
                    new_topics=[NewTopic(name=topic, num_partitions=n_partitions, replication_factor=1)],
                    validate_only=False,
                )
    finally:
        await admin_client.close()


_MONGODB_CLIENT: AsyncMongoClient | None = None


def get_mongodb_client() -> AsyncMongoClient:
    global _MONGODB_CLIENT
    if _MONGODB_CLIENT is None:
        _MONGODB_CLIENT = AsyncMongoClient(settings.MONGO_CONNECTION_STRING)
        logger.info(f"Connected to MongoDB at {settings.MONGO_CONNECTION_STRING}")
    return _MONGODB_CLIENT


_REDIS_CLIENT: Redis | None = None


def get_redis_client() -> Redis:
    global _REDIS_CLIENT
    if _REDIS_CLIENT is None:
        _REDIS_CLIENT = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PWD, db=0)
        logger.info(f"Connected to Redis at {settings.REDIS_HOST}")
    return _REDIS_CLIENT


_MINIO_CLIENT: Minio | None = None


def get_minio_client() -> Minio:
    global _MINIO_CLIENT
    if _MINIO_CLIENT is None:
        _MINIO_CLIENT = Minio(
            endpoint=f"{settings.MINIO_ENDPOINT}:{settings.MINIO_PORT}",
            access_key=settings.MINIO_ACCESS_KEY_ID,
            secret_key=settings.MINIO_SECRET_ACCESS_KEY,
            secure=False
        )
        logger.info(f"Connected to Minio at {settings.MINIO_ENDPOINT}")
    return _MINIO_CLIENT


_REDIS_STACK_CLIENT: Redis | None = None


def get_redis_stack_client() -> Redis:
    global _REDIS_STACK_CLIENT
    if _REDIS_STACK_CLIENT is None:
        _REDIS_STACK_CLIENT = Redis.from_url(settings.REDIS_STACK)
        logger.info(f"Connected to Redis stack at {settings.REDIS_STACK}")
    return _REDIS_STACK_CLIENT


CHECKPOINTER_MONGO: AsyncMongoDBSaver | None = None


async def init_checkpointer_mongo():
    global CHECKPOINTER_MONGO
    client = get_mongodb_client()
    CHECKPOINTER_MONGO = AsyncMongoDBSaver(
        client=client,
        db_name="BotGraph",
        checkpoint_collection_name="BotCheckpoint",
        writes_collection_name="BotWrite",
    )
    return CHECKPOINTER_MONGO

S3_CLIENT: None = None


def get_s3_client():
    global S3_CLIENT
    if S3_CLIENT is None:
        S3_CLIENT = boto3.client(
            's3',
            endpoint_url=settings.MINIO_DOMAIN,
            aws_access_key_id=settings.MINIO_ACCESS_KEY_ID,
            aws_secret_access_key=settings.MINIO_SECRET_ACCESS_KEY,
            config=Config(signature_version='s3v4')
        )
    return S3_CLIENT


async def get_async_mongo_checkpoint():
    global CHECKPOINTER_MONGO
    if CHECKPOINTER_MONGO is None:
        logger.info("Initializing MongoDB checkpoint saver")
        os.environ["MONGO_URI"] = settings.MONGO_CONNECTION_STRING
        return await init_checkpointer_mongo()
    return CHECKPOINTER_MONGO

CHECKPOINTER_REDIS: AsyncRedisSaver | None = None


async def init_checkpointer_redis():
    global CHECKPOINTER_REDIS
    client = get_redis_stack_client()
    try:
        CHECKPOINTER_REDIS = AsyncRedisSaver(
            redis_client=client,
        )
        await CHECKPOINTER_REDIS.asetup()
        logger.info("Redis checkpoint saver initialized successfully")
        return CHECKPOINTER_REDIS
    except Exception as e:
        logger.error(f"Failed to initialize Redis checkpoint saver: {type(e).__name__}: {str(e)}")
        logger.error(f"Exception args: {e.args}")
        raise


async def get_async_redis_checkpoint():
    global CHECKPOINTER_REDIS
    if CHECKPOINTER_REDIS is None:
        logger.info("Initializing Redis checkpoint saver")
        os.environ["REDIS_URL"] = settings.REDIS_STACK
        return await init_checkpointer_redis()
    return CHECKPOINTER_REDIS

def get_embeddings(db_key: str = "", model_name: str = ""):
    """
    Trả về instance Embeddings phù hợp:
    1. Nếu EMBEDDING_MODEL là BAAI/bge... hoặc không có db_key/OPEN_AI_KEY: dùng FastEmbed chạy local 100% miễn phí.
    2. Nếu EMBEDDING_MODEL là Gemini (models/...): dùng GoogleGenerativeAIEmbeddings.
    3. Mặc định nếu có OpenAI key: dùng OpenAIEmbeddings.
    """
    effective_model = model_name or settings.EMBEDDING_MODEL or "BAAI/bge-small-en-v1.5"
    key = db_key or settings.OPEN_AI_KEY

    if "bge" in effective_model.lower() or "fastembed" in effective_model.lower() or not key or key.startswith("DUMMY") or not key.startswith("sk-"):
        from langchain_community.embeddings.fastembed import FastEmbedEmbeddings
        fastembed_model = effective_model if "bge" in effective_model.lower() else "BAAI/bge-small-en-v1.5"
        return FastEmbedEmbeddings(model_name=fastembed_model)
    elif "models/" in effective_model.lower() or "gemini" in effective_model.lower():
        from langchain_google_genai import GoogleGenerativeAIEmbeddings
        return GoogleGenerativeAIEmbeddings(
            model=effective_model,
            google_api_key=settings.GEMINI_KEY
        )
    else:
        return OpenAIEmbeddings(
            model=effective_model,
            openai_api_key=key,
        )


ADMIN_VECTOR_STORE = None


async def get_admin_vector_db(db_key: str):
    global ADMIN_VECTOR_STORE
    if ADMIN_VECTOR_STORE is None:
        embeddings = get_embeddings(db_key)
        try:
            from qdrant_client import AsyncQdrantClient
            from qdrant_client.models import VectorParams, Distance
            qc = AsyncQdrantClient(url=settings.QDRANT_URL)
            if not await qc.collection_exists("admin_user_guide"):
                dim = 384 if not db_key or db_key.startswith("DUMMY") or not db_key.startswith("sk-") else 1536
                await qc.create_collection(
                    collection_name="admin_user_guide",
                    vectors_config=VectorParams(size=dim, distance=Distance.COSINE)
                )
            await qc.close()
        except Exception as e:
            logger.warning(f"Could not verify/create admin_user_guide collection: {e}")

        ADMIN_VECTOR_STORE = QdrantVectorStore.from_existing_collection(
            url=settings.QDRANT_URL,
            collection_name="admin_user_guide",
            embedding=embeddings
        )
    return ADMIN_VECTOR_STORE


async def init_singleton() -> None:
    _ = get_mongodb_client() 
    _ = get_redis_client()
    _ = get_minio_client() 
    _ = get_s3_client()
    _ = await get_async_mongo_checkpoint()
    _ = await get_async_redis_checkpoint()


async def send_health_check(
    id: str, 
    status: int, 
    error: int | None = None, 
    error_message: str | None = None, 
    ttl: int = 300, 
    event_type: str = "", 
    topic: str = ""
):
    if id and id != "admin":
        # get_redis_client() is a SYNC factory returning an ASYNC Redis client and it
        # initialises the client on first use. Do not `await` it (previous code did).
        redis_client = get_redis_client()
        url = f"http://{settings.REDIS_HOST}:4002/health-check/{id}"
        headers = {
            "accept": "application/json",
            "x-api-key": settings.API_KEY_ID,
            "Content-Type": "application/json"
        }
        data = {
            "eventType": event_type,
            "processStatus": status,
            "aiError": {
                "errorCode": error,
                "error": error_message
            }
        }
        logger.info(f"{event_type} - {topic} - {id} - {data}")
        key = f"AI_STATUS:{id}"

        # Only call the BE webhook when processStatus actually changed.
        previous_status = None
        try:
            status_bytes = await redis_client.get(key)
            if status_bytes is not None:
                previous_status = json.loads(status_bytes).get("processStatus")
        except Exception as read_error:
            logger.error(f"[health-check] Cannot read previous status for {key}: {read_error}")

        if previous_status != status:
            limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
            transport = httpx.AsyncHTTPTransport(retries=3)
            async with httpx.AsyncClient(limits=limits, transport=transport, timeout=(3.0, 5.0)) as client:
                try:
                    response = await client.put(url, headers=headers, json=data)
                    response.raise_for_status()
                except Exception as e:
                    logger.error(AIERROR(600, str(e)))
        await redis_client.set(key, json.dumps(data), ex=ttl)