import asyncio
import json
import logging
import os
import sys

# Đảm bảo import được các module từ root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from document_setup.src.configs.app import settings
from redis.asyncio import Redis
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


async def seed_redis():
    logger.info("--- [1/4] Khởi tạo dữ liệu Redis ---")
    host = settings.REDIS_HOST.strip("\"'") if settings.REDIS_HOST else "localhost"
    pwd = settings.REDIS_PWD.strip("\"'") if settings.REDIS_PWD else None
    r = Redis(
        host=host,
        port=settings.REDIS_PORT or 6379,
        password=pwd,
        decode_responses=True
    )
    
    # Danh sách model_id phổ biến cần seed key vào Redis
    gemini_key = (settings.GEMINI_KEY or "").strip("\"'") or "DUMMY_GEMINI_KEY"
    openai_key = (settings.OPEN_AI_KEY or "").strip("\"'") or "DUMMY_OPENAI_KEY"
    serp_key = (settings.SERP_KEY or "").strip("\"'") or "DUMMY_SERP_KEY"

    keys_to_set = {
        "AI_KEY:db_key": openai_key,
        "AI_KEY:search_web_key": serp_key,
        "AI_KEY:gemini-2.5-flash": gemini_key,
        "AI_KEY:gemini-1.5-flash": gemini_key,
        "AI_KEY:gemini-1.5-pro": gemini_key,
        "AI_KEY:gemini-2.0-flash": gemini_key,
        "AI_KEY:gpt-4o": openai_key,
        "AI_KEY:gpt-4o-mini": openai_key,
        "AI_KEY:localhost": "None",
        "AI_KEY:groq": "None",
        "AI_KEY:default": gemini_key or openai_key,
    }

    for k, v in keys_to_set.items():
        # listener.py get_key() mong đợi JSON string: json.loads(bytes)
        await r.set(k, json.dumps(v))
        logger.info(f"  ✓ Đã nạp Redis key: {k}")

    total_keys = len(await r.keys("AI_KEY:*"))
    logger.info(f"Tổng số AI_KEY trong Redis: {total_keys}")
    await r.aclose()


def seed_minio():
    logger.info("--- [2/4] Khởi tạo MinIO Buckets ---")
    endpoint = (settings.MINIO_ENDPOINT or "localhost").strip("\"'")
    port = settings.MINIO_PORT or 9000
    access_key = settings.MINIO_ACCESS_KEY_ID.strip("\"'")
    secret_key = settings.MINIO_SECRET_ACCESS_KEY.strip("\"'")
    client = Minio(
        f"{endpoint}:{port}",
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )

    bucket_main = (settings.MINIO_BUCKET or "autoresearching").strip("\"'")
    bucket_analysis = (settings.MINIO_BUCKET_ANALYSIS or "analysis").strip("\"'")
    buckets = [bucket_main, bucket_analysis, "users", "documents"]

    for bucket in set(buckets):
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                logger.info(f"  ✓ Đã tạo bucket mới: {bucket}")
            else:
                logger.info(f"  - Bucket đã tồn tại: {bucket}")
        except S3Error as e:
            logger.error(f"  ✗ Lỗi tạo bucket {bucket}: {e}")


def check_mongo():
    logger.info("--- [3/4] Kiểm tra kết nối MongoDB ---")
    mongo_uri = (settings.MONGO_CONNECTION_STRING or "").strip("\"'")
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        res = client.admin.command('ping')
        logger.info(f"  ✓ MongoDB kết nối thành công: {res}")
    except Exception as e:
        logger.error(f"  ✗ Lỗi kết nối MongoDB: {e}")


async def check_qdrant():
    logger.info("--- [4/4] Kiểm tra Qdrant ---")
    qdrant_url = (settings.QDRANT_URL or "http://localhost:6333").strip("\"'")
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            res = await client.get(f"{qdrant_url}/readyz")
            logger.info(f"  ✓ Qdrant readyz: {res.status_code} - {res.text.strip()}")
        except Exception as e:
            logger.error(f"  ✗ Lỗi kết nối Qdrant: {e}")


async def main():
    logger.info("🚀 BẮT ĐẦU SEED DỮ LIỆU VÀ KIỂM TRA HẠ TẦNG LOCAL")
    await seed_redis()
    seed_minio()
    check_mongo()
    await check_qdrant()
    logger.info("✅ HOÀN THÀNH SEED DỮ LIỆU THÀNH CÔNG!")


if __name__ == "__main__":
    asyncio.run(main())
