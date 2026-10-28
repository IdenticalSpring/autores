"""Script seed toàn bộ fixture cho bước P2 (Artifact Verification).

Tạo:
1. Dữ liệu khảo sát mẫu (35 dòng, 2 biến định lượng `tuoi`, `thu_nhap` + 1 định tính `gioi_tinh` + `chi_tieu`).
2. Upload CSV lên MinIO.
3. Seed MongoDB:
   - `admin.document_configurations`
   - `admin.articles`
   - `admin.proposal_titles`
   - `user_documents.reports_refs`
   - `admin.reports_refs_chunks`
4. Seed Qdrant: collection `650000000000000000000001` kèm vector point.
"""

import asyncio
import io
import os
import sys

import boto3
from bson import ObjectId
import pandas as pd
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from document_setup.src.configs.app import settings
from utils import get_embeddings, get_mongodb_client

DOC_ID = "650000000000000000000001"
TITLE_ID = "650000000000000000000002"
USER_ID = "smoke-user"

# 35 mẫu khảo sát: 2 biến định lượng (tuoi, thu_nhap), 1 biến định tính (gioi_tinh), + chi_tieu
SURVEY_DATA = {
    "tuoi": [21, 25, 23, 28, 30, 22, 24, 26, 29, 31, 35, 27, 24, 22, 33, 40, 28, 26, 23, 25, 27, 32, 34, 22, 21, 29, 30, 24, 26, 28, 31, 33, 25, 27, 36],
    "thu_nhap": [8.5, 12.0, 10.5, 15.0, 20.0, 9.0, 11.5, 14.0, 18.5, 22.0, 25.0, 16.0, 11.0, 9.5, 21.0, 30.0, 17.5, 13.0, 10.0, 12.5, 15.0, 22.5, 24.0, 9.0, 8.0, 19.0, 20.5, 12.0, 14.5, 16.5, 21.0, 23.5, 13.0, 15.5, 28.0],
    "gioi_tinh": ["Nam", "Nu", "Nu", "Nam", "Nu", "Nam", "Nu", "Nam", "Nu", "Nam", "Nu", "Nam", "Nu", "Nu", "Nam", "Nam", "Nu", "Nam", "Nu", "Nam", "Nu", "Nam", "Nu", "Nam", "Nu", "Nam", "Nu", "Nam", "Nu", "Nam", "Nu", "Nam", "Nu", "Nam", "Nu"],
    "chi_tieu": [3.0, 5.0, 4.0, 6.5, 8.0, 3.5, 4.5, 6.0, 7.5, 9.0, 10.5, 7.0, 4.5, 3.8, 8.5, 12.0, 7.0, 5.5, 4.0, 5.0, 6.0, 9.0, 9.5, 3.5, 3.2, 7.8, 8.2, 5.0, 5.8, 6.8, 8.5, 9.5, 5.2, 6.2, 11.0],
}


def seed_minio_csv():
    df = pd.DataFrame(SURVEY_DATA)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{settings.MINIO_ENDPOINT}:{settings.MINIO_PORT}",
        aws_access_key_id=settings.MINIO_ACCESS_KEY_ID,
        aws_secret_access_key=settings.MINIO_SECRET_ACCESS_KEY,
    )
    # Upload vào bucket users và autoresearching
    keys = [
        ("users", f"{USER_ID}/article/{DOC_ID}/survey_sample_35rows.csv"),
        ("autoresearching", f"users/{USER_ID}/article/{DOC_ID}/survey_sample_35rows.csv"),
        ("analysis", f"{USER_ID}/article/{DOC_ID}/survey_sample_35rows.csv"),
    ]
    for bucket, key in keys:
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=csv_bytes, ContentType="text/csv")
            print(f"[MinIO] Uploaded s3://{bucket}/{key} ({len(csv_bytes)} bytes)")
        except Exception as e:
            raise RuntimeError(f"Lỗi upload MinIO s3://{bucket}/{key}: {e}") from e


async def seed_mongo():
    mongo = get_mongodb_client()
    await mongo.aconnect()
    admin_db = mongo["admin"]

    # 1. admin.document_configurations
    await admin_db["document_configurations"].update_one(
        {"documentId": DOC_ID},
        {"$set": {
            "documentId": DOC_ID, "createdBy": USER_ID, "finalProposal": ObjectId(TITLE_ID),
            "keywords": {"mainKeywords": ["livestream bán hàng", "quyết định mua sắm"], "subKeywords": ["thương mại điện tử", "độ tin cậy"]},
            "referencesStyle": "APA", "researchType": {"type": 1, "reason": "Nghiên cứu định lượng khảo sát hành vi mua sắm qua livestream"},
        }},
        upsert=True,
    )
    print(f"[Mongo] admin.document_configurations seeded for {DOC_ID}")

    # 2. admin.articles
    await admin_db["articles"].update_one(
        {"_id": ObjectId(DOC_ID)},
        {"$set": {
            "_id": ObjectId(DOC_ID), "title": ObjectId(TITLE_ID),
            "docsPrepare": {"field": "Kinh tế", "domain": "Thương mại điện tử", "subDomain": "Livestream bán hàng"},
        }},
        upsert=True,
    )
    print(f"[Mongo] admin.articles seeded for {DOC_ID}")

    # 3. admin.proposal_titles
    await admin_db["proposal_titles"].update_one(
        {"_id": ObjectId(TITLE_ID)},
        {
            "$set": {
                "_id": ObjectId(TITLE_ID),
                "title": "Tác động của livestream bán hàng đến quyết định mua sắm của người tiêu dùng",
                "problemStatement": "Chưa rõ các yếu tố ảnh hưởng đến quyết định mua sắm trên livestream",
                "motivation": "Sự phát triển bùng nổ của thương mại điện tử qua video trực tiếp",
                "webSearch": "Xu hướng livestream thương mại điện tử 2025",
                "researchGap": "Thiếu các nghiên cứu thực nghiệm định lượng tại thị trường Việt Nam",
                "hypothesis": [
                    "H1: Độ tin cậy của người bán tác động tích cực đến quyết định mua",
                    "H2: Tính tương tác tác động tích cực đến quyết định mua",
                ],
                "variables": "tuoi, thu_nhap, gioi_tinh, chi_tieu",
                "finalModel": "Quyết định mua = f(Độ tin cậy, Sự khan hiếm, Tương tác)",
                "surveyQuestions": "Khảo sát ý kiến người tiêu dùng trực tuyến",
                "questions": "Những yếu tố nào thúc đẩy quyết định mua?",
            }
        },
        upsert=True,
    )
    print(f"[Mongo] admin.proposal_titles seeded for {TITLE_ID}")

    # 4. user_documents.reports_refs
    user_db = mongo["user_documents"]
    await user_db["reports_refs"].update_one(
        {"_id": DOC_ID},
        {"$set": {
            "_id": DOC_ID,
            "documents": [{
                "title": "Nghiên cứu về Livestream bán hàng và hành vi mua sắm",
                "authors": "Nguyen Van A, Tran Van B", "year": 2024,
                "key_points": {
                    "main_topic": "Livestream bán hàng", "background": "Thương mại điện tử Việt Nam",
                    "importance": "Tăng doanh số và tương tác", "research_gap": "Chưa rõ vai trò của độ tin cậy người bán",
                    "problem": "Hành vi mua ngẫu hứng", "objectives": "Xác định các yếu tố ảnh hưởng đến quyết định mua qua livestream",
                    "scope": "Người tiêu dùng trực tuyến tại TP.HCM", "methodology": "Khảo sát định lượng SEM",
                    "data": "400 mẫu khảo sát", "analysis": "SmartPLS",
                    "results": "Độ tin cậy và sự khan hiếm tác động tích cực đến quyết định mua",
                    "variables": "Độ tin cậy, Sự khan hiếm, Tính tương tác, Quyết định mua",
                    "hypotheses": "H1: Độ tin cậy tác động tích cực; H2: Sự khan hiếm tác động tích cực",
                    "measurement_items": "Thang đo Likert 5 điểm", "model_design": "Mô hình hồi quy đa biến và SEM",
                    "main_conclusions": "Livestream thúc đẩy quyết định mua", "contributions": "Mô hình thực nghiệm",
                    "future_directions": "Mở rộng nền tảng TikTok",
                },
                "source": "USER", "ref_id": "ref-paper-1",
            }],
        }},
        upsert=True,
    )
    print(f"[Mongo] user_documents.reports_refs seeded for {DOC_ID}")

    # 5. admin.reports_refs_chunks
    await admin_db["reports_refs_chunks"].update_one(
        {"document_id": DOC_ID, "chunk_id": "chunk-1"},
        {
            "$set": {
                "document_id": DOC_ID,
                "chunk_id": "chunk-1",
                "text": "Livestream bán hàng đang trở thành xu hướng chủ đạo trong thương mại điện tử hiện đại tại Việt Nam.",
                "paper_title": "Nghiên cứu về Livestream bán hàng và hành vi mua sắm",
            }
        },
        upsert=True,
    )
    print(f"[Mongo] admin.reports_refs_chunks seeded for {DOC_ID}")


async def seed_qdrant():
    qdrant = AsyncQdrantClient(url=settings.QDRANT_URL)
    collections = await qdrant.get_collections()
    exists = any(c.name == DOC_ID for c in collections.collections)
    if not exists:
        await qdrant.create_collection(
            collection_name=DOC_ID,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE),
        )
        print(f"[Qdrant] Collection {DOC_ID} created")

    emb = get_embeddings("DUMMY_KEY")
    text = "Livestream bán hàng đang trở thành xu hướng chủ đạo trong thương mại điện tử hiện đại tại Việt Nam."
    vec = emb.embed_query(text)
    await qdrant.upsert(
        collection_name=DOC_ID,
        points=[
            PointStruct(
                id=1,
                vector=vec,
                payload={
                    "page_content": text,
                    "metadata": {
                        "paper_title": "Nghiên cứu về Livestream bán hàng và hành vi mua sắm",
                        "chunk_id": "chunk-1",
                    },
                },
            )
        ],
    )
    print(f"[Qdrant] Point inserted into {DOC_ID}")


async def cleanup_fixtures():
    """Xoá toàn bộ dữ liệu test P2 (MinIO CSV, MongoDB documents, Qdrant collection)."""
    print("=== CLEANING UP P2 FIXTURES ===")
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{settings.MINIO_ENDPOINT}:{settings.MINIO_PORT}",
        aws_access_key_id=settings.MINIO_ACCESS_KEY_ID,
        aws_secret_access_key=settings.MINIO_SECRET_ACCESS_KEY,
    )
    for b, k in [
        ("users", f"{USER_ID}/article/{DOC_ID}/survey_sample_35rows.csv"),
        ("autoresearching", f"users/{USER_ID}/article/{DOC_ID}/survey_sample_35rows.csv"),
        ("analysis", f"{USER_ID}/article/{DOC_ID}/survey_sample_35rows.csv"),
    ]:
        try:
            s3.delete_object(Bucket=b, Key=k)
            print(f"[MinIO] Deleted s3://{b}/{k}")
        except Exception as e:
            print(f"[MinIO] Warn: {e}")

    mongo = get_mongodb_client()
    await mongo.aconnect()
    await mongo["admin"]["document_configurations"].delete_one({"documentId": DOC_ID})
    await mongo["admin"]["articles"].delete_one({"_id": ObjectId(DOC_ID)})
    await mongo["admin"]["proposal_titles"].delete_one({"_id": ObjectId(TITLE_ID)})
    await mongo["user_documents"]["reports_refs"].delete_one({"_id": DOC_ID})
    await mongo["admin"]["reports_refs_chunks"].delete_many({"document_id": DOC_ID})
    print(f"[Mongo] Deleted test documents for {DOC_ID}")

    qdrant = AsyncQdrantClient(url=settings.QDRANT_URL)
    try:
        colls = await qdrant.get_collections()
        if any(c.name == DOC_ID for c in colls.collections):
            await qdrant.delete_collection(collection_name=DOC_ID)
            print(f"[Qdrant] Collection {DOC_ID} deleted")
    except Exception as e:
        print(f"[Qdrant] Warn: {e}")
    print("=== HOÀN TẤT CLEANUP P2 ===")


async def main():
    print("=== SEEDING FIXTURES CHO BƯỚC P2 ===")
    seed_minio_csv()
    await seed_mongo()
    await seed_qdrant()
    print("=== HOÀN TẤT SEEDING P2 ===")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--cleanup":
        asyncio.run(cleanup_fixtures())
    else:
        asyncio.run(main())
