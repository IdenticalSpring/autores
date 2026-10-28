import asyncio
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
from utils import get_mongodb_client, get_embeddings
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from document_setup.src.configs.app import settings


DOC_ID = "smoke-outline-refs-1"


async def main():
    print(f"--- Seeding fixture for {DOC_ID} ---")
    
    # 1. MongoDB: user_documents.reports_refs
    mongo = get_mongodb_client()
    await mongo.aconnect()
    user_db = mongo["user_documents"]
    await user_db["reports_refs"].update_one(
        {"_id": DOC_ID},
        {"$set": {
            "_id": DOC_ID,
            "documents": [
                {
                    "title": "Nghiên cứu về Livestream bán hàng và hành vi mua sắm",
                    "key_points": {
                        "main_topic": "Livestream bán hàng",
                        "background": "Thương mại điện tử Việt Nam",
                        "importance": "Tăng doanh số và tương tác",
                        "research_gap": "Chưa rõ vai trò của độ tin cậy người bán",
                        "problem": "Hành vi mua ngẫu hứng",
                        "objectives": "Xác định các yếu tố ảnh hưởng",
                        "scope": "Người tiêu dùng trực tuyến tại TP.HCM",
                        "methodology": "Khảo sát định lượng SEM",
                        "data": "400 mẫu khảo sát",
                        "analysis": "SmartPLS",
                        "results": "Độ tin cậy và sự khan hiếm tác động tích cực",
                        "main_conclusions": "Livestream thúc đẩy quyết định mua",
                        "contributions": "Mô hình thực nghiệm",
                        "future_directions": "Mở rộng nền tảng TikTok"
                    },
                    "source": "USER",
                    "ref_id": "ref-paper-1"
                }
            ]
        }},
        upsert=True
    )
    print("MongoDB user_documents.reports_refs seeded")

    # 2. MongoDB: admin.reports_refs_chunks
    admin_db = mongo["admin"]
    await admin_db["reports_refs_chunks"].update_one(
        {"document_id": DOC_ID, "chunk_id": "chunk-1"},
        {"$set": {
            "document_id": DOC_ID,
            "chunk_id": "chunk-1",
            "text": "Livestream bán hàng đang trở thành xu hướng chủ đạo trong thương mại điện tử hiện đại.",
            "paper_title": "Nghiên cứu về Livestream bán hàng và hành vi mua sắm"
        }},
        upsert=True
    )
    print("MongoDB admin.reports_refs_chunks seeded")

    # 3. Qdrant: collection DOC_ID
    qdrant = AsyncQdrantClient(url=settings.QDRANT_URL)
    collections = await qdrant.get_collections()
    exists = any(c.name == DOC_ID for c in collections.collections)
    if not exists:
        await qdrant.create_collection(
            collection_name=DOC_ID,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE)
        )
        print(f"Qdrant collection {DOC_ID} created")
    
    # Embed text into Qdrant point
    emb = get_embeddings("DUMMY_KEY")
    vec = emb.embed_query("Livestream bán hàng đang trở thành xu hướng chủ đạo trong thương mại điện tử hiện đại.")
    await qdrant.upsert(
        collection_name=DOC_ID,
        points=[
            PointStruct(
                id=1,
                vector=vec,
                payload={
                    "page_content": "Livestream bán hàng đang trở thành xu hướng chủ đạo trong thương mại điện tử hiện đại.",
                    "metadata": {
                        "paper_title": "Nghiên cứu về Livestream bán hàng và hành vi mua sắm",
                        "chunk_id": "chunk-1"
                    }
                }
            )
        ]
    )
    print("Qdrant point inserted")
    print("--- Fixture seeded successfully ---")


if __name__ == "__main__":
    asyncio.run(main())
