import asyncio
import json
import logging
import os
import sys
import uuid

# Đảm bảo import được các module từ root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from document_setup.src.configs.app import settings
from event_type import DocumentSetupEvent
import topics
from scripts._smoke_topics import model_id as smoke_model_id, topic

# Mặc định topic production (khớp docker-compose.yaml -> main.py).
# Thêm `--local` để test với topic *_local.
REQ_TOPIC = topic(topics.DOCUMENT_SETUP_REQUEST_LOCAL, topics.DOCUMENT_SETUP_REQUEST)
RES_TOPIC = topic(topics.DOCUMENT_SETUP_RESPONSE_LOCAL, topics.DOCUMENT_SETUP_RESPONSE)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


async def run_test():
    doc_id = f"test_{uuid.uuid4().hex[:8]}"
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    user_id = "test_user_001"

    request_payload = {
        "event_type": DocumentSetupEvent.SEARCH_PAPERS.value,
        "model_id": smoke_model_id(),
        "user_id": user_id,
        "document_id": doc_id,
        "session_id": session_id,
        "language": "Vietnamese",
        "field": "Kinh tế",
        "domain": "Thương mại điện tử",
        "subdomains": ["Livestream commerce"],
        "query": "E-commerce livestream consumer behavior",
        "country_filter": "both",
        "oa_filter": False,
        "is_oa": False,
        "is_tool": False,
        "proposal": {
            "title": "Nghiên cứu tác động của livestream bán hàng đến quyết định mua sắm"
        },
        "downloaded_papers": [],
        "cut_off_year_low": 2022,
        "cut_off_year_high": 2025,
        "search_range": "all",
        "use_web_search": False,
    }

    logger.info("==================================================================")
    logger.info("🚀 BẮT ĐẦU SMOKE TEST KAFKA CHO EVENT: search_papers")
    logger.info(f"   Kafka Broker: {settings.KAFKA}")
    logger.info(f"   Request Topic: {REQ_TOPIC}")
    logger.info(f"   Response Topic: {RES_TOPIC}")
    logger.info(f"   Document ID: {doc_id}")
    logger.info("==================================================================")

    # 1. Khởi tạo Consumer để nghe kết quả trả về
    consumer = AIOKafkaConsumer(
        RES_TOPIC,
        bootstrap_servers=settings.KAFKA,
        group_id=f"smoke_test_group_{uuid.uuid4().hex[:6]}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    await consumer.start()
    logger.info("✓ Consumer đã kết nối Kafka và sẵn sàng chờ kết quả...")

    # 2. Khởi tạo Producer để bắn message
    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA)
    await producer.start()

    try:
        # Gửi request
        payload_bytes = json.dumps(request_payload).encode("utf-8")
        logger.info("📤 Đang gửi message request lên Kafka...")
        await producer.send_and_wait(
            topic=REQ_TOPIC,
            value=payload_bytes,
            key=doc_id.encode("utf-8"),
        )
        logger.info("✓ Message đã được gửi thành công vào topic request!")

        # 3. Lắng nghe phản hồi từ response topic
        logger.info("⏳ Đang chờ phản hồi từ service autoresearching-doc (tối đa 60 giây)...")
        received = False
        start_time = asyncio.get_event_loop().time()

        while not received and (asyncio.get_event_loop().time() - start_time) < 60:
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)
                data = json.loads(msg.value.decode("utf-8"))
                
                # Kiểm tra xem có đúng message của test này không
                if data.get("document_id") == doc_id or data.get("session_id") == session_id:
                    received = True
                    logger.info("==================================================================")
                    logger.info("🎉 ĐÃ NHẬN ĐƯỢC RESPONSE THÀNH CÔNG TỪ KAFKA!")
                    logger.info("==================================================================")
                    logger.info(f"   Event Type: {data.get('event_type')}")
                    logger.info(f"   Document ID: {data.get('document_id')}")
                    
                    if "error_code" in data:
                        logger.error(f"   ✗ Error Code: {data.get('error_code')}")
                        logger.error(f"   ✗ Error Message: {data.get('error')}")
                    else:
                        papers = data.get("papers", [])
                        logger.info(f"   ✓ Số lượng bài báo tìm được: {len(papers)}")
                        logger.info(f"   ✓ Input Tokens: {data.get('input_tokens')}")
                        logger.info(f"   ✓ Output Tokens: {data.get('output_tokens')}")
                        
                        if papers:
                            logger.info("\n--- TOP 3 BÀI BÁO TIÊU BIỂU TÌM ĐƯỢC ---")
                            for idx, p in enumerate(papers[:3]):
                                logger.info(f"[{idx+1}] {p.get('title')}")
                                logger.info(f"    - Tiếng Việt: {p.get('user_language_title')}")
                                logger.info(f"    - Năm: {p.get('year')} | Tác giả: {', '.join(p.get('authors', [])[:2])}")
                                logger.info(f"    - Tạp chí: {p.get('journal')} (Xếp hạng Q: {p.get('q')})")
                                logger.info(f"    - URL: {p.get('url')}\n")
                        return True
            except asyncio.TimeoutError:
                elapsed = int(asyncio.get_event_loop().time() - start_time)
                logger.info(f"   ...vẫn đang chờ service xử lý (đã trôi qua {elapsed}s)...")

        if not received:
            logger.error("❌ Quá thời gian chờ (timeout 60s) mà không nhận được response!")
            return False

    finally:
        await producer.stop()
        await consumer.stop()


if __name__ == "__main__":
    success = asyncio.run(run_test())
    sys.exit(0 if success else 1)
