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
from event_type import WriteEvent
import topics
from scripts._smoke_topics import model_id as smoke_model_id, topic

# Mặc định topic production (khớp docker-compose.yaml -> main.py). `--local` để dùng topic *_local.
REQ_TOPIC = topic(topics.WRITE_SECTION_REQUEST_LOCAL, topics.WRITE_SECTION_REQUEST)
RES_TOPIC = topic(topics.WRITE_SECTION_RESPONSE_LOCAL, topics.WRITE_SECTION_RESPONSE)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


async def run_test():
    doc_id = f"test_slide_{uuid.uuid4().hex[:8]}"
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    user_id = "test_user_write"

    request_payload = {
        "event_type": WriteEvent.GEN_SLIDES.value,
        "model_id": smoke_model_id(),
        "user_id": user_id,
        "document_id": doc_id,
        "session_id": session_id,
        "language": "Vietnamese",
        "manual_mode": False,
        "resume_error": False,
        "user_preference": 1,
        "raw_report": "Nghiên cứu về ứng dụng của trí tuệ nhân tạo trong tự động hóa nghiên cứu khoa học. Báo cáo phân tích các mô hình ngôn ngữ lớn và tiềm năng ứng dụng.",
    }

    logger.info("==================================================================")
    logger.info("🚀 BẮT ĐẦU SMOKE TEST KAFKA CHO SERVICE: WRITE (gen_slides)")
    logger.info(f"   Kafka Broker: {settings.KAFKA}")
    logger.info(f"   Request Topic: {REQ_TOPIC}")
    logger.info(f"   Response Topic: {RES_TOPIC}")
    logger.info(f"   Document ID: {doc_id}")
    logger.info("==================================================================")

    # 1. Khởi tạo Consumer để nghe kết quả trả về
    consumer = AIOKafkaConsumer(
        RES_TOPIC,
        bootstrap_servers=settings.KAFKA,
        group_id=f"smoke_test_write_{uuid.uuid4().hex[:6]}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    # 2. Khởi tạo Producer để gửi request
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await consumer.start()
    await producer.start()

    try:
        logger.info(f"📤 Gửi request test vào topic '{REQ_TOPIC}'...")
        await producer.send_and_wait(
            topic=REQ_TOPIC,
            key=doc_id.encode("utf-8"),
            value=request_payload,
        )
        logger.info("⏳ Đã gửi message thành công! Đang chờ response từ Write Service (tối đa 60s)...")

        start_wait = asyncio.get_event_loop().time()
        timeout_seconds = 60.0

        while True:
            remaining = timeout_seconds - (asyncio.get_event_loop().time() - start_wait)
            if remaining <= 0:
                raise asyncio.TimeoutError("Timeout chờ kết quả từ Kafka")

            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=remaining)
                res_data = msg.value
                logger.info(f"📥 Nhận được response từ partition {msg.partition}, offset {msg.offset}")

                if res_data.get("document_id") == doc_id or res_data.get("event_type") == WriteEvent.GEN_SLIDES.value:
                    logger.info("==================================================================")
                    logger.info("✅ TEST THÀNH CÔNG! WRITE SERVICE PHẢN HỒI KẾT QUẢ:")
                    logger.info(json.dumps(res_data, indent=2, ensure_ascii=False))
                    logger.info("==================================================================")
                    return True
            except asyncio.TimeoutError:
                raise

    except Exception as e:
        logger.error(f"❌ Test thất bại: {e}")
        return False
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    success = asyncio.run(run_test())
    sys.exit(0 if success else 1)
