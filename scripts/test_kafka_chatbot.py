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
from event_type import AIEvent
import topics
from scripts._smoke_topics import model_id as smoke_model_id, topic

# Mặc định topic production (khớp docker-compose.yaml -> main.py). `--local` để dùng topic *_local.
REQ_TOPIC = topic(topics.CHATBOT_REQUEST_LOCAL, topics.CHATBOT_REQUEST)
RES_TOPIC = topic(topics.CHATBOT_RESPONSE_LOCAL, topics.CHATBOT_RESPONSE)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


async def run_test():
    conv_id = f"conv_{uuid.uuid4().hex[:8]}"
    user_id = "test_user_chat"

    request_payload = {
        "event_type": AIEvent.OUTER_CHATBOT.value,
        "model_id": smoke_model_id(),
        "user_id": user_id,
        "conv_id": conv_id,
        "language": "Vietnamese",
        "question": "Hãy giải thích ngắn gọn trong 2 câu sự khác biệt giữa nghiên cứu định tính và định lượng?",
        "chat_history": [],
        "short_answer": True,
        "direct_answer": True,
        "use_web_search_chatbot": False,
    }

    logger.info("==================================================================")
    logger.info("🚀 BẮT ĐẦU SMOKE TEST KAFKA CHO EVENT: outer_chatbot")
    logger.info(f"   Kafka Broker: {settings.KAFKA}")
    logger.info(f"   Request Topic: {REQ_TOPIC}")
    logger.info(f"   Response Topic: {RES_TOPIC}")
    logger.info(f"   Conversation ID: {conv_id}")
    logger.info("==================================================================")

    # 1. Khởi tạo Consumer để nghe kết quả trả về
    consumer = AIOKafkaConsumer(
        RES_TOPIC,
        bootstrap_servers=settings.KAFKA,
        group_id=f"smoke_test_chat_{uuid.uuid4().hex[:6]}",
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
    logger.info("✓ Consumer đã kết nối Kafka và sẵn sàng chờ kết quả...")

    try:
        # Gửi message request
        logger.info("📤 Đang gửi tin nhắn câu hỏi lên Kafka...")
        await producer.send_and_wait(
            REQ_TOPIC,
            key=conv_id.encode("utf-8"),
            value=request_payload,
        )
        logger.info("✓ Tin nhắn đã được gửi thành công vào topic chatbot request!")

        # Chờ nhận response
        logger.info("⏳ Đang chờ AI Chatbot trả lời (tối đa 60 giây)...")
        start_time = asyncio.get_event_loop().time()
        timeout = 60.0

        while True:
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > timeout:
                logger.error("❌ TIMEOUT: Không nhận được phản hồi sau 60 giây!")
                break

            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)
                res_val = msg.value
                curr_conv_id = res_val.get("conv_id") or res_val.get("convId")
                event_t = res_val.get("event_type") or res_val.get("eventType")

                if curr_conv_id == conv_id or event_t == AIEvent.OUTER_CHATBOT.value:
                    if "processStatus" in res_val and res_val.get("processStatus") != 3:
                        logger.info(f"   ...trạng thái tiến trình: {res_val.get('processStatus')}")
                        continue

                    logger.info("==================================================================")
                    logger.info("🎉 ĐÃ NHẬN ĐƯỢC CÂU TRẢ LỜI TỪ CHATBOT KAFKA!")
                    logger.info("==================================================================")
                    logger.info(f"   Conversation ID: {curr_conv_id}")
                    
                    if "error_code" in res_val or ("aiError" in res_val and res_val["aiError"].get("errorCode")):
                        logger.error(f"   ✗ Error Code: {res_val.get('error_code') or res_val['aiError'].get('errorCode')}")
                        logger.error(f"   ✗ Error Message: {res_val.get('error') or res_val['aiError'].get('error')}")
                    else:
                        answer = res_val.get("answer") or res_val.get("response") or res_val.get("message", "")
                        logger.info(f"   💬 Câu trả lời của AI:\n\n{answer}\n")
                        logger.info(f"   ✓ Input Tokens: {res_val.get('input_tokens', 'N/A')}")
                        logger.info(f"   ✓ Output Tokens: {res_val.get('output_tokens', 'N/A')}")
                    break

            except asyncio.TimeoutError:
                logger.info(f"   ...vẫn đang chờ AI Chatbot xử lý (đã trôi qua {int(elapsed)}s)...")

    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run_test())
