import asyncio

from document_setup.src.configs.app import settings

from document_setup_listener import start_listener as DocumentSetupListener
from enhance_listener import start_listener as EnhancementListener
from chatbot_listener import start_listener as ChatbotListener
from write_listener import start_listener as WriteListener
from utils import set_num_partitions, init_singleton
import topics


async def main():
    await set_num_partitions(settings.KAFKA, [topics.DOCUMENT_SETUP_REQUEST, topics.DOCUMENT_SETUP_RESPONSE], 3)
    await set_num_partitions(settings.KAFKA, [topics.WRITE_SECTION_REQUEST, topics.WRITE_SECTION_RESPONSE], 3)
    await set_num_partitions(settings.KAFKA, [topics.CHATBOT_REQUEST, topics.CHATBOT_RESPONSE], 3)
    await set_num_partitions(settings.KAFKA, [topics.ENHANCEMENT_REQUEST, topics.ENHANCEMENT_RESPONSE], 3)
    await init_singleton()
    task1 = asyncio.create_task(
        DocumentSetupListener(
            [topics.DOCUMENT_SETUP_REQUEST],
            "document_setup_05_20_2025"
        )
    )
    task2 = asyncio.create_task(
        WriteListener(
            [topics.WRITE_SECTION_REQUEST],
            "write_section_05_20_2025"
        )
    )
    task3 = asyncio.create_task(
        EnhancementListener(
            [topics.ENHANCEMENT_REQUEST],
            "enhancement_09_06_2025"
        )
    )
    task4 = asyncio.create_task(
        ChatbotListener(
            [topics.CHATBOT_REQUEST],
            "chatbot_09_06_2025"
        )
    )
    await asyncio.gather(task1, task2, task3, task4)


if __name__ == "__main__":
    asyncio.run(main())