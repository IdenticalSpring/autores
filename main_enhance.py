import asyncio

from document_setup.src.configs.app import settings

from enhance_listener import start_listener as EnhancementListener
from utils import set_num_partitions, init_singleton
import topics


async def main():
    await set_num_partitions(settings.KAFKA, [topics.ENHANCEMENT_REQUEST, topics.ENHANCEMENT_RESPONSE], 3)
    await init_singleton()
    await EnhancementListener(
        [topics.ENHANCEMENT_REQUEST],
        "enhancement_09_06_2025"
    )


if __name__ == "__main__":
    asyncio.run(main())