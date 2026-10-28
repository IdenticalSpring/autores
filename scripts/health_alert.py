"""Health-alert cho tầng Kafka: kiểm tra 4 listener còn sống và LAG của consumer group.

Chạy trong container (hoặc máy có quyền truy cập Kafka):
    docker exec autoresearching-ai-dev python scripts/health_alert.py
    docker exec autoresearching-ai-dev python scripts/health_alert.py --lag-threshold 50

Exit code:
    0 = tất cả group đủ member và LAG <= ngưỡng
    1 = có group không có member / thiếu member / LAG vượt ngưỡng,
        hoặc --only không khớp group-topic nào
    2 = không kết nối được Kafka

Vì sao cần: 4 listener chạy trong **cùng 1 process**, dùng chung event loop.
Nếu 1 listener chết (exception không bắt) hoặc bị block, message sẽ dồn lại —
chỉ LAG mới phát hiện được, log có thể không báo gì.
"""

import argparse
import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiokafka import AIOKafkaConsumer, TopicPartition  # noqa: E402
from aiokafka.admin import AIOKafkaAdminClient  # noqa: E402

import topics  # noqa: E402
from document_setup.src.configs.app import settings  # noqa: E402

# group_id -> danh sách topic mà group đó phải tiêu thụ (khớp main.py)
GROUPS: dict[str, list[str]] = {
    "document_setup_05_20_2025": [topics.DOCUMENT_SETUP_REQUEST],
    "write_section_05_20_2025": [topics.WRITE_SECTION_REQUEST],
    "chatbot_09_06_2025": [topics.CHATBOT_REQUEST, topics.ENHANCEMENT_REQUEST],
}

# Số lượng listener member kỳ vọng cho từng group (chatbot có 2 listener: chatbot & enhance)
EXPECTED_MEMBERS: dict[str, int] = {
    "document_setup_05_20_2025": 1,
    "write_section_05_20_2025": 1,
    "chatbot_09_06_2025": 2,
}


def _count_members(described) -> int:
    """Đếm member của group, tương thích cả kiểu trả về cũ và raw response (aiokafka 0.12).

    - aiokafka cũ: object có thuộc tính `members`
    - aiokafka 0.12: `DescribeGroupsResponse_v2.groups = [(error, group, state, ..., members)]`
    """
    total = 0
    for entry in described or []:
        members = getattr(entry, "members", None)
        if members is not None:
            total += len(members)
            continue
        for raw_group in getattr(entry, "groups", []) or []:
            if isinstance(raw_group, (list, tuple)):
                total += len(raw_group[-1] or [])
            else:
                total += len(getattr(raw_group, "members", []) or [])
    return total


async def _group_lag(admin: AIOKafkaAdminClient, group_id: str, topic: str) -> tuple[int, int]:
    """(member_count, total_lag) cho 1 group trên 1 topic."""
    members = 0
    try:
        described = await admin.describe_consumer_groups([group_id])
        members = _count_members(described)
    except Exception as error:  # group mới chưa từng commit
        print(f"[warn] describe_consumer_groups({group_id}): {error}")

    assigned = {}
    try:
        offsets = await admin.list_consumer_group_offsets(group_id)
        for tp, offset_meta in offsets.items():
            if tp.topic == topic:
                assigned[tp] = offset_meta.offset
    except Exception as error:
        print(f"[warn] list_consumer_group_offsets({group_id}): {error}")

    consumer = AIOKafkaConsumer(bootstrap_servers=settings.KAFKA, group_id=None)
    await consumer.start()
    try:
        partitions = consumer.partitions_for_topic(topic) or set()
        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        if not tps:
            return members, 0
        end_offsets = await consumer.end_offsets(tps)
        total_lag = 0
        for tp in tps:
            committed = assigned.get(tp)
            if committed is None:
                # Chưa từng commit ⇒ coi toàn bộ message là tồn đọng
                total_lag += end_offsets[tp]
            else:
                total_lag += max(0, end_offsets[tp] - committed)
        return members, total_lag
    finally:
        await consumer.stop()


async def run(lag_threshold: int, only: str | None) -> int:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.KAFKA)
    try:
        await admin.start()
    except Exception as error:
        print(f"[FAIL] không kết nối được Kafka: {error}")
        return 2

    problems: list[str] = []
    matched_any = False
    try:
        print(f"{'group':28} {'topic':34} {'members':>8} {'lag':>8}  status")
        for group_id, topic_list in GROUPS.items():
            if only and (only not in group_id and not any(only in t for t in topic_list)):
                continue

            expected = EXPECTED_MEMBERS.get(group_id, 1)
            group_member_alerted = False

            for topic in topic_list:
                if only and (only not in group_id and only not in topic):
                    continue

                matched_any = True
                try:
                    members, lag = await asyncio.wait_for(_group_lag(admin, group_id, topic), timeout=30)
                except asyncio.TimeoutError:
                    print(f"{group_id:28} {topic:34} {'-':>8} {'-':>8}  TIMEOUT (30s)")
                    problems.append(f"{group_id} [{topic}]: timeout khi đọc LAG")
                    continue

                status = "OK"
                if members == 0:
                    status = f"NO_MEMBER (0/{expected})"
                    if not group_member_alerted:
                        problems.append(f"{group_id}: không có consumer member (0/{expected})")
                        group_member_alerted = True
                elif members < expected:
                    status = f"MISSING_MEMBER ({members}/{expected})"
                    if not group_member_alerted:
                        problems.append(f"{group_id}: thiếu consumer member ({members}/{expected})")
                        group_member_alerted = True

                if lag > lag_threshold:
                    status = f"LAG_HIGH (>{lag_threshold})" if status == "OK" else f"{status} + LAG_HIGH"
                    problems.append(f"{group_id} [{topic}]: lag={lag}")

                print(f"{group_id:28} {topic:34} {members:>8} {lag:>8}  {status}")
    finally:
        await admin.close()

    if not matched_any:
        print(f"\n[ALERT] Không tìm thấy group hoặc topic nào khớp với filter --only='{only}'")
        return 1

    if problems:
        print("\n[ALERT] " + "; ".join(problems))
        return 1
    print("\n[OK] Tất cả listener còn sống, không tồn đọng.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Health-alert Kafka cho autoresearching AI")
    parser.add_argument("--lag-threshold", type=int, default=100, help="Ngưỡng LAG cảnh báo (mặc định 100)")
    parser.add_argument("--only", default=None, help="Chỉ kiểm tra group có tên chứa chuỗi này")
    args = parser.parse_args()
    return asyncio.run(run(args.lag_threshold, args.only))


if __name__ == "__main__":
    sys.exit(main())
