"""Regression: offset chỉ được commit khi message đã xử lý xong (tránh mất message).

Trước đây `_ingestor_loop` commit ngay sau khi đẩy record vào queue ⇒ container chết
giữa lúc xử lý là message đã commit nhưng chưa có response (BE treo).
Nay dùng cơ chế commit "contiguous": chỉ tiến offset khi mọi offset trước đã xong.
"""

import asyncio
import types

from aiokafka.structs import OffsetAndMetadata, TopicPartition

from listener import BaseAsyncListener

TP = TopicPartition("t", 0)


class _ConcreteListener(BaseAsyncListener):
    """Subclass cụ thể để có thể khởi tạo (Base là ABC, có abstract _handle_record)."""

    async def _handle_record(self, record):  # pragma: no cover - không dùng trong test
        return None


def build_listener(pending_offsets, last_seen):
    """Tạo listener tối giản (bỏ qua __init__ vì cần Kafka/Redis thật)."""
    listener = object.__new__(_ConcreteListener)
    listener._pending_offsets = {TP: set(pending_offsets)}
    listener._commit_offset = {TP: min(pending_offsets)}
    listener._last_seen = {TP: last_seen}
    listener._commit_lock = asyncio.Lock()

    committed = []

    class FakeConsumer:
        async def commit(self, offsets):
            committed.append(offsets[TP].offset)

    listener._consumer = FakeConsumer()
    return listener, committed


def rec(offset):
    return types.SimpleNamespace(topic="t", partition=0, offset=offset)


def test_out_of_order_ack_does_not_commit_gap():
    listener, committed = build_listener({0, 1, 2}, last_seen=2)

    asyncio.run(listener._ack(rec(2)))
    assert committed == [], "chưa xử lý offset 0,1 thì không được commit"

    asyncio.run(listener._ack(rec(0)))
    assert committed == [1], "chỉ commit tới trước khoảng trống (offset 1 còn pending)"


def test_contiguous_ack_advances_commit():
    listener, committed = build_listener({0, 1, 2}, last_seen=2)

    for offset in (0, 1, 2):
        asyncio.run(listener._ack(rec(offset)))

    assert committed == [1, 2, 3], f"kỳ vọng commit tiến dần 1→2→3, nhận {committed}"
    assert committed[-1] == 3, "offset commit cuối phải là last_offset + 1"


def test_duplicate_ack_is_noop():
    listener, committed = build_listener({0}, last_seen=0)

    asyncio.run(listener._ack(rec(0)))
    asyncio.run(listener._ack(rec(0)))

    assert committed == [1], "ack trùng không được commit lại"


def test_commit_error_does_not_crash():
    listener, _ = build_listener({0}, last_seen=0)

    class FailingConsumer:
        async def commit(self, offsets):
            raise RuntimeError("broker down")

    listener._consumer = FailingConsumer()
    asyncio.run(listener._ack(rec(0)))  # không được raise


def test_ack_before_registration_still_commits():
    """Bug #16: `_ack` đến TRƯỚC khi partition được đăng ký (race với `queue.put`).

    Trước đây `_ack` return sớm khi `_pending_offsets` chưa có partition ⇒ offset kẹt
    vĩnh viễn dù message đã xử lý (LAG không về 0). Nay phải commit bình thường.
    """
    listener = object.__new__(_ConcreteListener)
    listener._pending_offsets = {}
    listener._commit_offset = {}
    listener._last_seen = {}
    listener._commit_lock = asyncio.Lock()

    committed = []

    class FakeConsumer:
        async def commit(self, offsets):
            committed.append(offsets[TP].offset)

    listener._consumer = FakeConsumer()
    asyncio.run(listener._ack(rec(5)))

    assert committed == [6], f"phải commit offset kế tiếp (6), nhận {committed}"
