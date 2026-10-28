"""Unit tests for validation and utility methods in BaseAsyncListener (listener.py)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from listener import BaseAsyncListener
from status_type import AIStatus


class DummyListener(BaseAsyncListener):
    """Concrete dummy implementation for testing BaseAsyncListener logic."""

    def __init__(self, concurency=None, concurrency=None):
        with patch("listener.get_mongodb_client"), patch("listener.get_redis_stack_client"), patch("listener.get_redis_client"):
            super().__init__(["test_topic"], {}, concurency=concurency, concurrency=concurrency)
        self.mongo_client = MagicMock()
        self.redis_client = MagicMock()
        self.redis = AsyncMock()

    async def _handle_record(self, record):
        pass


@pytest.fixture
def listener():
    return DummyListener(concurrency=2)


def test_listener_concurrency_reads_env(monkeypatch):
    monkeypatch.setenv("LISTENER_CONCURRENCY", "8")
    d = DummyListener()
    assert d.concurrency == 8
    assert d._semaphore._value == 8
    assert d._work_queue.maxsize == 16


def test_listener_concurrency_default_without_env(monkeypatch):
    monkeypatch.delenv("LISTENER_CONCURRENCY", raising=False)
    d = DummyListener()
    assert d.concurrency == 100
    assert d._semaphore._value == 100
    assert d._work_queue.maxsize == 200


def test_listener_concurrency_explicit_param_overrides_env(monkeypatch):
    monkeypatch.setenv("LISTENER_CONCURRENCY", "8")
    d = DummyListener(concurrency=16)
    assert d.concurrency == 16
    assert d._semaphore._value == 16
    assert d._work_queue.maxsize == 32


def test_field_validate_str(listener):
    listener.field_validate_str({"title": "Test Title", "user_id": "123"})
    with pytest.raises(ValueError, match="count must be a string"):
        listener.field_validate_str({"count": 123})


def test_field_validate_list_str(listener):
    listener.field_validate_list_str({"tags": ["a", "b", "c"]})
    with pytest.raises(ValueError, match="tags must be a list"):
        listener.field_validate_list_str({"tags": "not_a_list"})
    with pytest.raises(ValueError, match="tags must be a list of string"):
        listener.field_validate_list_str({"tags": ["a", 123]})


def test_field_validate_bool(listener):
    listener.field_validate_bool({"is_active": True, "debug": False})
    with pytest.raises(ValueError, match="is_active must be a boolean"):
        listener.field_validate_bool({"is_active": "true"})


def test_field_validate_number(listener):
    listener.field_validate_number({"count": 5, "ratio": 3.14})
    with pytest.raises(ValueError, match="count must be a number"):
        listener.field_validate_number({"count": "5"})


def test_field_validate_list_number(listener):
    listener.field_validate_list_number({"values": [1, 2.5, 3]})
    with pytest.raises(ValueError, match="values must be a list"):
        listener.field_validate_list_number({"values": 123})
    with pytest.raises(ValueError, match="values must be a list of number"):
        listener.field_validate_list_number({"values": [1, "two"]})


def test_field_validate_dict(listener):
    listener.field_validate_dict({"config": {"key": "val"}})
    with pytest.raises(ValueError, match="config must be a dictionary"):
        listener.field_validate_dict({"config": [1, 2]})


def test_field_validate_list_dict(listener):
    listener.field_validate_list_dict({"items": [{"id": 1}, {"id": 2}]})
    with pytest.raises(ValueError, match="items must be a list"):
        listener.field_validate_list_dict({"items": {"id": 1}})
    with pytest.raises(ValueError, match="items must be a dictionary"):
        listener.field_validate_list_dict({"items": [{"id": 1}, "invalid"]})


def test_empty_value_check(listener):
    listener.empty_value_check({"name": "OK", "items": [1], "cfg": {"a": 1}})
    with pytest.raises(ValueError, match="name cannot be empty"):
        listener.empty_value_check({"name": "   "})
    with pytest.raises(ValueError, match="items cannot be empty"):
        listener.empty_value_check({"items": []})
    with pytest.raises(ValueError, match="cfg cannot be empty"):
        listener.empty_value_check({"cfg": {}})


def test_get_key(listener):
    async def _run():
        listener.redis.get.side_effect = [
            b'"fake-llm-key"',
            b'"fake-db-key"',
            b'"fake-search-key"',
        ]
        llm, db, search = await listener.get_key("model-xyz")
        assert llm == "fake-llm-key"
        assert db == "fake-db-key"
        assert search == "fake-search-key"

        # Test None branches
        listener.redis.get.side_effect = [None, None, None]
        llm2, db2, search2 = await listener.get_key("model-empty")
        assert llm2 == ""
        assert db2 == ""
        assert search2 == ""

    asyncio.run(_run())


def test_test_error(listener):
    async def _run():
        # Ordinary user -> empty dict
        res = await listener.test_error("user-normal", 500)
        assert res == {}

        # Target user with forced probability
        with patch("random.random", return_value=0.8):
            res2 = await listener.test_error("683802b787b2175089765cc8", 500)
            assert res2 == {}

        with patch("random.random", return_value=0.2):
            with pytest.raises(Exception):
                await listener.test_error("683802b787b2175089765cc8", 500)

    asyncio.run(_run())


def test_finalize_task_branches(listener):
    async def _run():
        mock_producer = AsyncMock()
        listener.send_message = AsyncMock()

        # Success branch
        with patch("listener.send_health_check", new_callable=AsyncMock) as mock_health:
            timer = asyncio.create_task(asyncio.sleep(10))
            await listener._finalize_task(
                timer_task=timer, success=True, _id="doc123", producer=mock_producer,
                topic="resp_topic", message={"event_type": "test_ev"}, event_type="test_ev", is_suspended=False,
            )
            mock_health.assert_called_once_with(
                id="doc123", status=AIStatus.FINISHED, topic="resp_topic", event_type="test_ev"
            )

        # Error branch
        with patch("listener.send_health_check", new_callable=AsyncMock) as mock_health:
            await listener._finalize_task(
                timer_task=None, success=False, _id="doc123", producer=mock_producer,
                topic="resp_topic", message={"event_type": "test_ev", "error_code": 630, "error": "Failed"},
                event_type="test_ev", is_suspended=False,
            )
            mock_health.assert_called_once_with(
                id="doc123", status=AIStatus.ERROR, topic="resp_topic", event_type="test_ev",
                error=630, error_message="Failed"
            )

        # Suspended branch
        with patch("listener.send_health_check", new_callable=AsyncMock) as mock_health:
            await listener._finalize_task(
                timer_task=None, success=False, _id="doc123", producer=mock_producer,
                topic="resp_topic", message={"event_type": "test_ev", "error_code": 608, "error": "Suspended"},
                event_type="test_ev", is_suspended=True,
            )
            mock_health.assert_called_once_with(
                id="doc123", status=AIStatus.SUSPENDED, topic="resp_topic", event_type="test_ev",
                error=608, error_message="Suspended"
            )

    asyncio.run(_run())


def test_send_message(listener):
    async def _run():
        mock_producer = AsyncMock()
        await listener.send_message(
            producer=mock_producer,
            topic="test-topic",
            message={"data": "hello"},
            document_id="doc-999",
        )
        assert mock_producer.send_and_wait.called
        assert listener.redis.sadd.called

        # Test fallback branch when send_and_wait raises Exception
        mock_producer.send_and_wait.side_effect = Exception("Kafka send failed")
        fallback_prod = AsyncMock()
        with patch("listener.get_producer", new_callable=AsyncMock, return_value=fallback_prod):
            await listener.send_message(
                producer=mock_producer, topic="test-topic", message={"data": "fallback"}, document_id="doc-999"
            )
            assert fallback_prod.send_and_wait.called

    asyncio.run(_run())


def test_clean_up(listener):
    async def _run():
        async def fake_scan(match):
            yield b"prefix:thread-1:key1"
            yield b"prefix:thread-1:key2"

        listener.redis_client.scan_iter = fake_scan
        listener.redis_client.delete = AsyncMock()

        mock_checkpoint = MagicMock(delete_many=AsyncMock())
        mock_writes = MagicMock(delete_many=AsyncMock())
        mock_outlines = MagicMock(update_many=AsyncMock())

        mock_db_bot = MagicMock()
        mock_db_bot.__getitem__.side_effect = lambda name: (
            mock_checkpoint if "Checkpoint" in name else mock_writes
        )
        mock_db_admin = MagicMock()
        mock_db_admin.__getitem__.return_value = mock_outlines

        listener.mongo_client.__getitem__.side_effect = lambda db_name: (
            mock_db_admin if db_name == "admin" else mock_db_bot
        )

        await listener.clean_up("thread-1", document_id="doc-123")
        listener.redis_client.delete.assert_called_once()
        mock_checkpoint.delete_many.assert_called_once()
        mock_writes.delete_many.assert_called_once()
        mock_outlines.update_many.assert_called_once()

    asyncio.run(_run())

