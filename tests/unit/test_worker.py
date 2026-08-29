"""Unit tests for the worker: retry/backoff, DLQ routing, dedup, ES best-effort."""

import asyncio

import pytest

from app.config import Settings
from app.domain.models import EventIn, EventRecord
from app.processing.worker import EventWorker, backoff_delay
from app.queueing.queue import InProcessQueue

from tests.unit.fakes import FakeDeduplicator, FakeRepository, FakeSearchStore


def _settings(**overrides) -> Settings:
    values = {
        "worker_concurrency": 1,
        "max_retries": 3,
        "backoff_base_seconds": 0.01,
        "backoff_max_seconds": 0.05,
        "visibility_timeout_seconds": 5.0,
    }
    values.update(overrides)
    return Settings(**values)


def _event_body(event_id: str = "evt-1") -> dict:
    record = EventRecord.from_input(
        EventIn(
            event_type="click",
            timestamp="2026-08-29T12:00:00Z",
            user_id="user-1",
            source_url="https://example.com",
            metadata={"button": "signup"},
            event_id=event_id,
        )
    )
    return record.model_dump(mode="json")


async def _wait_for(predicate, timeout: float = 2.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


@pytest.fixture
async def queue():
    q = InProcessQueue(max_size=100, visibility_timeout_seconds=5.0)
    yield q
    await q.close()


async def test_happy_path_persists_indexes_and_acks(queue):
    repo, search, dedup = FakeRepository(), FakeSearchStore(), FakeDeduplicator()
    worker = EventWorker(queue, repo, search, dedup, _settings())
    worker.start()
    try:
        await queue.send(_event_body())
        await _wait_for(lambda: len(repo.events) == 1)
        assert len(search.indexed) == 1
        assert queue.in_flight_count() == 0
        assert queue.dlq.size() == 0
    finally:
        await worker.stop()


async def test_transient_mongo_failure_is_retried_until_success(queue):
    repo = FakeRepository(fail_times=2)  # fails twice, succeeds on 3rd attempt
    worker = EventWorker(queue, repo, FakeSearchStore(), FakeDeduplicator(), _settings())
    worker.start()
    try:
        await queue.send(_event_body())
        await _wait_for(lambda: len(repo.events) == 1)
        assert repo.insert_calls == 3
        assert queue.dlq.size() == 0
    finally:
        await worker.stop()


async def test_exhausted_retries_route_to_dlq(queue):
    repo = FakeRepository(fail_times=100)  # never recovers
    worker = EventWorker(queue, repo, FakeSearchStore(), FakeDeduplicator(), _settings(max_retries=3))
    worker.start()
    try:
        await queue.send(_event_body())
        await _wait_for(lambda: queue.dlq.size() == 1)
        entry = queue.dlq.peek()[0]
        assert entry["attempts"] == 3
        assert "storage unavailable" in entry["reason"]
        assert repo.events == []
    finally:
        await worker.stop()


async def test_duplicate_event_is_acked_and_skipped(queue):
    repo = FakeRepository()
    dedup = FakeDeduplicator(duplicates={"evt-dup"})
    worker = EventWorker(queue, repo, FakeSearchStore(), dedup, _settings())
    worker.start()
    try:
        await queue.send(_event_body(event_id="evt-dup"))
        await _wait_for(lambda: "evt-dup" in dedup.claimed)
        await asyncio.sleep(0.05)
        assert repo.events == []
        assert queue.in_flight_count() == 0
        assert queue.dlq.size() == 0
    finally:
        await worker.stop()


async def test_es_failure_does_not_block_ingestion(queue):
    repo = FakeRepository()
    search = FakeSearchStore(fail=True)
    worker = EventWorker(queue, repo, search, FakeDeduplicator(), _settings())
    worker.start()
    try:
        await queue.send(_event_body())
        await _wait_for(lambda: len(repo.events) == 1)
        assert queue.dlq.size() == 0  # ES outage never dead-letters an event
    finally:
        await worker.stop()


def test_backoff_delay_is_exponential_and_capped():
    assert backoff_delay(1, 0.5, 30) == 0.5
    assert backoff_delay(2, 0.5, 30) == 1.0
    assert backoff_delay(3, 0.5, 30) == 2.0
    assert backoff_delay(10, 0.5, 30) == 30  # capped
