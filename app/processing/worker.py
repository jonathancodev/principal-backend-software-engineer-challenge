"""Background worker: consumes the queue and persists events.

Per-message flow (idempotent consumer):
    receive -> dedup seen? -> Mongo insert (authoritative) -> dedup mark
            -> ES index (best-effort) -> ack

Ordering is load-bearing: the dedup marker is written only *after* the
durable Mongo write, so a failed attempt leaves no dedup residue and retries
are never misclassified as duplicates. (An earlier claim-before-write version
silently dropped events on retry — caught in review, kept as a regression
test in tests/unit/test_worker.py.)

Failure policy:
- Mongo failure: nack with exponential backoff (base * 2^(attempt-1), capped).
  After ``max_retries`` receives the message moves to the DLQ.
- ES failure: logged and skipped — search lags but ingestion is never blocked
  by the search tier (Mongo remains the source of truth).
- Duplicate (dedup marker present or Mongo unique index hit): ack and drop.
- Crash mid-message: no ack happens, so the queue's visibility timeout
  redelivers; the dedup marker + unique index prevent double-writes.
"""

import asyncio
import logging
from typing import List, Optional

from app.config import Settings
from app.domain.models import EventRecord
from app.errors import DuplicateEventError, StorageUnavailableError
from app.processing.dedup import RedisDeduplicator
from app.queueing.queue import InProcessQueue, QueueMessage
from app.storage.elastic import ElasticEventStore
from app.storage.mongo import MongoEventRepository

logger = logging.getLogger(__name__)


def backoff_delay(attempt: int, base_seconds: float, max_seconds: float) -> float:
    """Exponential backoff for the Nth delivery attempt (1-based)."""
    return min(base_seconds * (2 ** max(attempt - 1, 0)), max_seconds)


class EventWorker:
    def __init__(
        self,
        queue: InProcessQueue,
        repository: MongoEventRepository,
        search_store: Optional[ElasticEventStore],
        deduplicator: RedisDeduplicator,
        settings: Settings,
    ) -> None:
        self._queue = queue
        self._repository = repository
        self._search_store = search_store
        self._dedup = deduplicator
        self._settings = settings
        self._tasks: List[asyncio.Task] = []

    def start(self) -> None:
        for i in range(self._settings.worker_concurrency):
            self._tasks.append(asyncio.create_task(self._run(), name=f"event-worker-{i}"))
        logger.info("started %d event worker(s)", len(self._tasks))

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("event workers stopped")

    async def _run(self) -> None:
        while True:
            try:
                message = await self._queue.receive()
            except asyncio.CancelledError:
                raise
            try:
                await self.process_message(message)
            except asyncio.CancelledError:
                # Shutdown mid-message: leave it in flight; visibility timeout
                # would redeliver on a real (multi-process) queue.
                raise
            except Exception:
                logger.exception("unexpected worker error message_id=%s", message.id)
                await self._handle_failure(message, "unexpected error")

    async def process_message(self, message: QueueMessage) -> None:
        record = EventRecord.model_validate(message.body)

        if await self._dedup.seen(record.event_id):
            logger.info("duplicate skipped event_id=%s", record.event_id)
            await self._queue.ack(message.receipt_handle)
            return

        try:
            await self._repository.insert_event(record)
        except DuplicateEventError:
            # Lost the insert race to a concurrent delivery; backfill the fast path.
            logger.info("duplicate at storage layer skipped event_id=%s", record.event_id)
            await self._dedup.mark(record.event_id)
            await self._queue.ack(message.receipt_handle)
            return
        except StorageUnavailableError as exc:
            # Nothing marked: the retry must not look like a duplicate.
            logger.warning(
                "mongo write failed event_id=%s attempt=%d error=%s",
                record.event_id, message.attempts, exc,
            )
            await self._handle_failure(message, f"storage unavailable: {exc}")
            return

        # Mark only after the durable write; a redelivery from here on is a duplicate.
        await self._dedup.mark(record.event_id)

        if self._search_store is not None:
            try:
                await self._search_store.index_event(record)
            except Exception as exc:
                # Best-effort: search lags behind rather than blocking ingestion.
                logger.warning("es index failed event_id=%s error=%s", record.event_id, exc)

        await self._queue.ack(message.receipt_handle)
        logger.info(
            "event processed event_id=%s type=%s attempt=%d",
            record.event_id, record.event_type, message.attempts,
        )

    async def _handle_failure(self, message: QueueMessage, reason: str) -> None:
        if message.attempts >= self._settings.max_retries:
            await self._queue.move_to_dlq(message.receipt_handle, reason)
        else:
            delay = backoff_delay(
                message.attempts,
                self._settings.backoff_base_seconds,
                self._settings.backoff_max_seconds,
            )
            logger.info(
                "retrying message_id=%s attempt=%d delay=%.2fs",
                message.id, message.attempts, delay,
            )
            await self._queue.nack(message.receipt_handle, delay_seconds=delay)
