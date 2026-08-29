"""Ingestion service: validate -> normalize -> enqueue.

The HTTP hot path never touches MongoDB; it only serializes the event onto
the queue and returns 202. The payload crosses the queue boundary as plain
JSON (not Python objects) to mirror what a real SQS producer would send.
"""

import logging

from app.domain.models import EventIn, EventRecord
from app.queueing.queue import InProcessQueue

logger = logging.getLogger(__name__)


class IngestionService:
    def __init__(self, queue: InProcessQueue) -> None:
        self._queue = queue

    async def ingest(self, event: EventIn) -> str:
        record = EventRecord.from_input(event)
        await self._queue.send(record.model_dump(mode="json"))
        logger.info(
            "event accepted event_id=%s type=%s queue_depth=%d",
            record.event_id, record.event_type, self._queue.depth(),
        )
        return record.event_id
