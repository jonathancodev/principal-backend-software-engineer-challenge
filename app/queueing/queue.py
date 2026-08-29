"""In-process, SQS-style event queue.

Modeled after SQS semantics on purpose so the "swap in real SQS" story is a
transport change, not a redesign:

- ``send``          -> SendMessage (raises ``QueueFullError`` for backpressure)
- ``receive``       -> ReceiveMessage: the message becomes *in flight* and
                       invisible to other consumers; the receive attempt count
                       is incremented (SQS ``ApproximateReceiveCount``).
- ``ack``           -> DeleteMessage.
- ``nack(delay)``   -> ChangeMessageVisibility(0)-style early redelivery,
                       with an optional backoff delay.
- visibility timeout: if a consumer neither acks nor nacks within the window
  (e.g. it crashed mid-batch), the message is automatically redelivered.

Guarantees (documented honestly in ARCHITECTURE.md): at-least-once delivery
*within the process lifetime*; no durability across process crashes; FIFO-ish
ordering that is not preserved across retries.
"""

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set

from app.errors import QueueFullError
from app.queueing.dlq import DeadLetterQueue

logger = logging.getLogger(__name__)


@dataclass
class QueueMessage:
    id: str
    body: Dict[str, Any]
    attempts: int = 0  # incremented on each receive, like ApproximateReceiveCount
    enqueued_at: float = field(default_factory=time.monotonic)
    receipt_handle: Optional[str] = None


class InProcessQueue:
    def __init__(
        self,
        max_size: int,
        visibility_timeout_seconds: float,
        dlq: Optional[DeadLetterQueue] = None,
    ) -> None:
        self._queue: "asyncio.Queue[QueueMessage]" = asyncio.Queue(maxsize=max_size)
        self._visibility_timeout = visibility_timeout_seconds
        self._in_flight: Dict[str, QueueMessage] = {}
        self._visibility_tasks: Dict[str, asyncio.Task] = {}
        self._redelivery_tasks: Set[asyncio.Task] = set()
        self._closed = False
        self.dlq = dlq or DeadLetterQueue()

    # --- Producer API ---

    async def send(self, body: Dict[str, Any]) -> str:
        if self._closed:
            raise QueueFullError("queue is shut down")
        message = QueueMessage(id=str(uuid.uuid4()), body=body)
        try:
            self._queue.put_nowait(message)
        except asyncio.QueueFull:
            raise QueueFullError("queue is at capacity") from None
        return message.id

    # --- Consumer API ---

    async def receive(self) -> QueueMessage:
        message = await self._queue.get()
        message.attempts += 1
        message.receipt_handle = str(uuid.uuid4())
        self._in_flight[message.receipt_handle] = message
        self._visibility_tasks[message.receipt_handle] = asyncio.create_task(
            self._redeliver_on_visibility_timeout(message.receipt_handle)
        )
        return message

    async def ack(self, receipt_handle: str) -> None:
        self._drop_in_flight(receipt_handle)

    async def nack(self, receipt_handle: str, delay_seconds: float = 0.0) -> None:
        message = self._drop_in_flight(receipt_handle)
        if message is None:
            return
        task = asyncio.create_task(self._requeue_after(message, delay_seconds))
        self._redelivery_tasks.add(task)
        task.add_done_callback(self._redelivery_tasks.discard)

    async def move_to_dlq(self, receipt_handle: str, reason: str) -> None:
        message = self._drop_in_flight(receipt_handle)
        if message is None:
            return
        self.dlq.add(message, reason)
        logger.warning(
            "message moved to DLQ id=%s attempts=%d reason=%s",
            message.id, message.attempts, reason,
        )

    # --- Introspection ---

    def depth(self) -> int:
        return self._queue.qsize()

    def in_flight_count(self) -> int:
        return len(self._in_flight)

    # --- Lifecycle ---

    async def close(self) -> None:
        self._closed = True
        pending = list(self._visibility_tasks.values()) + list(self._redelivery_tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._visibility_tasks.clear()
        self._in_flight.clear()

    # --- Internals ---

    def _drop_in_flight(self, receipt_handle: str) -> Optional[QueueMessage]:
        message = self._in_flight.pop(receipt_handle, None)
        task = self._visibility_tasks.pop(receipt_handle, None)
        if task is not None and not task.done():
            task.cancel()
        return message

    async def _redeliver_on_visibility_timeout(self, receipt_handle: str) -> None:
        try:
            await asyncio.sleep(self._visibility_timeout)
        except asyncio.CancelledError:
            return
        message = self._in_flight.pop(receipt_handle, None)
        self._visibility_tasks.pop(receipt_handle, None)
        if message is not None:
            logger.warning(
                "visibility timeout expired, redelivering id=%s attempts=%d",
                message.id, message.attempts,
            )
            await self._queue.put(message)

    async def _requeue_after(self, message: QueueMessage, delay_seconds: float) -> None:
        try:
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            await self._queue.put(message)
        except asyncio.CancelledError:
            pass
