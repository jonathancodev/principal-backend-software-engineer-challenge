"""Dead letter queue simulation.

Bounded in-memory store for messages that exhausted their retries. In a real
deployment this would be a separate SQS queue with alerting on depth; here it
is inspectable via GET /admin/queue so operators (and reviewers) can see what
failed and why.
"""

import time
from collections import deque
from typing import Any, Dict, List

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # avoid circular import at runtime
    from app.queueing.queue import QueueMessage


class DeadLetterQueue:
    def __init__(self, max_size: int = 1000) -> None:
        self._items: "deque[Dict[str, Any]]" = deque(maxlen=max_size)

    def add(self, message: "QueueMessage", reason: str) -> None:
        self._items.append(
            {
                "message_id": message.id,
                "body": message.body,
                "attempts": message.attempts,
                "reason": reason,
                "dead_lettered_at": time.time(),
            }
        )

    def size(self) -> int:
        return len(self._items)

    def peek(self, limit: int = 20) -> List[Dict[str, Any]]:
        return list(self._items)[-limit:]
