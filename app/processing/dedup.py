"""Worker-side event deduplication (idempotent-consumer pattern).

Two operations, deliberately split:

- ``seen(event_id)``  — checked *before* the Mongo write.
- ``mark(event_id)``  — recorded *after* the durable write succeeds.

The split matters. An earlier version used a single SETNX "claim" taken
before the write; when a transient Mongo failure triggered a retry, the
retry found its own claim and misclassified the message as a duplicate,
silently dropping the event. Check-before-write / mark-after-write means a
failed attempt leaves no trace, so retries stay clean.

Concurrency note: two deliveries of the same event can both pass ``seen``
(neither has marked yet). That race is resolved atomically by the unique
Mongo index on ``event_id`` — Redis here is a fast-path filter, the index is
the guarantee. Both operations therefore fail open on Redis errors.
"""

import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


class RedisDeduplicator:
    def __init__(self, redis: Redis, ttl_seconds: int, key_prefix: str = "dedup") -> None:
        self._redis = redis
        self._ttl = ttl_seconds
        self._prefix = key_prefix

    async def seen(self, event_id: str) -> bool:
        """Return True if event_id was already durably processed (fast path)."""
        try:
            return await self._redis.get(f"{self._prefix}:{event_id}") is not None
        except RedisError as exc:
            logger.warning("dedup check failed open event_id=%s error=%s", event_id, exc)
            return False  # proceed; the unique Mongo index is the backstop

    async def mark(self, event_id: str) -> None:
        """Record event_id as processed. Best-effort: a lost mark only costs
        a future round trip to the unique-index rejection."""
        try:
            await self._redis.set(f"{self._prefix}:{event_id}", "1", ex=self._ttl)
        except RedisError as exc:
            logger.warning("dedup mark failed event_id=%s error=%s", event_id, exc)
