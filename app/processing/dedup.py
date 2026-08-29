"""Worker-side event deduplication.

First line of defense: a Redis SETNX claim keyed by event_id with a TTL, which
absorbs the common duplicate source (queue redelivery after a visibility
timeout or a crash between write and ack). Second line: the unique Mongo index
on event_id, which guarantees no duplicate rows even if Redis is down or the
TTL has lapsed — so this check fails open on Redis errors.
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

    async def claim(self, event_id: str) -> bool:
        """Return True if this worker is first to process event_id."""
        try:
            claimed = await self._redis.set(
                f"{self._prefix}:{event_id}", "1", nx=True, ex=self._ttl
            )
            return bool(claimed)
        except RedisError as exc:
            logger.warning("dedup check failed open event_id=%s error=%s", event_id, exc)
            return True
