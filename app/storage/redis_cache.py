"""Redis-backed cache for the realtime stats endpoint.

Strategy: cache-aside with a short TTL and a single-flight lock.

- TTL (default 10s) bounds staleness while collapsing read load: at any QPS,
  Mongo sees at most ~1 aggregation per TTL window.
- Single-flight: on a miss, one caller takes a short lock and recomputes;
  concurrent callers briefly poll for the freshly cached value instead of
  stampeding Mongo.
- Redis outage fails open: we recompute from Mongo directly (slower but
  correct) rather than failing the endpoint. X-Cache: BYPASS signals this.
"""

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Dict, Tuple

from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

CACHE_HIT = "HIT"
CACHE_MISS = "MISS"
CACHE_BYPASS = "BYPASS"

_LOCK_TTL_MS = 2000
_WAIT_ATTEMPTS = 20
_WAIT_INTERVAL_S = 0.05


class RealtimeStatsCache:
    def __init__(self, redis: Redis, key_prefix: str = "realtime-stats") -> None:
        self._redis = redis
        self._prefix = key_prefix

    async def get_or_compute(
        self,
        key_suffix: str,
        ttl_seconds: int,
        compute: Callable[[], Awaitable[Dict[str, Any]]],
    ) -> Tuple[Dict[str, Any], str]:
        """Return (data, cache_status) where status is HIT / MISS / BYPASS."""
        key = f"{self._prefix}:{key_suffix}"
        lock_key = f"{key}:lock"

        try:
            cached = await self._redis.get(key)
            if cached is not None:
                return json.loads(cached), CACHE_HIT

            got_lock = await self._redis.set(lock_key, "1", nx=True, px=_LOCK_TTL_MS)
            if not got_lock:
                # Another request is computing; wait briefly for its result.
                for _ in range(_WAIT_ATTEMPTS):
                    await asyncio.sleep(_WAIT_INTERVAL_S)
                    cached = await self._redis.get(key)
                    if cached is not None:
                        return json.loads(cached), CACHE_HIT
                # Lock holder was too slow or died; fall through and compute.

            data = await compute()
            await self._redis.set(key, json.dumps(data, default=str), ex=ttl_seconds)
            await self._redis.delete(lock_key)
            return data, CACHE_MISS
        except RedisError as exc:
            logger.warning("redis unavailable, bypassing cache error=%s", exc)
            data = await compute()
            return data, CACHE_BYPASS
