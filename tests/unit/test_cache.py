"""Unit tests for the realtime stats cache: TTL, single-flight, fail-open."""

import asyncio

from app.storage.redis_cache import CACHE_BYPASS, CACHE_HIT, CACHE_MISS, RealtimeStatsCache

from tests.unit.fakes import FakeRedis


def _compute_counter():
    state = {"calls": 0}

    async def compute():
        state["calls"] += 1
        return {"total": 42, "call": state["calls"]}

    return compute, state


async def test_miss_then_hit():
    cache = RealtimeStatsCache(FakeRedis())
    compute, state = _compute_counter()

    data, status = await cache.get_or_compute("k", ttl_seconds=10, compute=compute)
    assert status == CACHE_MISS
    assert data["total"] == 42

    data, status = await cache.get_or_compute("k", ttl_seconds=10, compute=compute)
    assert status == CACHE_HIT
    assert state["calls"] == 1  # served from cache, no recompute


async def test_ttl_expiry_triggers_recompute():
    cache = RealtimeStatsCache(FakeRedis())
    compute, state = _compute_counter()

    await cache.get_or_compute("k", ttl_seconds=1, compute=compute)
    await asyncio.sleep(1.05)
    _, status = await cache.get_or_compute("k", ttl_seconds=1, compute=compute)
    assert status == CACHE_MISS
    assert state["calls"] == 2


async def test_single_flight_collapses_concurrent_misses():
    cache = RealtimeStatsCache(FakeRedis())
    state = {"calls": 0}

    async def slow_compute():
        state["calls"] += 1
        await asyncio.sleep(0.1)
        return {"total": 1}

    results = await asyncio.gather(
        *(cache.get_or_compute("k", ttl_seconds=10, compute=slow_compute) for _ in range(5))
    )
    assert state["calls"] == 1  # only the lock holder computed
    statuses = sorted(status for _, status in results)
    assert statuses.count(CACHE_MISS) == 1
    assert statuses.count(CACHE_HIT) == 4


async def test_redis_outage_fails_open_with_bypass():
    cache = RealtimeStatsCache(FakeRedis(fail=True))
    compute, state = _compute_counter()

    data, status = await cache.get_or_compute("k", ttl_seconds=10, compute=compute)
    assert status == CACHE_BYPASS
    assert data["total"] == 42
    assert state["calls"] == 1
