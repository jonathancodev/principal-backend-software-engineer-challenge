"""Unit tests for RedisDeduplicator: seen/mark contract and fail-open behavior."""

from app.processing.dedup import RedisDeduplicator

from tests.unit.fakes import FakeRedis


async def test_unseen_then_marked_then_seen():
    dedup = RedisDeduplicator(FakeRedis(), ttl_seconds=60)
    assert await dedup.seen("evt-1") is False
    await dedup.mark("evt-1")
    assert await dedup.seen("evt-1") is True


async def test_seen_is_readonly_until_mark():
    """seen() must not create state — a failed processing attempt that only
    checked must leave the retry path clean."""
    dedup = RedisDeduplicator(FakeRedis(), ttl_seconds=60)
    assert await dedup.seen("evt-1") is False
    assert await dedup.seen("evt-1") is False  # still unseen: no implicit claim


async def test_mark_respects_ttl():
    redis = FakeRedis()
    dedup = RedisDeduplicator(redis, ttl_seconds=60)
    await dedup.mark("evt-1")
    assert "dedup:evt-1" in redis.expiries  # marker expires, index is the backstop


async def test_seen_fails_open_on_redis_error():
    dedup = RedisDeduplicator(FakeRedis(fail=True), ttl_seconds=60)
    assert await dedup.seen("evt-1") is False  # proceed; unique index guards


async def test_mark_swallows_redis_error():
    dedup = RedisDeduplicator(FakeRedis(fail=True), ttl_seconds=60)
    await dedup.mark("evt-1")  # must not raise into the worker loop
