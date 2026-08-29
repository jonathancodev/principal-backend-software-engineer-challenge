"""Unit tests for query validation logic and the stats pipeline builder."""

from datetime import datetime, timezone

import pytest

from app.config import Settings
from app.errors import InvalidQueryError
from app.querying.service import QueryService
from app.storage.mongo import build_stats_pipeline
from app.storage.redis_cache import RealtimeStatsCache

from tests.unit.fakes import FakeRedis


class _StubRepo:
    async def aggregate_stats(self, bucket, start, end, event_type=None):
        return []

    async def realtime_summary(self, window_start):
        return {"total": 0, "by_type": {}}


def _service(**settings_overrides) -> QueryService:
    settings = Settings(**settings_overrides)
    return QueryService(_StubRepo(), None, RealtimeStatsCache(FakeRedis()), settings)


async def test_stats_rejects_unknown_bucket():
    with pytest.raises(InvalidQueryError, match="bucket must be one of"):
        await _service().stats(bucket="monthly", start=None, end=None, event_type=None)


async def test_stats_rejects_inverted_range():
    with pytest.raises(InvalidQueryError, match="start must be before end"):
        await _service().stats(
            bucket="daily",
            start=datetime(2026, 8, 20, tzinfo=timezone.utc),
            end=datetime(2026, 8, 10, tzinfo=timezone.utc),
            event_type=None,
        )


async def test_stats_rejects_ranges_with_too_many_buckets():
    with pytest.raises(InvalidQueryError, match="buckets"):
        await _service(stats_max_buckets=100).stats(
            bucket="hourly",
            start=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end=datetime(2026, 3, 1, tzinfo=timezone.utc),  # ~1400 hourly buckets
            event_type=None,
        )


async def test_stats_applies_default_range():
    result = await _service().stats(bucket="daily", start=None, end=None, event_type=None)
    assert result["bucket"] == "daily"
    assert result["start"] < result["end"]


@pytest.mark.parametrize("ttl", [0, -5, 10_000])
async def test_realtime_ttl_bounds_enforced(ttl):
    with pytest.raises(InvalidQueryError, match="ttl must be between"):
        await _service().realtime_stats(ttl_override=ttl)


async def test_realtime_returns_cache_status_and_stats():
    result = await _service().realtime_stats(ttl_override=None)
    assert result["cache"] == "MISS"
    assert result["stats"]["total"] == 0
    assert "generated_at" in result["stats"]


def test_stats_pipeline_shape():
    start = datetime(2026, 8, 1, tzinfo=timezone.utc)
    end = datetime(2026, 8, 29, tzinfo=timezone.utc)
    pipeline = build_stats_pipeline("daily", start, end, event_type="click")

    match = pipeline[0]["$match"]
    assert match["timestamp"] == {"$gte": start, "$lt": end}
    assert match["event_type"] == "click"

    group = pipeline[1]["$group"]
    assert group["_id"]["bucket"]["$dateTrunc"]["unit"] == "day"
    assert group["count"] == {"$sum": 1}


def test_stats_pipeline_bucket_units():
    start = datetime(2026, 8, 1, tzinfo=timezone.utc)
    end = datetime(2026, 8, 29, tzinfo=timezone.utc)
    units = {
        "hourly": "hour",
        "daily": "day",
        "weekly": "week",
    }
    for bucket, unit in units.items():
        pipeline = build_stats_pipeline(bucket, start, end)
        assert pipeline[1]["$group"]["_id"]["bucket"]["$dateTrunc"]["unit"] == unit
