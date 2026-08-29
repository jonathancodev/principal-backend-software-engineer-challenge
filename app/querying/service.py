"""Query orchestration: routes each read to the store that owns it.

- Filters and stats  -> MongoDB (source of truth, aggregation pipelines)
- Full-text search   -> Elasticsearch
- Realtime summary   -> Redis cache in front of a Mongo aggregation
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from app.config import Settings
from app.errors import InvalidQueryError
from app.storage.elastic import ElasticEventStore
from app.storage.mongo import BUCKET_UNITS, MongoEventRepository
from app.storage.redis_cache import RealtimeStatsCache

logger = logging.getLogger(__name__)

_BUCKET_SPAN = {"hourly": timedelta(hours=1), "daily": timedelta(days=1), "weekly": timedelta(weeks=1)}
_DEFAULT_RANGE = {"hourly": timedelta(days=1), "daily": timedelta(days=7), "weekly": timedelta(weeks=12)}


class QueryService:
    def __init__(
        self,
        repository: MongoEventRepository,
        search_store: ElasticEventStore,
        cache: RealtimeStatsCache,
        settings: Settings,
    ) -> None:
        self._repository = repository
        self._search_store = search_store
        self._cache = cache
        self._settings = settings

    async def list_events(
        self,
        event_type: Optional[str],
        user_id: Optional[str],
        source_url: Optional[str],
        start: Optional[datetime],
        end: Optional[datetime],
        limit: int,
        offset: int,
    ) -> Dict[str, Any]:
        start, end = _validate_range(start, end)
        events = await self._repository.find_events(
            event_type=event_type,
            user_id=user_id,
            source_url=source_url,
            start=start,
            end=end,
            limit=limit,
            offset=offset,
        )
        return {"count": len(events), "limit": limit, "offset": offset, "events": events}

    async def stats(
        self,
        bucket: str,
        start: Optional[datetime],
        end: Optional[datetime],
        event_type: Optional[str],
    ) -> Dict[str, Any]:
        if bucket not in BUCKET_UNITS:
            raise InvalidQueryError(
                f"bucket must be one of {sorted(BUCKET_UNITS)}, got '{bucket}'"
            )
        now = datetime.now(timezone.utc)
        end = end or now
        start = start or (end - _DEFAULT_RANGE[bucket])
        start, end = _validate_range(start, end)

        bucket_count = (end - start) / _BUCKET_SPAN[bucket]
        if bucket_count > self._settings.stats_max_buckets:
            raise InvalidQueryError(
                f"range spans ~{int(bucket_count)} {bucket} buckets; "
                f"maximum is {self._settings.stats_max_buckets}. Narrow the range "
                f"or use a coarser bucket."
            )

        rows = await self._repository.aggregate_stats(bucket, start, end, event_type)
        return {
            "bucket": bucket,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "buckets": rows,
        }

    async def search(
        self,
        query_text: str,
        event_type: Optional[str],
        user_id: Optional[str],
        start: Optional[datetime],
        end: Optional[datetime],
        limit: int,
    ) -> Dict[str, Any]:
        start, end = _validate_range(start, end)
        return await self._search_store.search(
            query_text=query_text,
            event_type=event_type,
            user_id=user_id,
            start=start,
            end=end,
            limit=limit,
        )

    async def realtime_stats(self, ttl_override: Optional[int]) -> Dict[str, Any]:
        # Explicit None check: `ttl_override or default` would silently swallow
        # an (invalid) ttl=0 instead of rejecting it.
        ttl = self._settings.realtime_ttl_seconds if ttl_override is None else ttl_override
        if ttl < 1 or ttl > self._settings.realtime_max_ttl_seconds:
            raise InvalidQueryError(
                f"ttl must be between 1 and {self._settings.realtime_max_ttl_seconds} seconds"
            )
        window = self._settings.realtime_window_minutes

        async def compute() -> Dict[str, Any]:
            window_start = datetime.now(timezone.utc) - timedelta(minutes=window)
            summary = await self._repository.realtime_summary(window_start)
            summary["window_minutes"] = window
            summary["generated_at"] = datetime.now(timezone.utc).isoformat()
            return summary

        # TTL participates in the key so different freshness requirements
        # don't serve each other stale entries.
        data, cache_status = await self._cache.get_or_compute(
            key_suffix=f"w{window}:t{ttl}", ttl_seconds=ttl, compute=compute
        )
        return {"ttl_seconds": ttl, "cache": cache_status, "stats": data}


def _validate_range(
    start: Optional[datetime], end: Optional[datetime]
) -> "tuple[Optional[datetime], Optional[datetime]]":
    start = _to_utc(start)
    end = _to_utc(end)
    if start and end and start >= end:
        raise InvalidQueryError("start must be before end")
    return start, end


def _to_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
