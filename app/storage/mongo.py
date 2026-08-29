"""MongoDB storage: source of truth for events, filter queries and aggregations.

Index strategy (see ARCHITECTURE.md for the full rationale):

- ``(event_type, timestamp desc)``  — serves GET /events filtered by type and
  the stats aggregation's $match; equality field first, range/sort second.
- ``(user_id, timestamp desc)``     — per-user event timelines.
- ``(timestamp desc)``              — date-range-only queries and realtime window scans.
- ``event_id`` unique               — idempotency backstop behind the Redis dedup check.

Deliberately NOT indexed:
- ``source_url`` — long, low-selectivity values with high write amplification;
  URL filtering rides the timestamp index, and text lookups belong to ES.
- anything under ``metadata`` — unbounded key space; that is Elasticsearch's job.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from pymongo import AsyncMongoClient, DESCENDING
from pymongo.errors import DuplicateKeyError, PyMongoError

from app.domain.models import EventRecord
from app.errors import DuplicateEventError, StorageUnavailableError

logger = logging.getLogger(__name__)

BUCKET_UNITS = {"hourly": "hour", "daily": "day", "weekly": "week"}


class MongoEventRepository:
    def __init__(self, client: AsyncMongoClient, db_name: str) -> None:
        self._collection = client[db_name]["events"]

    async def ensure_indexes(self) -> None:
        await self._collection.create_index(
            [("event_type", 1), ("timestamp", DESCENDING)], name="event_type_timestamp"
        )
        await self._collection.create_index(
            [("user_id", 1), ("timestamp", DESCENDING)], name="user_id_timestamp"
        )
        await self._collection.create_index([("timestamp", DESCENDING)], name="timestamp")
        await self._collection.create_index("event_id", unique=True, name="event_id_unique")

    async def ping(self) -> None:
        await self._collection.database.command("ping")

    # --- Writes (worker path) ---

    async def insert_event(self, record: EventRecord) -> None:
        doc = record.model_dump()
        try:
            await self._collection.insert_one(doc)
        except DuplicateKeyError:
            raise DuplicateEventError(record.event_id) from None
        except PyMongoError as exc:
            raise StorageUnavailableError(str(exc)) from exc

    # --- Reads (query path) ---

    async def find_events(
        self,
        event_type: Optional[str] = None,
        user_id: Optional[str] = None,
        source_url: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {}
        if event_type:
            query["event_type"] = event_type
        if user_id:
            query["user_id"] = user_id
        if source_url:
            query["source_url"] = source_url
        time_range: Dict[str, Any] = {}
        if start:
            time_range["$gte"] = start
        if end:
            time_range["$lt"] = end
        if time_range:
            query["timestamp"] = time_range
        try:
            cursor = (
                self._collection.find(query, projection={"_id": False})
                .sort("timestamp", DESCENDING)
                .skip(offset)
                .limit(limit)
            )
            return await cursor.to_list(length=limit)
        except PyMongoError as exc:
            raise StorageUnavailableError(str(exc)) from exc

    async def aggregate_stats(
        self,
        bucket: str,
        start: datetime,
        end: datetime,
        event_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        pipeline = build_stats_pipeline(bucket, start, end, event_type)
        try:
            cursor = await self._collection.aggregate(pipeline)
            rows = await cursor.to_list(length=None)
        except PyMongoError as exc:
            raise StorageUnavailableError(str(exc)) from exc
        return [
            {
                "bucket_start": row["_id"]["bucket"],
                "event_type": row["_id"]["event_type"],
                "count": row["count"],
            }
            for row in rows
        ]

    async def realtime_summary(self, window_start: datetime) -> Dict[str, Any]:
        pipeline = [
            {"$match": {"timestamp": {"$gte": window_start}}},
            {"$group": {"_id": "$event_type", "count": {"$sum": 1}}},
        ]
        try:
            cursor = await self._collection.aggregate(pipeline)
            rows = await cursor.to_list(length=None)
        except PyMongoError as exc:
            raise StorageUnavailableError(str(exc)) from exc
        by_type = {row["_id"]: row["count"] for row in rows}
        return {"total": sum(by_type.values()), "by_type": by_type}


def build_stats_pipeline(
    bucket: str,
    start: datetime,
    end: datetime,
    event_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Build the stats aggregation pipeline (pure function, unit-testable)."""
    unit = BUCKET_UNITS[bucket]
    match: Dict[str, Any] = {"timestamp": {"$gte": start, "$lt": end}}
    if event_type:
        match["event_type"] = event_type
    return [
        {"$match": match},
        {
            "$group": {
                "_id": {
                    "bucket": {
                        "$dateTrunc": {"date": "$timestamp", "unit": unit, "timezone": "UTC"}
                    },
                    "event_type": "$event_type",
                },
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"_id.bucket": 1, "_id.event_type": 1}},
    ]
