"""Read endpoints: /events, /events/stats, /events/search, /events/stats/realtime."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Query, Request, Response

router = APIRouter(tags=["querying"])


@router.get("/events")
async def list_events(
    request: Request,
    event_type: Optional[str] = Query(None, max_length=64),
    user_id: Optional[str] = Query(None, max_length=128),
    source_url: Optional[str] = Query(None, max_length=2048),
    start: Optional[datetime] = Query(None, description="Inclusive ISO-8601 lower bound"),
    end: Optional[datetime] = Query(None, description="Exclusive ISO-8601 upper bound"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0, le=100_000),
) -> dict:
    """Filter events by type, user, source URL and/or date range (MongoDB)."""
    return await request.app.state.query_service.list_events(
        event_type=event_type,
        user_id=user_id,
        source_url=source_url,
        start=start,
        end=end,
        limit=limit,
        offset=offset,
    )


@router.get("/events/stats")
async def event_stats(
    request: Request,
    bucket: str = Query("daily", description="hourly | daily | weekly"),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    event_type: Optional[str] = Query(None, max_length=64),
) -> dict:
    """Counts grouped by event type and time bucket (MongoDB aggregation)."""
    return await request.app.state.query_service.stats(
        bucket=bucket, start=start, end=end, event_type=event_type
    )


@router.get("/events/search")
async def search_events(
    request: Request,
    q: str = Query(..., min_length=1, max_length=512, description="Full-text query"),
    event_type: Optional[str] = Query(None, max_length=64),
    user_id: Optional[str] = Query(None, max_length=128),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    limit: int = Query(20, ge=1, le=100),
) -> dict:
    """Full-text search across event metadata (Elasticsearch)."""
    return await request.app.state.query_service.search(
        query_text=q, event_type=event_type, user_id=user_id, start=start, end=end, limit=limit
    )


@router.get("/events/stats/realtime")
async def realtime_stats(
    request: Request,
    response: Response,
    ttl: Optional[int] = Query(None, ge=1, description="Cache TTL override in seconds"),
) -> dict:
    """Lightweight stats summary served from Redis with a configurable TTL."""
    result = await request.app.state.query_service.realtime_stats(ttl_override=ttl)
    response.headers["X-Cache"] = result["cache"]
    return result
