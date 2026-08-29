"""Operational endpoints: health and queue/DLQ introspection."""

import logging

from fastapi import APIRouter, Query, Request

logger = logging.getLogger(__name__)

router = APIRouter(tags=["admin"])


@router.get("/health")
async def health(request: Request) -> dict:
    """Dependency health. The app reports degraded rather than dying."""
    state = request.app.state
    checks = {}

    try:
        await state.repository.ping()
        checks["mongodb"] = "up"
    except Exception as exc:
        logger.warning("health: mongo down error=%s", exc)
        checks["mongodb"] = "down"

    try:
        checks["elasticsearch"] = "up" if await state.search_store.ping() else "down"
    except Exception as exc:
        logger.warning("health: elasticsearch down error=%s", exc)
        checks["elasticsearch"] = "down"

    try:
        await state.redis.ping()
        checks["redis"] = "up"
    except Exception as exc:
        logger.warning("health: redis down error=%s", exc)
        checks["redis"] = "down"

    status = "ok" if all(v == "up" for v in checks.values()) else "degraded"
    return {"status": status, "checks": checks}


@router.get("/admin/queue")
async def queue_state(request: Request, dlq_limit: int = Query(20, ge=1, le=100)) -> dict:
    """Queue depth, in-flight count and a peek at dead-lettered messages."""
    queue = request.app.state.queue
    return {
        "depth": queue.depth(),
        "in_flight": queue.in_flight_count(),
        "dlq": {"size": queue.dlq.size(), "items": queue.dlq.peek(limit=dlq_limit)},
    }
