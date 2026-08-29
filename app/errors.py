"""Domain errors and their HTTP mappings.

Internal layers raise these instead of HTTPException so storage/queue code
stays framework-agnostic; the API layer translates them at the boundary.
"""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


class QueueFullError(Exception):
    """The ingestion queue is at capacity (backpressure)."""


class DuplicateEventError(Exception):
    """An event with the same event_id was already persisted."""


class StorageUnavailableError(Exception):
    """MongoDB could not serve the request."""


class SearchUnavailableError(Exception):
    """Elasticsearch could not serve the request."""


class InvalidQueryError(Exception):
    """A query parameter combination is invalid (bad range, too many buckets...)."""


def _error_body(code: str, message: str) -> dict:
    return {"error": {"code": code, "message": message}}


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(QueueFullError)
    async def _queue_full(request: Request, exc: QueueFullError) -> JSONResponse:
        logger.warning("ingestion rejected: queue full path=%s", request.url.path)
        return JSONResponse(
            status_code=503,
            content=_error_body("queue_full", "Ingestion queue is full, retry later."),
            headers={"Retry-After": "1"},
        )

    @app.exception_handler(InvalidQueryError)
    async def _invalid_query(request: Request, exc: InvalidQueryError) -> JSONResponse:
        return JSONResponse(status_code=422, content=_error_body("invalid_query", str(exc)))

    @app.exception_handler(StorageUnavailableError)
    async def _storage_down(request: Request, exc: StorageUnavailableError) -> JSONResponse:
        logger.error("storage unavailable path=%s error=%s", request.url.path, exc)
        return JSONResponse(
            status_code=503,
            content=_error_body("storage_unavailable", "Event store is temporarily unavailable."),
        )

    @app.exception_handler(SearchUnavailableError)
    async def _search_down(request: Request, exc: SearchUnavailableError) -> JSONResponse:
        logger.error("search unavailable path=%s error=%s", request.url.path, exc)
        return JSONResponse(
            status_code=503,
            content=_error_body("search_unavailable", "Search is temporarily unavailable."),
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error path=%s", request.url.path)
        return JSONResponse(
            status_code=500,
            content=_error_body("internal_error", "An unexpected error occurred."),
        )
