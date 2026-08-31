"""Application factory and composition root.

All wiring happens here: clients are created in the lifespan, dependencies are
injected into services by constructor, and services are attached to app.state.
Everything below this module is framework-agnostic and unit-testable with fakes.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Awaitable, Callable, Optional, TypeVar

from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from app.config import Settings, get_settings
from app.errors import register_error_handlers
from app.ingestion.router import router as ingestion_router
from app.ingestion.service import IngestionService
from app.logging_setup import setup_logging
from app.middleware.rate_limit import RateLimitMiddleware
from app.processing.dedup import RedisDeduplicator
from app.processing.worker import EventWorker
from app.queueing.dlq import DeadLetterQueue
from app.queueing.queue import InProcessQueue
from app.querying.admin_router import router as admin_router
from app.querying.router import router as querying_router
from app.querying.service import QueryService
from app.storage.elastic import ElasticEventStore
from app.storage.mongo import MongoEventRepository
from app.storage.redis_cache import RealtimeStatsCache

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def _retry_startup(
    name: str, action: Callable[[], Awaitable[T]], retries: int, delay: float
) -> T:
    """Retry dependency bootstrap so the app tolerates container start ordering."""
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return await action()
        except Exception as exc:
            last_exc = exc
            logger.warning("startup: %s not ready attempt=%d/%d error=%s", name, attempt, retries, exc)
            await asyncio.sleep(delay)
    raise RuntimeError(f"could not initialize {name} after {retries} attempts") from last_exc


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        setup_logging(settings.log_level)

        mongo_client: AsyncMongoClient = AsyncMongoClient(
            settings.mongo_uri,
            tz_aware=True,
            serverSelectionTimeoutMS=settings.mongo_server_selection_timeout_ms,
        )
        es_client = AsyncElasticsearch(
            settings.es_url, request_timeout=settings.es_request_timeout_seconds
        )
        # Socket timeouts are load-bearing: without them a frozen Redis hangs
        # callers forever instead of tripping the RedisError fail-open paths.
        redis = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_timeout=settings.redis_socket_timeout_seconds,
            socket_connect_timeout=settings.redis_socket_timeout_seconds,
        )

        repository = MongoEventRepository(mongo_client, settings.mongo_db)
        search_store = ElasticEventStore(es_client, settings.es_index)

        await _retry_startup(
            "mongodb", repository.ensure_indexes,
            settings.startup_connect_retries, settings.startup_connect_delay_seconds,
        )
        await _retry_startup(
            "elasticsearch", search_store.ensure_index,
            settings.startup_connect_retries, settings.startup_connect_delay_seconds,
        )

        dlq = DeadLetterQueue(max_size=settings.dlq_max_size)
        queue = InProcessQueue(
            max_size=settings.queue_max_size,
            visibility_timeout_seconds=settings.visibility_timeout_seconds,
            dlq=dlq,
        )
        deduplicator = RedisDeduplicator(redis, ttl_seconds=settings.dedup_ttl_seconds)
        worker = EventWorker(queue, repository, search_store, deduplicator, settings)
        cache = RealtimeStatsCache(redis)

        app.state.settings = settings
        app.state.redis = redis
        app.state.queue = queue
        app.state.repository = repository
        app.state.search_store = search_store
        app.state.ingestion_service = IngestionService(queue)
        app.state.query_service = QueryService(repository, search_store, cache, settings)

        worker.start()
        logger.info("application started name=%s", settings.app_name)
        try:
            yield
        finally:
            await worker.stop()
            await queue.close()
            await es_client.close()
            await redis.aclose()
            await mongo_client.close()
            logger.info("application stopped")

    app = FastAPI(
        title="Distributed Event Processing Platform",
        description="Async event ingestion with MongoDB, Elasticsearch and Redis.",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.add_middleware(RateLimitMiddleware, settings=settings)
    app.include_router(ingestion_router)
    app.include_router(querying_router)
    app.include_router(admin_router)
    register_error_handlers(app)
    return app


app = create_app()
