"""Integration test fixtures.

These tests exercise the real app (lifespan, worker, stores) against live
MongoDB / Elasticsearch / Redis — typically the docker-compose services.
They skip automatically when the services are unreachable, so `pytest` is
always safe to run; use `pytest -m integration` to run only these.

Isolation: each session gets a unique Mongo database and ES index (dropped on
teardown) and Redis db 15 is flushed, so runs never pollute dev data.
"""

import asyncio
import os
import uuid

import httpx
import pytest
from elasticsearch import AsyncElasticsearch
from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from app.config import Settings
from app.main import create_app

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/15")


async def _services_reachable() -> bool:
    try:
        mongo = AsyncMongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        await mongo.admin.command("ping")
        await mongo.close()

        es = AsyncElasticsearch(ES_URL, request_timeout=2)
        es_ok = await es.ping()
        await es.close()

        redis = Redis.from_url(REDIS_URL)
        await redis.ping()
        await redis.aclose()
        return bool(es_ok)
    except Exception:
        return False


@pytest.fixture
def settings() -> Settings:
    run_id = uuid.uuid4().hex[:8]
    return Settings(
        mongo_uri=MONGO_URI,
        mongo_db=f"event_platform_test_{run_id}",
        es_url=ES_URL,
        es_index=f"events-test-{run_id}",
        redis_url=REDIS_URL,
        worker_concurrency=2,
        max_retries=3,
        backoff_base_seconds=0.05,
        visibility_timeout_seconds=10.0,
        realtime_ttl_seconds=5,
        startup_connect_retries=3,
        startup_connect_delay_seconds=0.5,
        log_level="WARNING",
    )


@pytest.fixture
async def client(settings):
    if not await _services_reachable():
        pytest.skip("MongoDB/Elasticsearch/Redis not reachable; start docker-compose services")

    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
            yield c

    # Teardown: drop the per-run database/index and flush the test Redis db.
    mongo = AsyncMongoClient(settings.mongo_uri)
    await mongo.drop_database(settings.mongo_db)
    await mongo.close()

    es = AsyncElasticsearch(settings.es_url)
    await es.options(ignore_status=404).indices.delete(index=settings.es_index)
    await es.close()

    redis = Redis.from_url(settings.redis_url)
    await redis.flushdb()
    await redis.aclose()


async def poll_until(check, timeout: float = 15.0, interval: float = 0.2):
    """Await an async predicate until it returns a truthy value or times out."""
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        last = await check()
        if last:
            return last
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s (last={last!r})")
