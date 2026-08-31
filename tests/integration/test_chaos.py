"""Chaos tests: the ARCHITECTURE.md failure-mode table, executable.

These pause/unpause docker-compose containers mid-test, so they are opt-in:

    CHAOS=1 pytest -m chaos

Each test uses shrunken timeouts/backoff so outages resolve in seconds. The
Mongo test is the regression guard for a real bug: an earlier dedup design
(claim-before-write) dropped events on retry after a transient outage —
exactly the scenario simulated here.
"""

import asyncio
import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.integration.conftest import poll_until, running_app

REPO_ROOT = Path(__file__).parents[2]

pytestmark = [
    pytest.mark.integration,
    pytest.mark.chaos,
    pytest.mark.skipif(
        os.environ.get("CHAOS") != "1",
        reason="chaos tests pause docker containers; opt in with CHAOS=1",
    ),
]


def _compose(*args: str) -> None:
    subprocess.run(
        ["docker", "compose", *args], cwd=REPO_ROOT, check=True, capture_output=True
    )


def _event(user_id: str, **overrides) -> dict:
    payload = {
        "event_type": "pageview",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "source_url": "https://example.com/pricing",
        "metadata": {"browser": "firefox"},
    }
    payload.update(overrides)
    return payload


@pytest.fixture
def chaos_settings(settings):
    """Short timeouts and fast backoff so outages resolve within the test."""
    return settings.model_copy(
        update={
            "mongo_server_selection_timeout_ms": 2000,
            "es_request_timeout_seconds": 2.0,
            "redis_socket_timeout_seconds": 1.0,
            "backoff_base_seconds": 0.2,
            "backoff_max_seconds": 1.0,
            "max_retries": 10,
            "visibility_timeout_seconds": 15.0,
        }
    )


async def test_transient_mongo_outage_is_retried_not_dropped(chaos_settings):
    """Failure table row: 'MongoDB down'. The event accepted during the
    outage must be persisted exactly once after recovery — not dropped as a
    duplicate of its own failed attempt, and not dead-lettered."""
    async with running_app(chaos_settings) as client:
        user_id = f"user-{uuid.uuid4().hex[:8]}"
        _compose("pause", "mongo")
        try:
            response = await client.post("/events", json=_event(user_id))
            assert response.status_code == 202  # ingestion is decoupled from Mongo
            await asyncio.sleep(4)  # let several retry cycles fail
        finally:
            _compose("unpause", "mongo")

        async def stored():
            result = await client.get("/events", params={"user_id": user_id})
            if result.status_code != 200:
                return None
            events = result.json()["events"]
            return events if events else None

        events = await poll_until(stored, timeout=30)
        assert len(events) == 1  # exactly once: retries did not duplicate or drop

        admin = await client.get("/admin/queue")
        assert admin.json()["dlq"]["size"] == 0
        assert admin.json()["depth"] == 0


async def test_elasticsearch_outage_degrades_search_only(chaos_settings):
    """Failure table row: 'Elasticsearch down'. Ingestion and Mongo reads
    keep working; only /events/search returns 503."""
    async with running_app(chaos_settings) as client:
        user_id = f"user-{uuid.uuid4().hex[:8]}"
        _compose("pause", "elasticsearch")
        try:
            response = await client.post("/events", json=_event(user_id))
            assert response.status_code == 202

            async def stored():
                result = await client.get("/events", params={"user_id": user_id})
                events = result.json()["events"]
                return events if events else None

            await poll_until(stored, timeout=20)  # Mongo path unaffected

            search = await client.get("/events/search", params={"q": "firefox"})
            assert search.status_code == 503
            assert search.json()["error"]["code"] == "search_unavailable"

            health = await client.get("/health")
            assert health.json()["status"] == "degraded"
            assert health.json()["checks"]["elasticsearch"] == "down"
        finally:
            _compose("unpause", "elasticsearch")

        admin = await client.get("/admin/queue")
        assert admin.json()["dlq"]["size"] == 0  # ES failure never dead-letters


async def test_redis_outage_fails_open_everywhere(chaos_settings):
    """Failure table row: 'Redis down'. Realtime stats bypass the cache,
    dedup fails open (unique index still guards), ingestion keeps working."""
    async with running_app(chaos_settings) as client:
        user_id = f"user-{uuid.uuid4().hex[:8]}"
        _compose("pause", "redis")
        try:
            realtime = await client.get("/events/stats/realtime")
            assert realtime.status_code == 200
            assert realtime.headers["X-Cache"] == "BYPASS"

            response = await client.post("/events", json=_event(user_id))
            assert response.status_code == 202

            async def stored():
                result = await client.get("/events", params={"user_id": user_id})
                events = result.json()["events"]
                return events if events else None

            await poll_until(stored, timeout=20)  # processed despite Redis outage
        finally:
            _compose("unpause", "redis")

        # Cache resumes normal operation after recovery.
        recovered = await client.get("/events/stats/realtime")
        assert recovered.headers["X-Cache"] in ("MISS", "HIT")
