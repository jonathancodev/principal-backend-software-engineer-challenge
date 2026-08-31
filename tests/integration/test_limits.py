"""Backpressure and rate limiting against the real app (no chaos required)."""

import uuid
from datetime import datetime, timezone

import pytest

from tests.integration.conftest import running_app

pytestmark = pytest.mark.integration


def _event(user_id: str) -> dict:
    return {
        "event_type": "pageview",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "source_url": "https://example.com",
        "metadata": {},
    }


async def test_queue_backpressure_returns_503(settings):
    # No consumers + tiny queue: the third accepted event has nowhere to go.
    constrained = settings.model_copy(
        update={"worker_concurrency": 0, "queue_max_size": 2}
    )
    async with running_app(constrained) as client:
        user_id = f"user-{uuid.uuid4().hex[:8]}"
        assert (await client.post("/events", json=_event(user_id))).status_code == 202
        assert (await client.post("/events", json=_event(user_id))).status_code == 202

        third = await client.post("/events", json=_event(user_id))
        assert third.status_code == 503
        assert third.json()["error"]["code"] == "queue_full"
        assert "Retry-After" in third.headers

        admin = await client.get("/admin/queue")
        assert admin.json()["depth"] == 2  # accepted events are still buffered


async def test_rate_limit_returns_429_with_retry_after(settings):
    limited = settings.model_copy(update={"rate_limit_requests": 2})
    async with running_app(limited) as client:
        user_id = f"user-{uuid.uuid4().hex[:8]}"
        assert (await client.post("/events", json=_event(user_id))).status_code == 202
        assert (await client.post("/events", json=_event(user_id))).status_code == 202

        third = await client.post("/events", json=_event(user_id))
        assert third.status_code == 429
        assert third.json()["error"]["code"] == "rate_limited"
        assert "Retry-After" in third.headers

        # Reads are not rate limited.
        assert (await client.get("/events", params={"user_id": user_id})).status_code == 200
