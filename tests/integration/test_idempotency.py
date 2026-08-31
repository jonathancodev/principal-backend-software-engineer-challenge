"""End-to-end idempotency: what happens when the same event arrives twice.

Two contracts are pinned here:
1. A client-supplied ``event_id`` is an idempotency key — duplicate POSTs
   with the same key result in exactly one stored event.
2. Without an ``event_id``, identical payloads are treated as distinct
   events (each gets a server-generated UUID). This is a deliberate,
   documented semantic choice — this test is its executable statement, and
   it would flip if content-hash deduplication were ever adopted.
"""

import asyncio
import uuid
from datetime import datetime, timezone

import pytest

from tests.integration.conftest import poll_until

pytestmark = pytest.mark.integration


def _event(user_id: str, **overrides) -> dict:
    payload = {
        "event_type": "conversion",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "source_url": "https://example.com/checkout",
        "metadata": {"plan": "pro"},
    }
    payload.update(overrides)
    return payload


async def test_duplicate_client_event_id_is_stored_once(client):
    user_id = f"user-{uuid.uuid4().hex[:8]}"
    idempotency_key = f"idem-{uuid.uuid4().hex}"
    payload = _event(user_id, event_id=idempotency_key)

    first = await client.post("/events", json=payload)
    second = await client.post("/events", json=payload)
    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["event_id"] == idempotency_key
    assert second.json()["event_id"] == idempotency_key

    async def one_stored():
        result = await client.get("/events", params={"user_id": user_id})
        events = result.json()["events"]
        return events if events else None

    await poll_until(one_stored)
    # Give the second (duplicate) message time to be processed and dropped.
    await asyncio.sleep(1.0)

    result = await client.get("/events", params={"user_id": user_id})
    events = result.json()["events"]
    assert len(events) == 1
    assert events[0]["event_id"] == idempotency_key

    admin = await client.get("/admin/queue")
    assert admin.json()["dlq"]["size"] == 0


async def test_identical_payloads_without_event_id_are_distinct_events(client):
    """Documents current semantics: no event_id means no dedup at the POST
    boundary — two identical payloads are two events."""
    user_id = f"user-{uuid.uuid4().hex[:8]}"
    payload = _event(user_id)

    for _ in range(2):
        response = await client.post("/events", json=payload)
        assert response.status_code == 202

    async def two_stored():
        result = await client.get("/events", params={"user_id": user_id})
        events = result.json()["events"]
        return events if len(events) == 2 else None

    events = await poll_until(two_stored)
    assert events[0]["event_id"] != events[1]["event_id"]
