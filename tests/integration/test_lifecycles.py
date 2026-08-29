"""Full request lifecycles: ingest -> worker processes -> query returns result."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from tests.integration.conftest import poll_until

pytestmark = pytest.mark.integration


def _event(user_id: str, event_type: str = "pageview", **overrides) -> dict:
    payload = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "source_url": "https://example.com/pricing",
        "metadata": {"browser": "firefox", "device": "mobile"},
    }
    payload.update(overrides)
    return payload


async def test_ingest_to_query_lifecycle(client):
    """Lifecycle 1: POST /events -> worker persists -> GET /events returns it."""
    user_id = f"user-{uuid.uuid4().hex[:8]}"

    response = await client.post("/events", json=_event(user_id, event_type="click"))
    assert response.status_code == 202
    accepted = response.json()
    assert accepted["status"] == "accepted"
    event_id = accepted["event_id"]

    async def fetch():
        result = await client.get("/events", params={"user_id": user_id})
        assert result.status_code == 200
        events = result.json()["events"]
        return events if events else None

    events = await poll_until(fetch)
    assert len(events) == 1
    stored = events[0]
    assert stored["event_id"] == event_id
    assert stored["event_type"] == "click"
    assert stored["source_url"] == "https://example.com/pricing"
    assert stored["metadata"] == {"browser": "firefox", "device": "mobile"}

    # Filters actually filter.
    miss = await client.get("/events", params={"user_id": user_id, "event_type": "pageview"})
    assert miss.json()["events"] == []


async def test_ingest_to_stats_lifecycle(client):
    """Lifecycle 2: ingest a batch -> aggregation buckets reflect it."""
    event_type = f"type-{uuid.uuid4().hex[:8]}"
    user_id = f"user-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)

    # 3 events today, 2 yesterday.
    timestamps = [now, now, now, now - timedelta(days=1), now - timedelta(days=1)]
    for ts in timestamps:
        response = await client.post(
            "/events", json=_event(user_id, event_type=event_type, timestamp=ts.isoformat())
        )
        assert response.status_code == 202

    async def fetch_stats():
        result = await client.get(
            "/events/stats",
            params={
                "bucket": "daily",
                "event_type": event_type,
                "start": (now - timedelta(days=2)).isoformat(),
                "end": (now + timedelta(hours=1)).isoformat(),
            },
        )
        assert result.status_code == 200
        buckets = result.json()["buckets"]
        return buckets if sum(b["count"] for b in buckets) == 5 else None

    buckets = await poll_until(fetch_stats)
    counts = sorted(b["count"] for b in buckets)
    assert counts == [2, 3]
    assert all(b["event_type"] == event_type for b in buckets)


async def test_ingest_to_search_lifecycle(client):
    """Lifecycle 3: ingest -> ES indexes -> full-text search over metadata."""
    user_id = f"user-{uuid.uuid4().hex[:8]}"
    token = f"campaign{uuid.uuid4().hex[:8]}"

    response = await client.post(
        "/events",
        json=_event(
            user_id,
            event_type="conversion",
            metadata={"campaign_name": f"summer {token} launch", "device": "tablet"},
        ),
    )
    assert response.status_code == 202

    async def search():
        result = await client.get("/events/search", params={"q": token})
        assert result.status_code == 200
        body = result.json()
        return body if body["total"] >= 1 else None

    body = await poll_until(search)
    hit = body["results"][0]["event"]
    assert hit["user_id"] == user_id
    assert token in hit["metadata"]["campaign_name"]
    assert "metadata_text" not in hit  # internal field must not leak


async def test_realtime_stats_cache_lifecycle(client):
    """Lifecycle 4: realtime summary is computed once then served from Redis."""
    user_id = f"user-{uuid.uuid4().hex[:8]}"
    response = await client.post("/events", json=_event(user_id))
    assert response.status_code == 202

    async def first_call():
        result = await client.get("/events/stats/realtime")
        assert result.status_code == 200
        return result if result.json()["stats"]["total"] >= 1 else None

    first = await poll_until(first_call)
    assert first.headers["X-Cache"] == "MISS"

    second = await client.get("/events/stats/realtime")
    assert second.headers["X-Cache"] == "HIT"
    assert second.json()["stats"] == first.json()["stats"]  # frozen for the TTL window

    # A different TTL uses a different cache entry (recomputed).
    other_ttl = await client.get("/events/stats/realtime", params={"ttl": 60})
    assert other_ttl.headers["X-Cache"] == "MISS"
    assert other_ttl.json()["ttl_seconds"] == 60


async def test_health_reports_dependencies(client):
    response = await client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["checks"] == {"mongodb": "up", "elasticsearch": "up", "redis": "up"}
