"""Unit tests for the event domain models (validation contract)."""

from datetime import datetime, timezone, timedelta

import pytest
from pydantic import ValidationError

from app.domain.models import EventIn, EventRecord, MAX_METADATA_BYTES


def _payload(**overrides):
    base = {
        "event_type": "pageview",
        "timestamp": "2026-08-29T12:00:00Z",
        "user_id": "user-1",
        "source_url": "https://example.com/pricing",
        "metadata": {"browser": "firefox", "device": "mobile"},
    }
    base.update(overrides)
    return base


def test_valid_event_parses():
    event = EventIn(**_payload())
    assert event.event_type == "pageview"
    assert event.timestamp.tzinfo is not None


def test_naive_timestamp_is_assumed_utc():
    event = EventIn(**_payload(timestamp=datetime(2026, 8, 29, 12, 0, 0)))
    assert event.timestamp.tzinfo == timezone.utc


def test_aware_timestamp_is_converted_to_utc():
    tz = timezone(timedelta(hours=-3))
    event = EventIn(**_payload(timestamp=datetime(2026, 8, 29, 12, 0, 0, tzinfo=tz)))
    assert event.timestamp.hour == 15
    assert event.timestamp.tzinfo == timezone.utc


@pytest.mark.parametrize("bad_url", ["ftp://example.com", "example.com", "javascript:alert(1)"])
def test_source_url_scheme_is_enforced(bad_url):
    with pytest.raises(ValidationError):
        EventIn(**_payload(source_url=bad_url))


def test_empty_event_type_rejected():
    with pytest.raises(ValidationError):
        EventIn(**_payload(event_type=""))


def test_oversized_metadata_rejected():
    with pytest.raises(ValidationError):
        EventIn(**_payload(metadata={"blob": "x" * (MAX_METADATA_BYTES + 1)}))


def test_record_generates_event_id_when_absent():
    record = EventRecord.from_input(EventIn(**_payload()))
    assert record.event_id
    assert record.ingested_at.tzinfo is not None


def test_record_preserves_client_event_id():
    record = EventRecord.from_input(EventIn(**_payload(event_id="idempotency-key-1")))
    assert record.event_id == "idempotency-key-1"


def test_record_round_trips_through_json():
    """The queue carries JSON dicts; the worker must reconstruct losslessly."""
    record = EventRecord.from_input(EventIn(**_payload()))
    restored = EventRecord.model_validate(record.model_dump(mode="json"))
    assert restored == record
