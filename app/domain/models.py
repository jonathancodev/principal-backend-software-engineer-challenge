"""Event domain models.

``EventIn`` is the public ingestion contract; ``EventRecord`` is the internal
representation that travels through the queue and is persisted. The queue
carries JSON-serializable dicts (``EventRecord.model_dump(mode="json")``) to
mirror the serialization boundary a real SQS integration would have.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator

MAX_METADATA_BYTES = 32 * 1024


class EventIn(BaseModel):
    """Payload accepted by POST /events."""

    event_type: str = Field(..., min_length=1, max_length=64, examples=["pageview"])
    timestamp: datetime
    user_id: str = Field(..., min_length=1, max_length=128)
    source_url: str = Field(..., min_length=1, max_length=2048)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    # Optional client-supplied idempotency key; generated server-side when absent.
    event_id: Optional[str] = Field(default=None, min_length=1, max_length=128)

    @field_validator("timestamp")
    @classmethod
    def _normalize_timestamp(cls, value: datetime) -> datetime:
        # Naive timestamps are assumed UTC; aware ones are converted to UTC so
        # Mongo range queries and time-bucket aggregations are consistent.
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("source_url")
    @classmethod
    def _validate_source_url(cls, value: str) -> str:
        if not (value.startswith("http://") or value.startswith("https://")):
            raise ValueError("source_url must start with http:// or https://")
        return value

    @field_validator("metadata")
    @classmethod
    def _bound_metadata(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        try:
            size = len(json.dumps(value, default=str).encode("utf-8"))
        except (TypeError, ValueError) as exc:
            raise ValueError("metadata must be JSON-serializable") from exc
        if size > MAX_METADATA_BYTES:
            raise ValueError(f"metadata exceeds {MAX_METADATA_BYTES} bytes")
        return value


class EventRecord(BaseModel):
    """Internal event representation (queue payload and Mongo document shape)."""

    event_id: str
    event_type: str
    timestamp: datetime
    user_id: str
    source_url: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    ingested_at: datetime

    @classmethod
    def from_input(cls, event: EventIn) -> "EventRecord":
        return cls(
            event_id=event.event_id or str(uuid.uuid4()),
            event_type=event.event_type,
            timestamp=event.timestamp,
            user_id=event.user_id,
            source_url=event.source_url,
            metadata=event.metadata,
            ingested_at=datetime.now(timezone.utc),
        )
