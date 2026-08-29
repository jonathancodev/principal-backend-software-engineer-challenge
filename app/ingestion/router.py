"""POST /events — async ingestion endpoint."""

from fastapi import APIRouter, Request, status

from app.domain.models import EventIn

router = APIRouter(tags=["ingestion"])


@router.post("/events", status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(event: EventIn, request: Request) -> dict:
    """Validate the event and enqueue it for async processing.

    Returns 202 with the assigned event_id; the write to MongoDB happens in
    the background worker. 422 for invalid payloads, 503 when the queue is
    full (backpressure), 429 when rate limited.
    """
    event_id = await request.app.state.ingestion_service.ingest(event)
    return {"status": "accepted", "event_id": event_id}
