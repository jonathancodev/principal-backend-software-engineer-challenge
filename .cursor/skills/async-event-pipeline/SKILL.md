---
name: async-event-pipeline
description: >-
  Designs and implements the async event ingestion pipeline: POST /events
  validation and enqueue, in-process SQS-style queue, background worker, retry
  with backoff, optional DLQ/dedup. Use when building ingestion, workers,
  queues, retries, event schemas, or documenting queue guarantees vs real SQS.
---

# Async Event Ingestion Pipeline

## Requirements (must satisfy)

- `POST /events` — accept event data, **validate**, **enqueue** (do not sync-write Mongo on the request path)
- Background **worker** consumes queue and writes to MongoDB
- On failure: **retry with basic backoff**
- Document queue design: guarantees now vs changes for real SQS

### Event schema

| Field | Type | Notes |
|-------|------|-------|
| event_type | string | e.g. `pageview`, `click`, `conversion` |
| timestamp | datetime | Prefer timezone-aware UTC |
| user_id | string | |
| source_url | string | Validate URL-ish shape; don't over-block |
| metadata | object | Flexible JSON (browser, device, feature data) |

Reject invalid payloads with clear **4xx** and structured error bodies. Log internally with correlation/request IDs where practical.

## Module boundaries

Keep distinct packages/modules:

- `ingestion` — HTTP accept + validate + enqueue
- `processing` — worker, retry/backoff, (bonus) DLQ/dedup
- `storage` — Mongo (and ES indexing side effects if co-located carefully)
- Do **not** put queue internals inside route handlers

## Queue design checklist

Document explicitly in ARCHITECTURE.md / README:

1. **Delivery** — at-most-once / at-least-once / best-effort? (be honest for in-process)
2. **Durability** — survives process crash? (likely no for in-memory)
3. **Ordering** — per-partition / none
4. **Visibility / ack** — how "in flight" is modeled (SQS-style visibility timeout analog)
5. **Backpressure** — what if enqueue is full?
6. **Idempotency** — worker-side (bonus: dedup)

Always present **2 alternatives** (e.g. in-memory asyncio.Queue vs persistent local SQLite/file journal) with Pros/Cons when choosing the simulation approach.

## Retry / backoff

Minimum viable:

- Max attempts N (configurable)
- Exponential or linear backoff between attempts
- After exhaustion: drop with loud log **or** (bonus) dead-letter queue simulation
- Never block the HTTP worker unbounded waiting on Mongo

## Worker crash mid-batch

Design and document:

- What is ack'd vs still visible
- Whether partial Mongo writes can duplicate events
- How restart recovers

Stress-test any design that acks before durable write.

## Real SQS drop-in notes (bonus / architecture)

Call out what would change:

- Persistence, fan-out, DLQ native, visibility timeout, batch receive, IAM, poison-pill handling
- API stays enqueue-oriented; worker becomes SQS consumer
- Exactly-once is still not free — idempotent writes remain

## Bonus hooks (optional, only if asked or time allows)

- Event **deduplication** in worker (e.g. event_id / hash + TTL set)
- **Dead letter queue** simulation after retries exhausted
- Rate limiting / abuse prevention on `POST /events`

## Implementation guardrails

- Prefer FastAPI + background task/worker lifecycle tied to app startup/shutdown
- Configurable via env (queue size, max retries, backoff base)
- Meaningful logs: enqueue, process start, success, retry, DLQ
- Unit-test: validation, backoff/retry classification; integration: ingest → worker → query
