# Distributed Event Processing Platform

A production-grade backend for ingesting, processing, querying and caching
high-volume web events. Events are accepted over HTTP, buffered on an
SQS-style in-process queue, persisted to MongoDB by a retrying background
worker, indexed into Elasticsearch for full-text search, and summarized
through a Redis-cached realtime endpoint.

**[ARCHITECTURE.md](ARCHITECTURE.md) is the companion deliverable** — system
diagram, component responsibilities, storage rationale, failure modes, scaling
analysis, and the queue-vs-real-SQS discussion live there.

## Stack

FastAPI · MongoDB (source of truth + aggregations) · Elasticsearch (full-text
search) · Redis (caching, rate limiting, dedup) · in-process SQS-style queue
with retry/backoff and a dead letter queue.

## Quickstart (Docker)

```bash
docker compose up --build
# API on http://localhost:8000 — interactive docs at http://localhost:8000/docs
```

This starts MongoDB 7, Elasticsearch 8 (single node, security off), Redis 7
and the API. The app creates its MongoDB indexes and the ES index mapping on
startup, retrying until the stores are ready.

Smoke test:

```bash
curl -s http://localhost:8000/health
```

## Endpoints

### `POST /events` — ingest (async)

Validates and enqueues; the write happens in the background worker. Returns
`202` with the assigned `event_id`. `422` invalid payload, `429` rate limited,
`503` queue full (backpressure, includes `Retry-After`).

```bash
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_type": "pageview",
    "timestamp": "2026-08-29T12:00:00Z",
    "user_id": "user-42",
    "source_url": "https://example.com/pricing",
    "metadata": {"browser": "firefox", "device": "mobile", "campaign": "summer-launch"}
  }'
```

Optional `event_id` field acts as a client idempotency key (duplicates are
dropped by the worker).

### `GET /events` — filtered listing (MongoDB)

Filters: `event_type`, `user_id`, `source_url`, `start`, `end` (ISO-8601);
pagination via `limit` (≤500) and `offset`. Sorted by timestamp descending.

```bash
curl -s 'http://localhost:8000/events?event_type=pageview&user_id=user-42&start=2026-08-01T00:00:00Z'
```

### `GET /events/stats` — aggregated counts (MongoDB pipeline)

Counts grouped by event type and time bucket. `bucket` is `hourly`, `daily`
or `weekly`; optional `start`/`end`/`event_type`. Ranges producing more than
1000 buckets are rejected with `422`.

```bash
curl -s 'http://localhost:8000/events/stats?bucket=daily&event_type=pageview'
```

### `GET /events/search` — full-text search (Elasticsearch)

Searches across event metadata (and URL tokens). Optional filters:
`event_type`, `user_id`, `start`, `end`, `limit`.

```bash
curl -s 'http://localhost:8000/events/search?q=summer+launch&event_type=pageview'
```

### `GET /events/stats/realtime` — cached summary (Redis)

Event counts for the last 5 minutes (configurable), served from Redis with a
configurable TTL (`?ttl=30`, capped at 300s). The `X-Cache` response header
reports `HIT`, `MISS`, or `BYPASS` (Redis unavailable, computed directly).

```bash
curl -si 'http://localhost:8000/events/stats/realtime?ttl=30' | grep -i x-cache
```

### Operational

- `GET /health` — per-dependency status (`ok` / `degraded`).
- `GET /admin/queue` — queue depth, in-flight count, and a peek at
  dead-lettered messages with failure reasons.

## Configuration

All settings are environment variables (see `app/config.py` for the full
list). The most relevant:

| Variable | Default | Purpose |
|---|---|---|
| `MONGO_URI` / `MONGO_DB` | `mongodb://localhost:27017` / `event_platform` | Primary store |
| `ES_URL` / `ES_INDEX` | `http://localhost:9200` / `events` | Search |
| `REDIS_URL` | `redis://localhost:6379/0` | Cache / rate limit / dedup |
| `QUEUE_MAX_SIZE` | `10000` | Backpressure threshold |
| `VISIBILITY_TIMEOUT_SECONDS` | `30` | Redelivery window for crashed consumers |
| `MAX_RETRIES` | `5` | Delivery attempts before the DLQ |
| `BACKOFF_BASE_SECONDS` / `BACKOFF_MAX_SECONDS` | `0.5` / `30` | Exponential retry backoff |
| `WORKER_CONCURRENCY` | `2` | Parallel consumer tasks |
| `REALTIME_TTL_SECONDS` / `REALTIME_WINDOW_MINUTES` | `10` / `5` | Realtime cache freshness / window |
| `RATE_LIMIT_REQUESTS` / `RATE_LIMIT_WINDOW_SECONDS` | `120` / `60` | Per-IP ingest limit |

## Testing

Requires Python 3.9+ for local runs (the app container uses 3.11).

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"

# Unit tests — no external services needed
.venv/bin/pytest tests/unit

# Integration tests — need the stores running:
docker compose up -d mongo elasticsearch redis
.venv/bin/pytest tests/integration -m "not chaos"

# Chaos tests — pause/unpause the docker containers mid-test to verify the
# failure modes documented in ARCHITECTURE.md (opt-in):
CHAOS=1 .venv/bin/pytest -m chaos

# Everything non-chaos (integration auto-skips if services are unreachable)
.venv/bin/pytest -m "not chaos"
```

Integration tests create a uniquely-named Mongo database and ES index per run
and use Redis db 15, cleaning up on teardown — they never touch dev data.

### Testing philosophy

Coverage is aimed at the places this system can actually lose or corrupt
data, not at a line-count number:

- **Unit tests** pin the failure-path behavior with in-memory fakes: queue
  semantics (ack/nack, visibility-timeout redelivery — i.e. "worker crashed
  mid-message"), retry backoff and DLQ routing, dedup skips, ES outages not
  blocking ingestion, cache single-flight and Redis fail-open, rate-limit
  windows, and the validation contract. They run in ~3 seconds, which keeps
  them in the inner dev loop.
- **Integration tests** cover full lifecycles against real stores —
  ingest→query, ingest→stats buckets, ingest→search, realtime cache
  MISS→HIT, end-to-end idempotency (duplicate `event_id` stored once;
  identical payloads without one deliberately stored twice), queue
  backpressure (503), rate limiting (429), and dependency health — because
  the riskiest bugs (aggregation pipeline shape, ES mapping behavior,
  serialization across the queue boundary) only show up against real engines.
- **Chaos tests** (`CHAOS=1 pytest -m chaos`) make the failure-mode table in
  ARCHITECTURE.md executable rather than prose: they pause the Mongo /
  Elasticsearch / Redis containers mid-test and assert the documented
  degradation actually happens — transient Mongo outage → retried, stored
  exactly once, no DLQ; ES outage → only search degrades; Redis outage →
  cache bypass and fail-open everywhere.

With more time I'd prioritize, in order: **load tests** to find the real
queue saturation point and validate backpressure under burst; and
**property-based tests** (hypothesis) on the validation and dedup logic.

## AI in My Workflow

AI (Cursor with Grok 4.5 and Fable 5) was used as an architectural sparring partner and
force multiplier throughout, under a project rule requiring it to present
two alternatives with pros/cons for every significant design decision.

**Tools used:** Cursor (agent mode) with Grok 4.5 and Fable 5, configured with
project-specific rules (`.cursorrules`) and custom skills (`.cursor/skills/`)
encoding the assessment constraints, module boundaries, and a requirements
checklist.

**How AI helped, concretely:**

- **Constraint encoding before code.** Before any implementation, I had the
  agent distill the assessment into reusable skills (architecture template,
  pipeline requirements, storage/query patterns, quality checklist). Every
  later prompt was then checked against those constraints automatically —
  this prevented scope drift and kept the architecture document requirements
  in view from day one.
- **Tradeoff exploration.** The queue design went through explicit
  alternatives (bare `asyncio.Queue` vs SQS-semantics wrapper vs persistent
  SQLite journal). I chose the SQS-semantics wrapper because it makes
  visibility-timeout crash recovery *testable* and turns the "what would
  change with real SQS" question into a transport swap rather than a rewrite.
  Similar alternative-driven decisions: `flattened` vs dynamic ES mappings,
  cache-aside vs write-through counters (the latter documented as the
  high-volume evolution), fixed-window vs sliding-window rate limiting.
- **Scaffolding + tests.** The agent generated the module skeleton, fakes,
  and the bulk of the test suite from the agreed design, which left my time
  for the parts that needed judgment (failure semantics, index rationale,
  this document's honesty).

**Where I pushed back or corrected AI output:**

- **A serious ordering bug found by human review of AI-written code:** the
  worker originally took its Redis dedup claim (SETNX) *before* the Mongo
  write. On a transient Mongo failure, the retry found the claim from its own
  failed attempt and dropped the event as a "duplicate" — silent data loss on
  the retry path. Worse, AI's own unit tests passed, because the
  `FakeDeduplicator` returned success on every claim instead of modeling real
  SETNX semantics — the fake hid the bug it was supposed to catch. Reviewing
  the idempotency boundary myself surfaced it. The fix is the classic
  idempotent-consumer shape (check-before-write, mark-after-durable-write,
  unique index as the atomic tiebreaker), the fake now keeps real state, and
  a chaos test that pauses the Mongo container mid-ingest pins the scenario
  end-to-end.
- **A bug the tests caught in AI-written code:** the realtime TTL
  override used `ttl_override or default`, which silently swallowed an
  invalid `ttl=0` instead of rejecting it (0 is falsy). The unit test
  parametrized on boundary values exposed it; the fix is an explicit
  `is None` check. A good example of why boundary-value tests on AI code
  are non-negotiable.
- **Company references in a public repo.** AI-generated project files
  initially carried the company name; I had them scrubbed since the repo may
  be public.
- **Kept AI from over-engineering:** early suggestions included
  change-stream-based ES sync and pre-aggregated rollup collections; both are
  the *right* production answers but wrong for this scope, so they were moved
  to ARCHITECTURE.md's "what I'd do differently" instead of the code.

**Impact on speed and quality:** the full system (pipeline, three storage
integrations, 60+ tests including a chaos suite, Docker setup, and the
architecture document) was
built in a small fraction of the time a solo implementation would take. More
importantly, the "always present two alternatives" rule meant every layer has
a *considered* design with the rejected option documented — the architecture
document largely wrote itself from decisions that were already argued out.

## Project structure

```
app/
├── main.py               # composition root: wiring, lifespan, middleware
├── config.py             # env-driven settings
├── errors.py             # domain errors + HTTP mappings
├── domain/models.py      # EventIn / EventRecord (validation contract)
├── ingestion/            # POST /events → validate → enqueue
├── queueing/             # SQS-style queue (ack/nack/visibility) + DLQ
├── processing/           # worker: dedup → Mongo → ES, retry/backoff
├── storage/              # Mongo repository, ES store, Redis cache
├── querying/             # read endpoints + query orchestration
└── middleware/           # rate limiting
tests/
├── unit/                 # fakes, no services required
└── integration/          # full lifecycles against real stores
```
