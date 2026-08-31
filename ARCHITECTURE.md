# Architecture — Distributed Event Processing Platform

This document explains how the system is put together, why responsibilities are
split the way they are, what happens when parts of it fail, and what I would
change with more time or a real production environment. It is written to be
defended in a review, so it favors honest tradeoffs over aspirational claims.

## System diagram

```
                                  WRITE PATH
   Client ──POST /events──▶ ┌──────────────────────────┐
                            │        API (FastAPI)     │
                            │  rate limit → validate   │
                            │  → enqueue → 202         │
                            └────────────┬─────────────┘
                                         │ JSON message
                                         ▼
                            ┌──────────────────────────┐
                            │  In-process queue        │
                            │  (SQS-style: in-flight,  │
                            │  visibility timeout,     │──▶ DLQ (retries exhausted,
                            │  ack/nack, backpressure) │     inspect via /admin/queue)
                            └────────────┬─────────────┘
                                         │ receive
                                         ▼
                            ┌──────────────────────────┐
                            │   Worker (async tasks)   │
                            │ dedup → Mongo (must      │
                            │ succeed, retry+backoff)  │
                            │ → ES index (best-effort) │
                            └──────┬────────────┬──────┘
                                   │            │
                                   ▼            ▼
                            ┌──────────┐  ┌──────────────┐
                            │ MongoDB  │  │Elasticsearch │
                            │ source   │  │ search index │
                            │ of truth │  │ (lags ≤ ~1s) │
                            └──────────┘  └──────────────┘

                                  READ PATH
   GET /events, /events/stats ────────────────▶ MongoDB (filters, aggregations)
   GET /events/search ────────────────────────▶ Elasticsearch (full-text)
   GET /events/stats/realtime ──▶ Redis cache ──miss──▶ MongoDB (recompute, then cache w/ TTL)
```

## Component responsibilities

**API (FastAPI).** Owns the HTTP contract: payload validation (Pydantic),
rate limiting, translating domain errors into status codes, and routing each
read to the store that owns it. It deliberately does *not* write to MongoDB on
the ingestion path — `POST /events` only validates and enqueues, so ingestion
latency is decoupled from storage latency and Mongo backpressure never
propagates directly to clients.

**Queue (in-process, SQS-style).** Owns buffering and delivery semantics.
It is intentionally modeled on SQS: messages become *in flight* on receive,
must be explicitly acked (deleted), can be nacked with a delay (retry
backoff), and are automatically redelivered when a visibility timeout expires
(consumer crash). It enforces backpressure by rejecting sends at capacity
(HTTP 503 + Retry-After) rather than growing without bound.

*Guarantees it provides:* at-least-once delivery **within the process
lifetime**; bounded memory; per-message receive counts.
*Guarantees it does NOT provide:* durability (a process crash loses queued
messages), cross-process scaling, strict ordering after retries. These gaps
are exactly what real SQS would close — see "SQS drop-in notes" below.

**Worker.** Owns the write pipeline: dedup check ("seen?") → Mongo insert
(authoritative, retried with exponential backoff, dead-lettered after
`max_retries`) → dedup mark → Elasticsearch index (best-effort) → ack. The
ordering is load-bearing: the dedup marker is written only *after* the
durable Mongo write, so a failed attempt leaves no marker and its retry is
never misclassified as a duplicate. (An earlier claim-before-write design had
exactly that flaw — a transient Mongo outage caused retries to be dropped as
"duplicates". It was caught in review and is now pinned by both a unit
regression test and a chaos test that pauses the Mongo container.) Two
concurrent deliveries can both pass the "seen" check; that race is resolved
atomically by the unique index on `event_id` — Redis is the fast path, the
index is the guarantee. The worker runs as N asyncio
tasks inside the API process (configurable via `WORKER_CONCURRENCY`), which is
honest to the "simulated queue" constraint; in production it would be a
separate deployable so ingestion and query traffic don't share a failure
domain.

**MongoDB.** Source of truth. Owns the canonical event documents, the
filterable query patterns (`GET /events`) and the aggregation pipelines
(`GET /events/stats`, realtime summary computation). If Mongo and ES ever
disagree, Mongo wins.

**Elasticsearch.** Owns full-text search over event metadata — the one
workload document databases are bad at. It is a derived, disposable view: it
can be rebuilt from Mongo, and its unavailability degrades only `/events/search`.

**Redis.** Owns two small, latency-sensitive concerns: the realtime stats
cache (TTL-bounded) and cross-cutting counters (rate limiting windows, dedup
claims). Nothing in Redis is a system of record; every Redis failure path
fails open or falls back to Mongo.

## Storage rationale

The split follows "each store does the one thing it's best at, and exactly one
store is the source of truth":

- **MongoDB** for events because the payload is semi-structured (flexible
  `metadata`), write-heavy, and queried by exact/range predicates that
  compound indexes serve well. Its aggregation framework covers the stats
  requirements natively (`$dateTrunc` + `$group`).
- **Elasticsearch** for search because relevance-ranked full-text over an
  unbounded metadata key space is what inverted indexes are for. Doing this
  in Mongo would mean either `$text` indexes (single per collection,
  no per-field analyzers, weak relevance) or regex scans (unindexable).
  ES is kept *derived* so consistency between stores is a freshness question
  (~1s refresh + worker lag), never a correctness question.
- **Redis** for the realtime summary because the endpoint's contract is "cheap
  and recent", which is precisely a TTL cache. Caching in-process would break
  the moment the API scales past one replica; Redis keeps cache state shared.

An alternative considered: **Mongo-only** (with `$text` search and an
aggregation-backed "realtime" endpoint). Fewer moving parts, but it puts
search relevance and hot read amplification on the primary store — the two
things most likely to hurt at high volume. The three-store split costs
operational complexity and buys isolation of failure domains, which is the
right trade for an event platform.

## Failure modes

| Failure | System behavior | Client impact | Recovery |
|---|---|---|---|
| **MongoDB down** | Worker retries with exponential backoff (0.5s → 30s cap); events accumulate in the queue; after `max_retries` receives a message goes to the DLQ. Reads raise `StorageUnavailableError` → 503. Ingestion keeps accepting until the queue fills, then 503 + `Retry-After`. | Writes: accepted (202) until queue capacity, then 503. Reads: 503. | Mongo returns → worker drains the backlog. DLQ'd events are inspectable at `/admin/queue`; replay is manual (a real system would automate it). |
| **Worker crashes mid-batch** | Un-acked messages hit the visibility timeout and are redelivered. If the crash happened *after* the Mongo write but *before* the ack, redelivery is deduplicated (dedup marker written post-write, unique index on `event_id` as backstop) — no double-writes. If it crashed *before* the write, no marker exists, so the redelivery processes normally — no drops. | None visible; processing latency blips. | Automatic. This is the standard at-least-once + idempotent-consumer pattern. |
| **Whole process crashes** | Queued (not-yet-written) events are lost — the queue is in-memory by design. | Silent loss of buffered events. | This is the documented gap vs real SQS; it's the #1 reason to swap the transport in production. |
| **Elasticsearch down** | Worker logs the index failure and continues; events still land in Mongo and get acked. `/events/search` returns 503 (`search_unavailable`). | Search degraded; ingestion, filters, stats, realtime all unaffected. | ES returns → *new* events index normally. Events ingested during the outage are missing from ES until re-indexed from Mongo (manual backfill; see "differently"). |
| **Redis down** | Realtime stats bypass the cache and compute from Mongo (`X-Cache: BYPASS`). Rate limiting fails open. Dedup fails open — the Mongo unique index still prevents duplicates. | Realtime endpoint slower; no 5xx. | Automatic on reconnect. Every Redis dependency is deliberately non-critical. |
| **Queue at capacity** | `POST /events` returns 503 with `Retry-After: 1`. | Producers must retry; no silent drops. | Backpressure is preferable to unbounded memory growth; a real deployment alerts on queue depth long before this. |

The degradation principle throughout: **only the feature whose store is down
degrades**, and the source of truth is never sacrificed for a derived view.

## Scaling considerations (what breaks first at 10× volume?)

In order of failure:

1. **The in-process queue + worker share the API's event loop and memory.**
   This breaks first — not just capacity, but the coupling itself: a burst
   fills the in-memory queue (memory pressure → 503s), and worker CPU steals
   cycles from request handling. *Fix:* replace the queue with SQS (or
   Redis Streams as a low-lift intermediate) and move workers to their own
   horizontally-scaled deployment. The code is already shaped for this — the
   queue interface is SQS-like and the worker only touches that interface.
2. **Per-event round trips.** One `insert_one` + one ES `index` call per event
   caps throughput. *Fix:* batch — receive up to N messages, `insert_many`
   (ordered=False) + ES `_bulk`, ack per batch with per-item error handling.
3. **The realtime aggregation on cache miss.** A 5-minute window scan every
   TTL is fine at current volume; at 10× with many API replicas it becomes a
   recurring spike. *Fix:* have the worker increment Redis counters
   (write-through) so realtime reads never touch Mongo — see caching notes.
4. **Mongo write amplification from four indexes.** Acceptable now; at
   sustained high volume, shard on `{user_id: hashed}` (write distribution +
   the most selective query pattern) and consider dropping/TTL-ing raw events
   (see "differently").
5. **ES mapping/refresh pressure.** `flattened` already prevents mapping
   explosion; next steps are index-per-time-period (ILM) and a longer refresh
   interval for bulk ingest.

## Caching strategy (`/events/stats/realtime`)

- **Pattern:** cache-aside in Redis. Miss → compute a Mongo aggregation over
  the last `REALTIME_WINDOW_MINUTES` (default 5) → store with TTL → serve.
  The response carries `X-Cache: HIT | MISS | BYPASS` for observability.
- **TTL rationale:** default 10s, client-overridable up to a capped 300s. A
  "realtime" summary tolerates ~seconds of staleness by definition; 10s means
  Mongo sees at most ~6 aggregations/minute per (window, ttl) key *regardless
  of read QPS* — the cache converts unbounded read load into a constant.
  TTL participates in the cache key so a 60s-tolerant caller never poisons a
  10s-freshness caller.
- **Stampede control:** single-flight lock (`SET NX PX`) so concurrent misses
  produce one Mongo query; the losers briefly poll for the winner's result.
- **Invalidation:** none beyond TTL, deliberately. Event ingestion is
  continuous — invalidating on every write would make the cache useless, and
  "at most TTL seconds stale" is the endpoint's actual contract. This is the
  honest tradeoff of TTL caching: bounded staleness in exchange for bounded load.
- **Under higher write volume:** flip from read-side compute to write-side
  maintenance — the worker increments per-type Redis counters (e.g. per-minute
  keys summed over the window, or a sorted-set sliding window) as it processes
  events. Reads become O(1) Redis ops, Mongo is out of the hot path entirely,
  and freshness improves from "TTL seconds" to "worker lag". Cost: counter
  drift on partial failures (mitigate by periodically reconciling from Mongo).

## Indexing strategy

### MongoDB indexes (created at startup, `app/storage/mongo.py`)

| Index | Serves | Why this shape |
|---|---|---|
| `(event_type, timestamp desc)` | `GET /events?event_type=…` and the stats `$match` | Equality field first, range/sort field second (ESR rule); covers type+range scans without in-memory sorts. |
| `(user_id, timestamp desc)` | Per-user timelines | Same ESR reasoning; `user_id` is the most selective filter. |
| `(timestamp desc)` | Date-range-only listings, realtime window, stats without a type filter | The leading-field rule means the compound indexes can't serve range-only queries. |
| `event_id` (unique) | Idempotency backstop | Correctness, not speed: makes duplicate writes structurally impossible even if Redis dedup fails open. |

**Deliberately not indexed:**

- **`source_url`** — long strings (up to 2KB), heavily skewed distribution
  (a few hot pages dominate), so poor selectivity per unit of index size, and
  every event pays the write cost. URL-filtered queries ride `(timestamp)` and
  filter residually; if URL filtering became a hot pattern I'd index a derived
  `url_host`/`url_path` pair rather than the raw string.
- **Anything under `metadata`** — client-controlled, unbounded key space. In
  Mongo that means either a per-key index sprawl or a wildcard index whose
  write amplification scales with metadata size. Searching metadata is
  exactly the workload delegated to Elasticsearch.
- **A text index** — Mongo allows one text index per collection with no
  per-field analyzer control and weak relevance scoring; it would duplicate
  ES's job, worse, while taxing every write.

### Elasticsearch mapping (`app/storage/elastic.py`)

- `event_type`, `user_id`, `event_id`: **`keyword`** — always exact-match
  filters or aggregations; analyzing them would only corrupt filtering
  (`sign-up` tokenizing into `sign`, `up`).
- `timestamp`, `ingested_at`: **`date`** for range filters and future
  date-histogram aggregations.
- `source_url`: **`keyword` + `text` subfield** — exact filtering by default,
  tokenized matching ("pricing", "checkout") available to the search query
  via `source_url.text`.
- `metadata`: **`flattened`** — one field in the mapping regardless of how
  many distinct keys clients send, killing the mapping-explosion risk while
  keeping every key filterable at keyword precision.
- `metadata_text`: **analyzed `text` catch-all** built at index time from all
  metadata keys and values. `flattened` fields don't support full-text
  scoring, so this field is what `multi_match` actually searches. **Standard
  analyzer**, deliberately: metadata is mixed-language, mixed-format
  (browser strings, device names, campaign labels), so language-specific
  stemming would guess wrong as often as right. With real corpus knowledge
  I'd revisit (e.g. an English analyzer plus a keyword-lowercase subfield).

Single shard, zero replicas — right for a single-node assessment setup, stated
so it's clearly a conscious choice (production: ≥1 replica, ILM rollover).

## Queue design vs. real SQS

What the simulation shares with SQS: visibility timeout with automatic
redelivery, explicit delete (ack), per-message receive count, delay on retry
(≈ per-message backoff), DLQ after max receives, at-least-once semantics.

Drop-in swap — what changes:

- **The transport, not the worker logic.** `send` → `SendMessage`, `receive` →
  long-poll `ReceiveMessage` (batch 10), `ack` → `DeleteMessage`, `nack(delay)`
  → `ChangeMessageVisibility`. Retry state (receive count) and DLQ routing
  move from my code into queue configuration (`RedrivePolicy`, `maxReceiveCount`).
- **What I'd gain:** durability across process crashes (closing this design's
  main gap), independent scaling of producers/consumers, and operational
  tooling (CloudWatch depth/age alarms, DLQ redrive console).
- **What I'd still own:** idempotent consumption. SQS standard queues are
  at-least-once and can reorder — the dedup check/mark + unique index stay
  exactly as they are. This is worth stating plainly: *moving to real SQS does
  not buy exactly-once; the consumer-side idempotency is permanent.*
- **What I'd reconsider:** FIFO queues (per-`user_id` `MessageGroupId`) if
  ordering ever became a product requirement — accepting the ~300 msg/s/group
  throughput cost. For analytics events, standard + idempotency is the better
  trade.
- **New failure modes:** poison-pill messages burning receive counts (DLQ
  handles), visibility timeout shorter than worst-case processing (must be
  tuned ≥ max batch time), and SQS itself being a network dependency on the
  ingest path (mitigate with a small local buffer + async flush, or accept
  the 5xx and let clients retry).

## What I'd do differently

Given more time or a real production environment:

1. **Real broker first.** The in-memory queue's non-durability is the single
   biggest gap between this and production. SQS (or Redis Streams to stay in
   this stack) before any other improvement.
2. **Separate worker deployment.** Same code, different process/container, so
   ingestion CPU and query latency stop sharing a fate, and workers scale on
   queue depth instead of HTTP traffic.
3. **Batching end-to-end.** `insert_many` + ES `_bulk` with per-item error
   handling; this is the difference between hundreds and tens of thousands of
   events/sec per worker.
4. **Automated ES reconciliation.** Currently an ES outage leaves a gap until
   manual backfill. A periodic job comparing Mongo/ES watermarks (or Mongo
   change streams feeding ES) makes search self-healing.
5. **DLQ replay endpoint + alerting.** The DLQ is inspectable but replay is
   manual; production needs `POST /admin/dlq/replay`, depth alarms, and
   poison-message quarantine.
6. **Observability.** Prometheus metrics (queue depth, processing latency
   histograms, retry/DLQ rates, cache hit ratio), OpenTelemetry traces across
   API → queue → worker → stores, JSON structured logs. The log lines are
   already key=value pairs to make that migration mechanical.
7. **Data lifecycle.** Raw events grow unboundedly; I'd add Mongo TTL or
   archival to object storage, pre-aggregated rollup collections for common
   stats queries, and ES ILM (hot/warm/delete).
8. **Security.** The API is currently unauthenticated (out of assessment
   scope): API keys or OAuth client credentials on ingest, plus per-tenant
   rate limits instead of per-IP.
9. **POST-boundary idempotency.** Today the contract is: a client-supplied
   `event_id` is an idempotency key (duplicates collapse to one event);
   without one, identical payloads are *distinct* events — a deliberate
   choice, pinned by an integration test. The alternative considered was
   content-hash dedup (hash of type+timestamp+user+url+metadata as the
   default `event_id`): it absorbs HTTP retries without client cooperation,
   but silently merges legitimately identical events (two clicks in the same
   second), which is a business decision the server shouldn't guess. The
   middle ground I'd ship in production: keep the client key as the contract
   and add a short-TTL content-hash window (~minutes) at the API layer that
   detects *retries* specifically — in a separate Redis namespace from the
   worker's dedup markers, since sharing a key would make the worker drop
   the first legitimate delivery.
