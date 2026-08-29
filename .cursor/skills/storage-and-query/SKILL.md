---
name: storage-and-query
description: >-
  Implements MongoDB schema/indexes/aggregations, Elasticsearch mappings and
  search, and Redis realtime stats caching for the event platform. Use when
  designing indexes, GET /events filters, /events/stats buckets, /events/search,
  /events/stats/realtime, cache TTL/invalidation, or ES vs Mongo query routing.
---

# Storage & Query Layers

## Endpoints to support

| Endpoint | Store | Behavior |
|----------|-------|----------|
| `GET /events` | MongoDB | Filter by type, date range, user_id, source_url |
| `GET /events/stats` | MongoDB aggregation | Counts by event_type + time bucket (`hourly` / `daily` / `weekly`) |
| `GET /events/search` | Elasticsearch | Full-text across event metadata |
| `GET /events/stats/realtime` | Redis | Lightweight stats summary; configurable TTL |

Route queries to the store that owns them. Do not full-text scan Mongo to fake search.

## MongoDB

### Document shape

Align with ingestion schema; add server fields as needed (`_id`, `created_at`, optional `event_id` for dedup). Keep `metadata` as a subdocument.

### Indexes — design & document

Implement indexes matching real filters/sorts. For each index, state:

- Query pattern it serves
- Field order rationale (equality → range → sort)
- Cost (write amplification, storage)

**Deliberately skip** indexes that don't match access patterns; document why (write-heavy ingest, low selectivity, covered by compound elsewhere).

### Aggregations

`GET /events/stats`:

- `$match` on date range (and optional filters)
- Bucket via `$dateTrunc` / `$group` by configurable unit
- Group counts by `event_type` (+ bucket)
- Bound result size; reject absurd ranges with 4xx

Always offer **2 alternatives** for bucketing strategy (e.g. `$dateTrunc` vs pre-bucketed rollup collection) with Pros/Cons when designing.

## Elasticsearch

### Mapping

Define an index mapping for event documents. Explain:

- Field types (`keyword` vs `text` vs `date` vs `object`/`flattened` for metadata)
- Analyzer choices for free-text metadata fields
- What stays `enabled: false` / excluded from `_source` if anything

Prefer searchable metadata fields without mapping explosion (e.g. `flattened` or controlled dynamic templates) — justify the choice.

### Queries

`GET /events/search` should use appropriate full-text queries (multi-match / query_string with limits). Document why ES not Mongo for this path.

### Failure

If ES is down: return clear 503/degraded response for search only; filter/stats from Mongo should still work if designed that way.

## Redis caching

### Scope

Cache **only** (or primarily) `/events/stats/realtime` summary.

Document in ARCHITECTURE.md / README:

1. **TTL rationale** — freshness vs load; default configurable
2. **Invalidation** — TTL-only vs explicit invalidate on write; stampede controls (single-flight)
3. **Higher write volume** — what you'd change (write-through counters, pub/sub invalidation, read replicas)

### Alternatives ritual

When choosing cache strategy, present **2 options** (TTL-only passive vs write-time invalidation/update) with Pros/Cons.

### Failure

Redis down → recompute from Mongo (slower) or fail closed for realtime endpoint; document the choice.

## Cross-cutting

- Config via env: Mongo URI, ES URL/index, Redis URL, cache TTL
- Repository/adapters per store; query services orchestrate
- Never leak connection strings or stack traces to clients
- Indexing & caching reasoning is an **evaluation criterion** — write it down, not only in code comments
