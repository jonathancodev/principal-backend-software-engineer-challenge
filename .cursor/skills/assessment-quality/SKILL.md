---
name: assessment-quality
description: >-
  Enforces Principal assessment code quality, module boundaries, testing
  philosophy (pytest unit + integration), and evaluation checklist. Use when
  structuring packages, writing tests, reviewing maintainability, preparing
  README testing notes, docker-compose bonus work, or checking deliverable
  completeness before submission.
---

# Assessment Quality & Delivery

## Module boundaries (required)

Distinct concerns — not a single god-module:

| Concern | Responsibility |
|---------|----------------|
| Ingestion | Validate + enqueue |
| Processing | Worker, retry, backoff, optional DLQ/dedup |
| Storage | Mongo (+ ES index writes) adapters |
| Querying | Filter, stats, search orchestration |
| Caching | Redis realtime stats |

Shared: config, logging, domain models/schemas.

## Error handling & ops

- HTTP: appropriate status codes; structured error payloads
- Internal: structured logging (level, event, attempt, ids) — no silent swallow
- Timeouts/failures from stores mapped to 5xx or degraded behavior as documented
- Code as if a **team** will maintain it: clear names, typed models, small functions

## Testing (pytest preferred)

### Required

1. **Unit tests** — core business logic + error paths (validation, retry classification, aggregation query building, cache key/TTL helpers)
2. **Integration tests** — at least **two** full lifecycles, e.g.:
   - ingest → worker processes → `GET /events` returns event
   - ingest → process → stats and/or search/realtime path
3. **README note** — testing philosophy + what you'd prioritize with more time

### Philosophy to encode in README

Prefer: critical paths and failure modes over line-count coverage; fast unit tests as default; integration against dockerized deps when asserting cross-component contracts.

### Alternatives when designing test harness

Present **2 options** if relevant (e.g. Testcontainers vs docker-compose pytest fixtures) with Pros/Cons.

## Evaluation checklist (pre-submit)

Score yourself against:

1. Architecture document — clarity, honesty, systems depth
2. Async pipeline — correctness, failure handling, real queue awareness
3. MongoDB — schema, aggregations, index rationale
4. Elasticsearch — mapping, queries, ES vs Mongo appropriateness
5. Redis — TTL, invalidation, strategy soundness
6. Code quality — readability, modularity, maintainability
7. Testing — meaningful coverage + philosophy
8. AI workflow — evidence of strategic use (README AI log / workflow-logger)

## Bonus (optional)

- Dockerfile + docker-compose: MongoDB, Elasticsearch, Redis, app
- Rate limiting / abuse prevention middleware
- Worker event deduplication
- DLQ simulation
- AWS SQS drop-in design notes

## Full requirements checklist

Before submission, walk [requirements-checklist.md](requirements-checklist.md).

## Out of scope

No points for frontend, UI, or styling — do not spend assessment time there.

## AI workflow

On major pivots, AI corrections you made, or algorithm optimizations, output:

```text
📝 README AI LOG
- Decision: ...
- Why not default path: ...
```

Then offer to append via `log-ai-workflow` skill.
