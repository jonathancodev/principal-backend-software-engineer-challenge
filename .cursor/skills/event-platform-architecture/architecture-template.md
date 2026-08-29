# ARCHITECTURE.md template

Use this structure. Fill with honest tradeoffs; prefer concrete failure behavior over aspirational language.

```markdown
# Architecture — Distributed Event Processing Platform

## System diagram

[ASCII data flow: Client → API → Queue → Worker → MongoDB
                                      ↘ Elasticsearch (index)
                         Query path: API → Mongo / ES / Redis]

## Component responsibilities

### API
- ...

### In-process queue
- Guarantees: ...
- Non-guarantees: ...

### Worker
- ...

### MongoDB
- ...

### Elasticsearch
- ...

### Redis
- ...

## Storage rationale

Why this split; what each store is optimized for; query routing rules.

## Failure modes

| Failure | Behavior | Client impact | Recovery |
|---------|----------|---------------|----------|
| Mongo unavailable | ... | ... | ... |
| Worker crash mid-batch | ... | ... | ... |
| Elasticsearch down | ... | ... | ... |
| Redis down | ... | ... | ... |
| Queue backlog | ... | ... | ... |

## Scaling considerations (10× volume)

What breaks first; mitigations (partitioning, real SQS, worker pool, index changes, cache stampede controls).

## Caching strategy (summary)

TTL rationale; invalidation; higher write-volume alternative. Detail may live in README or here.

## Indexing strategy (summary)

Mongo indexes chosen / deliberately skipped; ES mapping & analyzers. Detail may link to code.

## Queue design vs real SQS

In-process guarantees vs SQS; what would change for drop-in SQS.

## What we'd do differently
```

## Diagram starter (ASCII)

```
                 ┌─────────────────────────────────────────┐
                 │                 API (FastAPI)            │
                 │  POST /events → validate → enqueue       │
                 │  GET  /events|/stats|/search|/realtime   │
                 └───────┬─────────────────┬────────────────┘
                         │                 │
                         ▼                 ▼
                  ┌─────────────┐   ┌──────────────┐
                  │ In-process  │   │ Redis cache  │
                  │ event queue │   │ (realtime)   │
                  └──────┬──────┘   └──────────────┘
                         │
                         ▼
                  ┌─────────────┐
                  │   Worker    │  retry + backoff (+ DLQ)
                  └──────┬──────┘
                         │
            ┌────────────┼────────────┐
            ▼            ▼            ▼
       ┌─────────┐ ┌──────────┐ ┌──────────┐
       │ MongoDB │ │   ES     │ │ (update  │
       │ events  │ │ search   │ │  Redis)  │
       └─────────┘ └──────────┘ └──────────┘
```

## Evaluation lens

Reviewers score clarity of thinking, honesty about tradeoffs, and depth of systems reasoning.
When editing architecture text, prefer explicit "we accept X because Y" over feature lists.
