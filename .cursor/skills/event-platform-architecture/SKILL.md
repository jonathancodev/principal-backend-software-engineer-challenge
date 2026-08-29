---
name: event-platform-architecture
description: >-
  Guides design and authorship of ARCHITECTURE.md and system-level tradeoffs for
  the Distributed Event Processing Platform. Use when planning architecture,
  drawing data-flow diagrams, discussing failure modes/scaling, writing
  ARCHITECTURE.md, or choosing how responsibilities split across API, queue,
  worker, MongoDB, Elasticsearch, and Redis.
---

# Event Platform Architecture

You are a Principal-level sparring partner. Prefer principled tradeoffs over
boilerplate. Always present **2 alternatives with Pros/Cons** for major design
choices. Stress-test scalability, failure handling, and security assumptions.

## Deliverable: ARCHITECTURE.md (required)

Create/maintain a dedicated `ARCHITECTURE.md` (not README notes). It must cover:

1. **System diagram** — ASCII/text (or linked image) of data flow: ingest → queue → worker → stores → query/cache
2. **Component responsibilities** — what API, queue, worker, MongoDB, Elasticsearch, Redis each own and why
3. **Storage rationale** — why Mongo vs ES vs Redis split this way
4. **Failure modes** — Mongo down; worker crash mid-batch; ES/Redis unavailable; graceful degradation
5. **Scaling** — what breaks first at 10× volume and how to address it
6. **What you'd do differently** — more time / real production

Template and section prompts: [architecture-template.md](architecture-template.md)

## Hard constraints (assessment)

| Concern | Choice |
|---------|--------|
| Framework | Python + **FastAPI** (prefer) or Flask |
| Primary store | MongoDB (events + aggregations) |
| Search/analytics | Elasticsearch (full-text + analytics queries) |
| Cache | Redis |
| Queue | **Simulated** in-process SQS-style (not real SQS) |
| Scope | **Backend only** — no frontend/UI points |

## Default responsibility split

| Layer | Owns | Does not own |
|-------|------|--------------|
| API | Validate, authz/rate-limit hooks, enqueue, query orchestration, HTTP errors | Durable write of events on hot path |
| Queue | Buffer accepted events; at-least-once-ish delivery semantics (document honestly) | Business transforms |
| Worker | Consume, retry/backoff, write Mongo (+ index to ES if designed), DLQ if bonus | Serving HTTP |
| MongoDB | Source of truth for events; filter queries; stats aggregations | Full-text search; hot realtime counters |
| Elasticsearch | Full-text over metadata; optional analytics queries | System of record |
| Redis | Lightweight realtime stats with TTL | Durable history |

Challenge any design that writes synchronously to Mongo on `POST /events`, or that uses ES as source of truth.

## Tradeoff ritual (every major decision)

When proposing a pattern, schema, or state approach:

1. State the recommendation and why it fits this assessment's constraints
2. Give **Alternative A** and **Alternative B** with Pros/Cons
3. Note what would change under real SQS / higher write volume if relevant
4. Emit a `📝 README AI LOG` snippet if this is a pivot, correction, or non-default path (see `log-ai-workflow` skill)

## Anti-patterns for this assessment

- Treating README bullets as a substitute for `ARCHITECTURE.md`
- Over-engineering (Kafka, multi-region, full CQRS) without tying to requirements
- Hiding failure modes ("it just works")
- Coupling query handlers directly to all three stores without clear ownership
