# Requirements checklist (Principal assessment)

Use before submission. Mark done only when code + docs exist.

## Core

- [ ] `POST /events` validates and enqueues (async; no sync Mongo write on request)
- [ ] Worker consumes queue → MongoDB
- [ ] Retry + basic backoff on processing failure
- [ ] Queue guarantees documented; SQS comparison noted
- [ ] Event fields: type, timestamp, user_id, source_url, metadata
- [ ] `GET /events` filters: type, date range, user_id, source_url
- [ ] `GET /events/stats` Mongo aggregation; hourly/daily/weekly buckets
- [ ] `GET /events/search` Elasticsearch full-text on metadata
- [ ] `GET /events/stats/realtime` Redis + configurable TTL
- [ ] Caching strategy documented (TTL, invalidation, high-write alternative)
- [ ] Mongo indexes implemented + rationale; deliberate non-indexes called out
- [ ] ES index mapping + analyzer/field-type rationale
- [ ] `ARCHITECTURE.md` with all required sections
- [ ] Unit tests (logic + error paths)
- [ ] ≥2 integration lifecycles (ingest → process → query)
- [ ] README testing philosophy note
- [ ] Clear module boundaries; HTTP errors + logging

## Bonus

- [ ] docker-compose (Mongo, ES, Redis, app)
- [ ] Rate limiting / abuse prevention
- [ ] Worker deduplication
- [ ] DLQ simulation
- [ ] SQS drop-in design notes

## Evaluation reminders

Architecture honesty > feature count. Failures and 10× scaling must be explicit.
Backend only — no frontend work.
