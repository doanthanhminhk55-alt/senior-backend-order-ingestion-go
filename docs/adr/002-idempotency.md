# ADR 002: Idempotency

- Status: proposed
- Decision: Persist an event identity and enforce uniqueness in PostgreSQL.

## Context

Redis Streams is expected to provide at-least-once delivery, so the same event
may be processed more than once.

## Details to decide

- Event identifier contract
- Transaction boundary between deduplication and state updates
- Retention policy for idempotency records
- Behavior when duplicate payloads disagree
