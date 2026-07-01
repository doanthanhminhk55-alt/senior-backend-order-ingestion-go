# ADR 002: Idempotency

- Status: accepted
- Decision: Use `processed_events.event_id` as the PostgreSQL source of truth
  for event idempotency.

## Context

Redis Streams provides at-least-once delivery. A worker can receive the same
event again after a crash, timeout, or pending-message recovery, so event
processing must be safe to repeat.

Redis acknowledgement cannot establish database idempotency: acknowledging
before commit risks losing an event, while committing before acknowledgement
can cause a redelivery. The database therefore needs to recognize an already
committed event.

## Decision

The `processed_events` table is the idempotency source of truth. Its
`event_id` primary key provides the uniqueness guarantee.

For each event, the repository:

1. Starts a PostgreSQL transaction.
2. Acquires the per-order transaction advisory lock described in
   [ADR 003](003-concurrency-control.md).
3. Inserts the `event_id` into `processed_events` with
   `ON CONFLICT (event_id) DO NOTHING`.
4. Checks the affected-row count before loading or changing order state.
5. If the row was inserted, processes the state transition and records its
   final outcome in the same transaction.
6. If the row already existed, returns `DUPLICATE_EVENT`, does not update
   `orders`, and commits the no-op transaction safely.

The order update, status history, pending/dead-letter routing, and final
processed-event outcome are committed atomically with the idempotency record.

The processor intentionally does not call Redis `XACK`. A future worker will
acknowledge the stream message only after `Process` returns success, which means
the PostgreSQL commit has completed. If acknowledgement fails or the worker
crashes after commit, redelivery is harmless because the unique `event_id`
returns the duplicate result.

## Rejected alternative

A Redis distributed lock was considered but rejected as the primary
idempotency mechanism. A lock coordinates concurrent work but does not provide
the durable record that an event committed successfully. PostgreSQL already
owns the order state, so a unique constraint inside the same transaction is
simpler and gives a stronger source of truth.

## Consequences

- Duplicate event IDs never mutate order state after the first committed event.
- Delivery remains at least once, while durable state changes are effectively
  once per `event_id`.
- A duplicate payload is not compared with the original payload; the committed
  `event_id` wins.
- Retention of `processed_events` must preserve the desired deduplication
  window.
- Redis acknowledgement remains a worker responsibility, not a repository or
  service responsibility.

## Implementation

Implemented in commit `cf968b2`:

- `internal/domain/order_event.go`
- `internal/repository/order_repository.go`
- `internal/service/processor.go`
- `internal/repository/order_repository_test.go`
- `internal/service/processor_test.go`
