# ADR 006: Schema Design

- Status: accepted
- Decision: Separate current order state, processed-event idempotency records,
  transition history, deferred events, and dead-letter events.

## Context

The ingestion service must handle duplicate and out-of-order events while
preserving the current state of each order and enough evidence for operational
investigation. Workers also need durable places for events that must be retried
later or cannot be processed automatically.

## Decision

The initial PostgreSQL schema contains five tables:

- `orders` is the current materialized order state. `last_event_timestamp`
  provides the ordering boundary used to reject or defer stale events.
- `processed_events` records each handled `event_id` and its outcome. Its
  primary key is the database-level idempotency guard.
- `order_status_history` is the audit trail of accepted transitions. It has a
  foreign key to `orders` and a unique `event_id`, preventing the same accepted
  transition from being recorded twice.
- `pending_events` stores retryable or out-of-order events, including the
  original JSON payload, reason, attempt count, and last-attempt timestamp.
- `dead_letter_events` stores events that require manual investigation,
  retaining their payload and failure reason.

Order and event identifiers use `TEXT` because their external format is not
owned by this service. All business and processing timestamps use
`TIMESTAMPTZ`. Generated history and dead-letter row identifiers use
`BIGSERIAL`.

Allowed order statuses are constrained in the database to `CREATED`, `PAID`,
`SHIPPED`, and `CANCELLED`. The nullable history `from_status` supports the
initial transition into `CREATED`.

Only status history has an order foreign key. Processed, pending, and
dead-letter events deliberately do not require an existing order row, allowing
the service to record duplicate, invalid, or out-of-order input before an order
has been materialized.

Indexes support order-based investigation and retry scans:

- `processed_events(order_id)`
- `order_status_history(order_id, event_timestamp)`
- `pending_events(order_id)`
- `dead_letter_events(order_id)`
- `orders(current_status)`

The migration runs inside a transaction so Docker's PostgreSQL initialization
does not leave a partially created schema if a statement fails.

## Consequences

- Idempotency, current state, audit history, retries, and terminal failures have
  distinct retention and query paths.
- Applying an accepted event will require one database transaction to coordinate
  the current order, processed-event record, and history row.
- JSON payloads are retained only for pending and dead-letter events, reducing
  duplication in normal processing while preserving failed input for recovery.
- Status changes require a schema migration because `CHECK` constraints encode
  the allowed values.
- Retry scheduling and dead-letter promotion remain application concerns; the
  schema provides durable state but does not implement those policies.

## Implementation

Implemented in `migrations/001_init.sql`.
