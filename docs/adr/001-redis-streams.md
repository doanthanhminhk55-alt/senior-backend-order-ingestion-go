# ADR 001: Redis Streams

- Status: accepted
- Decision: Use Redis Streams with consumer groups instead of Redis Lists.

## Context

The ingestion service needs competing consumers without losing an event when a
worker crashes between receiving it and committing its database transaction. It
also needs explicit acknowledgement, pending-delivery visibility, and metrics
for stream depth and unacknowledged messages.

A Redis List with `BRPOP` removes an event as it is delivered. A worker crash
after `BRPOP` but before the database commit can therefore lose that event.
Building reliable delivery around Lists would require an additional processing
list, atomic movement between lists, acknowledgement bookkeeping, and custom
recovery logic.

Redis Streams provides consumer groups and a pending entries list as native
queue semantics. Messages remain pending until the application explicitly
acknowledges them, allowing a later recovery worker to reclaim abandoned
deliveries.

## Decision

Use Redis Streams through `github.com/redis/go-redis/v9`:

- Create the consumer group with `XGROUP CREATE ... MKSTREAM`. Treat
  `BUSYGROUP` as success so initialization is repeatable.
- Publish order events with `XADD`.
- Read only never-delivered messages with `XREADGROUP` and the `>` ID.
- Do not acknowledge while reading. Call `XACK` only through the adapter's
  `Ack` method, after the future worker has committed its database transaction.
- Report total stream entries with `XLEN` and delivered-but-unacknowledged group
  entries with `XPENDING`.
- Encode the event timestamp as RFC3339 with nanosecond precision and the event
  payload as a JSON object. Reject missing or malformed fields during parsing.

This gives the service at-least-once delivery once pending-entry reclamation is
implemented. Duplicate delivery remains expected and must be handled by the
PostgreSQL idempotency transaction.

## Implementation

Commit `b5a1f54` implements the decision:

- `internal/queue/redis_stream.go` defines `OrderEvent`, `StreamMessage`, and
  `RedisStreamQueue`.
- `EnsureGroup` maps to `XGROUP CREATE ... MKSTREAM`.
- `Publish` maps to `XADD`.
- `ReadGroup` maps to `XREADGROUP`, reads only `>`, and does not acknowledge.
- `Ack` is the sole `XACK` path.
- `QueueDepth` and `PendingCount` map to `XLEN` and `XPENDING`.
- `internal/queue/redis_stream_test.go` tests field serialization, field
  parsing, and malformed-message errors without requiring Redis.
- `go.mod` and `go.sum` pin the go-redis dependency.

## Consequences

- Workers can acknowledge only after durable database processing.
- Consumer-group pending state provides the basis for worker crash recovery.
- Redis may redeliver an event, so consumers must remain idempotent.
- Stream length and pending count can be exposed as operational metrics.
- Stream retention limits, consumer naming, batch/block configuration, and
  pending-entry reclaim policy remain runtime or worker-level decisions.
- The current adapter reports pending counts but does not yet reclaim pending
  messages; that belongs to the future worker implementation.
