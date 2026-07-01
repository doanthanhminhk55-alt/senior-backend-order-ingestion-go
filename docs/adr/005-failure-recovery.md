# ADR 005: Failure Recovery

- Status: accepted
- Decision: Acknowledge Redis messages only after a successful database commit
  and recover abandoned deliveries through Redis Streams pending entries.

## Context

Redis Streams provides at-least-once delivery through consumer groups. Once a
worker reads a message, Redis records it in the consumer group's pending entries
list until the service explicitly acknowledges it.

A worker can fail while processing, crash after reading, or lose its connection
after the database commit but before `XACK`. Recovery must preserve the event in
all of these cases without relying on logs or process-local state.

## Decision

### Normal worker processing

Workers read new messages from Redis Streams with `XREADGROUP` and stable
consumer names. For each message, the worker:

1. Calls the existing order event processor.
2. Waits for the processor's PostgreSQL transaction to commit.
3. Calls Redis `XACK` only when processing returned success.

If processing fails, the worker does not acknowledge the message. If the worker
crashes after reading but before `XACK`, Redis retains the message in the
consumer group's pending entries.

A successful processor result includes durable no-op and routing outcomes such
as `DUPLICATE_EVENT`, `DUPLICATE_STATUS`, `OUT_OF_ORDER`, and
`INVALID_TRANSITION`. These results are acknowledged because their database
transactions committed successfully.

### Pending message recovery

A dedicated reclaimer runs immediately at startup and then on a configurable
interval. It uses `XAUTOCLAIM` to transfer messages that have been pending
longer than the configured minimum idle duration to the reclaimer consumer.

Claimed messages are not acknowledged during the claim. The reclaimer passes
each message through the same processor used by normal workers:

- Successful reprocessing is followed by `XACK`.
- Failed reprocessing is not acknowledged and becomes eligible for a future
  reclaim attempt after the minimum idle duration.
- Malformed claimed messages produce a controlled claim/parsing error. The
  reclaimer logs the error, remains running, and retries later.

`XAUTOCLAIM` does not expose the pending entry delivery count through the chosen
adapter API. This initial implementation therefore does not impose an
infrastructure retry limit. Unsuccessful infrastructure processing remains
pending until a later attempt succeeds.

### Application failures versus infrastructure failures

Application-level invalid transitions are handled durably by the order event
processor. They are written to PostgreSQL `dead_letter_events`, committed, and
then acknowledged in Redis.

Redis pending entries serve a different purpose from PostgreSQL
`pending_events`:

- Redis pending entries recover deliveries that were read but not acknowledged.
- PostgreSQL `pending_events` holds valid domain events waiting for an earlier
  order transition.

Infrastructure recovery belongs to the Redis pending-message reclaimer.

### Runtime and shutdown

The API runtime starts both the worker pool and the reclaimer. Both receive the
same cancellation context, and a `sync.WaitGroup` waits for both components to
stop before Redis and PostgreSQL connections are closed.

Cancellation stops new reads and claims. Messages already returned to a worker
or reclaimer are allowed to finish processing, preserving the rule that `XACK`
comes only after a successful commit.

No Redis distributed lock is used. No global Go mutex is used. Order-level
concurrency remains protected by the PostgreSQL transaction-scoped advisory
lock described in [ADR 003](003-concurrency-control.md).

## Consequences

- A crash before `XACK` cannot silently remove the event from Redis pending
  state.
- A crash after database commit but before `XACK` causes a safe duplicate
  delivery, handled by PostgreSQL idempotency.
- Reclaimer interval, minimum idle duration, and batch size are configurable
  runtime tradeoffs between recovery latency and Redis load.
- Persistent infrastructure failures retry indefinitely in this initial
  implementation and require operational visibility in a later monitoring
  step.
- HTTP statistics and monitoring endpoints are outside this decision.

## Implementation

- Worker pool: `internal/worker/pool.go`, commit `8f90de0`
- Pending reclaimer: `internal/worker/reclaimer.go` and
  `internal/worker/reclaimer_test.go`, commit `5e4722a`
- Redis `XAUTOCLAIM` adapter: `internal/queue/redis_stream.go`, commit `5e4722a`
- Runtime wiring: `cmd/api/main.go`, commit `ffafec4`
