# ADR 003: Concurrency Control

- Status: accepted
- Decision: Serialize events per `order_id` with a PostgreSQL
  transaction-scoped advisory lock.

## Context

Multiple workers may process events for the same `order_id` concurrently. Those
events must not both read the same current state and then apply conflicting
transitions.

A row-level lock on `orders` is insufficient for the first event because there
is no row to lock yet. Concurrent first events could both observe a missing
order and race to create or classify it.

## Decision

At the start of the event transaction, before inserting the idempotency record
or reading the order, the repository executes:

```sql
SELECT pg_advisory_xact_lock(hashtext($1));
```

The argument is `order_id`. This serializes transactions for the same order
even when its `orders` row does not exist. Transactions for different order IDs
can proceed concurrently, subject to the normal possibility of conservative
serialization if two values produce the same hash.

The lock is transaction-scoped. PostgreSQL releases it automatically on commit
or rollback, including error paths and lost connections.

No global Go mutex is used because it would serialize only one process and
would not coordinate multiple service instances. No Redis distributed lock is
used because PostgreSQL is already the transactional source of truth and owns
the protected state.

## Transaction ordering

1. Begin the PostgreSQL transaction.
2. Acquire the advisory transaction lock for `order_id`.
3. Insert/check the event idempotency record.
4. Load and classify the current order state.
5. Apply, defer, reject, or replay events.
6. Commit all changes and release the lock.

## Consequences

- Events for one order are processed serially across concurrent workers and
  service instances.
- Events for unrelated orders remain concurrent, preserving throughput.
- The missing-row race is protected without process-local synchronization.
- Correctness depends on every order-processing path acquiring the same lock
  before reading or changing state.
- Advisory locks are held for the transaction duration, so transactions should
  remain small and avoid external calls.
- Worker-pool size, queue backpressure, and database connection-pool sizing are
  separate operational decisions and remain to be implemented.

## Implementation

Implemented in `internal/repository/order_repository.go` and tested in
`internal/repository/order_repository_test.go` by commit `cf968b2`.
