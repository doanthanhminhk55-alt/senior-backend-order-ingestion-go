# ADR 003: Concurrency Control

- Status: proposed
- Decision: Use bounded worker concurrency and database-enforced consistency.

## Context

High throughput must not allow unbounded goroutines or unsafe concurrent updates
to the same order.

## Details to decide

- Worker pool size and configuration
- Per-order serialization or optimistic concurrency
- Transaction isolation and retry policy
- Queue-to-database backpressure thresholds
