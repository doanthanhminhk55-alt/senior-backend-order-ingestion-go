# ADR 005: Failure Recovery

- Status: proposed
- Decision: Recover abandoned pending entries and acknowledge only after a
  successful database transaction.

## Context

A worker can crash after reading an event or while persisting it.

## Details to decide

- Idle time before reclaim
- `XAUTOCLAIM` cadence and batch size
- Retry limits and poison-message handling
- Shutdown ordering for reads, in-flight work, and acknowledgements
