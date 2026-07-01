# ADR 006: Schema Design

- Status: proposed
- Decision: Keep current order state separate from event idempotency metadata.

## Context

The database must support atomic deduplication, ordering checks, concurrent
workers, and operational investigation.

## Details to decide

- Order primary key and monetary representations
- Event identity and version constraints
- State history/audit requirements
- Indexes for the 100,000-event workload
- Timestamps and payload retention
