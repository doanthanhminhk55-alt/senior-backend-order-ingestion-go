# ADR 004: Out-of-Order Events

- Status: proposed
- Decision: Compare an event sequence/version before changing order state.

## Context

Events for one order can arrive or be retried in a different order from the one
in which they were created.

## Details to decide

- Source-of-truth sequence field
- Handling of stale, equal, and gapped versions
- Whether gaps wait, retry, quarantine, or apply
- Monitoring for rejected or deferred events
