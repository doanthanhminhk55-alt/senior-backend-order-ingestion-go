# ADR 001: Redis Streams

- Status: proposed
- Decision: Use Redis Streams with consumer groups as the ingestion queue.

## Context

The service needs competing consumers, explicit acknowledgements, backlog
visibility, and worker crash recovery.

## Details to decide

- Stream, group, and consumer naming
- `XREADGROUP` batch and block settings
- Stream retention policy
- Pending-entry reclaim strategy
- Delivery and acknowledgement guarantees
