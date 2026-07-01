# AI Notes

Record AI-assisted proposals and the human review applied to each material
decision. Add one row whenever AI contributes to architecture or implementation.

| Decision | What AI first proposed | What I changed / kept / rejected | Why | Where in the code |
|---|---|---|---|---|
| PostgreSQL schema for order event tracking | Five tables separating current order state, idempotency records, transition history, retryable events, and dead-letter events; status checks; operational indexes; transactional initialization | Kept the five-table separation, requested columns, allowed-status checks, table comments, and indexes. Kept processed, pending, and dead-letter events independent of an order foreign key; only accepted history references `orders`. No Go behavior was added. | The split supports duplicate detection, out-of-order deferral, auditability, and failure investigation without coupling every received event to an already-materialized order. Transactional DDL avoids a partial Docker initialization. | `migrations/001_init.sql`, `docs/adr/006-schema-design.md` |
