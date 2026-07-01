# ADR 004: Out-of-Order Event Handling

* Status: accepted
* Date: 2026-07-01

## Context

Order events can arrive out of order because upstream sales channels may send events in bursts, retry old events, or deliver messages with different delays. The ingestion service must therefore avoid blindly overwriting the current order status with the latest message it receives.

Examples from the test requirements include:

* `PAID` arriving before `CREATED`
* `CANCELLED` arriving after `SHIPPED`

These cases are not equivalent. Some events are temporarily unprocessable because a prerequisite status has not arrived yet. Other events are permanently invalid because they violate the order lifecycle.

The service must classify these cases clearly so workers can decide whether to apply the transition, skip it as a duplicate, defer it for later replay, or reject it into a dead-letter table.

## Decision

The service uses an explicit domain-level order status state machine.

Valid statuses are:

* `CREATED`
* `PAID`
* `SHIPPED`
* `CANCELLED`

Valid transitions are:

* No existing order status → `CREATED`
* `CREATED` → `PAID`
* `PAID` → `SHIPPED`
* `CREATED` → `CANCELLED`
* `PAID` → `CANCELLED`

Duplicate/no-op transitions are classified separately:

* `CREATED` → `CREATED`
* `PAID` → `PAID`
* `SHIPPED` → `SHIPPED`
* `CANCELLED` → `CANCELLED`

Recoverable out-of-order cases are classified as `OUT_OF_ORDER`:

* No existing order status → `PAID`
* No existing order status → `SHIPPED`
* `CREATED` → `SHIPPED`, because `PAID` is missing

Invalid/unrecoverable transitions are classified as `INVALID_TRANSITION`:

* `SHIPPED` → `CANCELLED`
* `SHIPPED` → `PAID`
* `SHIPPED` → `CREATED`
* `CANCELLED` → `CREATED`
* `CANCELLED` → `PAID`
* `CANCELLED` → `SHIPPED`
* `PAID` → `CREATED`
* Any unknown status

The state machine returns a structured result containing:

* `Allowed`
* `Classification`
* `Reason`

This keeps status rules independent from persistence code. The worker can use the classification to decide the next persistence action:

* `APPLIED` → update `orders` and insert `order_status_history`
* `DUPLICATE_STATUS` → treat as already observed/no-op
* `OUT_OF_ORDER` → store in `pending_events` for later replay
* `INVALID_TRANSITION` → store in `dead_letter_events`

## Options Considered

### Option 1: Blind upsert latest status

This option would directly update the order row with the incoming event status.

Example:

```sql
UPDATE orders
SET current_status = $1
WHERE order_id = $2;
```

This was rejected because it can corrupt the order lifecycle. For example, a late `CREATED` event could overwrite a newer `PAID` or `SHIPPED` status. A `CANCELLED` event after `SHIPPED` could also be accepted even though the lifecycle forbids it.

### Option 2: Timestamp-only ordering

This option would compare event timestamps and only apply newer events.

This was rejected as the only rule because timestamp ordering does not prove that a transition is valid. A newer `CANCELLED` event after `SHIPPED` is still invalid even if its timestamp is later.

### Option 3: Explicit state machine

This option defines allowed transitions and classifications in domain code.

This was accepted because it makes the lifecycle testable, explainable, and reusable by the worker, repository, and documentation.

## Consequences

Positive consequences:

* Prevents invalid status overwrites.
* Separates recoverable out-of-order events from permanently invalid transitions.
* Makes lifecycle behavior easy to test with table-driven unit tests.
* Gives the worker clear routing behavior for accepted, pending, duplicate, and dead-letter events.
* Makes the implementation easier for another engineer or AI agent to extend safely.

Trade-offs:

* More code than a blind update.
* The service must maintain state machine tests whenever lifecycle rules change.
* Some out-of-order events require a pending/replay path instead of immediate processing.

## Code Mapping

Implemented in:

* `internal/domain/status.go`
* `internal/domain/state_machine.go`
* `internal/domain/state_machine_test.go`

Related schema support:

* `pending_events` in `migrations/001_init.sql`
* `dead_letter_events` in `migrations/001_init.sql`
* `order_status_history` in `migrations/001_init.sql`

Related commits:

* Schema support: `7ee9f30`
* State machine implementation: `dd749c7`

## Verification

The state machine is covered by table-driven tests for:

* Valid transitions
* Duplicate/no-op statuses
* Recoverable out-of-order events
* Invalid transitions
* Unknown statuses

Run:

```bash
go test ./...
```
