# Data Flow and Sequence Diagrams

This document explains how an order event moves through the system from producer to Redis, worker, PostgreSQL, monitoring, and recovery.

## Normal Processing Flow

```mermaid
sequenceDiagram
    autonumber
    participant Producer
    participant Redis as Redis Stream
    participant Worker
    participant Processor
    participant DB as PostgreSQL
    participant Stats as /stats

    Producer->>Redis: XADD order event
    Worker->>Redis: XREADGROUP order-events
    Redis-->>Worker: stream message
    Worker->>Processor: ProcessEvent(event)

    Processor->>DB: BEGIN
    Processor->>DB: pg_advisory_xact_lock(hash(order_id))
    Processor->>DB: INSERT processed_events(event_id)
    Processor->>DB: SELECT current order status
    Processor->>Processor: classify with state machine

    alt APPLIED
        Processor->>DB: INSERT/UPDATE orders
        Processor->>DB: INSERT order_status_history
        Processor->>DB: replay pending_events for order_id
        Processor->>DB: UPDATE processed_events outcome=APPLIED
    else DUPLICATE_EVENT
        Processor->>DB: no order update
    else DUPLICATE_STATUS
        Processor->>DB: UPDATE processed_events outcome=DUPLICATE_STATUS
    else OUT_OF_ORDER
        Processor->>DB: INSERT pending_events
        Processor->>DB: UPDATE processed_events outcome=OUT_OF_ORDER
    else INVALID_TRANSITION
        Processor->>DB: INSERT dead_letter_events
        Processor->>DB: UPDATE processed_events outcome=INVALID_TRANSITION
    end

    Processor->>DB: COMMIT
    Processor-->>Worker: success result
    Worker->>Redis: XACK after DB commit
    Worker->>Stats: increment metrics
```

## Key Ordering Rule

Redis `XACK` happens only after the PostgreSQL transaction commits.

This protects against event loss:

- If DB processing fails, the message is not acknowledged.
- If the worker crashes before `XACK`, the message remains in Redis pending entries.
- The reclaimer can claim and reprocess the pending message later.

## Duplicate Event Flow

```mermaid
sequenceDiagram
    autonumber
    participant Redis as Redis Stream
    participant Worker
    participant Processor
    participant DB as PostgreSQL

    Worker->>Redis: XREADGROUP duplicate event
    Redis-->>Worker: message
    Worker->>Processor: ProcessEvent(event)
    Processor->>DB: BEGIN
    Processor->>DB: pg_advisory_xact_lock(hash(order_id))
    Processor->>DB: INSERT processed_events(event_id)
    DB-->>Processor: unique violation / already exists
    Processor->>DB: COMMIT safe duplicate result
    Processor-->>Worker: DUPLICATE_EVENT
    Worker->>Redis: XACK
```

A duplicate event is not treated as an infrastructure failure. It is a successful idempotent handling result and is safe to acknowledge.

## Recoverable Out-of-Order Flow

Example: `PAID` arrives before `CREATED`.

```mermaid
sequenceDiagram
    autonumber
    participant Worker
    participant Processor
    participant DB as PostgreSQL

    Worker->>Processor: ProcessEvent(PAID)
    Processor->>DB: BEGIN
    Processor->>DB: lock by order_id
    Processor->>DB: insert processed_events
    Processor->>Processor: state machine => OUT_OF_ORDER
    Processor->>DB: insert pending_events
    Processor->>DB: commit
    Worker->>Worker: count out_of_order
```

Later, when `CREATED` arrives:

```mermaid
sequenceDiagram
    autonumber
    participant Worker
    participant Processor
    participant DB as PostgreSQL

    Worker->>Processor: ProcessEvent(CREATED)
    Processor->>DB: BEGIN
    Processor->>DB: lock by order_id
    Processor->>DB: apply CREATED to orders
    Processor->>DB: read pending_events for order_id
    Processor->>Processor: replay pending PAID if now valid
    Processor->>DB: apply PAID
    Processor->>DB: delete replayed pending event
    Processor->>DB: commit
    Worker->>Worker: count pending_replayed
```

## Invalid Transition Flow

Example: `CANCELLED` after `SHIPPED`.

```mermaid
sequenceDiagram
    autonumber
    participant Worker
    participant Processor
    participant DB as PostgreSQL

    Worker->>Processor: ProcessEvent(CANCELLED after SHIPPED)
    Processor->>DB: BEGIN
    Processor->>DB: lock by order_id
    Processor->>DB: insert processed_events
    Processor->>Processor: state machine => INVALID_TRANSITION
    Processor->>DB: insert dead_letter_events
    Processor->>DB: update processed_events outcome=INVALID_TRANSITION
    Processor->>DB: commit
    Worker->>Worker: count invalid_transitions and dead_letters
```

Invalid transitions are acknowledged after they are safely recorded in PostgreSQL. They do not block the queue forever.

## Worker Crash and Recovery Flow

The project includes deterministic failure simulation for this case.

Failure mode:

- worker processes message
- PostgreSQL transaction commits successfully
- worker intentionally skips Redis `XACK`
- message remains in Redis pending entries
- reclaimer later claims the message
- processor sees the same `event_id` in `processed_events`
- processor returns duplicate result
- reclaimer acknowledges the message

```mermaid
sequenceDiagram
    autonumber
    participant Redis as Redis Stream
    participant Worker
    participant Processor
    participant DB as PostgreSQL
    participant Reclaimer

    Worker->>Redis: XREADGROUP message
    Redis-->>Worker: message
    Worker->>Processor: ProcessEvent(event)
    Processor->>DB: BEGIN
    Processor->>DB: insert processed_events and update order state
    Processor->>DB: COMMIT
    Worker--xRedis: simulated crash / skip XACK
    Note over Redis: message remains pending

    Reclaimer->>Redis: XAUTOCLAIM stale pending message
    Redis-->>Reclaimer: claimed message
    Reclaimer->>Processor: ProcessEvent(event)
    Processor->>DB: BEGIN
    Processor->>DB: insert processed_events(event_id)
    DB-->>Processor: duplicate event_id
    Processor->>DB: COMMIT duplicate result
    Reclaimer->>Redis: XACK
```

## Monitoring Flow

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant API
    participant Metrics
    participant Redis as Redis Stream

    Client->>API: GET /stats
    API->>Metrics: snapshot counters
    API->>Redis: queue depth / stream length / pending depth
    Redis-->>API: stream stats
    API-->>Client: JSON stats
```

Important fields:

- `processed` — successful processing attempts
- `applied` — events that changed order state
- `duplicates_skipped` — duplicate event IDs safely skipped
- `out_of_order` — recoverable events stored/replayed through pending path
- `invalid_transitions` — invalid lifecycle transitions
- `dead_letters` — events isolated in dead-letter storage
- `simulated_failures` — deterministic skipped-ack failures
- `recovered_messages` — messages reclaimed and acknowledged
- `queue_depth` — unread backlog
- `stream_length` — total retained Redis Stream entries
- `pending_depth` — messages read but not yet acknowledged
