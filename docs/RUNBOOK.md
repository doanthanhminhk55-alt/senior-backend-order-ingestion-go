# Runbook

This runbook explains how to run, monitor, diagnose, and recover the order ingestion service.

## Prerequisites

- Docker and Docker Compose
- GitHub Codespaces or a local machine capable of running Docker
- No local PostgreSQL or Redis installation is required

Recommended environment for this project:

```text
GitHub Codespaces
```

## Start the Runtime

Start PostgreSQL, Redis, and the app service:

```bash
docker compose up --build -d postgres redis app
```

Check containers:

```bash
docker compose ps
```

Expected services:

- `postgres`
- `redis`
- `app`

## Health Check

```bash
curl http://localhost:8080/healthz
```

Expected response:

```text
ok
```

## Live Stats

```bash
curl http://localhost:8080/stats
```

The response is JSON.

Important fields:

| Field | Meaning |
|---|---|
| `processed` | successful processing attempts after DB commit |
| `applied` | events that changed `orders.current_status` |
| `duplicates_skipped` | duplicate `event_id` values skipped idempotently |
| `duplicate_status` | no-op status duplicates |
| `out_of_order` | recoverable out-of-order events routed through pending handling |
| `invalid_transitions` | invalid state-machine transitions |
| `dead_letters` | events persisted to `dead_letter_events` |
| `failures` | processing failures where Redis message is not acknowledged |
| `simulated_failures` | deterministic skipped-ack failures for recovery demonstration |
| `recovered_messages` | pending Redis messages reclaimed and acknowledged |
| `pending_replayed` | pending events replayed after prerequisites arrived |
| `queue_depth` | unread Redis Stream backlog for the consumer group |
| `stream_length` | total Redis Stream entries retained |
| `pending_depth` | messages read by the group but not yet acknowledged |
| `throughput_per_second` | average successful processing attempts per second |
| `worker_count` | configured number of workers |

## Run the Producer

Small smoke test:

```bash
docker compose run --rm producer   --total 1000   --duplicate-ratio 0.05   --out-of-order-ratio 0.03   --invalid-ratio 0.01
```

100k load test:

```bash
docker compose run --rm producer   --total 100000   --duplicate-ratio 0.05   --out-of-order-ratio 0.03   --invalid-ratio 0.01
```

After running the producer, watch stats:

```bash
curl http://localhost:8080/stats
```

A completed run should eventually show:

```text
queue_depth=0
pending_depth=0
failures=0
```

`stream_length` may remain equal to the number of produced events because Redis Streams retain entries.

## Recovery Demonstration

Start from a clean environment:

```bash
docker compose down -v
```

Start the app with deterministic worker failure simulation enabled:

```bash
SIMULATE_ACK_FAILURE_AFTER=5000 SIMULATE_ACK_FAILURE_COUNT=20 docker compose up --build -d postgres redis app
```

Run the producer:

```bash
docker compose run --rm producer   --total 100000   --duplicate-ratio 0.05   --out-of-order-ratio 0.03   --invalid-ratio 0.01
```

Check stats:

```bash
curl http://localhost:8080/stats
```

Expected evidence:

```text
simulated_failures > 0
recovered_messages > 0
pending_depth = 0
queue_depth = 0
failures = 0
```

Explanation:

The simulated failure happens after PostgreSQL commit but before Redis `XACK`. The Redis message remains pending. The reclaimer claims it, reprocesses it through the same processor path, detects the already-processed `event_id`, and acknowledges it safely.

A completed example is available in:

```text
reports/sample-run-100k.md
```

## Graceful Shutdown Behavior

When the app receives shutdown:

1. The API context is cancelled.
2. Workers stop reading new Redis messages.
3. In-flight processing is allowed to finish.
4. Successfully committed events are acknowledged with Redis `XACK`.
5. Messages not acknowledged remain in Redis pending entries.
6. The reclaimer can recover those messages after restart.

This means shutdown should not lose events.

## Common Failure Modes

### App Cannot Connect to PostgreSQL

Symptoms:

- app container restarts
- logs mention PostgreSQL connection failure

Commands:

```bash
docker compose ps
docker compose logs --tail=100 postgres
docker compose logs --tail=100 app
```

Recovery:

```bash
docker compose down -v
docker compose up --build -d postgres redis app
```

### App Cannot Connect to Redis

Symptoms:

- app logs mention Redis connection failure
- `/stats` may show queue stats error

Commands:

```bash
docker compose ps
docker compose logs --tail=100 redis
docker compose logs --tail=100 app
```

Recovery:

```bash
docker compose restart redis app
```

### `queue_depth` Is Not Decreasing

Meaning:

Workers are not draining unread Redis Stream entries.

Check:

```bash
docker compose logs --tail=200 app
curl http://localhost:8080/stats
```

Possible causes:

- app is not running
- worker count is too low for current load
- database is slow or unavailable
- processing errors are preventing progress

Recovery actions:

```bash
docker compose restart app
curl http://localhost:8080/stats
```

### `pending_depth` Is Not Decreasing

Meaning:

Messages were read by workers but not acknowledged.

Check:

```bash
curl http://localhost:8080/stats
docker compose logs --tail=200 app
```

Expected recovery:

The reclaimer should claim stale pending messages and process them. If `pending_depth` remains high, inspect app logs for repeated processor errors.

### `failures` Is Increasing

Meaning:

The processor or queue handling is returning errors.

Check:

```bash
docker compose logs --tail=200 app
curl http://localhost:8080/stats
```

Recovery:

- inspect app logs
- verify PostgreSQL and Redis are healthy
- restart app if the issue was transient
- use `docker compose down -v` for a full clean reset in test environments

### Invalid Transitions Are Increasing

Meaning:

The producer or upstream system is sending lifecycle-invalid events such as `CANCELLED` after `SHIPPED`.

This is not an infrastructure failure if `dead_letters` increases with `invalid_transitions`. It means the service is isolating invalid business events instead of corrupting order state.

## Clean Reset

Use this when preparing a fresh test run:

```bash
docker compose down -v
docker compose up --build -d postgres redis app
```

## Useful Commands

```bash
docker compose ps
docker compose logs --tail=200 app
docker compose logs --tail=100 postgres
docker compose logs --tail=100 redis
curl http://localhost:8080/healthz
curl http://localhost:8080/stats
```

## Related Documents

- `docs/ERD.md`
- `docs/DATA_FLOW.md`
- `docs/adr/001-redis-streams.md`
- `docs/adr/002-idempotency.md`
- `docs/adr/003-concurrency-control.md`
- `docs/adr/004-out-of-order-events.md`
- `docs/adr/005-failure-recovery.md`
- `docs/adr/006-schema-design.md`
- `reports/sample-run-100k.md`
- `AI_NOTES.md`
