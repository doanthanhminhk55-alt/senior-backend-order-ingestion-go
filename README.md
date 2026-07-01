# Senior Backend Order Ingestion Go

High-traffic e-commerce order ingestion service written in Go. The service ingests order events through Redis Streams, processes them with a concurrent worker pool, persists order state in PostgreSQL, exposes live `/stats`, and includes recovery behavior for worker failure before Redis acknowledgement.

This project was implemented for the Senior Backend Go test. It is designed to be runnable by reviewers with Docker Compose and to be operable by another engineer without relying on undocumented knowledge.

## What This Project Demonstrates

- Go API service with worker pool and graceful shutdown
- Redis Streams queue with consumer groups and explicit `XACK`
- PostgreSQL persistence with idempotency and transactional processing
- Duplicate event detection using `processed_events.event_id`
- Order lifecycle validation with a domain-level state machine
- Recoverable out-of-order event deferral and replay
- Invalid transition isolation through `dead_letter_events`
- Pending Redis message reclaim for worker crash recovery
- Deterministic worker failure simulation
- Live `GET /stats` monitoring
- Configurable load producer for 100,000+ events
- Design-first documentation, ADRs, runbook, AI notes, and a real 100k report

## Repository Structure

```text
cmd/
  api/                  # API server, worker pool runtime, reclaimer runtime
  producer/             # Configurable load producer

internal/
  config/               # Configuration helpers/placeholders
  db/                   # PostgreSQL pool setup
  domain/               # Order event and status state machine
  monitoring/           # Runtime metrics and /stats snapshot
  queue/                # Redis Streams adapter
  repository/           # PostgreSQL order repository
  service/              # Idempotent order event processor
  worker/               # Worker pool and pending-message reclaimer

migrations/
  001_init.sql          # PostgreSQL schema

docs/
  ERD.md
  DATA_FLOW.md
  RUNBOOK.md
  adr/

reports/
  sample-run-100k.md

AI_NOTES.md
Dockerfile
docker-compose.yml
README.md
```

## Tested Environment

The project was tested on **GitHub Codespaces** using Docker Compose. This is the recommended environment if the local machine cannot comfortably run Docker, Redis, PostgreSQL, and the 100k load test.

## Run the Service

Start PostgreSQL, Redis, and the API/worker service:

```bash
docker compose up --build -d postgres redis app
```

Check container status:

```bash
docker compose ps
```

Health check:

```bash
curl http://localhost:8080/healthz
```

Live stats:

```bash
curl http://localhost:8080/stats
```

Clean reset:

```bash
docker compose down -v
```

## Run the Producer

The producer does not start automatically during normal `docker compose up`. Run it explicitly:

```bash
docker compose run --rm producer   --total 100000   --duplicate-ratio 0.05   --out-of-order-ratio 0.03   --invalid-ratio 0.01
```

The producer prints a summary containing:

- `total_requested`
- `total_published`
- `unique_event_ids`
- `duplicate_events`
- `unique_order_ids`
- `out_of_order_events`
- `invalid_transition_events`
- `seed`
- `redis_stream`

## Recovery Demonstration

To demonstrate recovery from a worker failure after database commit but before Redis `XACK`, start the app with deterministic failure simulation enabled:

```bash
docker compose down -v

SIMULATE_ACK_FAILURE_AFTER=5000 SIMULATE_ACK_FAILURE_COUNT=20 docker compose up --build -d postgres redis app
```

Then run the 100k producer:

```bash
docker compose run --rm producer   --total 100000   --duplicate-ratio 0.05   --out-of-order-ratio 0.03   --invalid-ratio 0.01
```

Check final stats:

```bash
curl http://localhost:8080/stats
```

Expected recovery evidence:

- `simulated_failures > 0`
- `recovered_messages > 0`
- `queue_depth = 0`
- `pending_depth = 0`
- `failures = 0`

The sample run in [`reports/sample-run-100k.md`](reports/sample-run-100k.md) demonstrates this with 20 simulated failures and 20 recovered messages.

## API

### `GET /healthz`

Returns a simple health response:

```bash
curl http://localhost:8080/healthz
```

Expected:

```text
ok
```

### `GET /stats`

Returns live ingestion metrics:

```bash
curl http://localhost:8080/stats
```

Example fields:

```json
{
  "processed": 100020,
  "applied": 89716,
  "duplicates_skipped": 5020,
  "out_of_order": 3860,
  "invalid_transitions": 1424,
  "dead_letters": 1424,
  "failures": 0,
  "simulated_failures": 20,
  "recovered_messages": 20,
  "pending_replayed": 3860,
  "queue_depth": 0,
  "stream_length": 100000,
  "pending_depth": 0,
  "throughput_per_second": 291.26,
  "worker_count": 8
}
```

## Key Design Decisions

| Area | Decision |
|---|---|
| Queue | Redis Streams with consumer groups |
| DB | PostgreSQL |
| Idempotency | `processed_events.event_id` unique constraint |
| Concurrency | PostgreSQL transaction-scoped advisory lock by `order_id` |
| Out-of-order handling | Recoverable events go to `pending_events`; invalid transitions go to `dead_letter_events` |
| Recovery | Redis pending message reclaimer |
| Monitoring | Live `/stats` JSON endpoint |
| Report | Real 100k run with failure simulation and recovery evidence |

## Documentation

- [ERD](docs/ERD.md)
- [Data Flow](docs/DATA_FLOW.md)
- [Runbook](docs/RUNBOOK.md)

Architecture Decision Records:

- [ADR 001 — Redis Streams](docs/adr/001-redis-streams.md)
- [ADR 002 — Idempotency](docs/adr/002-idempotency.md)
- [ADR 003 — Concurrency Control](docs/adr/003-concurrency-control.md)
- [ADR 004 — Out-of-Order Events](docs/adr/004-out-of-order-events.md)
- [ADR 005 — Failure Recovery](docs/adr/005-failure-recovery.md)
- [ADR 006 — Schema Design](docs/adr/006-schema-design.md)

Other deliverables:

- [AI_NOTES.md](AI_NOTES.md)
- [Sample 100k Monitoring Report](reports/sample-run-100k.md)

## Final 100k Report

The final report is available at:

```text
reports/sample-run-100k.md
```

It includes:

- producer summary
- final `/stats` snapshot
- throughput
- duplicate handling
- out-of-order replay
- invalid transition isolation
- deterministic worker failure simulation
- recovery evidence
- correctness checks

Final correctness status in the report: **PASS**
