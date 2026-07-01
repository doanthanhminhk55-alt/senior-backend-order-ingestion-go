# Senior Backend Order Ingestion

Initial project skeleton for a high-traffic e-commerce order ingestion service
built with Go, Redis Streams, PostgreSQL, and Docker Compose.

> This repository is design-first and now includes the core order-processing
> pipeline. Monitoring/report generation remains a later step.

## Planned capabilities

- Redis Streams consumer groups
- Idempotent handling of duplicate events
- Explicit out-of-order event policy
- Worker crash recovery and pending-entry reclamation
- Backpressure and bounded concurrency
- Graceful shutdown
- A 100,000-event validation run
- Health and monitoring endpoints

The design placeholders live in [`docs/`](docs/), with architecture decisions
under [`docs/adr/`](docs/adr/).

## Draft run instructions

Prerequisite: Docker with Docker Compose.

```sh
docker compose up --build
```

The initial API skeleton will be available at:

- `GET http://localhost:8080/healthz`
- `GET http://localhost:8080/metrics` (placeholder response)

Stop the stack gracefully:

```sh
docker compose down
```

The `producer` service publishes 100,000 deterministic load-test events by
default. To override its generation settings:

```sh
docker compose run --rm producer \
  --total 100000 \
  --duplicate-ratio 0.05 \
  --out-of-order-ratio 0.03 \
  --invalid-ratio 0.01 \
  --seed 1
```

The producer intentionally includes duplicate, recoverable out-of-order, and
invalid-transition events and prints progress plus a final summary.

## Local compile check

With Go 1.23 or later:

```sh
go test ./...
```

## Status

The core queue, database processor, worker recovery, and load producer are
implemented. Monitoring/report generation remains a later step.
