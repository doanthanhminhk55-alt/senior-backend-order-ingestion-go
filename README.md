# Senior Backend Order Ingestion

Initial project skeleton for a high-traffic e-commerce order ingestion service
built with Go, Redis Streams, PostgreSQL, and Docker Compose.

> This repository is design-first and does not contain the order-processing
> business logic yet.

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

The `producer` service is wired into Compose but does not emit events yet.

## Local compile check

With Go 1.23 or later:

```sh
go test ./...
```

## Status

Skeleton only. Queue, database, producer, worker, and domain behavior are TODOs
for later implementation steps.
