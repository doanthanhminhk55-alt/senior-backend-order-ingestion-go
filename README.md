# Senior Backend Order Ingestion

High-traffic e-commerce order ingestion service built with Go, Redis Streams,
PostgreSQL, and Docker Compose.

> This repository is design-first and now includes the core order-processing
> pipeline, pending-message recovery, load producer, and live statistics.

## Capabilities

- Redis Streams consumer groups
- Idempotent handling of duplicate events
- Explicit out-of-order event policy
- Worker crash recovery and pending-entry reclamation
- Backpressure and bounded concurrency
- Graceful shutdown
- A deterministic 100,000-event load producer
- Health and monitoring endpoints

The design placeholders live in [`docs/`](docs/), with architecture decisions
under [`docs/adr/`](docs/adr/).

## Run with Docker Compose

Prerequisite: Docker with Docker Compose.

Start PostgreSQL, Redis, and the ingestion app:

```sh
docker compose up --build
```

Verify the API from another terminal:

```sh
curl http://localhost:8080/healthz
curl http://localhost:8080/stats
```

The producer is behind the `load` profile and does not start during ordinary
`docker compose up`. Run the 100,000-event workload explicitly:

```sh
docker compose run --rm producer \
  --total 100000 \
  --duplicate-ratio 0.05 \
  --out-of-order-ratio 0.03 \
  --invalid-ratio 0.01
```

The producer intentionally includes duplicate, recoverable out-of-order, and
invalid-transition events and prints progress plus a final summary.

## Recovery demonstration

PostgreSQL commit-to-Redis-acknowledgement failures can be simulated
deterministically for a recovery test. The simulation is disabled by default.
This example skips `XACK` for 20 successful messages after the first 5,000:

```sh
SIMULATE_ACK_FAILURE_AFTER=5000 \
SIMULATE_ACK_FAILURE_COUNT=20 \
docker compose up --build -d postgres redis app
```

Publish the workload and inspect recovery:

```sh
docker compose run --rm producer \
  --total 100000 \
  --duplicate-ratio 0.05 \
  --out-of-order-ratio 0.03 \
  --invalid-ratio 0.01

curl http://localhost:8080/stats
```

The affected messages remain in Redis pending entries because the worker skips
acknowledgement only after the database transaction succeeds. After the
configured minimum idle period, the reclaimer processes them through the same
idempotent processor and acknowledges them. During the demonstration,
`simulated_failures` should increase by 20 and `recovered_messages` should rise
as the reclaimer succeeds.

Stop the stack while preserving data:

```sh
docker compose down
```

Remove the stack and volumes for a completely clean database and Redis reset:

```sh
docker compose down -v
```

## GitHub Codespaces

The Compose workflow is designed to run in GitHub Codespaces when Docker is not
available on the reviewer's local machine. Open the repository in a Codespace,
run the same commands above in its terminal, and use the forwarded port `8080`
for `/healthz` and `/stats`.

## Local compile check

With Go 1.23 or later:

```sh
go test ./...
```

## Status

The core queue, database processor, worker recovery, and load producer are
implemented. Final monitoring report generation remains a later step.
