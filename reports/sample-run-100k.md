# Sample Monitoring Report — 100,000 Order Events

## Run Summary

- Environment: GitHub Codespaces
- Runtime: Docker Compose
- App: Go API + Redis Streams worker pool + pending message reclaimer
- Queue: Redis Streams
- Database: PostgreSQL
- Run date: 2026-07-01
- Producer seed: `1`
- Worker count: `8`
- Failure simulation: enabled
- Simulated failure mode: DB commit succeeds, then Redis XACK is intentionally skipped for 20 messages

## Commands Used

```bash
docker compose down -v

SIMULATE_ACK_FAILURE_AFTER=5000 \
SIMULATE_ACK_FAILURE_COUNT=20 \
docker compose up --build -d postgres redis app

curl http://localhost:8080/stats

docker compose run --rm producer \
  --total 100000 \
  --duplicate-ratio 0.05 \
  --out-of-order-ratio 0.03 \
  --invalid-ratio 0.01

sleep 30
curl http://localhost:8080/stats
```

## Producer Summary

```text
total_requested=100000
total_published=100000
unique_event_ids=95000
duplicate_events=5000
unique_order_ids=42078
out_of_order_events=3000
invalid_transition_events=1000
seed=1
redis_stream=order-events
```

## Final `/stats` Snapshot

```json
{
  "started_at": "2026-07-01T16:22:35.68885857Z",
  "uptime_seconds": 343.400629367,
  "processed": 100020,
  "applied": 89716,
  "duplicates_skipped": 5020,
  "duplicate_status": 0,
  "out_of_order": 3860,
  "invalid_transitions": 1424,
  "dead_letters": 1424,
  "failures": 0,
  "simulated_failures": 20,
  "recovered_messages": 20,
  "pending_replayed": 3860,
  "throughput_per_second": 291.26329845221795,
  "queue_depth": 0,
  "stream_length": 100000,
  "pending_depth": 0,
  "worker_count": 8
}
```

## Throughput

- Final observed average throughput: `291.26 events/sec`
- Final app uptime at completion: `343.40 seconds`
- Redis Stream entries published: `100,000`
- Total successful processing attempts: `100,020`
- Extra processing attempts from simulated recovery: `20`

## Recovery Demonstration

This run enabled deterministic worker failure simulation:

- `SIMULATE_ACK_FAILURE_AFTER=5000`
- `SIMULATE_ACK_FAILURE_COUNT=20`

For 20 messages, the worker completed database processing but intentionally skipped Redis `XACK`. Those messages stayed in the Redis Streams Pending Entries List.

The pending message reclaimer later claimed and reprocessed them through the same processor path. Because the database transaction had already committed the original `event_id`, the reprocessed messages were detected as duplicates and acknowledged safely.

Recovery result:

| Metric | Value |
|---|---:|
| Simulated failures | `20` |
| Recovered messages | `20` |
| Final pending depth | `0` |
| Final failures | `0` |

## Correctness Checks

| Check | Result | Evidence |
|---|---:|---|
| Producer published all requested stream events | PASS | `total_published=100000`, `stream_length=100000` |
| All Redis Stream entries were drained | PASS | `queue_depth=0` |
| No unacknowledged pending messages remained | PASS | `pending_depth=0` |
| Simulated worker failures were recovered | PASS | `simulated_failures=20`, `recovered_messages=20` |
| Duplicate events were skipped safely | PASS | `duplicates_skipped=5020` |
| No processing failures remained | PASS | `failures=0` |
| Invalid transitions were isolated | PASS | `invalid_transitions=1424`, `dead_letters=1424` |
| Recoverable out-of-order events were replayed | PASS | `out_of_order=3860`, `pending_replayed=3860` |

## Notes on `processed`

`processed=100020` counts successful processing attempts, not only unique Redis Stream entries.

The producer published `100000` stream entries. The additional `20` processing attempts came from the deterministic failure simulation: those messages were processed once before the skipped `XACK`, then reprocessed by the reclaimer and safely identified as duplicates through the `processed_events` idempotency table.

This confirms that a worker failure after database commit but before Redis acknowledgement does not lose data or corrupt order state.

## Result

The 100,000-event run completed successfully with deterministic failure recovery enabled.

The final state proves that the system drained the Redis Stream workload, recovered messages left pending by simulated worker failures, acknowledged all successfully processed messages, skipped duplicate events, isolated invalid transitions, replayed recoverable out-of-order events, and completed with no remaining failures.

Final correctness status: **PASS**
