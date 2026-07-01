# Sample Monitoring Report — 100,000 Order Events

## Run Summary

* Environment: GitHub Codespaces
* Runtime: Docker Compose
* App: Go API + Redis Streams worker pool + pending message reclaimer
* Queue: Redis Streams
* Database: PostgreSQL
* Run date: 2026-07-01
* Producer seed: `1`
* Worker count: `8`

## Commands Used

```bash
docker compose down -v
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
  "started_at": "2026-07-01T09:36:53.214834802Z",
  "uptime_seconds": 287.983948005,
  "processed": 100000,
  "applied": 89565,
  "duplicates_skipped": 5000,
  "duplicate_status": 0,
  "out_of_order": 3964,
  "invalid_transitions": 1471,
  "dead_letters": 1471,
  "failures": 0,
  "recovered_messages": 0,
  "pending_replayed": 3964,
  "throughput_per_second": 347.24157611126225,
  "queue_depth": 0,
  "stream_length": 100000,
  "pending_depth": 0,
  "worker_count": 8
}
```

## Throughput

* Final observed average throughput: `347.24 events/sec`
* Peak observed throughput from captured `/stats` samples: `397.39 events/sec`
* Final app uptime at completion: `287.98 seconds`

## Correctness Checks

| Check                                         | Result | Evidence                                        |
| --------------------------------------------- | -----: | ----------------------------------------------- |
| Producer published all requested events       |   PASS | `total_published=100000`                        |
| All published events were handled             |   PASS | `processed=100000`                              |
| Duplicate events were skipped safely          |   PASS | `duplicates_skipped=5000`                       |
| No processing failures occurred               |   PASS | `failures=0`                                    |
| No unread backlog remained                    |   PASS | `queue_depth=0`                                 |
| No unacknowledged pending messages remained   |   PASS | `pending_depth=0`                               |
| Redis Stream retained the full event history  |   PASS | `stream_length=100000`                          |
| Invalid transitions were isolated             |   PASS | `invalid_transitions=1471`, `dead_letters=1471` |
| Recoverable out-of-order events were replayed |   PASS | `out_of_order=3964`, `pending_replayed=3964`    |

## Result

The 100,000-event run completed successfully.

The final state proves that the system drained the Redis Stream workload, acknowledged all successfully processed messages, skipped duplicate events, isolated invalid transitions, replayed recoverable out-of-order events, and completed with no processing failures.

Final correctness status: **PASS**
