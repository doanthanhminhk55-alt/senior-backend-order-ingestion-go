# Data Flow

Status: draft placeholder.

Planned flow:

```text
producer -> Redis Stream -> consumer group worker -> PostgreSQL
                              |
                              +-> monitoring
```

The detailed design must document duplicate detection, ordering rules, pending
entry recovery, acknowledgements, bounded concurrency, and shutdown sequencing
before implementation.
