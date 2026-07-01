BEGIN;

CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    current_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_event_timestamp TIMESTAMPTZ NOT NULL,
    CONSTRAINT orders_current_status_check
        CHECK (current_status IN ('CREATED', 'PAID', 'SHIPPED', 'CANCELLED'))
);

COMMENT ON TABLE orders IS
    'Stores the latest materialized state of each order.';

CREATE TABLE processed_events (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    status TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    outcome TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT processed_events_status_check
        CHECK (status IN ('CREATED', 'PAID', 'SHIPPED', 'CANCELLED'))
);

COMMENT ON TABLE processed_events IS
    'Records handled event IDs and outcomes for idempotency and auditing.';

CREATE TABLE order_status_history (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    from_status TEXT,
    to_status TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT order_status_history_from_status_check
        CHECK (
            from_status IS NULL
            OR from_status IN ('CREATED', 'PAID', 'SHIPPED', 'CANCELLED')
        ),
    CONSTRAINT order_status_history_to_status_check
        CHECK (to_status IN ('CREATED', 'PAID', 'SHIPPED', 'CANCELLED'))
);

COMMENT ON TABLE order_status_history IS
    'Stores the accepted status transitions for each order.';

CREATE TABLE pending_events (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    status TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempt_at TIMESTAMPTZ,
    CONSTRAINT pending_events_status_check
        CHECK (status IN ('CREATED', 'PAID', 'SHIPPED', 'CANCELLED'))
);

COMMENT ON TABLE pending_events IS
    'Holds deferred or retryable events that cannot yet be applied.';

CREATE TABLE dead_letter_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT dead_letter_events_status_check
        CHECK (status IN ('CREATED', 'PAID', 'SHIPPED', 'CANCELLED'))
);

COMMENT ON TABLE dead_letter_events IS
    'Retains events that cannot be processed automatically for investigation.';

CREATE INDEX idx_processed_events_order_id
    ON processed_events (order_id);

CREATE INDEX idx_order_status_history_order_timestamp
    ON order_status_history (order_id, event_timestamp);

CREATE INDEX idx_pending_events_order_id
    ON pending_events (order_id);

CREATE INDEX idx_dead_letter_events_order_id
    ON dead_letter_events (order_id);

CREATE INDEX idx_orders_current_status
    ON orders (current_status);

COMMIT;
