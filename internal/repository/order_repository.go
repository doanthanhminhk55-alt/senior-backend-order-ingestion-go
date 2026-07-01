package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ClassificationDuplicateEvent = "DUPLICATE_EVENT"
	outcomeReceived              = "RECEIVED"
)

// ProcessResult describes the durable outcome of processing one event.
type ProcessResult struct {
	EventID         string
	OrderID         string
	Classification  string
	Applied         bool
	Duplicate       bool
	Pending         bool
	DeadLetter      bool
	ReplayedPending int
	Reason          string
}

// OrderRepository processes one event atomically.
type OrderRepository interface {
	ProcessEventTx(
		ctx context.Context,
		event domain.OrderEvent,
	) (ProcessResult, error)
}

type dbTransaction interface {
	Exec(
		ctx context.Context,
		sql string,
		arguments ...any,
	) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// PostgresOrderRepository persists order events with PostgreSQL transactions.
type PostgresOrderRepository struct {
	begin func(context.Context) (dbTransaction, error)
}

// NewPostgresOrderRepository creates a PostgreSQL-backed order repository.
func NewPostgresOrderRepository(pool *pgxpool.Pool) *PostgresOrderRepository {
	return &PostgresOrderRepository{
		begin: func(ctx context.Context) (dbTransaction, error) {
			return pool.Begin(ctx)
		},
	}
}

// ProcessEventTx applies, defers, rejects, or deduplicates one event atomically.
func (r *PostgresOrderRepository) ProcessEventTx(
	ctx context.Context,
	event domain.OrderEvent,
) (ProcessResult, error) {
	payload, err := validateEvent(event)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("validate event: %w", err)
	}

	tx, err := r.begin(ctx)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("begin order event transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// A transaction-scoped advisory lock serializes every event for one order,
	// including the first event before an orders row exists. It is released
	// automatically by PostgreSQL on commit or rollback.
	if _, err := tx.Exec(
		ctx,
		`SELECT pg_advisory_xact_lock(hashtext($1))`,
		event.OrderID,
	); err != nil {
		return ProcessResult{}, fmt.Errorf(
			"acquire advisory lock for order %q: %w",
			event.OrderID,
			err,
		)
	}

	inserted, err := insertProcessedEvent(ctx, tx, event)
	if err != nil {
		return ProcessResult{}, err
	}
	if !inserted {
		result := ProcessResult{
			EventID:        event.EventID,
			OrderID:        event.OrderID,
			Classification: ClassificationDuplicateEvent,
			Duplicate:      true,
			Reason:         "event ID has already been processed",
		}
		return commitResult(ctx, tx, result)
	}

	current, exists, err := loadCurrentStatus(ctx, tx, event.OrderID)
	if err != nil {
		return ProcessResult{}, err
	}

	transition := domain.EvaluateTransition(current, event.Status)
	result := ProcessResult{
		EventID:        event.EventID,
		OrderID:        event.OrderID,
		Classification: transition.Classification,
		Reason:         transition.Reason,
	}

	switch transition.Classification {
	case domain.ClassificationApplied:
		if err := applyEvent(ctx, tx, event, current, exists); err != nil {
			return ProcessResult{}, err
		}
		if err := updateOutcome(
			ctx,
			tx,
			event.EventID,
			domain.ClassificationApplied,
		); err != nil {
			return ProcessResult{}, err
		}

		replayed, err := replayPending(
			ctx,
			tx,
			event.OrderID,
			event.Status,
		)
		if err != nil {
			return ProcessResult{}, err
		}
		result.Applied = true
		result.ReplayedPending = replayed

	case domain.ClassificationDuplicateStatus:
		if err := updateOutcome(
			ctx,
			tx,
			event.EventID,
			domain.ClassificationDuplicateStatus,
		); err != nil {
			return ProcessResult{}, err
		}
		result.Duplicate = true

	case domain.ClassificationOutOfOrder:
		if err := insertPendingEvent(
			ctx,
			tx,
			event,
			transition.Reason,
			payload,
		); err != nil {
			return ProcessResult{}, err
		}
		if err := updateOutcome(
			ctx,
			tx,
			event.EventID,
			domain.ClassificationOutOfOrder,
		); err != nil {
			return ProcessResult{}, err
		}
		result.Pending = true

	case domain.ClassificationInvalidTransition:
		if err := insertDeadLetterEvent(
			ctx,
			tx,
			event,
			transition.Reason,
			payload,
		); err != nil {
			return ProcessResult{}, err
		}
		if err := updateOutcome(
			ctx,
			tx,
			event.EventID,
			domain.ClassificationInvalidTransition,
		); err != nil {
			return ProcessResult{}, err
		}
		result.DeadLetter = true

	default:
		return ProcessResult{}, fmt.Errorf(
			"classify event %q: unsupported classification %q",
			event.EventID,
			transition.Classification,
		)
	}

	return commitResult(ctx, tx, result)
}

func validateEvent(event domain.OrderEvent) (json.RawMessage, error) {
	if event.EventID == "" {
		return nil, errors.New("event ID is required")
	}
	if event.OrderID == "" {
		return nil, errors.New("order ID is required")
	}
	if event.Timestamp.IsZero() {
		return nil, errors.New("event timestamp is required")
	}
	if !isPersistableStatus(event.Status) {
		return nil, fmt.Errorf("status %q is not supported", event.Status)
	}

	payload := event.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		return nil, fmt.Errorf("payload must be a JSON object: %w", err)
	}
	if object == nil {
		return nil, errors.New("payload must be a JSON object")
	}

	return payload, nil
}

func isPersistableStatus(status domain.Status) bool {
	switch status {
	case domain.StatusCreated,
		domain.StatusPaid,
		domain.StatusShipped,
		domain.StatusCancelled:
		return true
	default:
		return false
	}
}

func insertProcessedEvent(
	ctx context.Context,
	tx dbTransaction,
	event domain.OrderEvent,
) (bool, error) {
	tag, err := tx.Exec(
		ctx,
		`INSERT INTO processed_events (
			event_id,
			order_id,
			status,
			event_timestamp,
			outcome
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) DO NOTHING`,
		event.EventID,
		event.OrderID,
		string(event.Status),
		event.Timestamp,
		outcomeReceived,
	)
	if err != nil {
		return false, fmt.Errorf(
			"insert processed event %q: %w",
			event.EventID,
			err,
		)
	}

	return tag.RowsAffected() == 1, nil
}

func loadCurrentStatus(
	ctx context.Context,
	tx dbTransaction,
	orderID string,
) (domain.Status, bool, error) {
	var current string
	err := tx.QueryRow(
		ctx,
		`SELECT current_status
		FROM orders
		WHERE order_id = $1`,
		orderID,
	).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf(
			"load current status for order %q: %w",
			orderID,
			err,
		)
	}

	return domain.Status(current), true, nil
}

func applyEvent(
	ctx context.Context,
	tx dbTransaction,
	event domain.OrderEvent,
	current domain.Status,
	orderExists bool,
) error {
	if orderExists {
		if _, err := tx.Exec(
			ctx,
			`UPDATE orders
			SET current_status = $2,
				updated_at = now(),
				last_event_timestamp = $3
			WHERE order_id = $1`,
			event.OrderID,
			string(event.Status),
			event.Timestamp,
		); err != nil {
			return fmt.Errorf("update order %q: %w", event.OrderID, err)
		}
	} else {
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO orders (
				order_id,
				current_status,
				last_event_timestamp
			) VALUES ($1, $2, $3)`,
			event.OrderID,
			string(event.Status),
			event.Timestamp,
		); err != nil {
			return fmt.Errorf("insert order %q: %w", event.OrderID, err)
		}
	}

	var fromStatus any
	if orderExists {
		fromStatus = string(current)
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO order_status_history (
			event_id,
			order_id,
			from_status,
			to_status,
			event_timestamp
		) VALUES ($1, $2, $3, $4, $5)`,
		event.EventID,
		event.OrderID,
		fromStatus,
		string(event.Status),
		event.Timestamp,
	); err != nil {
		return fmt.Errorf(
			"insert status history for event %q: %w",
			event.EventID,
			err,
		)
	}

	return nil
}

func insertPendingEvent(
	ctx context.Context,
	tx dbTransaction,
	event domain.OrderEvent,
	reason string,
	payload json.RawMessage,
) error {
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO pending_events (
			event_id,
			order_id,
			status,
			event_timestamp,
			reason,
			payload
		) VALUES ($1, $2, $3, $4, $5, $6)`,
		event.EventID,
		event.OrderID,
		string(event.Status),
		event.Timestamp,
		reason,
		payload,
	); err != nil {
		return fmt.Errorf("insert pending event %q: %w", event.EventID, err)
	}

	return nil
}

func insertDeadLetterEvent(
	ctx context.Context,
	tx dbTransaction,
	event domain.OrderEvent,
	reason string,
	payload json.RawMessage,
) error {
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO dead_letter_events (
			event_id,
			order_id,
			status,
			reason,
			payload
		) VALUES ($1, $2, $3, $4, $5)`,
		event.EventID,
		event.OrderID,
		string(event.Status),
		reason,
		payload,
	); err != nil {
		return fmt.Errorf("insert dead-letter event %q: %w", event.EventID, err)
	}

	return nil
}

func updateOutcome(
	ctx context.Context,
	tx dbTransaction,
	eventID string,
	outcome string,
) error {
	if _, err := tx.Exec(
		ctx,
		`UPDATE processed_events
		SET outcome = $2
		WHERE event_id = $1`,
		eventID,
		outcome,
	); err != nil {
		return fmt.Errorf(
			"update outcome for event %q to %q: %w",
			eventID,
			outcome,
			err,
		)
	}

	return nil
}

func replayPending(
	ctx context.Context,
	tx dbTransaction,
	orderID string,
	current domain.Status,
) (int, error) {
	appliedCount := 0

	for {
		event, found, err := loadNextPending(ctx, tx, orderID)
		if err != nil {
			return 0, err
		}
		if !found {
			return appliedCount, nil
		}

		transition := domain.EvaluateTransition(current, event.Status)
		switch transition.Classification {
		case domain.ClassificationApplied:
			if err := applyEvent(ctx, tx, event, current, true); err != nil {
				return 0, fmt.Errorf(
					"replay pending event %q: %w",
					event.EventID,
					err,
				)
			}
			if err := updateOutcome(
				ctx,
				tx,
				event.EventID,
				domain.ClassificationApplied,
			); err != nil {
				return 0, err
			}
			if err := deletePending(ctx, tx, event.EventID); err != nil {
				return 0, err
			}
			current = event.Status
			appliedCount++

		case domain.ClassificationDuplicateStatus:
			if err := updateOutcome(
				ctx,
				tx,
				event.EventID,
				domain.ClassificationDuplicateStatus,
			); err != nil {
				return 0, err
			}
			if err := deletePending(ctx, tx, event.EventID); err != nil {
				return 0, err
			}

		case domain.ClassificationInvalidTransition:
			if err := insertDeadLetterEvent(
				ctx,
				tx,
				event,
				transition.Reason,
				event.Payload,
			); err != nil {
				return 0, err
			}
			if err := updateOutcome(
				ctx,
				tx,
				event.EventID,
				domain.ClassificationInvalidTransition,
			); err != nil {
				return 0, err
			}
			if err := deletePending(ctx, tx, event.EventID); err != nil {
				return 0, err
			}

		case domain.ClassificationOutOfOrder:
			return appliedCount, nil

		default:
			return 0, fmt.Errorf(
				"classify pending event %q: unsupported classification %q",
				event.EventID,
				transition.Classification,
			)
		}
	}
}

func loadNextPending(
	ctx context.Context,
	tx dbTransaction,
	orderID string,
) (domain.OrderEvent, bool, error) {
	var event domain.OrderEvent
	var status string
	var payload []byte
	err := tx.QueryRow(
		ctx,
		`SELECT event_id, order_id, status, event_timestamp, payload
		FROM pending_events
		WHERE order_id = $1
		ORDER BY event_timestamp, event_id
		LIMIT 1`,
		orderID,
	).Scan(
		&event.EventID,
		&event.OrderID,
		&status,
		&event.Timestamp,
		&payload,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.OrderEvent{}, false, nil
	}
	if err != nil {
		return domain.OrderEvent{}, false, fmt.Errorf(
			"load next pending event for order %q: %w",
			orderID,
			err,
		)
	}

	event.Status = domain.Status(status)
	event.Payload = json.RawMessage(payload)

	return event, true, nil
}

func deletePending(
	ctx context.Context,
	tx dbTransaction,
	eventID string,
) error {
	if _, err := tx.Exec(
		ctx,
		`DELETE FROM pending_events WHERE event_id = $1`,
		eventID,
	); err != nil {
		return fmt.Errorf("delete pending event %q: %w", eventID, err)
	}

	return nil
}

func commitResult(
	ctx context.Context,
	tx dbTransaction,
	result ProcessResult,
) (ProcessResult, error) {
	if err := tx.Commit(ctx); err != nil {
		return ProcessResult{}, fmt.Errorf(
			"commit processing for event %q: %w",
			result.EventID,
			err,
		)
	}

	return result, nil
}
