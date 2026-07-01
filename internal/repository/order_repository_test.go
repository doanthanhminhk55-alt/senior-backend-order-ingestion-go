package repository

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeRow struct {
	scan func(destinations ...any) error
}

func (r fakeRow) Scan(destinations ...any) error {
	return r.scan(destinations...)
}

type fakeTransaction struct {
	pending             []domain.OrderEvent
	processedInsertRows int64
	insertedOrderCount  int
	pendingInsertCount  int
	updatedStatuses     []domain.Status
	outcomes            map[string]string
	historyCount        int
	deadLetterCount     int
	operations          []string
	committed           bool
}

func (tx *fakeTransaction) Exec(
	_ context.Context,
	sql string,
	arguments ...any,
) (pgconn.CommandTag, error) {
	switch {
	case strings.Contains(sql, "pg_advisory_xact_lock"):
		tx.operations = append(tx.operations, "lock")
		return pgconn.NewCommandTag("SELECT 1"), nil

	case strings.Contains(sql, "INSERT INTO processed_events"):
		tx.operations = append(tx.operations, "insert_processed")
		if tx.processedInsertRows == 1 {
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		}
		return pgconn.NewCommandTag("INSERT 0 0"), nil

	case strings.Contains(sql, "INSERT INTO orders"):
		tx.insertedOrderCount++

	case strings.Contains(sql, "UPDATE orders"):
		tx.updatedStatuses = append(
			tx.updatedStatuses,
			domain.Status(arguments[1].(string)),
		)

	case strings.Contains(sql, "INSERT INTO order_status_history"):
		tx.historyCount++

	case strings.Contains(sql, "UPDATE processed_events"):
		if tx.outcomes == nil {
			tx.outcomes = make(map[string]string)
		}
		tx.outcomes[arguments[0].(string)] = arguments[1].(string)

	case strings.Contains(sql, "INSERT INTO pending_events"):
		tx.pendingInsertCount++

	case strings.Contains(sql, "INSERT INTO dead_letter_events"):
		tx.deadLetterCount++

	case strings.Contains(sql, "DELETE FROM pending_events"):
		eventID := arguments[0].(string)
		for index, event := range tx.pending {
			if event.EventID == eventID {
				tx.pending = append(tx.pending[:index], tx.pending[index+1:]...)
				break
			}
		}
	}

	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (tx *fakeTransaction) QueryRow(
	_ context.Context,
	sql string,
	_ ...any,
) pgx.Row {
	if !strings.Contains(sql, "FROM pending_events") || len(tx.pending) == 0 {
		return fakeRow{
			scan: func(...any) error {
				return pgx.ErrNoRows
			},
		}
	}

	event := tx.pending[0]
	return fakeRow{
		scan: func(destinations ...any) error {
			*destinations[0].(*string) = event.EventID
			*destinations[1].(*string) = event.OrderID
			*destinations[2].(*string) = string(event.Status)
			*destinations[3].(*time.Time) = event.Timestamp
			*destinations[4].(*[]byte) = append([]byte(nil), event.Payload...)
			return nil
		},
	}
}

func (tx *fakeTransaction) Commit(context.Context) error {
	tx.committed = true
	return nil
}

func (tx *fakeTransaction) Rollback(context.Context) error {
	return nil
}

func TestProcessEventTx_DuplicateEventCommitsWithoutLoadingOrder(t *testing.T) {
	tx := &fakeTransaction{processedInsertRows: 0}
	repo := &PostgresOrderRepository{
		begin: func(context.Context) (dbTransaction, error) {
			return tx, nil
		},
	}
	event := domain.OrderEvent{
		EventID:   "event-duplicate",
		OrderID:   "order-123",
		Status:    domain.StatusPaid,
		Timestamp: time.Date(2026, time.July, 1, 4, 0, 0, 0, time.UTC),
		Payload:   json.RawMessage(`{"source":"test"}`),
	}

	result, err := repo.ProcessEventTx(context.Background(), event)
	if err != nil {
		t.Fatalf("ProcessEventTx() error = %v", err)
	}

	if result.Classification != ClassificationDuplicateEvent {
		t.Errorf(
			"Classification = %q, want %q",
			result.Classification,
			ClassificationDuplicateEvent,
		)
	}
	if !result.Duplicate {
		t.Error("Duplicate = false, want true")
	}
	if !tx.committed {
		t.Error("transaction was not committed")
	}
	if len(tx.operations) != 2 ||
		tx.operations[0] != "lock" ||
		tx.operations[1] != "insert_processed" {
		t.Errorf(
			"operations = %v, want [lock insert_processed]",
			tx.operations,
		)
	}
}

func TestProcessEventTx_RoutesNewOrderEvents(t *testing.T) {
	tests := []struct {
		name                string
		status              domain.Status
		classification      string
		wantApplied         bool
		wantPending         bool
		wantDeadLetter      bool
		wantOrderInserts    int
		wantPendingInserts  int
		wantDeadLetterCount int
		wantHistoryCount    int
	}{
		{
			name:             "created is applied",
			status:           domain.StatusCreated,
			classification:   domain.ClassificationApplied,
			wantApplied:      true,
			wantOrderInserts: 1,
			wantHistoryCount: 1,
		},
		{
			name:               "paid is pending",
			status:             domain.StatusPaid,
			classification:     domain.ClassificationOutOfOrder,
			wantPending:        true,
			wantPendingInserts: 1,
		},
		{
			name:                "cancelled is dead lettered",
			status:              domain.StatusCancelled,
			classification:      domain.ClassificationInvalidTransition,
			wantDeadLetter:      true,
			wantDeadLetterCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := &fakeTransaction{processedInsertRows: 1}
			repo := &PostgresOrderRepository{
				begin: func(context.Context) (dbTransaction, error) {
					return tx, nil
				},
			}
			event := domain.OrderEvent{
				EventID:   "event-123",
				OrderID:   "order-123",
				Status:    tt.status,
				Timestamp: time.Date(2026, time.July, 1, 4, 0, 0, 0, time.UTC),
				Payload:   json.RawMessage(`{"source":"test"}`),
			}

			result, err := repo.ProcessEventTx(context.Background(), event)
			if err != nil {
				t.Fatalf("ProcessEventTx() error = %v", err)
			}

			if result.Classification != tt.classification {
				t.Errorf(
					"Classification = %q, want %q",
					result.Classification,
					tt.classification,
				)
			}
			if result.Applied != tt.wantApplied {
				t.Errorf("Applied = %v, want %v", result.Applied, tt.wantApplied)
			}
			if result.Pending != tt.wantPending {
				t.Errorf("Pending = %v, want %v", result.Pending, tt.wantPending)
			}
			if result.DeadLetter != tt.wantDeadLetter {
				t.Errorf(
					"DeadLetter = %v, want %v",
					result.DeadLetter,
					tt.wantDeadLetter,
				)
			}
			if tx.insertedOrderCount != tt.wantOrderInserts {
				t.Errorf(
					"order inserts = %d, want %d",
					tx.insertedOrderCount,
					tt.wantOrderInserts,
				)
			}
			if tx.pendingInsertCount != tt.wantPendingInserts {
				t.Errorf(
					"pending inserts = %d, want %d",
					tx.pendingInsertCount,
					tt.wantPendingInserts,
				)
			}
			if tx.deadLetterCount != tt.wantDeadLetterCount {
				t.Errorf(
					"dead-letter inserts = %d, want %d",
					tx.deadLetterCount,
					tt.wantDeadLetterCount,
				)
			}
			if tx.historyCount != tt.wantHistoryCount {
				t.Errorf(
					"history inserts = %d, want %d",
					tx.historyCount,
					tt.wantHistoryCount,
				)
			}
			if tx.outcomes[event.EventID] != tt.classification {
				t.Errorf(
					"outcome = %q, want %q",
					tx.outcomes[event.EventID],
					tt.classification,
				)
			}
			if !tx.committed {
				t.Error("transaction was not committed")
			}
		})
	}
}

func TestReplayPending_AppliesContiguousTransitionsInTimestampOrder(t *testing.T) {
	orderID := "order-123"
	tx := &fakeTransaction{
		pending: []domain.OrderEvent{
			{
				EventID:   "event-paid",
				OrderID:   orderID,
				Status:    domain.StatusPaid,
				Timestamp: time.Date(2026, time.July, 1, 4, 1, 0, 0, time.UTC),
				Payload:   json.RawMessage(`{"step":1}`),
			},
			{
				EventID:   "event-shipped",
				OrderID:   orderID,
				Status:    domain.StatusShipped,
				Timestamp: time.Date(2026, time.July, 1, 4, 2, 0, 0, time.UTC),
				Payload:   json.RawMessage(`{"step":2}`),
			},
		},
	}

	replayed, err := replayPending(
		context.Background(),
		tx,
		orderID,
		domain.StatusCreated,
	)
	if err != nil {
		t.Fatalf("replayPending() error = %v", err)
	}

	if replayed != 2 {
		t.Errorf("replayed = %d, want 2", replayed)
	}
	if len(tx.pending) != 0 {
		t.Errorf("pending events = %d, want 0", len(tx.pending))
	}
	if tx.historyCount != 2 {
		t.Errorf("history inserts = %d, want 2", tx.historyCount)
	}
	if len(tx.updatedStatuses) != 2 ||
		tx.updatedStatuses[0] != domain.StatusPaid ||
		tx.updatedStatuses[1] != domain.StatusShipped {
		t.Errorf(
			"updated statuses = %v, want [PAID SHIPPED]",
			tx.updatedStatuses,
		)
	}
	if tx.outcomes["event-paid"] != domain.ClassificationApplied {
		t.Errorf(
			"paid outcome = %q, want %q",
			tx.outcomes["event-paid"],
			domain.ClassificationApplied,
		)
	}
	if tx.outcomes["event-shipped"] != domain.ClassificationApplied {
		t.Errorf(
			"shipped outcome = %q, want %q",
			tx.outcomes["event-shipped"],
			domain.ClassificationApplied,
		)
	}
}

func TestReplayPending_StopsAtRecoverableGap(t *testing.T) {
	tx := &fakeTransaction{
		pending: []domain.OrderEvent{
			{
				EventID:   "event-shipped",
				OrderID:   "order-123",
				Status:    domain.StatusShipped,
				Timestamp: time.Date(2026, time.July, 1, 4, 2, 0, 0, time.UTC),
				Payload:   json.RawMessage(`{}`),
			},
		},
	}

	replayed, err := replayPending(
		context.Background(),
		tx,
		"order-123",
		domain.StatusCreated,
	)
	if err != nil {
		t.Fatalf("replayPending() error = %v", err)
	}

	if replayed != 0 {
		t.Errorf("replayed = %d, want 0", replayed)
	}
	if len(tx.pending) != 1 {
		t.Errorf("pending events = %d, want 1", len(tx.pending))
	}
	if tx.historyCount != 0 {
		t.Errorf("history inserts = %d, want 0", tx.historyCount)
	}
}
