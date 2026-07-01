package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/domain"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/repository"
)

type fakeOrderRepository struct {
	result repository.ProcessResult
	err    error
	got    domain.OrderEvent
	calls  int
}

func (f *fakeOrderRepository) ProcessEventTx(
	_ context.Context,
	event domain.OrderEvent,
) (repository.ProcessResult, error) {
	f.calls++
	f.got = event
	return f.result, f.err
}

func TestProcessor_Process(t *testing.T) {
	event := domain.OrderEvent{
		EventID:   "event-123",
		OrderID:   "order-456",
		Status:    domain.StatusCreated,
		Timestamp: time.Date(2026, time.July, 1, 3, 0, 0, 0, time.UTC),
		Payload:   []byte(`{"source":"test"}`),
	}

	tests := []struct {
		name   string
		result repository.ProcessResult
	}{
		{
			name: "applied transition with pending replay",
			result: repository.ProcessResult{
				EventID:         event.EventID,
				OrderID:         event.OrderID,
				Classification:  domain.ClassificationApplied,
				Applied:         true,
				ReplayedPending: 2,
				Reason:          "transition applied",
			},
		},
		{
			name: "duplicate event",
			result: repository.ProcessResult{
				EventID:        event.EventID,
				OrderID:        event.OrderID,
				Classification: repository.ClassificationDuplicateEvent,
				Duplicate:      true,
				Reason:         "event ID has already been processed",
			},
		},
		{
			name: "duplicate status",
			result: repository.ProcessResult{
				EventID:        event.EventID,
				OrderID:        event.OrderID,
				Classification: domain.ClassificationDuplicateStatus,
				Duplicate:      true,
				Reason:         "order already has the requested status",
			},
		},
		{
			name: "out of order event is pending",
			result: repository.ProcessResult{
				EventID:        event.EventID,
				OrderID:        event.OrderID,
				Classification: domain.ClassificationOutOfOrder,
				Pending:        true,
				Reason:         "earlier transition is missing",
			},
		},
		{
			name: "invalid transition is dead lettered",
			result: repository.ProcessResult{
				EventID:        event.EventID,
				OrderID:        event.OrderID,
				Classification: domain.ClassificationInvalidTransition,
				DeadLetter:     true,
				Reason:         "transition is not allowed",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeOrderRepository{result: tt.result}
			processor := NewProcessor(fake)

			got, err := processor.Process(context.Background(), event)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			if got != tt.result {
				t.Errorf("Process() result = %+v, want %+v", got, tt.result)
			}
			if fake.calls != 1 {
				t.Errorf("repository calls = %d, want 1", fake.calls)
			}
			if fake.got.EventID != event.EventID {
				t.Errorf(
					"repository event ID = %q, want %q",
					fake.got.EventID,
					event.EventID,
				)
			}
		})
	}
}

func TestProcessor_ProcessError(t *testing.T) {
	repositoryError := errors.New("database unavailable")
	fake := &fakeOrderRepository{err: repositoryError}
	processor := NewProcessor(fake)

	_, err := processor.Process(context.Background(), domain.OrderEvent{
		EventID: "event-123",
	})
	if !errors.Is(err, repositoryError) {
		t.Fatalf("Process() error = %v, want wrapped repository error", err)
	}
}
