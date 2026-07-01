package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/domain"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/repository"
)

type fakeDepthProvider struct {
	queueDepth   int64
	streamLength int64
	pendingDepth int64
	queueErr     error
	streamErr    error
	pendingErr   error
}

func (f fakeDepthProvider) QueueDepth(context.Context) (int64, error) {
	return f.queueDepth, f.queueErr
}

func (f fakeDepthProvider) StreamLength(context.Context) (int64, error) {
	return f.streamLength, f.streamErr
}

func (f fakeDepthProvider) PendingCount(context.Context) (int64, error) {
	return f.pendingDepth, f.pendingErr
}

func TestCollector_RecordsProcessorOutcomes(t *testing.T) {
	startedAt := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	collector := newCollectorAt(startedAt)

	collector.RecordResult(repository.ProcessResult{
		Classification:  domain.ClassificationApplied,
		Applied:         true,
		ReplayedPending: 2,
	})
	collector.RecordResult(repository.ProcessResult{
		Classification: repository.ClassificationDuplicateEvent,
		Duplicate:      true,
	})
	collector.RecordResult(repository.ProcessResult{
		Classification: domain.ClassificationDuplicateStatus,
		Duplicate:      true,
	})
	collector.RecordResult(repository.ProcessResult{
		Classification: domain.ClassificationOutOfOrder,
		Pending:        true,
	})
	collector.RecordResult(repository.ProcessResult{
		Classification: domain.ClassificationInvalidTransition,
		DeadLetter:     true,
	})
	collector.RecordFailure()
	collector.RecordRecovered()

	got := collector.snapshotAt(startedAt.Add(10 * time.Second))

	if got.Processed != 5 || got.Applied != 1 {
		t.Errorf(
			"processed/applied = %d/%d, want 5/1",
			got.Processed,
			got.Applied,
		)
	}
	if got.DuplicatesSkipped != 1 {
		t.Errorf("DuplicatesSkipped = %d, want 1", got.DuplicatesSkipped)
	}
	if got.DuplicateStatus != 1 {
		t.Errorf("DuplicateStatus = %d, want 1", got.DuplicateStatus)
	}
	if got.OutOfOrder != 1 {
		t.Errorf("OutOfOrder = %d, want 1", got.OutOfOrder)
	}
	if got.InvalidTransitions != 1 || got.DeadLetters != 1 {
		t.Errorf(
			"invalid/dead letters = %d/%d, want 1/1",
			got.InvalidTransitions,
			got.DeadLetters,
		)
	}
	if got.Failures != 1 {
		t.Errorf("Failures = %d, want 1", got.Failures)
	}
	if got.RecoveredMessages != 1 {
		t.Errorf("RecoveredMessages = %d, want 1", got.RecoveredMessages)
	}
	if got.PendingReplayed != 2 {
		t.Errorf("PendingReplayed = %d, want 2", got.PendingReplayed)
	}
	if got.UptimeSeconds != 10 {
		t.Errorf("UptimeSeconds = %v, want 10", got.UptimeSeconds)
	}
	if got.ThroughputPerSecond != 0.5 {
		t.Errorf(
			"ThroughputPerSecond = %v, want 0.5",
			got.ThroughputPerSecond,
		)
	}
}

func TestCollector_FailureDoesNotIncrementProcessed(t *testing.T) {
	collector := NewCollector()

	collector.RecordFailure()
	got := collector.Snapshot()

	if got.Failures != 1 {
		t.Errorf("Failures = %d, want 1", got.Failures)
	}
	if got.Processed != 0 {
		t.Errorf("Processed = %d, want 0", got.Processed)
	}
}

func TestCollector_ConcurrentIncrements(t *testing.T) {
	collector := NewCollector()
	const goroutines = 50
	const increments = 100

	var wait sync.WaitGroup
	wait.Add(goroutines)
	for range goroutines {
		go func() {
			defer wait.Done()
			for range increments {
				collector.RecordFailure()
			}
		}()
	}
	wait.Wait()

	if got := collector.Snapshot().Failures; got != goroutines*increments {
		t.Errorf("Failures = %d, want %d", got, goroutines*increments)
	}
}

func TestStatsHandler_ReturnsJSONSnapshot(t *testing.T) {
	collector := NewCollector()
	collector.RecordResult(repository.ProcessResult{
		Classification: domain.ClassificationApplied,
		Applied:        true,
	})
	handler := NewStatsHandler(
		collector,
		fakeDepthProvider{
			queueDepth:   23,
			streamLength: 123,
			pendingDepth: 7,
		},
		4,
	)
	request := httptest.NewRequest(http.MethodGet, "/stats", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}

	var got StatsResponse
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Processed != 1 || got.Applied != 1 {
		t.Errorf("processed/applied = %d/%d, want 1/1", got.Processed, got.Applied)
	}
	if got.QueueDepth != 23 ||
		got.StreamLength != 123 ||
		got.PendingDepth != 7 {
		t.Errorf(
			"queue/stream/pending depth = %d/%d/%d, want 23/123/7",
			got.QueueDepth,
			got.StreamLength,
			got.PendingDepth,
		)
	}
	if got.WorkerCount != 4 {
		t.Errorf("WorkerCount = %d, want 4", got.WorkerCount)
	}
	if got.QueueError != "" {
		t.Errorf("QueueError = %q, want empty", got.QueueError)
	}
}

func TestStatsHandler_ReportsQueueErrorsWithoutFailing(t *testing.T) {
	handler := NewStatsHandler(
		NewCollector(),
		fakeDepthProvider{
			queueErr:   errors.New("Redis unavailable"),
			streamErr:  errors.New("Redis unavailable"),
			pendingErr: errors.New("consumer group unavailable"),
		},
		2,
	)
	request := httptest.NewRequest(http.MethodGet, "/stats", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	var got StatsResponse
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.QueueError == "" {
		t.Fatal("QueueError is empty, want Redis error details")
	}
}
