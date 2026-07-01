package worker

import (
	"context"
	"errors"
	"io"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/domain"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/monitoring"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/queue"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/repository"
)

type fakePendingQueue struct {
	claim func(
		context.Context,
		string,
		time.Duration,
		int64,
	) ([]queue.StreamMessage, error)
	ack func(context.Context, string) error
}

func (f *fakePendingQueue) ClaimPending(
	ctx context.Context,
	consumerName string,
	minIdle time.Duration,
	count int64,
) ([]queue.StreamMessage, error) {
	return f.claim(ctx, consumerName, minIdle, count)
}

func (f *fakePendingQueue) Ack(
	ctx context.Context,
	redisID string,
) error {
	if f.ack == nil {
		return nil
	}
	return f.ack(ctx, redisID)
}

func TestReclaimer_SuccessfulProcessingAcksClaimedMessage(t *testing.T) {
	var operations []string
	pendingQueue := &fakePendingQueue{
		claim: func(
			context.Context,
			string,
			time.Duration,
			int64,
		) ([]queue.StreamMessage, error) {
			return []queue.StreamMessage{testMessage()}, nil
		},
		ack: func(_ context.Context, redisID string) error {
			operations = append(operations, "ack:"+redisID)
			return nil
		},
	}
	processor := &fakeProcessor{
		process: func(
			_ context.Context,
			event domain.OrderEvent,
		) (repository.ProcessResult, error) {
			operations = append(operations, "process:"+event.EventID)
			return repository.ProcessResult{
				EventID:        event.EventID,
				OrderID:        event.OrderID,
				Classification: domain.ClassificationApplied,
				Applied:        true,
			}, nil
		},
	}
	reclaimer := newTestReclaimer(t, pendingQueue, processor)
	metrics := monitoring.NewCollector()
	reclaimer.metrics = metrics

	reclaimer.reclaim(context.Background())

	if len(operations) != 2 ||
		operations[0] != "process:event-123" ||
		operations[1] != "ack:1-0" {
		t.Errorf(
			"operations = %v, want [process:event-123 ack:1-0]",
			operations,
		)
	}
	snapshot := metrics.Snapshot()
	if snapshot.RecoveredMessages != 1 {
		t.Errorf(
			"RecoveredMessages metric = %d, want 1",
			snapshot.RecoveredMessages,
		)
	}
	if snapshot.Applied != 1 {
		t.Errorf("Applied metric = %d, want 1", snapshot.Applied)
	}
}

func TestReclaimer_FailedProcessingDoesNotAck(t *testing.T) {
	ackCalls := 0
	pendingQueue := &fakePendingQueue{
		claim: func(
			context.Context,
			string,
			time.Duration,
			int64,
		) ([]queue.StreamMessage, error) {
			return []queue.StreamMessage{testMessage()}, nil
		},
		ack: func(context.Context, string) error {
			ackCalls++
			return nil
		},
	}
	processor := &fakeProcessor{
		process: func(
			context.Context,
			domain.OrderEvent,
		) (repository.ProcessResult, error) {
			return repository.ProcessResult{}, errors.New("database unavailable")
		},
	}
	reclaimer := newTestReclaimer(t, pendingQueue, processor)
	metrics := monitoring.NewCollector()
	reclaimer.metrics = metrics

	reclaimer.reclaim(context.Background())

	if ackCalls != 0 {
		t.Errorf("Ack() calls = %d, want 0", ackCalls)
	}
	if failures := metrics.Snapshot().Failures; failures != 1 {
		t.Errorf("Failures metric = %d, want 1", failures)
	}
}

func TestReclaimer_ContextCancellationStopsRun(t *testing.T) {
	claimStarted := make(chan struct{})
	pendingQueue := &fakePendingQueue{
		claim: func(
			ctx context.Context,
			_ string,
			_ time.Duration,
			_ int64,
		) ([]queue.StreamMessage, error) {
			close(claimStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	processor := &fakeProcessor{
		process: func(
			context.Context,
			domain.OrderEvent,
		) (repository.ProcessResult, error) {
			return repository.ProcessResult{}, nil
		},
	}
	reclaimer := newTestReclaimer(t, pendingQueue, processor)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		reclaimer.Run(ctx)
		close(done)
	}()

	waitForSignal(t, claimStarted)
	cancel()
	waitForDone(t, done)
}

func TestReclaimer_ClaimErrorDoesNotStopRun(t *testing.T) {
	var claimCalls atomic.Int32
	retried := make(chan struct{})
	pendingQueue := &fakePendingQueue{
		claim: func(
			context.Context,
			string,
			time.Duration,
			int64,
		) ([]queue.StreamMessage, error) {
			call := claimCalls.Add(1)
			if call == 1 {
				return nil, errors.New(
					`parse Redis stream message "1-0": malformed payload`,
				)
			}
			if call == 2 {
				close(retried)
			}
			return nil, nil
		},
	}
	processor := &fakeProcessor{
		process: func(
			context.Context,
			domain.OrderEvent,
		) (repository.ProcessResult, error) {
			return repository.ProcessResult{}, nil
		},
	}
	reclaimer := newTestReclaimer(t, pendingQueue, processor)
	reclaimer.config.Interval = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		reclaimer.Run(ctx)
		close(done)
	}()

	waitForSignal(t, retried)
	cancel()
	waitForDone(t, done)

	if claimCalls.Load() < 2 {
		t.Errorf("ClaimPending() calls = %d, want at least 2", claimCalls.Load())
	}
}

func newTestReclaimer(
	t *testing.T,
	pendingQueue PendingQueue,
	processor EventProcessor,
) *Reclaimer {
	t.Helper()

	reclaimer, err := NewReclaimer(
		pendingQueue,
		processor,
		ReclaimerConfig{
			ConsumerName: "test-reclaimer",
			Interval:     time.Hour,
			MinIdle:      time.Minute,
			Count:        10,
		},
		log.New(io.Discard, "", 0),
	)
	if err != nil {
		t.Fatalf("NewReclaimer() error = %v", err)
	}

	return reclaimer
}
