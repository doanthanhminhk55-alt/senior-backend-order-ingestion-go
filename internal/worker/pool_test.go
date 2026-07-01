package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"testing"
	"time"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/domain"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/monitoring"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/queue"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/repository"
)

type fakeQueue struct {
	read func(
		context.Context,
		string,
		int64,
		time.Duration,
	) ([]queue.StreamMessage, error)
	ack func(context.Context, string) error
}

func (f *fakeQueue) ReadGroup(
	ctx context.Context,
	consumerName string,
	count int64,
	block time.Duration,
) ([]queue.StreamMessage, error) {
	if f.read == nil {
		return nil, nil
	}
	return f.read(ctx, consumerName, count, block)
}

func (f *fakeQueue) Ack(ctx context.Context, redisID string) error {
	if f.ack == nil {
		return nil
	}
	return f.ack(ctx, redisID)
}

type fakeProcessor struct {
	process func(
		context.Context,
		domain.OrderEvent,
	) (repository.ProcessResult, error)
}

func (f *fakeProcessor) Process(
	ctx context.Context,
	event domain.OrderEvent,
) (repository.ProcessResult, error) {
	return f.process(ctx, event)
}

func TestHandleMessage_AcksOnlyAfterSuccessfulProcessing(t *testing.T) {
	var operations []string
	streamQueue := &fakeQueue{
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
	pool := newTestPool(t, streamQueue, processor, 1)
	metrics := monitoring.NewCollector()
	pool.metrics = metrics
	message := testMessage()

	pool.handleMessage(context.Background(), "consumer-1", message)

	want := []string{"process:event-123", "ack:1-0"}
	if !slices.Equal(operations, want) {
		t.Errorf("operations = %v, want %v", operations, want)
	}
	snapshot := metrics.Snapshot()
	if snapshot.Processed != 1 || snapshot.Applied != 1 {
		t.Errorf(
			"processed/applied metrics = %d/%d, want 1/1",
			snapshot.Processed,
			snapshot.Applied,
		)
	}
}

func TestHandleMessage_DoesNotAckFailedProcessing(t *testing.T) {
	ackCalls := 0
	streamQueue := &fakeQueue{
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
			return repository.ProcessResult{}, errors.New("database commit failed")
		},
	}
	pool := newTestPool(t, streamQueue, processor, 1)
	metrics := monitoring.NewCollector()
	pool.metrics = metrics

	pool.handleMessage(context.Background(), "consumer-1", testMessage())

	if ackCalls != 0 {
		t.Errorf("Ack() calls = %d, want 0", ackCalls)
	}
	if failures := metrics.Snapshot().Failures; failures != 1 {
		t.Errorf("Failures metric = %d, want 1", failures)
	}
}

func TestHandleMessage_AcksCommittedDuplicate(t *testing.T) {
	var ackedID string
	streamQueue := &fakeQueue{
		ack: func(_ context.Context, redisID string) error {
			ackedID = redisID
			return nil
		},
	}
	processor := &fakeProcessor{
		process: func(
			_ context.Context,
			event domain.OrderEvent,
		) (repository.ProcessResult, error) {
			return repository.ProcessResult{
				EventID:        event.EventID,
				OrderID:        event.OrderID,
				Classification: repository.ClassificationDuplicateEvent,
				Duplicate:      true,
			}, nil
		},
	}
	pool := newTestPool(t, streamQueue, processor, 1)
	metrics := monitoring.NewCollector()
	pool.metrics = metrics

	pool.handleMessage(context.Background(), "consumer-1", testMessage())

	if ackedID != "1-0" {
		t.Errorf("acked Redis ID = %q, want %q", ackedID, "1-0")
	}
	if duplicates := metrics.Snapshot().DuplicatesSkipped; duplicates != 1 {
		t.Errorf("DuplicatesSkipped metric = %d, want 1", duplicates)
	}
}

func TestHandleMessage_SimulatedAckFailureWindow(t *testing.T) {
	var acked []string
	streamQueue := &fakeQueue{
		ack: func(_ context.Context, redisID string) error {
			acked = append(acked, redisID)
			return nil
		},
	}
	processor := &fakeProcessor{
		process: func(
			_ context.Context,
			event domain.OrderEvent,
		) (repository.ProcessResult, error) {
			return repository.ProcessResult{
				EventID:        event.EventID,
				OrderID:        event.OrderID,
				Classification: domain.ClassificationApplied,
				Applied:        true,
			}, nil
		},
	}
	metrics := monitoring.NewCollector()
	pool, err := NewPool(
		streamQueue,
		processor,
		Config{
			WorkerCount:    1,
			ConsumerPrefix: "test-consumer",
			ReadCount:      1,
			Block:          time.Second,
			FailureSimulation: FailureSimulationConfig{
				After: 1,
				Count: 2,
			},
		},
		log.New(io.Discard, "", 0),
		metrics,
	)
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}

	for index := 1; index <= 4; index++ {
		message := testMessage()
		message.RedisID = fmt.Sprintf("%d-0", index)
		message.Event.EventID = fmt.Sprintf("event-%d", index)
		pool.handleMessage(context.Background(), "test-consumer-1", message)
	}

	wantAcked := []string{"1-0", "4-0"}
	if !slices.Equal(acked, wantAcked) {
		t.Errorf("acked IDs = %v, want %v", acked, wantAcked)
	}
	snapshot := metrics.Snapshot()
	if snapshot.SimulatedFailures != 2 {
		t.Errorf(
			"SimulatedFailures = %d, want 2",
			snapshot.SimulatedFailures,
		)
	}
	if snapshot.Processed != 4 {
		t.Errorf("Processed = %d, want 4", snapshot.Processed)
	}
	if snapshot.Failures != 0 {
		t.Errorf("Failures = %d, want 0", snapshot.Failures)
	}
}

func TestHandleMessage_DisabledSimulationAlwaysAcks(t *testing.T) {
	tests := []struct {
		name       string
		simulation FailureSimulationConfig
	}{
		{
			name:       "after is zero",
			simulation: FailureSimulationConfig{After: 0, Count: 2},
		},
		{
			name:       "count is zero",
			simulation: FailureSimulationConfig{After: 2, Count: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ackCalls := 0
			streamQueue := &fakeQueue{
				ack: func(context.Context, string) error {
					ackCalls++
					return nil
				},
			}
			processor := &fakeProcessor{
				process: func(
					_ context.Context,
					event domain.OrderEvent,
				) (repository.ProcessResult, error) {
					return repository.ProcessResult{
						EventID:        event.EventID,
						OrderID:        event.OrderID,
						Classification: domain.ClassificationApplied,
						Applied:        true,
					}, nil
				},
			}
			pool, err := NewPool(
				streamQueue,
				processor,
				Config{
					WorkerCount:       1,
					ConsumerPrefix:    "test-consumer",
					ReadCount:         1,
					Block:             time.Second,
					FailureSimulation: tt.simulation,
				},
				log.New(io.Discard, "", 0),
			)
			if err != nil {
				t.Fatalf("NewPool() error = %v", err)
			}

			pool.handleMessage(
				context.Background(),
				"test-consumer-1",
				testMessage(),
			)

			if ackCalls != 1 {
				t.Errorf("Ack() calls = %d, want 1", ackCalls)
			}
		})
	}
}

func TestPool_ContextCancellationStopsWorkers(t *testing.T) {
	consumers := make(chan string, 2)
	streamQueue := &fakeQueue{
		read: func(
			ctx context.Context,
			consumerName string,
			_ int64,
			_ time.Duration,
		) ([]queue.StreamMessage, error) {
			consumers <- consumerName
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	processor := &fakeProcessor{
		process: func(
			context.Context,
			domain.OrderEvent,
		) (repository.ProcessResult, error) {
			t.Fatal("processor should not be called")
			return repository.ProcessResult{}, nil
		},
	}
	pool := newTestPool(t, streamQueue, processor, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()

	gotConsumers := []string{
		receiveString(t, consumers),
		receiveString(t, consumers),
	}
	slices.Sort(gotConsumers)
	wantConsumers := []string{"test-consumer-1", "test-consumer-2"}
	if !slices.Equal(gotConsumers, wantConsumers) {
		t.Errorf("consumer names = %v, want %v", gotConsumers, wantConsumers)
	}

	cancel()
	waitForDone(t, done)
}

func TestPool_CancellationWaitsForInFlightMessage(t *testing.T) {
	messageSent := false
	streamQueue := &fakeQueue{
		read: func(
			ctx context.Context,
			_ string,
			_ int64,
			_ time.Duration,
		) ([]queue.StreamMessage, error) {
			if !messageSent {
				messageSent = true
				return []queue.StreamMessage{testMessage()}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	started := make(chan struct{})
	release := make(chan struct{})
	acked := make(chan struct{})
	processor := &fakeProcessor{
		process: func(
			ctx context.Context,
			event domain.OrderEvent,
		) (repository.ProcessResult, error) {
			close(started)
			<-release
			if ctx.Err() != nil {
				t.Errorf("in-flight processing context was cancelled: %v", ctx.Err())
			}
			return repository.ProcessResult{
				EventID:        event.EventID,
				OrderID:        event.OrderID,
				Classification: domain.ClassificationApplied,
				Applied:        true,
			}, nil
		},
	}
	streamQueue.ack = func(ctx context.Context, _ string) error {
		if ctx.Err() != nil {
			t.Errorf("acknowledgement context was cancelled: %v", ctx.Err())
		}
		close(acked)
		return nil
	}

	pool := newTestPool(t, streamQueue, processor, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()

	waitForSignal(t, started)
	cancel()

	select {
	case <-done:
		t.Fatal("pool stopped before in-flight processing completed")
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	waitForSignal(t, acked)
	waitForDone(t, done)
}

func newTestPool(
	t *testing.T,
	streamQueue StreamQueue,
	processor EventProcessor,
	workerCount int,
) *Pool {
	t.Helper()

	pool, err := NewPool(
		streamQueue,
		processor,
		Config{
			WorkerCount:    workerCount,
			ConsumerPrefix: "test-consumer",
			ReadCount:      10,
			Block:          time.Second,
		},
		log.New(io.Discard, "", 0),
	)
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}

	return pool
}

func testMessage() queue.StreamMessage {
	return queue.StreamMessage{
		RedisID: "1-0",
		Event: domain.OrderEvent{
			EventID: "event-123",
			OrderID: "order-456",
			Status:  domain.StatusCreated,
		},
	}
}

func receiveString(t *testing.T, values <-chan string) string {
	t.Helper()

	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for value")
		return ""
	}
}

func waitForSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func waitForDone(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for pool shutdown")
	}
}
