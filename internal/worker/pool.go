package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/domain"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/monitoring"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/queue"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/repository"
)

const defaultReadErrorBackoff = 250 * time.Millisecond

// StreamQueue is the Redis Streams behavior required by the worker pool.
type StreamQueue interface {
	ReadGroup(
		ctx context.Context,
		consumerName string,
		count int64,
		block time.Duration,
	) ([]queue.StreamMessage, error)
	Ack(ctx context.Context, redisID string) error
}

// EventProcessor commits one order event to durable storage.
type EventProcessor interface {
	Process(
		ctx context.Context,
		event domain.OrderEvent,
	) (repository.ProcessResult, error)
}

// Config controls worker count and Redis consumer reads.
type Config struct {
	WorkerCount      int
	ConsumerPrefix   string
	ReadCount        int64
	Block            time.Duration
	ReadErrorBackoff time.Duration
}

// Pool reads and processes Redis Stream messages with bounded concurrency.
type Pool struct {
	queue     StreamQueue
	processor EventProcessor
	config    Config
	logger    *log.Logger
	metrics   *monitoring.Collector
}

// NewPool validates configuration and creates a worker pool.
func NewPool(
	streamQueue StreamQueue,
	processor EventProcessor,
	config Config,
	logger *log.Logger,
	collectors ...*monitoring.Collector,
) (*Pool, error) {
	if streamQueue == nil {
		return nil, errors.New("stream queue is required")
	}
	if processor == nil {
		return nil, errors.New("event processor is required")
	}
	if config.WorkerCount <= 0 {
		return nil, errors.New("worker count must be greater than zero")
	}
	if config.ConsumerPrefix == "" {
		return nil, errors.New("consumer prefix is required")
	}
	if config.ReadCount <= 0 {
		return nil, errors.New("read count must be greater than zero")
	}
	if config.Block < 0 {
		return nil, errors.New("read block duration cannot be negative")
	}
	if config.ReadErrorBackoff <= 0 {
		config.ReadErrorBackoff = defaultReadErrorBackoff
	}
	if logger == nil {
		logger = log.Default()
	}
	var metrics *monitoring.Collector
	if len(collectors) > 0 {
		metrics = collectors[0]
	}

	return &Pool{
		queue:     streamQueue,
		processor: processor,
		config:    config,
		logger:    logger,
		metrics:   metrics,
	}, nil
}

// Run starts the configured workers and blocks until all workers have stopped.
func (p *Pool) Run(ctx context.Context) {
	var workers sync.WaitGroup
	workers.Add(p.config.WorkerCount)

	for index := 0; index < p.config.WorkerCount; index++ {
		consumerName := fmt.Sprintf(
			"%s-%d",
			p.config.ConsumerPrefix,
			index+1,
		)
		go func() {
			defer workers.Done()
			p.runWorker(ctx, consumerName)
		}()
	}

	workers.Wait()
}

func (p *Pool) runWorker(ctx context.Context, consumerName string) {
	p.logger.Printf("worker=%s status=started", consumerName)
	defer p.logger.Printf("worker=%s status=stopped", consumerName)

	for {
		if ctx.Err() != nil {
			return
		}

		messages, err := p.queue.ReadGroup(
			ctx,
			consumerName,
			p.config.ReadCount,
			p.config.Block,
		)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			p.logger.Printf(
				"worker=%s classification=FAILURE operation=read error=%q",
				consumerName,
				err,
			)
			if !waitForRetry(ctx, p.config.ReadErrorBackoff) {
				return
			}
			continue
		}

		// Messages returned by this read are already assigned to the consumer.
		// Finish the batch even if shutdown begins, and do not start another
		// read. WithoutCancel preserves context values while allowing the DB
		// transaction and the post-commit XACK to finish gracefully.
		processingCtx := context.WithoutCancel(ctx)
		for _, message := range messages {
			p.handleMessage(processingCtx, consumerName, message)
		}
	}
}

func (p *Pool) handleMessage(
	ctx context.Context,
	consumerName string,
	message queue.StreamMessage,
) {
	result, err := p.processor.Process(ctx, message.Event)
	if err != nil {
		if p.metrics != nil {
			p.metrics.RecordFailure()
		}
		p.logger.Printf(
			"worker=%s redis_id=%s event_id=%s order_id=%s classification=FAILURE error=%q",
			consumerName,
			message.RedisID,
			message.Event.EventID,
			message.Event.OrderID,
			err,
		)
		return
	}
	if p.metrics != nil {
		p.metrics.RecordResult(result)
	}

	// Process returns success only after the PostgreSQL transaction commits.
	// XACK must remain after this point so a failed transaction stays pending.
	if err := p.queue.Ack(ctx, message.RedisID); err != nil {
		p.logger.Printf(
			"worker=%s redis_id=%s event_id=%s order_id=%s classification=%s ack=failed error=%q",
			consumerName,
			message.RedisID,
			result.EventID,
			result.OrderID,
			result.Classification,
			err,
		)
		return
	}

	p.logger.Printf(
		"worker=%s redis_id=%s event_id=%s order_id=%s classification=%s ack=success replayed_pending=%d",
		consumerName,
		message.RedisID,
		result.EventID,
		result.OrderID,
		result.Classification,
		result.ReplayedPending,
	)
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
