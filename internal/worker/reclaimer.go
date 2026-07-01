package worker

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/queue"
)

// PendingQueue is the Redis Streams behavior required by the reclaimer.
type PendingQueue interface {
	ClaimPending(
		ctx context.Context,
		consumerName string,
		minIdle time.Duration,
		count int64,
	) ([]queue.StreamMessage, error)
	Ack(ctx context.Context, redisID string) error
}

// ReclaimerConfig controls stale-message recovery.
type ReclaimerConfig struct {
	ConsumerName string
	Interval     time.Duration
	MinIdle      time.Duration
	Count        int64
}

// Reclaimer periodically processes stale Redis pending entries.
type Reclaimer struct {
	queue     PendingQueue
	processor EventProcessor
	config    ReclaimerConfig
	logger    *log.Logger
}

// NewReclaimer validates configuration and creates a pending-entry reclaimer.
func NewReclaimer(
	pendingQueue PendingQueue,
	processor EventProcessor,
	config ReclaimerConfig,
	logger *log.Logger,
) (*Reclaimer, error) {
	if pendingQueue == nil {
		return nil, errors.New("pending queue is required")
	}
	if processor == nil {
		return nil, errors.New("event processor is required")
	}
	if config.ConsumerName == "" {
		return nil, errors.New("reclaimer consumer name is required")
	}
	if config.Interval <= 0 {
		return nil, errors.New("reclaim interval must be greater than zero")
	}
	if config.MinIdle <= 0 {
		return nil, errors.New("minimum idle duration must be greater than zero")
	}
	if config.Count <= 0 {
		return nil, errors.New("reclaim count must be greater than zero")
	}
	if logger == nil {
		logger = log.Default()
	}

	return &Reclaimer{
		queue:     pendingQueue,
		processor: processor,
		config:    config,
		logger:    logger,
	}, nil
}

// Run reclaims immediately, then at each configured interval, until cancelled.
func (r *Reclaimer) Run(ctx context.Context) {
	r.logger.Printf(
		"reclaimer=%s status=started",
		r.config.ConsumerName,
	)
	defer r.logger.Printf(
		"reclaimer=%s status=stopped",
		r.config.ConsumerName,
	)

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			r.reclaim(ctx)
			timer.Reset(r.config.Interval)
		}
	}
}

func (r *Reclaimer) reclaim(ctx context.Context) {
	messages, err := r.queue.ClaimPending(
		ctx,
		r.config.ConsumerName,
		r.config.MinIdle,
		r.config.Count,
	)
	if err != nil {
		if ctx.Err() == nil && !errors.Is(err, context.Canceled) {
			r.logger.Printf(
				"reclaimer=%s classification=FAILURE operation=claim error=%q",
				r.config.ConsumerName,
				err,
			)
		}
		return
	}

	// XAUTOCLAIM does not include Redis delivery counts. Domain-invalid events
	// are committed to the application's dead-letter table by the processor.
	// Infrastructure failures remain unacknowledged and become eligible for a
	// later reclaim attempt after MinIdle.
	processingCtx := context.WithoutCancel(ctx)
	for _, message := range messages {
		r.handleMessage(processingCtx, message)
	}
}

func (r *Reclaimer) handleMessage(
	ctx context.Context,
	message queue.StreamMessage,
) {
	result, err := r.processor.Process(ctx, message.Event)
	if err != nil {
		r.logger.Printf(
			"reclaimer=%s redis_id=%s event_id=%s order_id=%s classification=FAILURE error=%q",
			r.config.ConsumerName,
			message.RedisID,
			message.Event.EventID,
			message.Event.OrderID,
			err,
		)
		return
	}

	// Processing success means the PostgreSQL transaction committed. Only now
	// is it safe to remove the entry from the Redis pending list.
	if err := r.queue.Ack(ctx, message.RedisID); err != nil {
		r.logger.Printf(
			"reclaimer=%s redis_id=%s event_id=%s order_id=%s classification=%s ack=failed error=%q",
			r.config.ConsumerName,
			message.RedisID,
			result.EventID,
			result.OrderID,
			result.Classification,
			err,
		)
		return
	}

	r.logger.Printf(
		"reclaimer=%s redis_id=%s event_id=%s order_id=%s classification=%s ack=success replayed_pending=%d",
		r.config.ConsumerName,
		message.RedisID,
		result.EventID,
		result.OrderID,
		result.Classification,
		result.ReplayedPending,
	)
}
