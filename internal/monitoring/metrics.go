package monitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/domain"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/repository"
)

// Collector stores process-local ingestion counters safely across goroutines.
type Collector struct {
	startedAt          time.Time
	processed          atomic.Uint64
	applied            atomic.Uint64
	duplicatesSkipped  atomic.Uint64
	duplicateStatus    atomic.Uint64
	outOfOrder         atomic.Uint64
	invalidTransitions atomic.Uint64
	deadLetters        atomic.Uint64
	failures           atomic.Uint64
	recoveredMessages  atomic.Uint64
	pendingReplayed    atomic.Uint64
}

// Snapshot is a point-in-time view of ingestion metrics.
type Snapshot struct {
	StartedAt           time.Time `json:"started_at"`
	UptimeSeconds       float64   `json:"uptime_seconds"`
	Processed           uint64    `json:"processed"`
	Applied             uint64    `json:"applied"`
	DuplicatesSkipped   uint64    `json:"duplicates_skipped"`
	DuplicateStatus     uint64    `json:"duplicate_status"`
	OutOfOrder          uint64    `json:"out_of_order"`
	InvalidTransitions  uint64    `json:"invalid_transitions"`
	DeadLetters         uint64    `json:"dead_letters"`
	Failures            uint64    `json:"failures"`
	RecoveredMessages   uint64    `json:"recovered_messages"`
	PendingReplayed     uint64    `json:"pending_replayed"`
	ThroughputPerSecond float64   `json:"throughput_per_second"`
}

// StatsResponse combines process metrics with live Redis queue depths.
type StatsResponse struct {
	Snapshot
	QueueDepth   int64  `json:"queue_depth"`
	PendingDepth int64  `json:"pending_depth"`
	WorkerCount  int    `json:"worker_count"`
	QueueError   string `json:"queue_error,omitempty"`
}

// QueueDepthProvider supplies live Redis Stream depth information.
type QueueDepthProvider interface {
	QueueDepth(ctx context.Context) (int64, error)
	PendingCount(ctx context.Context) (int64, error)
}

// NewCollector creates an empty metrics collector starting now.
func NewCollector() *Collector {
	return newCollectorAt(time.Now().UTC())
}

func newCollectorAt(startedAt time.Time) *Collector {
	return &Collector{startedAt: startedAt.UTC()}
}

// RecordResult records one successfully committed processor outcome.
func (c *Collector) RecordResult(result repository.ProcessResult) {
	if result.ReplayedPending > 0 {
		c.pendingReplayed.Add(uint64(result.ReplayedPending))
	}

	switch result.Classification {
	case domain.ClassificationApplied:
		c.processed.Add(1)
		c.applied.Add(1)
	case repository.ClassificationDuplicateEvent:
		c.duplicatesSkipped.Add(1)
	case domain.ClassificationDuplicateStatus:
		c.duplicateStatus.Add(1)
	case domain.ClassificationOutOfOrder:
		c.outOfOrder.Add(1)
	case domain.ClassificationInvalidTransition:
		c.invalidTransitions.Add(1)
		if result.DeadLetter {
			c.deadLetters.Add(1)
		}
	}
}

// RecordFailure records a processor failure that remains unacknowledged.
func (c *Collector) RecordFailure() {
	c.failures.Add(1)
}

// RecordRecovered records a stale Redis pending message processed successfully.
func (c *Collector) RecordRecovered() {
	c.recoveredMessages.Add(1)
}

// Snapshot returns a consistent-enough point-in-time atomic metrics view.
func (c *Collector) Snapshot() Snapshot {
	return c.snapshotAt(time.Now().UTC())
}

func (c *Collector) snapshotAt(now time.Time) Snapshot {
	uptime := now.Sub(c.startedAt).Seconds()
	if uptime < 0 {
		uptime = 0
	}

	processed := c.processed.Load()
	throughput := 0.0
	if uptime > 0 {
		throughput = float64(processed) / uptime
	}

	return Snapshot{
		StartedAt:           c.startedAt,
		UptimeSeconds:       uptime,
		Processed:           processed,
		Applied:             c.applied.Load(),
		DuplicatesSkipped:   c.duplicatesSkipped.Load(),
		DuplicateStatus:     c.duplicateStatus.Load(),
		OutOfOrder:          c.outOfOrder.Load(),
		InvalidTransitions:  c.invalidTransitions.Load(),
		DeadLetters:         c.deadLetters.Load(),
		Failures:            c.failures.Load(),
		RecoveredMessages:   c.recoveredMessages.Load(),
		PendingReplayed:     c.pendingReplayed.Load(),
		ThroughputPerSecond: throughput,
	}
}

// NewStatsHandler creates the live GET /stats HTTP handler.
func NewStatsHandler(
	collector *Collector,
	queue QueueDepthProvider,
	workerCount int,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		response := StatsResponse{
			Snapshot:    collector.Snapshot(),
			WorkerCount: workerCount,
		}
		var queueErrors []string

		queueDepth, err := queue.QueueDepth(r.Context())
		if err != nil {
			queueErrors = append(
				queueErrors,
				fmt.Sprintf("queue_depth: %v", err),
			)
		} else {
			response.QueueDepth = queueDepth
		}

		pendingDepth, err := queue.PendingCount(r.Context())
		if err != nil {
			queueErrors = append(
				queueErrors,
				fmt.Sprintf("pending_depth: %v", err),
			)
		} else {
			response.PendingDepth = pendingDepth
		}

		response.QueueError = strings.Join(queueErrors, "; ")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(
				w,
				"encode stats response",
				http.StatusInternalServerError,
			)
		}
	})
}
