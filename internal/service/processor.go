package service

import (
	"context"
	"fmt"

	"github.com/example/senior-backend-order-ingestion-go/internal/domain"
	"github.com/example/senior-backend-order-ingestion-go/internal/repository"
)

// Processor coordinates durable processing of individual order events.
// Redis acknowledgement intentionally belongs to a future worker after this
// processor returns a successful, committed result.
type Processor struct {
	orders repository.OrderRepository
}

// NewProcessor creates an order event processor.
func NewProcessor(orders repository.OrderRepository) *Processor {
	return &Processor{orders: orders}
}

// Process persists one event through the repository transaction boundary.
func (p *Processor) Process(
	ctx context.Context,
	event domain.OrderEvent,
) (repository.ProcessResult, error) {
	result, err := p.orders.ProcessEventTx(ctx, event)
	if err != nil {
		return repository.ProcessResult{}, fmt.Errorf(
			"process order event %q: %w",
			event.EventID,
			err,
		)
	}

	return result, nil
}
