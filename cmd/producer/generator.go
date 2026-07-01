package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"time"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/domain"
)

var deterministicEpoch = time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)

type producerConfig struct {
	Total           int
	DuplicateRatio  float64
	OutOfOrderRatio float64
	InvalidRatio    float64
	RedisAddr       string
	Stream          string
	Group           string
	Seed            int64
	BatchSize       int
}

type generatedEvent struct {
	Event      domain.OrderEvent
	Duplicate  bool
	OutOfOrder bool
	Invalid    bool
}

type producerSummary struct {
	TotalRequested          int
	TotalPublished          int
	UniqueEventIDs          int
	DuplicateEvents         int
	UniqueOrderIDs          int
	OutOfOrderEvents        int
	InvalidTransitionEvents int
	Seed                    int64
	RedisStream             string
}

type eventPublisher interface {
	Publish(
		ctx context.Context,
		event domain.OrderEvent,
	) (string, error)
}

type eventGenerator struct {
	config              producerConfig
	random              *rand.Rand
	baseTime            time.Time
	eventSequence       int
	orderSequence       int
	timeSequence        int
	duplicatesRemaining int
	summary             producerSummary
}

func (c producerConfig) validate() error {
	if c.Total <= 0 {
		return errors.New("total must be greater than zero")
	}
	if c.BatchSize <= 0 {
		return errors.New("batch size must be greater than zero")
	}
	if c.RedisAddr == "" {
		return errors.New("Redis address is required")
	}
	if c.Stream == "" {
		return errors.New("Redis stream is required")
	}
	if c.Group == "" {
		return errors.New("Redis consumer group is required")
	}

	ratios := []struct {
		name  string
		value float64
	}{
		{name: "duplicate ratio", value: c.DuplicateRatio},
		{name: "out-of-order ratio", value: c.OutOfOrderRatio},
		{name: "invalid ratio", value: c.InvalidRatio},
	}
	for _, ratio := range ratios {
		if math.IsNaN(ratio.value) ||
			math.IsInf(ratio.value, 0) ||
			ratio.value < 0 ||
			ratio.value > 1 {
			return fmt.Errorf("%s must be between 0 and 1", ratio.name)
		}
	}

	duplicateCount := ratioCount(c.Total, c.DuplicateRatio)
	uniqueCount := c.Total - duplicateCount
	if duplicateCount > 0 && uniqueCount == 0 {
		return errors.New("duplicate ratio leaves no source event to duplicate")
	}

	outOfOrderCount := ratioCount(c.Total, c.OutOfOrderRatio)
	invalidCount := ratioCount(c.Total, c.InvalidRatio)
	requiredUnique := outOfOrderScenarioSize(outOfOrderCount) +
		(4 * invalidCount)
	if requiredUnique > uniqueCount {
		return fmt.Errorf(
			"ratios require %d unique lifecycle events but only %d fit in total",
			requiredUnique,
			uniqueCount,
		)
	}

	return nil
}

func generate(
	config producerConfig,
	emit func(generatedEvent) error,
) (producerSummary, error) {
	if err := config.validate(); err != nil {
		return producerSummary{}, err
	}

	duplicateCount := ratioCount(config.Total, config.DuplicateRatio)
	outOfOrderCount := ratioCount(config.Total, config.OutOfOrderRatio)
	invalidCount := ratioCount(config.Total, config.InvalidRatio)
	uniqueTarget := config.Total - duplicateCount

	generator := &eventGenerator{
		config:              config,
		random:              rand.New(rand.NewSource(config.Seed)),
		baseTime:            deterministicEpoch.Add(time.Duration(config.Seed) * time.Second),
		duplicatesRemaining: duplicateCount,
		summary: producerSummary{
			TotalRequested: config.Total,
			Seed:           config.Seed,
			RedisStream:    config.Stream,
		},
	}

	if err := generator.generateOutOfOrder(outOfOrderCount, emit); err != nil {
		return generator.summary, err
	}
	if err := generator.generateInvalid(invalidCount, emit); err != nil {
		return generator.summary, err
	}

	remainingUnique := uniqueTarget - generator.summary.UniqueEventIDs
	if err := generator.generateNormal(remainingUnique, emit); err != nil {
		return generator.summary, err
	}

	if generator.summary.TotalPublished != config.Total {
		return generator.summary, fmt.Errorf(
			"generated %d events, want %d",
			generator.summary.TotalPublished,
			config.Total,
		)
	}

	return generator.summary, nil
}

func (g *eventGenerator) generateOutOfOrder(
	count int,
	emit func(generatedEvent) error,
) error {
	for index := 0; index < count; index++ {
		orderID := g.nextOrderID()
		if index%2 == 0 {
			times := g.reserveTimes(2)
			created, err := g.newEvent(orderID, domain.StatusCreated, times[0])
			if err != nil {
				return err
			}
			paid, err := g.newEvent(orderID, domain.StatusPaid, times[1])
			if err != nil {
				return err
			}

			if err := g.emitUnique(
				generatedEvent{Event: paid, OutOfOrder: true},
				emit,
			); err != nil {
				return err
			}
			if err := g.emitUnique(generatedEvent{Event: created}, emit); err != nil {
				return err
			}
			continue
		}

		times := g.reserveTimes(3)
		created, err := g.newEvent(orderID, domain.StatusCreated, times[0])
		if err != nil {
			return err
		}
		paid, err := g.newEvent(orderID, domain.StatusPaid, times[1])
		if err != nil {
			return err
		}
		shipped, err := g.newEvent(orderID, domain.StatusShipped, times[2])
		if err != nil {
			return err
		}

		if err := g.emitUnique(
			generatedEvent{Event: shipped, OutOfOrder: true},
			emit,
		); err != nil {
			return err
		}
		if err := g.emitUnique(generatedEvent{Event: created}, emit); err != nil {
			return err
		}
		if err := g.emitUnique(generatedEvent{Event: paid}, emit); err != nil {
			return err
		}
	}

	return nil
}

func (g *eventGenerator) generateInvalid(
	count int,
	emit func(generatedEvent) error,
) error {
	for index := 0; index < count; index++ {
		orderID := g.nextOrderID()
		times := g.reserveTimes(4)
		statuses := []domain.Status{
			domain.StatusCreated,
			domain.StatusPaid,
			domain.StatusShipped,
			domain.StatusCancelled,
		}

		for statusIndex, status := range statuses {
			event, err := g.newEvent(orderID, status, times[statusIndex])
			if err != nil {
				return err
			}
			if err := g.emitUnique(
				generatedEvent{
					Event:   event,
					Invalid: status == domain.StatusCancelled,
				},
				emit,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *eventGenerator) generateNormal(
	count int,
	emit func(generatedEvent) error,
) error {
	lifecycles := [][]domain.Status{
		{domain.StatusCreated},
		{domain.StatusCreated, domain.StatusPaid},
		{domain.StatusCreated, domain.StatusCancelled},
		{domain.StatusCreated, domain.StatusPaid, domain.StatusShipped},
		{domain.StatusCreated, domain.StatusPaid, domain.StatusCancelled},
	}

	for count > 0 {
		orderID := g.nextOrderID()
		lifecycle := lifecycles[g.random.Intn(len(lifecycles))]
		if len(lifecycle) > count {
			lifecycle = lifecycle[:count]
		}
		times := g.reserveTimes(len(lifecycle))

		for index, status := range lifecycle {
			event, err := g.newEvent(orderID, status, times[index])
			if err != nil {
				return err
			}
			if err := g.emitUnique(generatedEvent{Event: event}, emit); err != nil {
				return err
			}
		}
		count -= len(lifecycle)
	}

	return nil
}

func (g *eventGenerator) emitUnique(
	generated generatedEvent,
	emit func(generatedEvent) error,
) error {
	if err := emit(generated); err != nil {
		return err
	}

	g.summary.TotalPublished++
	g.summary.UniqueEventIDs++
	if generated.OutOfOrder {
		g.summary.OutOfOrderEvents++
	}
	if generated.Invalid {
		g.summary.InvalidTransitionEvents++
	}

	if g.duplicatesRemaining == 0 {
		return nil
	}

	duplicate := generatedEvent{
		Event:     generated.Event,
		Duplicate: true,
	}
	if err := emit(duplicate); err != nil {
		return err
	}
	g.duplicatesRemaining--
	g.summary.TotalPublished++
	g.summary.DuplicateEvents++

	return nil
}

func (g *eventGenerator) nextOrderID() string {
	g.orderSequence++
	g.summary.UniqueOrderIDs++
	return fmt.Sprintf(
		"order-%d-%09d",
		g.config.Seed,
		g.orderSequence,
	)
}

func (g *eventGenerator) newEvent(
	orderID string,
	status domain.Status,
	timestamp time.Time,
) (domain.OrderEvent, error) {
	g.eventSequence++
	eventID := fmt.Sprintf(
		"event-%d-%09d",
		g.config.Seed,
		g.eventSequence,
	)
	payload, err := json.Marshal(map[string]any{
		"generator": "load-producer",
		"seed":      g.config.Seed,
		"sequence":  g.eventSequence,
		"status":    status,
	})
	if err != nil {
		return domain.OrderEvent{}, fmt.Errorf("encode event payload: %w", err)
	}

	return domain.OrderEvent{
		EventID:   eventID,
		OrderID:   orderID,
		Status:    status,
		Timestamp: timestamp,
		Payload:   payload,
	}, nil
}

func (g *eventGenerator) reserveTimes(count int) []time.Time {
	timestamps := make([]time.Time, count)
	for index := range timestamps {
		timestamps[index] = g.baseTime.Add(
			time.Duration(g.timeSequence+index) * time.Second,
		)
	}
	g.timeSequence += count
	return timestamps
}

func ratioCount(total int, ratio float64) int {
	return int(math.Floor(float64(total) * ratio))
}

func outOfOrderScenarioSize(count int) int {
	paidBeforeCreated := (count + 1) / 2
	shippedBeforePaid := count / 2
	return (2 * paidBeforeCreated) + (3 * shippedBeforePaid)
}

func publishGenerated(
	ctx context.Context,
	publisher eventPublisher,
	config producerConfig,
	output io.Writer,
) (producerSummary, error) {
	batch := make([]generatedEvent, 0, config.BatchSize)
	totalPublished := 0
	nextProgress := 10_000

	flush := func() error {
		for _, generated := range batch {
			if _, err := publisher.Publish(ctx, generated.Event); err != nil {
				return fmt.Errorf(
					"publish event %q after %d successful events: %w",
					generated.Event.EventID,
					totalPublished,
					err,
				)
			}
			totalPublished++
			if totalPublished >= nextProgress {
				fmt.Fprintf(
					output,
					"progress published=%d requested=%d\n",
					totalPublished,
					config.Total,
				)
				nextProgress += 10_000
			}
		}
		batch = batch[:0]
		return nil
	}

	summary, err := generate(config, func(event generatedEvent) error {
		batch = append(batch, event)
		if len(batch) < config.BatchSize {
			return nil
		}
		return flush()
	})
	if err != nil {
		return producerSummary{}, err
	}
	if err := flush(); err != nil {
		return producerSummary{}, err
	}

	summary.TotalPublished = totalPublished
	return summary, nil
}

func printSummary(output io.Writer, summary producerSummary) {
	fmt.Fprintln(output, "producer_summary")
	fmt.Fprintf(output, "total_requested=%d\n", summary.TotalRequested)
	fmt.Fprintf(output, "total_published=%d\n", summary.TotalPublished)
	fmt.Fprintf(output, "unique_event_ids=%d\n", summary.UniqueEventIDs)
	fmt.Fprintf(output, "duplicate_events=%d\n", summary.DuplicateEvents)
	fmt.Fprintf(output, "unique_order_ids=%d\n", summary.UniqueOrderIDs)
	fmt.Fprintf(output, "out_of_order_events=%d\n", summary.OutOfOrderEvents)
	fmt.Fprintf(
		output,
		"invalid_transition_events=%d\n",
		summary.InvalidTransitionEvents,
	)
	fmt.Fprintf(output, "seed=%d\n", summary.Seed)
	fmt.Fprintf(output, "redis_stream=%s\n", summary.RedisStream)
}
