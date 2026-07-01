package main

import (
	"reflect"
	"testing"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/domain"
)

func TestGenerate_CountsAndInjectedScenarios(t *testing.T) {
	config := testProducerConfig()
	var events []generatedEvent

	summary, err := generate(config, func(event generatedEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("generate() error = %v", err)
	}

	if len(events) != config.Total {
		t.Errorf("event count = %d, want %d", len(events), config.Total)
	}
	if summary.TotalPublished != config.Total {
		t.Errorf(
			"TotalPublished = %d, want %d",
			summary.TotalPublished,
			config.Total,
		)
	}

	wantDuplicates := ratioCount(config.Total, config.DuplicateRatio)
	if summary.DuplicateEvents != wantDuplicates {
		t.Errorf(
			"DuplicateEvents = %d, want %d",
			summary.DuplicateEvents,
			wantDuplicates,
		)
	}
	wantOutOfOrder := ratioCount(config.Total, config.OutOfOrderRatio)
	if summary.OutOfOrderEvents != wantOutOfOrder {
		t.Errorf(
			"OutOfOrderEvents = %d, want %d",
			summary.OutOfOrderEvents,
			wantOutOfOrder,
		)
	}
	wantInvalid := ratioCount(config.Total, config.InvalidRatio)
	if summary.InvalidTransitionEvents != wantInvalid {
		t.Errorf(
			"InvalidTransitionEvents = %d, want %d",
			summary.InvalidTransitionEvents,
			wantInvalid,
		)
	}

	assertDuplicatesReusePriorEvent(t, events)
	assertOutOfOrderPrerequisitesFollow(t, events)
	assertInvalidTransitionsFollowShipped(t, events)
}

func TestGenerate_DefaultScaleStreamsOneHundredThousandEvents(t *testing.T) {
	config := testProducerConfig()
	config.Total = 100_000
	config.Seed = 1
	emitted := 0

	summary, err := generate(config, func(generatedEvent) error {
		emitted++
		return nil
	})
	if err != nil {
		t.Fatalf("generate() error = %v", err)
	}

	if emitted != 100_000 {
		t.Errorf("emitted events = %d, want 100000", emitted)
	}
	if summary.UniqueEventIDs != 95_000 {
		t.Errorf("UniqueEventIDs = %d, want 95000", summary.UniqueEventIDs)
	}
	if summary.DuplicateEvents != 5_000 {
		t.Errorf("DuplicateEvents = %d, want 5000", summary.DuplicateEvents)
	}
	if summary.OutOfOrderEvents != 3_000 {
		t.Errorf("OutOfOrderEvents = %d, want 3000", summary.OutOfOrderEvents)
	}
	if summary.InvalidTransitionEvents != 1_000 {
		t.Errorf(
			"InvalidTransitionEvents = %d, want 1000",
			summary.InvalidTransitionEvents,
		)
	}
}

func TestGenerate_IsDeterministicForSeed(t *testing.T) {
	config := testProducerConfig()
	first := collectGenerated(t, config)
	second := collectGenerated(t, config)

	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed generated different event sequences")
	}

	config.Seed++
	third := collectGenerated(t, config)
	if reflect.DeepEqual(first, third) {
		t.Fatal("different seed generated the same event sequence")
	}
}

func TestProducerConfig_RejectsInfeasibleRatios(t *testing.T) {
	config := testProducerConfig()
	config.Total = 10
	config.DuplicateRatio = 0.5
	config.OutOfOrderRatio = 0.5
	config.InvalidRatio = 0.5

	if err := config.validate(); err == nil {
		t.Fatal("validate() error = nil, want infeasible-ratio error")
	}
}

func assertDuplicatesReusePriorEvent(
	t *testing.T,
	events []generatedEvent,
) {
	t.Helper()

	seen := make(map[string]domain.OrderEvent)
	for _, generated := range events {
		if generated.Duplicate {
			original, ok := seen[generated.Event.EventID]
			if !ok {
				t.Errorf(
					"duplicate event %q appeared before its source",
					generated.Event.EventID,
				)
				continue
			}
			if !reflect.DeepEqual(generated.Event, original) {
				t.Errorf(
					"duplicate event %q does not match its source",
					generated.Event.EventID,
				)
			}
			continue
		}
		seen[generated.Event.EventID] = generated.Event
	}
}

func assertOutOfOrderPrerequisitesFollow(
	t *testing.T,
	events []generatedEvent,
) {
	t.Helper()

	for index, generated := range events {
		if !generated.OutOfOrder {
			continue
		}

		required := domain.StatusCreated
		if generated.Event.Status == domain.StatusShipped {
			required = domain.StatusPaid
		}

		foundLater := false
		for later := index + 1; later < len(events); later++ {
			candidate := events[later]
			if candidate.Duplicate {
				continue
			}
			if candidate.Event.OrderID == generated.Event.OrderID &&
				candidate.Event.Status == required {
				foundLater = true
				break
			}
		}
		if !foundLater {
			t.Errorf(
				"out-of-order %s event for order %q has no later %s prerequisite",
				generated.Event.Status,
				generated.Event.OrderID,
				required,
			)
		}
	}
}

func assertInvalidTransitionsFollowShipped(
	t *testing.T,
	events []generatedEvent,
) {
	t.Helper()

	shippedOrders := make(map[string]bool)
	for _, generated := range events {
		if generated.Duplicate {
			continue
		}
		if generated.Event.Status == domain.StatusShipped {
			shippedOrders[generated.Event.OrderID] = true
		}
		if generated.Invalid &&
			!shippedOrders[generated.Event.OrderID] {
			t.Errorf(
				"invalid event %q for order %q did not follow SHIPPED",
				generated.Event.EventID,
				generated.Event.OrderID,
			)
		}
	}
}

func collectGenerated(
	t *testing.T,
	config producerConfig,
) []generatedEvent {
	t.Helper()

	var events []generatedEvent
	_, err := generate(config, func(event generatedEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("generate() error = %v", err)
	}
	return events
}

func testProducerConfig() producerConfig {
	return producerConfig{
		Total:           1_000,
		DuplicateRatio:  0.05,
		OutOfOrderRatio: 0.03,
		InvalidRatio:    0.01,
		RedisAddr:       "localhost:6379",
		Stream:          "order-events",
		Group:           "order-processors",
		Seed:            42,
		BatchSize:       100,
	}
}
