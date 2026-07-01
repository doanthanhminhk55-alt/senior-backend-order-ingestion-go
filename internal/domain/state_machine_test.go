package domain

import "testing"

func TestEvaluateTransition_ValidTransitions(t *testing.T) {
	tests := []struct {
		name    string
		current Status
		next    Status
	}{
		{name: "new order is created", current: "", next: StatusCreated},
		{name: "created order is paid", current: StatusCreated, next: StatusPaid},
		{name: "paid order is shipped", current: StatusPaid, next: StatusShipped},
		{name: "created order is cancelled", current: StatusCreated, next: StatusCancelled},
		{name: "paid order is cancelled", current: StatusPaid, next: StatusCancelled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertTransition(
				t,
				EvaluateTransition(tt.current, tt.next),
				true,
				ClassificationApplied,
			)
		})
	}
}

func TestEvaluateTransition_DuplicateStatus(t *testing.T) {
	tests := []struct {
		name   string
		status Status
	}{
		{name: "created", status: StatusCreated},
		{name: "paid", status: StatusPaid},
		{name: "shipped", status: StatusShipped},
		{name: "cancelled", status: StatusCancelled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertTransition(
				t,
				EvaluateTransition(tt.status, tt.status),
				false,
				ClassificationDuplicateStatus,
			)
		})
	}
}

func TestEvaluateTransition_RecoverableOutOfOrder(t *testing.T) {
	tests := []struct {
		name    string
		current Status
		next    Status
	}{
		{name: "paid arrives before created", current: "", next: StatusPaid},
		{name: "shipped arrives before created", current: "", next: StatusShipped},
		{name: "shipped arrives before paid", current: StatusCreated, next: StatusShipped},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertTransition(
				t,
				EvaluateTransition(tt.current, tt.next),
				false,
				ClassificationOutOfOrder,
			)
		})
	}
}

func TestEvaluateTransition_InvalidTransitions(t *testing.T) {
	tests := []struct {
		name    string
		current Status
		next    Status
	}{
		{name: "new order cannot start cancelled", current: "", next: StatusCancelled},
		{name: "shipped order cannot be cancelled", current: StatusShipped, next: StatusCancelled},
		{name: "shipped order cannot return to paid", current: StatusShipped, next: StatusPaid},
		{name: "shipped order cannot return to created", current: StatusShipped, next: StatusCreated},
		{name: "cancelled order cannot return to created", current: StatusCancelled, next: StatusCreated},
		{name: "cancelled order cannot be paid", current: StatusCancelled, next: StatusPaid},
		{name: "cancelled order cannot be shipped", current: StatusCancelled, next: StatusShipped},
		{name: "paid order cannot return to created", current: StatusPaid, next: StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertTransition(
				t,
				EvaluateTransition(tt.current, tt.next),
				false,
				ClassificationInvalidTransition,
			)
		})
	}
}

func TestEvaluateTransition_UnknownStatus(t *testing.T) {
	tests := []struct {
		name    string
		current Status
		next    Status
	}{
		{name: "unknown next status", current: StatusCreated, next: Status("REFUNDED")},
		{name: "unknown current status", current: Status("REFUNDED"), next: StatusCreated},
		{name: "both statuses unknown", current: Status("UNKNOWN"), next: Status("REFUNDED")},
		{name: "empty next status", current: StatusCreated, next: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertTransition(
				t,
				EvaluateTransition(tt.current, tt.next),
				false,
				ClassificationInvalidTransition,
			)
		})
	}
}

func assertTransition(
	t *testing.T,
	got TransitionResult,
	wantAllowed bool,
	wantClassification string,
) {
	t.Helper()

	if got.Allowed != wantAllowed {
		t.Errorf("Allowed = %v, want %v", got.Allowed, wantAllowed)
	}
	if got.Classification != wantClassification {
		t.Errorf("Classification = %q, want %q", got.Classification, wantClassification)
	}
	if got.Reason == "" {
		t.Error("Reason is empty")
	}
}
