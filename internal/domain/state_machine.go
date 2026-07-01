package domain

const (
	ClassificationApplied           = "APPLIED"
	ClassificationDuplicateStatus   = "DUPLICATE_STATUS"
	ClassificationOutOfOrder        = "OUT_OF_ORDER"
	ClassificationInvalidTransition = "INVALID_TRANSITION"
)

// TransitionResult describes whether an incoming status can be applied now.
type TransitionResult struct {
	Allowed        bool
	Classification string
	Reason         string
}

// EvaluateTransition classifies a requested order status transition.
// An empty current status means that the order does not exist yet.
func EvaluateTransition(current, next Status) TransitionResult {
	if !next.isKnown() {
		return invalidResult("next status is unknown")
	}

	if current != "" && !current.isKnown() {
		return invalidResult("current status is unknown")
	}

	if current == next {
		return TransitionResult{
			Allowed:        false,
			Classification: ClassificationDuplicateStatus,
			Reason:         "order already has the requested status",
		}
	}

	switch {
	case current == "" && next == StatusCreated:
		return appliedResult("new orders start in CREATED")
	case current == StatusCreated && next == StatusPaid:
		return appliedResult("CREATED orders can be paid")
	case current == StatusPaid && next == StatusShipped:
		return appliedResult("PAID orders can be shipped")
	case current == StatusCreated && next == StatusCancelled:
		return appliedResult("CREATED orders can be cancelled")
	case current == StatusPaid && next == StatusCancelled:
		return appliedResult("PAID orders can be cancelled")
	case current == "" && next == StatusPaid:
		return outOfOrderResult("CREATED event has not been applied")
	case current == "" && next == StatusShipped:
		return outOfOrderResult("CREATED and PAID events have not been applied")
	case current == StatusCreated && next == StatusShipped:
		return outOfOrderResult("PAID event has not been applied")
	default:
		return invalidResult("transition is not allowed")
	}
}

func appliedResult(reason string) TransitionResult {
	return TransitionResult{
		Allowed:        true,
		Classification: ClassificationApplied,
		Reason:         reason,
	}
}

func outOfOrderResult(reason string) TransitionResult {
	return TransitionResult{
		Allowed:        false,
		Classification: ClassificationOutOfOrder,
		Reason:         reason,
	}
}

func invalidResult(reason string) TransitionResult {
	return TransitionResult{
		Allowed:        false,
		Classification: ClassificationInvalidTransition,
		Reason:         reason,
	}
}
