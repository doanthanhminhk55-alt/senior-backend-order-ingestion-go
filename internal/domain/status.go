package domain

// Status is the current or requested state of an order.
type Status string

const (
	StatusCreated   Status = "CREATED"
	StatusPaid      Status = "PAID"
	StatusShipped   Status = "SHIPPED"
	StatusCancelled Status = "CANCELLED"
)

func (s Status) isKnown() bool {
	switch s {
	case StatusCreated, StatusPaid, StatusShipped, StatusCancelled:
		return true
	default:
		return false
	}
}
