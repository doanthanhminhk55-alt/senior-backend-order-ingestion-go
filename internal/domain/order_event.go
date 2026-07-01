package domain

import (
	"encoding/json"
	"time"
)

// OrderEvent is a transport-neutral request to change an order's status.
type OrderEvent struct {
	EventID   string
	OrderID   string
	Status    Status
	Timestamp time.Time
	Payload   json.RawMessage
}
