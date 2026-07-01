package queue

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/domain"
	"github.com/redis/go-redis/v9"
)

func TestEventFields(t *testing.T) {
	timestamp := time.Date(2026, time.July, 1, 9, 30, 15, 123456789, time.FixedZone("ICT", 7*60*60))
	event := OrderEvent{
		EventID:   "event-123",
		OrderID:   "order-456",
		Status:    domain.StatusPaid,
		Timestamp: timestamp,
		Payload:   json.RawMessage(`{"source":"checkout","total":42.5}`),
	}

	fields, err := eventFields(event)
	if err != nil {
		t.Fatalf("eventFields() error = %v", err)
	}

	if got := fields[fieldEventID]; got != event.EventID {
		t.Errorf("%s = %v, want %q", fieldEventID, got, event.EventID)
	}
	if got := fields[fieldOrderID]; got != event.OrderID {
		t.Errorf("%s = %v, want %q", fieldOrderID, got, event.OrderID)
	}
	if got := fields[fieldStatus]; got != string(event.Status) {
		t.Errorf("%s = %v, want %q", fieldStatus, got, event.Status)
	}
	if got := fields[fieldTimestamp]; got != timestamp.UTC().Format(time.RFC3339Nano) {
		t.Errorf("%s = %v, want canonical timestamp", fieldTimestamp, got)
	}

	payload, ok := fields[fieldPayload].(string)
	if !ok {
		t.Fatalf("%s type = %T, want string", fieldPayload, fields[fieldPayload])
	}
	if !strings.Contains(payload, `"source":"checkout"`) {
		t.Errorf("%s = %q, want serialized source", fieldPayload, payload)
	}
}

func TestParseStreamMessage(t *testing.T) {
	timestamp := time.Date(2026, time.July, 1, 2, 30, 15, 123456789, time.UTC)
	message := redis.XMessage{
		ID: "1751337015123-0",
		Values: map[string]any{
			fieldEventID:   "event-123",
			fieldOrderID:   []byte("order-456"),
			fieldStatus:    "SHIPPED",
			fieldTimestamp: timestamp.Format(time.RFC3339Nano),
			fieldPayload:   `{"carrier":"parcel-post","priority":true}`,
		},
	}

	got, err := parseStreamMessage(message)
	if err != nil {
		t.Fatalf("parseStreamMessage() error = %v", err)
	}

	if got.RedisID != message.ID {
		t.Errorf("RedisID = %q, want %q", got.RedisID, message.ID)
	}
	if got.Event.EventID != "event-123" {
		t.Errorf("EventID = %q, want %q", got.Event.EventID, "event-123")
	}
	if got.Event.OrderID != "order-456" {
		t.Errorf("OrderID = %q, want %q", got.Event.OrderID, "order-456")
	}
	if got.Event.Status != domain.StatusShipped {
		t.Errorf("Status = %q, want %q", got.Event.Status, domain.StatusShipped)
	}
	if !got.Event.Timestamp.Equal(timestamp) {
		t.Errorf("Timestamp = %v, want %v", got.Event.Timestamp, timestamp)
	}
	var payload map[string]any
	if err := json.Unmarshal(got.Event.Payload, &payload); err != nil {
		t.Fatalf("unmarshal parsed payload: %v", err)
	}
	if payload["carrier"] != "parcel-post" {
		t.Errorf("Payload carrier = %v, want %q", payload["carrier"], "parcel-post")
	}
	if payload["priority"] != true {
		t.Errorf("Payload priority = %v, want true", payload["priority"])
	}
	if got.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0", got.Attempts)
	}
}

func TestParseStreamMessage_Malformed(t *testing.T) {
	validValues := func() map[string]any {
		return map[string]any{
			fieldEventID:   "event-123",
			fieldOrderID:   "order-456",
			fieldStatus:    "CREATED",
			fieldTimestamp: "2026-07-01T02:30:15Z",
			fieldPayload:   `{"source":"checkout"}`,
		}
	}

	tests := []struct {
		name      string
		mutate    func(map[string]any)
		wantError string
	}{
		{
			name: "missing event ID",
			mutate: func(values map[string]any) {
				delete(values, fieldEventID)
			},
			wantError: "event_id",
		},
		{
			name: "empty order ID",
			mutate: func(values map[string]any) {
				values[fieldOrderID] = ""
			},
			wantError: "order_id",
		},
		{
			name: "unsupported status field type",
			mutate: func(values map[string]any) {
				values[fieldStatus] = 42
			},
			wantError: "unsupported type",
		},
		{
			name: "invalid timestamp",
			mutate: func(values map[string]any) {
				values[fieldTimestamp] = "yesterday"
			},
			wantError: "RFC3339",
		},
		{
			name: "invalid payload JSON",
			mutate: func(values map[string]any) {
				values[fieldPayload] = "{"
			},
			wantError: "JSON object",
		},
		{
			name: "payload is not an object",
			mutate: func(values map[string]any) {
				values[fieldPayload] = `["unexpected"]`
			},
			wantError: "JSON object",
		},
		{
			name: "payload is null",
			mutate: func(values map[string]any) {
				values[fieldPayload] = "null"
			},
			wantError: "must be a JSON object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := validValues()
			tt.mutate(values)

			_, err := parseStreamMessage(redis.XMessage{
				ID:     "1751337015123-0",
				Values: values,
			})
			if err == nil {
				t.Fatal("parseStreamMessage() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantError)
			}
		})
	}
}
