package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/example/senior-backend-order-ingestion-go/internal/domain"
	"github.com/redis/go-redis/v9"
)

const (
	fieldEventID   = "event_id"
	fieldOrderID   = "order_id"
	fieldStatus    = "status"
	fieldTimestamp = "timestamp"
	fieldPayload   = "payload"
)

// OrderEvent is retained as an alias for callers of the queue package.
// The canonical transport-neutral event belongs to the domain package.
type OrderEvent = domain.OrderEvent

// StreamMessage combines the Redis delivery identity with its order event.
type StreamMessage struct {
	RedisID string
	Event   OrderEvent
	// Attempts is zero for newly read messages because XREADGROUP does not
	// return a delivery count. Pending-entry recovery can populate it later.
	Attempts int
}

// RedisStreamQueue publishes and consumes order events through a consumer group.
type RedisStreamQueue struct {
	client     *redis.Client
	streamName string
	groupName  string
}

// NewRedisStreamQueue creates a Redis Streams queue adapter.
func NewRedisStreamQueue(
	client *redis.Client,
	streamName string,
	groupName string,
) *RedisStreamQueue {
	return &RedisStreamQueue{
		client:     client,
		streamName: streamName,
		groupName:  groupName,
	}
}

// EnsureGroup creates the stream and consumer group if they do not exist.
func (q *RedisStreamQueue) EnsureGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(
		ctx,
		q.streamName,
		q.groupName,
		"0",
	).Err()
	if err == nil || redis.HasErrorPrefix(err, "BUSYGROUP") {
		return nil
	}

	return fmt.Errorf(
		"create Redis consumer group %q for stream %q: %w",
		q.groupName,
		q.streamName,
		err,
	)
}

// Publish appends an order event to the stream and returns its Redis ID.
func (q *RedisStreamQueue) Publish(
	ctx context.Context,
	event OrderEvent,
) (string, error) {
	values, err := eventFields(event)
	if err != nil {
		return "", fmt.Errorf("serialize order event: %w", err)
	}

	redisID, err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.streamName,
		Values: values,
	}).Result()
	if err != nil {
		return "", fmt.Errorf(
			"publish event %q to Redis stream %q: %w",
			event.EventID,
			q.streamName,
			err,
		)
	}

	return redisID, nil
}

// ReadGroup reads new messages for a consumer without acknowledging them.
func (q *RedisStreamQueue) ReadGroup(
	ctx context.Context,
	consumerName string,
	count int64,
	block time.Duration,
) ([]StreamMessage, error) {
	if consumerName == "" {
		return nil, errors.New("consumer name is required")
	}
	if count <= 0 {
		return nil, errors.New("read count must be greater than zero")
	}

	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.groupName,
		Consumer: consumerName,
		Streams:  []string{q.streamName, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf(
			"read Redis stream %q as consumer %q: %w",
			q.streamName,
			consumerName,
			err,
		)
	}

	messages := make([]StreamMessage, 0)
	for _, stream := range streams {
		for _, message := range stream.Messages {
			parsed, parseErr := parseStreamMessage(message)
			if parseErr != nil {
				return nil, fmt.Errorf(
					"parse Redis stream message %q: %w",
					message.ID,
					parseErr,
				)
			}
			messages = append(messages, parsed)
		}
	}

	return messages, nil
}

// Ack acknowledges a message after its database transaction has committed.
func (q *RedisStreamQueue) Ack(ctx context.Context, redisID string) error {
	if redisID == "" {
		return errors.New("Redis message ID is required")
	}

	if err := q.client.XAck(
		ctx,
		q.streamName,
		q.groupName,
		redisID,
	).Err(); err != nil {
		return fmt.Errorf(
			"acknowledge Redis stream message %q: %w",
			redisID,
			err,
		)
	}

	return nil
}

// QueueDepth returns the total number of entries currently in the stream.
func (q *RedisStreamQueue) QueueDepth(ctx context.Context) (int64, error) {
	depth, err := q.client.XLen(ctx, q.streamName).Result()
	if err != nil {
		return 0, fmt.Errorf(
			"get depth of Redis stream %q: %w",
			q.streamName,
			err,
		)
	}

	return depth, nil
}

// PendingCount returns the number of delivered but unacknowledged group entries.
func (q *RedisStreamQueue) PendingCount(ctx context.Context) (int64, error) {
	pending, err := q.client.XPending(
		ctx,
		q.streamName,
		q.groupName,
	).Result()
	if err != nil {
		return 0, fmt.Errorf(
			"get pending count for Redis stream %q group %q: %w",
			q.streamName,
			q.groupName,
			err,
		)
	}

	return pending.Count, nil
}

func eventFields(event OrderEvent) (map[string]any, error) {
	if event.EventID == "" {
		return nil, errors.New("event ID is required")
	}
	if event.OrderID == "" {
		return nil, errors.New("order ID is required")
	}
	if event.Status == "" {
		return nil, errors.New("status is required")
	}
	if event.Timestamp.IsZero() {
		return nil, errors.New("timestamp is required")
	}

	encodedPayload, err := normalizePayload(event.Payload)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		fieldEventID:   event.EventID,
		fieldOrderID:   event.OrderID,
		fieldStatus:    string(event.Status),
		fieldTimestamp: event.Timestamp.UTC().Format(time.RFC3339Nano),
		fieldPayload:   string(encodedPayload),
	}, nil
}

func parseStreamMessage(message redis.XMessage) (StreamMessage, error) {
	eventID, err := requiredStringField(message.Values, fieldEventID)
	if err != nil {
		return StreamMessage{}, err
	}
	orderID, err := requiredStringField(message.Values, fieldOrderID)
	if err != nil {
		return StreamMessage{}, err
	}
	status, err := requiredStringField(message.Values, fieldStatus)
	if err != nil {
		return StreamMessage{}, err
	}
	timestampValue, err := requiredStringField(message.Values, fieldTimestamp)
	if err != nil {
		return StreamMessage{}, err
	}
	payloadValue, err := requiredStringField(message.Values, fieldPayload)
	if err != nil {
		return StreamMessage{}, err
	}

	timestamp, err := time.Parse(time.RFC3339Nano, timestampValue)
	if err != nil {
		return StreamMessage{}, fmt.Errorf(
			"field %q is not an RFC3339 timestamp: %w",
			fieldTimestamp,
			err,
		)
	}

	payload, err := normalizePayload(json.RawMessage(payloadValue))
	if err != nil {
		return StreamMessage{}, fmt.Errorf("field %q: %w", fieldPayload, err)
	}

	return StreamMessage{
		RedisID: message.ID,
		Event: OrderEvent{
			EventID:   eventID,
			OrderID:   orderID,
			Status:    domain.Status(status),
			Timestamp: timestamp,
			Payload:   payload,
		},
		Attempts: 0,
	}, nil
}

func requiredStringField(values map[string]any, name string) (string, error) {
	value, ok := values[name]
	if !ok {
		return "", fmt.Errorf("required field %q is missing", name)
	}

	var stringValue string
	switch typed := value.(type) {
	case string:
		stringValue = typed
	case []byte:
		stringValue = string(typed)
	default:
		return "", fmt.Errorf(
			"field %q has unsupported type %T",
			name,
			value,
		)
	}

	if stringValue == "" {
		return "", fmt.Errorf("required field %q is empty", name)
	}

	return stringValue, nil
}

func normalizePayload(payload json.RawMessage) (json.RawMessage, error) {
	if len(payload) == 0 {
		return json.RawMessage(`{}`), nil
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		return nil, fmt.Errorf("payload is not a JSON object: %w", err)
	}
	if object == nil {
		return nil, errors.New("payload must be a JSON object")
	}

	return payload, nil
}
