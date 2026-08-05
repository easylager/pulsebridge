// Package dlq quarantines events the worker cannot process.
//
// At-least-once delivery only holds if there is somewhere for permanently
// failing messages to go. Without a dead-letter path a poison message is either
// retried forever (blocking the pending list and inflating retry metrics) or
// silently acked and lost. Both are unacceptable in a system that carries
// business events, so failures are written to a separate stream with enough
// context to replay or investigate them.
package dlq

import (
	"context"
	"time"

	"github.com/maksimvasilenko/pulsebridge/worker/internal/domain"
	"github.com/redis/go-redis/v9"
)

// Reason classifies why an event was quarantined. It is a low-cardinality label
// so it is safe to expose on metrics.
type Reason string

const (
	// ReasonUndecodable means the stream entry never mapped to a valid envelope.
	// Retrying cannot help: the bytes will not change.
	ReasonUndecodable Reason = "undecodable"
	// ReasonRetriesExhausted means the handler kept failing past the retry budget.
	ReasonRetriesExhausted Reason = "retries_exhausted"
)

// Writer is the subset of the Redis API the dead-letter path needs.
type Writer interface {
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
}

// Publisher writes quarantined events to the dead-letter stream.
type Publisher struct {
	client Writer
	stream string
	maxLen int64
	now    func() time.Time
}

// New returns a Publisher writing to stream. maxLen caps retention so a flood of
// poison messages cannot exhaust Redis memory; it is approximate for the same
// reason the source stream is trimmed approximately.
func New(client Writer, stream string, maxLen int64) *Publisher {
	if maxLen <= 0 {
		maxLen = 100_000
	}
	return &Publisher{client: client, stream: stream, maxLen: maxLen, now: time.Now}
}

// Stream returns the destination stream key.
func (p *Publisher) Stream() string { return p.stream }

// Publish quarantines one event. The original envelope fields are preserved
// verbatim so a replay tool can re-publish to the source stream unchanged, and
// the failure context is added under a dlq_ prefix.
func (p *Publisher) Publish(
	ctx context.Context,
	event domain.Event,
	reason Reason,
	attempts int64,
	cause error,
) error {
	causeText := ""
	if cause != nil {
		causeText = cause.Error()
	}

	values := map[string]any{
		"event_id":       event.EventID,
		"tenant_id":      event.TenantID,
		"event_type":     event.EventType,
		"payload":        event.PayloadJSON,
		"occurred_at":    event.OccurredAt,
		"correlation_id": event.CorrelationID,
		"traceparent":    event.TraceParent,

		"dlq_reason":        string(reason),
		"dlq_error":         causeText,
		"dlq_attempts":      attempts,
		"dlq_at":            p.now().UTC().Format(time.RFC3339Nano),
		"dlq_source_stream": event.SourceStream,
		"dlq_original_id":   event.StreamID,
	}

	return p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.stream,
		MaxLen: p.maxLen,
		Approx: true,
		Values: values,
	}).Err()
}
