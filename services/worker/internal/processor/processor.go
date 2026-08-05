// Package processor performs the worker-side side effect.
//
// The handler is deliberately idempotent. Redis Streams give at-least-once
// delivery, and the consumer can only tighten that so far: a handler that
// succeeds and then fails to ack will be redelivered no matter how careful the
// consumer is. The guarantee callers actually want — effectively-once — has to
// be closed on this side, by making a repeated delivery a no-op instead of a
// second write.
package processor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/maksimvasilenko/pulsebridge/worker/internal/domain"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/metrics"
)

// ErrInvalidEnvelope marks an event that can never succeed, so the consumer can
// quarantine it instead of burning the retry budget.
var ErrInvalidEnvelope = errors.New("invalid event envelope")

// Store is the durable side of processing.
//
// Claim must be atomic: it returns true only for the caller that first claimed
// the event id. Two workers racing on the same redelivery must see exactly one
// true, otherwise the deduplication is decorative.
type Store interface {
	Claim(ctx context.Context, eventID string, ttl time.Duration) (bool, error)
	Apply(ctx context.Context, event domain.Event) error
	Release(ctx context.Context, eventID string) error
}

// Processor applies events exactly once per event id within the dedupe window.
type Processor struct {
	store     Store
	dedupeTTL time.Duration
}

func New(store Store, dedupeTTL time.Duration) *Processor {
	if dedupeTTL <= 0 {
		dedupeTTL = 24 * time.Hour
	}
	return &Processor{store: store, dedupeTTL: dedupeTTL}
}

func (p *Processor) Process(ctx context.Context, event domain.Event) error {
	start := time.Now()
	metrics.InFlight.Inc()
	defer func() {
		metrics.InFlight.Dec()
		metrics.Latency.Observe(time.Since(start).Seconds())
	}()

	if err := ctx.Err(); err != nil {
		metrics.Processed.WithLabelValues("canceled").Inc()
		return err
	}

	if err := event.Validate(); err != nil {
		metrics.Processed.WithLabelValues("invalid").Inc()
		return fmt.Errorf("%w: %s", ErrInvalidEnvelope, err)
	}
	if _, err := event.PayloadMap(); err != nil {
		metrics.Processed.WithLabelValues("invalid_payload").Inc()
		return fmt.Errorf("%w: payload decode: %s", ErrInvalidEnvelope, err)
	}

	claimed, err := p.store.Claim(ctx, event.EventID, p.dedupeTTL)
	if err != nil {
		metrics.Processed.WithLabelValues("error").Inc()
		return fmt.Errorf("claim: %w", err)
	}
	if !claimed {
		// A redelivery of an event already applied. Acknowledging it is correct:
		// the work is done, and refusing would loop the message until it hit the
		// dead-letter stream for no reason.
		metrics.Processed.WithLabelValues("duplicate").Inc()
		return nil
	}

	if err := p.store.Apply(ctx, event); err != nil {
		// The claim is dropped so the retry is not mistaken for a duplicate and
		// silently skipped. Worst case the release fails too and the event waits
		// out the dedupe TTL — visible in metrics, and safe.
		if releaseErr := p.store.Release(context.WithoutCancel(ctx), event.EventID); releaseErr != nil {
			metrics.Processed.WithLabelValues("claim_leaked").Inc()
		}
		metrics.Processed.WithLabelValues("error").Inc()
		return fmt.Errorf("apply: %w", err)
	}

	metrics.Processed.WithLabelValues("ok").Inc()
	return nil
}
