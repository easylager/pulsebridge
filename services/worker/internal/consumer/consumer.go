// Package consumer implements at-least-once consumption from a Redis Stream.
//
// Three loops run concurrently against one bounded worker pool:
//
//	read    — XREADGROUP ">" for messages never delivered to this group
//	reclaim — XPENDING + XCLAIM for messages a dead or wedged consumer never acked
//	sample  — XLEN + XPENDING summary, exported as backlog gauges
//
// The reclaim loop is what makes the delivery guarantee real. XREADGROUP alone
// hands a message to exactly one consumer; if that consumer dies mid-flight the
// message sits in the pending entries list forever and no amount of scaling
// brings it back. Reclaiming stale entries and capping redeliveries with a
// dead-letter path turns "at-least-once in principle" into a property the
// system actually holds under failure.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/maksimvasilenko/pulsebridge/worker/internal/dlq"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/domain"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/metrics"
	"github.com/redis/go-redis/v9"
)

// Handler processes a single event. Returning an error leaves the message
// unacknowledged so it can be redelivered; returning nil acknowledges it.
type Handler func(context.Context, domain.Event) error

// Client is the subset of the Redis API this consumer depends on. Narrowing it
// keeps the consumer testable against a fake and documents exactly which stream
// commands the delivery guarantee rests on.
type Client interface {
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd
	XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd
	XPending(ctx context.Context, stream, group string) *redis.XPendingCmd
	XClaim(ctx context.Context, a *redis.XClaimArgs) *redis.XMessageSliceCmd
	XLen(ctx context.Context, stream string) *redis.IntCmd
}

// DeadLetterer quarantines events that must not be retried again.
type DeadLetterer interface {
	Publish(ctx context.Context, event domain.Event, reason dlq.Reason, attempts int64, cause error) error
	Stream() string
}

// Options configures the consumer. Zero values fall back to the defaults in
// Normalise, so callers only set what they care about.
type Options struct {
	Stream      string
	Group       string
	Consumer    string
	Concurrency int

	// MaxAttempts is the total number of processing attempts allowed per
	// message before it is dead-lettered.
	MaxAttempts int64

	// ProcessTimeout bounds a single handler invocation.
	ProcessTimeout time.Duration

	// BlockTimeout is how long XREADGROUP parks waiting for new messages. It
	// also bounds how quickly the loop notices context cancellation.
	BlockTimeout time.Duration

	// ReclaimMinIdle is how long a message must sit unacknowledged before
	// another consumer may steal it. It must exceed ProcessTimeout, otherwise
	// consumers reclaim work that is still legitimately in flight and every
	// slow event is processed twice.
	ReclaimMinIdle time.Duration

	// ReclaimInterval is the polling period of the reclaim loop.
	ReclaimInterval time.Duration

	// SampleInterval is the polling period of the backlog gauge sampler.
	SampleInterval time.Duration
}

// Normalise fills defaults and repairs settings that would break the delivery
// guarantee. It is exported so configuration can be validated and logged at
// startup rather than silently drifting at runtime.
func (o Options) Normalise() Options {
	if o.Stream == "" {
		o.Stream = "pulsebridge.events"
	}
	if o.Group == "" {
		o.Group = "pulsebridge-workers"
	}
	if o.Consumer == "" {
		o.Consumer = "worker-1"
	}
	if o.Concurrency < 1 {
		o.Concurrency = 1
	}
	if o.MaxAttempts < 1 {
		o.MaxAttempts = 5
	}
	if o.ProcessTimeout <= 0 {
		o.ProcessTimeout = 5 * time.Second
	}
	if o.BlockTimeout <= 0 {
		o.BlockTimeout = 2 * time.Second
	}
	if o.ReclaimInterval <= 0 {
		o.ReclaimInterval = 10 * time.Second
	}
	if o.SampleInterval <= 0 {
		o.SampleInterval = 5 * time.Second
	}
	if o.ReclaimMinIdle <= 0 {
		o.ReclaimMinIdle = 30 * time.Second
	}
	// Reclaiming faster than a handler can finish turns every slow event into a
	// duplicate, so the floor is derived from ProcessTimeout rather than trusted
	// from configuration.
	if minIdle := 2 * o.ProcessTimeout; o.ReclaimMinIdle < minIdle {
		o.ReclaimMinIdle = minIdle
	}
	return o
}

// StreamConsumer owns the read, reclaim and sampling loops.
type StreamConsumer struct {
	client     Client
	deadLetter DeadLetterer
	handler    Handler
	opts       Options
	log        *slog.Logger

	sem chan struct{}
	wg  sync.WaitGroup
}

// New builds a consumer. deadLetter may be nil, in which case exhausted and
// undecodable messages are acknowledged and logged instead of quarantined —
// acceptable for local experiments, never for production.
func New(client Client, deadLetter DeadLetterer, handler Handler, opts Options, log *slog.Logger) *StreamConsumer {
	opts = opts.Normalise()
	return &StreamConsumer{
		client:     client,
		deadLetter: deadLetter,
		handler:    handler,
		opts:       opts,
		log:        log,
		sem:        make(chan struct{}, opts.Concurrency),
	}
}

// Options returns the normalised configuration in effect.
func (c *StreamConsumer) Options() Options { return c.opts }

// EnsureGroup creates the consumer group, tolerating the case where another
// replica created it first.
func (c *StreamConsumer) EnsureGroup(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(ctx, c.opts.Stream, c.opts.Group, "0").Err()
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

// Run blocks until ctx is cancelled, then drains in-flight work before
// returning. It returns ctx.Err() on normal shutdown.
func (c *StreamConsumer) Run(ctx context.Context) error {
	var loops sync.WaitGroup
	loops.Add(3)

	go func() { defer loops.Done(); c.readLoop(ctx) }()
	go func() { defer loops.Done(); c.reclaimLoop(ctx) }()
	go func() { defer loops.Done(); c.sampleLoop(ctx) }()

	loops.Wait()
	// Handlers dispatched before cancellation may still be running; a message is
	// only acked after its handler returns, so draining here is what prevents a
	// restart from duplicating work that had already succeeded.
	c.wg.Wait()
	return ctx.Err()
}

func (c *StreamConsumer) readLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.opts.Group,
			Consumer: c.opts.Consumer,
			Streams:  []string{c.opts.Stream, ">"},
			Count:    int64(c.opts.Concurrency),
			Block:    c.opts.BlockTimeout,
		}).Result()

		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Error("xreadgroup failed", "err", err)
			sleepCtx(ctx, 200*time.Millisecond)
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				c.dispatch(ctx, msg, 1)
			}
		}
	}
}

// reclaimLoop takes over messages that were delivered but never acknowledged.
// Without it, a consumer that is OOM-killed between XREADGROUP and XACK strands
// its in-flight messages permanently.
func (c *StreamConsumer) reclaimLoop(ctx context.Context) {
	ticker := time.NewTicker(c.opts.ReclaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.reclaimOnce(ctx); err != nil && ctx.Err() == nil {
				c.log.Error("reclaim failed", "err", err)
			}
		}
	}
}

func (c *StreamConsumer) reclaimOnce(ctx context.Context) error {
	pending, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: c.opts.Stream,
		Group:  c.opts.Group,
		Start:  "-",
		End:    "+",
		Count:  int64(c.opts.Concurrency),
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("xpending: %w", err)
	}

	// Idle is filtered here rather than pushed into XPENDING's IDLE argument so
	// the same code path works against Redis versions and test doubles that do
	// not implement the filter.
	attempts := make(map[string]int64, len(pending))
	ids := make([]string, 0, len(pending))
	for _, entry := range pending {
		if entry.Idle < c.opts.ReclaimMinIdle {
			continue
		}
		attempts[entry.ID] = entry.RetryCount
		ids = append(ids, entry.ID)
	}
	if len(ids) == 0 {
		return nil
	}

	messages, err := c.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   c.opts.Stream,
		Group:    c.opts.Group,
		Consumer: c.opts.Consumer,
		MinIdle:  c.opts.ReclaimMinIdle,
		Messages: ids,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("xclaim: %w", err)
	}

	for _, msg := range messages {
		delivered := attempts[msg.ID]
		metrics.Reclaimed.Inc()

		if delivered >= c.opts.MaxAttempts {
			event, mapErr := mapEvent(msg, c.opts.Stream)
			if mapErr != nil {
				event = partialEvent(msg, c.opts.Stream)
			}
			c.quarantine(ctx, event, dlq.ReasonRetriesExhausted, delivered,
				fmt.Errorf("exceeded %d delivery attempts", c.opts.MaxAttempts))
			continue
		}

		metrics.Retries.Inc()
		c.dispatch(ctx, msg, delivered+1)
	}
	return nil
}

// sampleLoop publishes backlog gauges. Pending depth and oldest-pending age are
// the two signals that distinguish "we are behind" from "we are stuck", and
// they drive the alerts in deploy/prometheus/alerts.yml.
func (c *StreamConsumer) sampleLoop(ctx context.Context) {
	ticker := time.NewTicker(c.opts.SampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.sampleOnce(ctx); err != nil && ctx.Err() == nil {
				metrics.SamplerErrors.Inc()
				c.log.Warn("backlog sampling failed", "err", err)
			}
		}
	}
}

func (c *StreamConsumer) sampleOnce(ctx context.Context) error {
	length, err := c.client.XLen(ctx, c.opts.Stream).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("xlen: %w", err)
	}
	metrics.StreamLength.Set(float64(length))

	summary, err := c.client.XPending(ctx, c.opts.Stream, c.opts.Group).Result()
	if errors.Is(err, redis.Nil) {
		metrics.PendingEntries.Set(0)
		metrics.OldestPendingSeconds.Set(0)
		return nil
	}
	if err != nil {
		return fmt.Errorf("xpending summary: %w", err)
	}
	metrics.PendingEntries.Set(float64(summary.Count))

	if summary.Count == 0 {
		metrics.OldestPendingSeconds.Set(0)
		return nil
	}

	oldest, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: c.opts.Stream,
		Group:  c.opts.Group,
		Start:  "-",
		End:    "+",
		Count:  1,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("xpending oldest: %w", err)
	}
	if len(oldest) > 0 {
		metrics.OldestPendingSeconds.Set(oldest[0].Idle.Seconds())
	}
	return nil
}

// dispatch runs one message on the bounded pool. It blocks when the pool is
// full, which is the backpressure mechanism: the read loop stops pulling new
// messages until capacity frees up, so the stream — not the worker's heap —
// absorbs a burst.
func (c *StreamConsumer) dispatch(ctx context.Context, msg redis.XMessage, attempt int64) {
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() { <-c.sem }()
		c.process(ctx, msg, attempt)
	}()
}

func (c *StreamConsumer) process(ctx context.Context, msg redis.XMessage, attempt int64) {
	event, err := mapEvent(msg, c.opts.Stream)
	if err != nil {
		// The bytes will not improve on redelivery, so this is quarantined on
		// the first attempt instead of consuming the retry budget.
		c.log.Error("undecodable message", "id", msg.ID, "err", err)
		metrics.Processed.WithLabelValues("undecodable").Inc()
		c.quarantine(ctx, partialEvent(msg, c.opts.Stream), dlq.ReasonUndecodable, attempt, err)
		return
	}
	event.Attempt = attempt

	// Detach from the shutdown context so an in-flight handler gets its full
	// timeout to finish and ack rather than being cut off mid-write.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.opts.ProcessTimeout)
	defer cancel()

	if err := c.handler(pctx, event); err != nil {
		// Deliberately not acked: the message stays in the pending list and the
		// reclaim loop retries it once ReclaimMinIdle has elapsed.
		c.log.Error("process failed",
			"event_id", event.EventID,
			"stream_id", msg.ID,
			"attempt", attempt,
			"max_attempts", c.opts.MaxAttempts,
			"err", err,
		)
		return
	}

	if err := c.ack(pctx, msg.ID); err != nil {
		// The handler succeeded but the ack did not, so the message will be
		// redelivered. This is the exact point where at-least-once becomes
		// visible to downstream consumers and why handlers must be idempotent.
		c.log.Error("ack failed", "id", msg.ID, "event_id", event.EventID, "err", err)
	}
}

func (c *StreamConsumer) quarantine(
	ctx context.Context,
	event domain.Event,
	reason dlq.Reason,
	attempts int64,
	cause error,
) {
	qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.opts.ProcessTimeout)
	defer cancel()

	if c.deadLetter == nil {
		c.log.Error("dropping message: no dead-letter stream configured",
			"stream_id", event.StreamID, "event_id", event.EventID, "reason", reason)
	} else if err := c.deadLetter.Publish(qctx, event, reason, attempts, cause); err != nil {
		// Leaving the message pending is the safer failure: it will be retried
		// and re-quarantined once the DLQ write recovers.
		metrics.DeadLetterFailures.Inc()
		c.log.Error("dead-letter publish failed",
			"stream_id", event.StreamID, "event_id", event.EventID, "err", err)
		return
	}

	metrics.DeadLettered.WithLabelValues(string(reason)).Inc()
	if err := c.ack(qctx, event.StreamID); err != nil {
		c.log.Error("ack after dead-letter failed", "id", event.StreamID, "err", err)
	}
}

func (c *StreamConsumer) ack(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	return c.client.XAck(ctx, c.opts.Stream, c.opts.Group, id).Err()
}

func mapEvent(msg redis.XMessage, sourceStream string) (domain.Event, error) {
	event := partialEvent(msg, sourceStream)
	if err := event.Validate(); err != nil {
		return event, err
	}
	if _, err := event.PayloadMap(); err != nil {
		return event, fmt.Errorf("payload decode: %w", err)
	}
	return event, nil
}

// partialEvent maps whatever fields are present without validating, so a
// message that fails validation still reaches the DLQ with usable context.
func partialEvent(msg redis.XMessage, sourceStream string) domain.Event {
	field := func(key string) string {
		if value, ok := msg.Values[key]; ok {
			return fmt.Sprint(value)
		}
		return ""
	}
	return domain.Event{
		StreamID:      msg.ID,
		EventID:       field("event_id"),
		TenantID:      field("tenant_id"),
		EventType:     field("event_type"),
		PayloadJSON:   field("payload"),
		OccurredAt:    field("occurred_at"),
		CorrelationID: field("correlation_id"),
		TraceParent:   field("traceparent"),
		SourceStream:  sourceStream,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
