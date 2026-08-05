package consumer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/dlq"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/domain"
	"github.com/redis/go-redis/v9"
)

const (
	testStream = "test.events"
	testDLQ    = "test.events.dlq"
	testGroup  = "test-workers"
)

func newHarness(t *testing.T, handler Handler, opts Options) (*StreamConsumer, *redis.Client, *miniredis.Miniredis) {
	t.Helper()

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	if opts.Stream == "" {
		opts.Stream = testStream
	}
	if opts.Group == "" {
		opts.Group = testGroup
	}
	if opts.Consumer == "" {
		opts.Consumer = "consumer-a"
	}
	if opts.ProcessTimeout == 0 {
		opts.ProcessTimeout = 200 * time.Millisecond
	}

	deadLetter := dlq.New(client, testDLQ, 1000)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	c := New(client, deadLetter, handler, opts, log)
	if err := c.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	return c, client, server
}

func publish(t *testing.T, client *redis.Client, values map[string]any) string {
	t.Helper()
	id, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: testStream,
		Values: values,
	}).Result()
	if err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	return id
}

func validEvent(eventID string) map[string]any {
	return map[string]any{
		"event_id":       eventID,
		"tenant_id":      "acme",
		"event_type":     "order.created",
		"payload":        `{"order_id":"o-1"}`,
		"occurred_at":    "2026-08-03T10:00:00Z",
		"correlation_id": "corr-1",
	}
}

// readOne pulls a message through XREADGROUP so it lands in the pending entries
// list exactly as it would in production.
func readOne(t *testing.T, c *StreamConsumer, client *redis.Client) redis.XMessage {
	t.Helper()
	streams, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    c.opts.Group,
		Consumer: c.opts.Consumer,
		Streams:  []string{c.opts.Stream, ">"},
		Count:    1,
		Block:    100 * time.Millisecond,
	}).Result()
	if err != nil {
		t.Fatalf("XReadGroup: %v", err)
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		t.Fatal("expected one message")
	}
	return streams[0].Messages[0]
}

func pendingCount(t *testing.T, client *redis.Client) int64 {
	t.Helper()
	summary, err := client.XPending(context.Background(), testStream, testGroup).Result()
	if errors.Is(err, redis.Nil) {
		return 0
	}
	if err != nil {
		t.Fatalf("XPending: %v", err)
	}
	return summary.Count
}

func dlqEntries(t *testing.T, client *redis.Client) []redis.XMessage {
	t.Helper()
	entries, err := client.XRange(context.Background(), testDLQ, "-", "+").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatalf("XRange DLQ: %v", err)
	}
	return entries
}

func TestSuccessfulProcessingAcknowledgesMessage(t *testing.T) {
	var seen atomic.Int64
	c, client, _ := newHarness(t, func(_ context.Context, e domain.Event) error {
		seen.Add(1)
		if e.EventID != "evt-1" {
			t.Errorf("unexpected event id %q", e.EventID)
		}
		if e.Attempt != 1 {
			t.Errorf("expected first attempt, got %d", e.Attempt)
		}
		return nil
	}, Options{})

	publish(t, client, validEvent("evt-1"))
	c.process(context.Background(), readOne(t, c, client), 1)

	if got := seen.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
	if got := pendingCount(t, client); got != 0 {
		t.Fatalf("pending = %d, want 0 after ack", got)
	}
}

// A failing handler must not ack. Leaving the entry pending is what allows the
// reclaim loop, or another replica, to try again.
func TestFailedProcessingLeavesMessagePending(t *testing.T) {
	c, client, _ := newHarness(t, func(context.Context, domain.Event) error {
		return errors.New("downstream unavailable")
	}, Options{})

	publish(t, client, validEvent("evt-1"))
	c.process(context.Background(), readOne(t, c, client), 1)

	if got := pendingCount(t, client); got != 1 {
		t.Fatalf("pending = %d, want 1 so the message can be retried", got)
	}
	if entries := dlqEntries(t, client); len(entries) != 0 {
		t.Fatalf("dead-lettered too early: %d entries", len(entries))
	}
}

// A message that cannot be decoded will never decode, so it is quarantined on
// the first attempt rather than consuming the whole retry budget.
func TestUndecodableMessageIsDeadLetteredImmediately(t *testing.T) {
	var called atomic.Bool
	c, client, _ := newHarness(t, func(context.Context, domain.Event) error {
		called.Store(true)
		return nil
	}, Options{})

	publish(t, client, map[string]any{"tenant_id": "acme", "event_type": "order.created"})
	c.process(context.Background(), readOne(t, c, client), 1)

	if called.Load() {
		t.Fatal("handler must not run for an undecodable message")
	}
	entries := dlqEntries(t, client)
	if len(entries) != 1 {
		t.Fatalf("dlq entries = %d, want 1", len(entries))
	}
	if got := entries[0].Values["dlq_reason"]; got != string(dlq.ReasonUndecodable) {
		t.Fatalf("dlq_reason = %v, want %s", got, dlq.ReasonUndecodable)
	}
	if got := pendingCount(t, client); got != 0 {
		t.Fatalf("pending = %d, want 0 once quarantined", got)
	}
}

func TestMalformedPayloadIsDeadLettered(t *testing.T) {
	c, client, _ := newHarness(t, func(context.Context, domain.Event) error { return nil }, Options{})

	values := validEvent("evt-broken")
	values["payload"] = "{not-json"
	publish(t, client, values)
	c.process(context.Background(), readOne(t, c, client), 1)

	entries := dlqEntries(t, client)
	if len(entries) != 1 {
		t.Fatalf("dlq entries = %d, want 1", len(entries))
	}
	if got := entries[0].Values["event_id"]; got != "evt-broken" {
		t.Fatalf("dlq event_id = %v, want evt-broken", got)
	}
}

// The core recovery property: a message stranded by a dead consumer is picked up
// again once it has been idle long enough.
func TestReclaimRetriesStaleMessage(t *testing.T) {
	var attempts atomic.Int64
	c, client, _ := newHarness(t, func(_ context.Context, e domain.Event) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient failure")
		}
		return nil
	}, Options{
		MaxAttempts:    5,
		ProcessTimeout: 20 * time.Millisecond,
		ReclaimMinIdle: 40 * time.Millisecond,
	})

	publish(t, client, validEvent("evt-1"))
	c.process(context.Background(), readOne(t, c, client), 1)

	if got := pendingCount(t, client); got != 1 {
		t.Fatalf("pending = %d, want 1 before reclaim", got)
	}

	time.Sleep(c.opts.ReclaimMinIdle + 20*time.Millisecond)
	if err := c.reclaimOnce(context.Background()); err != nil {
		t.Fatalf("reclaimOnce: %v", err)
	}
	c.wg.Wait()

	if got := attempts.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2 (initial + reclaimed retry)", got)
	}
	if got := pendingCount(t, client); got != 0 {
		t.Fatalf("pending = %d, want 0 after successful retry", got)
	}
}

// A message that is still fresh belongs to whoever is processing it. Reclaiming
// it early would duplicate every slow event.
func TestReclaimSkipsMessagesInsideTheIdleWindow(t *testing.T) {
	var attempts atomic.Int64
	c, client, _ := newHarness(t, func(context.Context, domain.Event) error {
		attempts.Add(1)
		return errors.New("still failing")
	}, Options{
		MaxAttempts:    5,
		ProcessTimeout: 50 * time.Millisecond,
		ReclaimMinIdle: 5 * time.Second,
	})

	publish(t, client, validEvent("evt-1"))
	c.process(context.Background(), readOne(t, c, client), 1)

	if err := c.reclaimOnce(context.Background()); err != nil {
		t.Fatalf("reclaimOnce: %v", err)
	}
	c.wg.Wait()

	if got := attempts.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1; message was reclaimed too early", got)
	}
}

// Once the retry budget is spent the message must leave the pending list, or it
// blocks reclaim scans forever and the backlog never drains.
func TestExhaustedRetriesAreDeadLettered(t *testing.T) {
	var attempts atomic.Int64
	c, client, _ := newHarness(t, func(context.Context, domain.Event) error {
		attempts.Add(1)
		return errors.New("permanent failure")
	}, Options{
		MaxAttempts:    2,
		ProcessTimeout: 20 * time.Millisecond,
		ReclaimMinIdle: 40 * time.Millisecond,
	})

	publish(t, client, validEvent("evt-1"))
	c.process(context.Background(), readOne(t, c, client), 1)

	// Each pass ages the entry past the idle window, reclaims it, and fails again
	// until the delivery count reaches MaxAttempts.
	for i := 0; i < 5; i++ {
		time.Sleep(c.opts.ReclaimMinIdle + 20*time.Millisecond)
		if err := c.reclaimOnce(context.Background()); err != nil {
			t.Fatalf("reclaimOnce: %v", err)
		}
		c.wg.Wait()
		if pendingCount(t, client) == 0 {
			break
		}
	}

	if got := pendingCount(t, client); got != 0 {
		t.Fatalf("pending = %d, want 0 after exhausting retries", got)
	}
	entries := dlqEntries(t, client)
	if len(entries) != 1 {
		t.Fatalf("dlq entries = %d, want 1", len(entries))
	}
	if got := entries[0].Values["dlq_reason"]; got != string(dlq.ReasonRetriesExhausted) {
		t.Fatalf("dlq_reason = %v, want %s", got, dlq.ReasonRetriesExhausted)
	}
	if got := attempts.Load(); got > 2 {
		t.Fatalf("handler ran %d times, want at most MaxAttempts=2", got)
	}
}

// If the DLQ write fails the message must stay pending. Acking it anyway would
// destroy the event with no record of it anywhere.
func TestDeadLetterFailureKeepsMessagePending(t *testing.T) {
	c, client, _ := newHarness(t, func(context.Context, domain.Event) error { return nil }, Options{})
	c.deadLetter = failingDeadLetterer{}

	publish(t, client, map[string]any{"tenant_id": "acme"})
	c.process(context.Background(), readOne(t, c, client), 1)

	if got := pendingCount(t, client); got != 1 {
		t.Fatalf("pending = %d, want 1 when the dead-letter write fails", got)
	}
}

func TestSampleOnceReportsBacklog(t *testing.T) {
	c, client, _ := newHarness(t, func(context.Context, domain.Event) error {
		return errors.New("fail")
	}, Options{})

	publish(t, client, validEvent("evt-1"))
	publish(t, client, validEvent("evt-2"))
	c.process(context.Background(), readOne(t, c, client), 1)

	if err := c.sampleOnce(context.Background()); err != nil {
		t.Fatalf("sampleOnce: %v", err)
	}
	if got := gaugeValue(t, "pulsebridge_worker_stream_length"); got != 2 {
		t.Fatalf("stream_length = %v, want 2", got)
	}
	if got := gaugeValue(t, "pulsebridge_worker_pending_entries"); got != 1 {
		t.Fatalf("pending_entries = %v, want 1", got)
	}
}

// Shutdown must not cut a handler off mid-write: Run returns only after
// in-flight work has finished and acked.
func TestRunDrainsInFlightWorkOnShutdown(t *testing.T) {
	var (
		started  = make(chan struct{})
		finished atomic.Bool
		once     sync.Once
	)

	c, client, _ := newHarness(t, func(context.Context, domain.Event) error {
		once.Do(func() { close(started) })
		time.Sleep(80 * time.Millisecond)
		finished.Store(true)
		return nil
	}, Options{
		BlockTimeout:    20 * time.Millisecond,
		ProcessTimeout:  2 * time.Second,
		ReclaimInterval: time.Hour,
		SampleInterval:  time.Hour,
	})

	publish(t, client, validEvent("evt-1"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("handler never started")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	if !finished.Load() {
		t.Fatal("Run returned before the in-flight handler finished")
	}
	if got := pendingCount(t, client); got != 0 {
		t.Fatalf("pending = %d, want 0; drained work should have been acked", got)
	}
}

func TestEnsureGroupIsIdempotent(t *testing.T) {
	c, _, _ := newHarness(t, func(context.Context, domain.Event) error { return nil }, Options{})
	if err := c.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("second EnsureGroup: %v", err)
	}
}

type failingDeadLetterer struct{}

func (failingDeadLetterer) Stream() string { return testDLQ }

func (failingDeadLetterer) Publish(context.Context, domain.Event, dlq.Reason, int64, error) error {
	return errors.New("dlq unavailable")
}
