package dlq

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/domain"
	"github.com/redis/go-redis/v9"
)

func TestPublishPreservesTheOriginalEnvelope(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	publisher := New(client, "test.dlq", 100)
	event := domain.Event{
		StreamID:      "1690000000000-0",
		EventID:       "evt-1",
		TenantID:      "acme",
		EventType:     "order.created",
		PayloadJSON:   `{"order_id":"o-1"}`,
		OccurredAt:    "2026-08-03T10:00:00Z",
		CorrelationID: "corr-1",
		SourceStream:  "test.events",
	}

	err := publisher.Publish(context.Background(), event, ReasonRetriesExhausted, 5, errors.New("boom"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	entries, err := client.XRange(context.Background(), "test.dlq", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}

	values := entries[0].Values
	// The original fields must survive verbatim so a replay tool can re-publish
	// the event to the source stream without reconstructing it.
	for field, want := range map[string]string{
		"event_id":       "evt-1",
		"tenant_id":      "acme",
		"event_type":     "order.created",
		"payload":        `{"order_id":"o-1"}`,
		"occurred_at":    "2026-08-03T10:00:00Z",
		"correlation_id": "corr-1",
	} {
		if got := values[field]; got != want {
			t.Errorf("%s = %v, want %v", field, got, want)
		}
	}

	if got := values["dlq_reason"]; got != string(ReasonRetriesExhausted) {
		t.Errorf("dlq_reason = %v", got)
	}
	if got := values["dlq_attempts"]; got != "5" {
		t.Errorf("dlq_attempts = %v, want 5", got)
	}
	if got := values["dlq_error"]; got != "boom" {
		t.Errorf("dlq_error = %v, want boom", got)
	}
	if got := values["dlq_original_id"]; got != "1690000000000-0" {
		t.Errorf("dlq_original_id = %v", got)
	}
	if got := values["dlq_source_stream"]; got != "test.events" {
		t.Errorf("dlq_source_stream = %v", got)
	}
	if values["dlq_at"] == "" {
		t.Error("dlq_at must be set so operators can age out quarantined events")
	}
}

func TestPublishSurfacesWriteFailures(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	server.Close()

	publisher := New(client, "test.dlq", 100)
	err := publisher.Publish(context.Background(), domain.Event{EventID: "evt-1"}, ReasonUndecodable, 1, nil)
	if err == nil {
		t.Fatal("expected the caller to learn that quarantining failed")
	}
}
