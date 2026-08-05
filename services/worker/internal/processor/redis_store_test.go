package processor

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/domain"
	"github.com/redis/go-redis/v9"
)

func newStore(t *testing.T) (*RedisStore, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisStore(client, "test"), client
}

func TestRedisStoreClaimIsExclusive(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	first, err := store.Claim(ctx, "evt-1", time.Minute)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	second, err := store.Claim(ctx, "evt-1", time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}

	if !first || second {
		t.Fatalf("claims = (%v, %v), want (true, false)", first, second)
	}
}

func TestRedisStoreReleaseAllowsReclaim(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	if _, err := store.Claim(ctx, "evt-1", time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Release(ctx, "evt-1"); err != nil {
		t.Fatalf("release: %v", err)
	}

	again, err := store.Claim(ctx, "evt-1", time.Minute)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if !again {
		t.Fatal("expected the event id to be claimable after release")
	}
}

func TestRedisStoreApplyAccumulatesPerTenantAndType(t *testing.T) {
	store, client := newStore(t)
	ctx := context.Background()

	events := []domain.Event{
		{TenantID: "acme", EventType: "order.created", OccurredAt: "2026-08-03T10:00:00Z"},
		{TenantID: "acme", EventType: "order.created", OccurredAt: "2026-08-03T10:00:01Z"},
		{TenantID: "globex", EventType: "order.created", OccurredAt: "2026-08-03T10:00:02Z"},
	}
	for _, event := range events {
		if err := store.Apply(ctx, event); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	counters, err := client.HGetAll(ctx, "test:worker:events").Result()
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	if counters["acme|order.created"] != "2" {
		t.Fatalf("acme counter = %q, want 2", counters["acme|order.created"])
	}
	if counters["globex|order.created"] != "1" {
		t.Fatalf("globex counter = %q, want 1", counters["globex|order.created"])
	}

	lastSeen, err := client.HGet(ctx, "test:worker:last_seen", "acme|order.created").Result()
	if err != nil {
		t.Fatalf("HGet: %v", err)
	}
	if lastSeen != "2026-08-03T10:00:01Z" {
		t.Fatalf("last_seen = %q, want the most recent occurred_at", lastSeen)
	}
}
