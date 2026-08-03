package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/maksimvasilenko/pulsebridge/worker/internal/domain"
	"github.com/redis/go-redis/v9"
)

// RedisStore is a small durable projection: a per-tenant, per-type event
// counter guarded by a SET NX dedupe claim.
//
// The projection itself is intentionally trivial — the point of this repository
// is the delivery semantics around it, not the business logic inside it. What
// matters is that the claim and the write are separate, ordered steps, which is
// the same shape a real handler has when it writes to Postgres or calls a
// downstream API.
type RedisStore struct {
	client      redis.Cmdable
	claimPrefix string
	counterKey  string
	lastSeenKey string
}

func NewRedisStore(client redis.Cmdable, namespace string) *RedisStore {
	if namespace == "" {
		namespace = "pb"
	}
	return &RedisStore{
		client:      client,
		claimPrefix: fmt.Sprintf("%s:worker:claim:", namespace),
		counterKey:  fmt.Sprintf("%s:worker:events", namespace),
		lastSeenKey: fmt.Sprintf("%s:worker:last_seen", namespace),
	}
}

func (s *RedisStore) claimKey(eventID string) string {
	return s.claimPrefix + eventID
}

// Claim reserves an event id. SET NX is atomic across replicas, so exactly one
// concurrent delivery wins.
func (s *RedisStore) Claim(ctx context.Context, eventID string, ttl time.Duration) (bool, error) {
	return s.client.SetNX(ctx, s.claimKey(eventID), "1", ttl).Result()
}

func (s *RedisStore) Release(ctx context.Context, eventID string) error {
	return s.client.Del(ctx, s.claimKey(eventID)).Err()
}

func (s *RedisStore) Apply(ctx context.Context, event domain.Event) error {
	field := fmt.Sprintf("%s|%s", event.TenantID, event.EventType)
	pipe := s.client.TxPipeline()
	pipe.HIncrBy(ctx, s.counterKey, field, 1)
	pipe.HSet(ctx, s.lastSeenKey, field, event.OccurredAt)
	_, err := pipe.Exec(ctx)
	return err
}
