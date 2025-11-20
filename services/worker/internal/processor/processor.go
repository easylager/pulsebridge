package processor

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/maksimvasilenko/pulsebridge/worker/internal/domain"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/metrics"
)

// Processor applies CPU-light but concurrency-heavy work:
// tenant sharding simulation + payload validation + artificial jitter-free checksum.
type Processor struct{}

func New() *Processor { return &Processor{} }

func (p *Processor) Process(ctx context.Context, event domain.Event) error {
	start := time.Now()
	metrics.InFlight.Inc()
	defer func() {
		metrics.InFlight.Dec()
		metrics.Latency.Observe(time.Since(start).Seconds())
	}()

	select {
	case <-ctx.Done():
		metrics.Processed.WithLabelValues("canceled").Inc()
		return ctx.Err()
	default:
	}

	if event.EventID == "" || event.TenantID == "" || event.EventType == "" {
		metrics.Processed.WithLabelValues("invalid").Inc()
		return fmt.Errorf("invalid event envelope")
	}

	payload, err := event.PayloadMap()
	if err != nil {
		metrics.Processed.WithLabelValues("invalid_payload").Inc()
		return fmt.Errorf("payload decode: %w", err)
	}

	// Deterministic shard key — stands in for partition affinity in a real broker.
	_ = shard(event.TenantID)
	_ = payload

	// Simulate durable side-effect boundary (would be DB/outbox/HTTP in production).
	metrics.Processed.WithLabelValues("ok").Inc()
	return nil
}

func shard(tenantID string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(tenantID))
	return h.Sum32() % 256
}
