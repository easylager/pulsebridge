package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	Processed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulsebridge_worker_processed_total",
		Help: "Processed events by result",
	}, []string{"result"})

	InFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulsebridge_worker_inflight",
		Help: "Events currently being processed",
	})

	Latency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pulsebridge_worker_process_seconds",
		Help:    "Event processing latency",
		Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
	})

	// Retries counts deliveries that were re-attempted after a previous failure,
	// i.e. messages picked up again from the pending entries list.
	Retries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsebridge_worker_retries_total",
		Help: "Redeliveries of events that previously failed",
	})

	// Reclaimed counts messages taken over from a stalled consumer via XAUTOCLAIM.
	Reclaimed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsebridge_worker_reclaimed_total",
		Help: "Messages reclaimed from stalled consumers",
	})

	// DeadLettered is the terminal outcome: the event exceeded its retry budget
	// or could never be parsed, and was moved to the dead-letter stream.
	DeadLettered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulsebridge_worker_dead_lettered_total",
		Help: "Events routed to the dead-letter stream by reason",
	}, []string{"reason"})

	// DeadLetterFailures means the DLQ write itself failed — the event is neither
	// processed nor quarantined, so this must alert.
	DeadLetterFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsebridge_worker_dead_letter_failures_total",
		Help: "Failed writes to the dead-letter stream",
	})

	StreamLength = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulsebridge_worker_stream_length",
		Help: "Approximate number of entries retained in the source stream",
	})

	// PendingEntries is the consumer-group backlog: delivered but not yet acked.
	// This is the closest Redis Streams equivalent of consumer lag.
	PendingEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulsebridge_worker_pending_entries",
		Help: "Messages delivered to the consumer group but not yet acknowledged",
	})

	// OldestPendingSeconds exposes how long the oldest unacked message has been
	// stuck. A growing value means a consumer died or a handler is wedged.
	OldestPendingSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulsebridge_worker_oldest_pending_seconds",
		Help: "Idle time of the oldest unacknowledged message",
	})

	SamplerErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsebridge_worker_sampler_errors_total",
		Help: "Failed backlog sampling attempts",
	})
)

func Handler() http.Handler {
	return promhttp.Handler()
}
