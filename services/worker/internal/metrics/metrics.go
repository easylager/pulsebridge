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
)

func Handler() http.Handler {
	return promhttp.Handler()
}
