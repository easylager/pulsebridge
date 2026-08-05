package consumer

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNormaliseAppliesDefaults(t *testing.T) {
	opts := Options{}.Normalise()

	if opts.Stream == "" || opts.Group == "" || opts.Consumer == "" {
		t.Fatalf("identity defaults missing: %+v", opts)
	}
	if opts.Concurrency < 1 {
		t.Fatalf("Concurrency = %d, want >= 1", opts.Concurrency)
	}
	if opts.MaxAttempts < 1 {
		t.Fatalf("MaxAttempts = %d, want >= 1", opts.MaxAttempts)
	}
}

// The reclaim window is derived from the process timeout rather than trusted
// from config: a window shorter than a handler's own deadline makes every slow
// event get picked up a second time while the first attempt is still running.
func TestNormaliseRaisesReclaimWindowAboveProcessTimeout(t *testing.T) {
	opts := Options{
		ProcessTimeout: 30 * time.Second,
		ReclaimMinIdle: time.Second,
	}.Normalise()

	if opts.ReclaimMinIdle <= opts.ProcessTimeout {
		t.Fatalf("ReclaimMinIdle = %s, must exceed ProcessTimeout = %s",
			opts.ReclaimMinIdle, opts.ProcessTimeout)
	}
}

func TestNormalisePreservesExplicitValues(t *testing.T) {
	opts := Options{
		Stream:         "custom.stream",
		Group:          "custom-group",
		Consumer:       "custom-consumer",
		Concurrency:    7,
		MaxAttempts:    9,
		ProcessTimeout: time.Second,
		ReclaimMinIdle: time.Minute,
	}.Normalise()

	if opts.Stream != "custom.stream" || opts.Group != "custom-group" || opts.Consumer != "custom-consumer" {
		t.Fatalf("identity overridden: %+v", opts)
	}
	if opts.Concurrency != 7 || opts.MaxAttempts != 9 {
		t.Fatalf("limits overridden: %+v", opts)
	}
	if opts.ReclaimMinIdle != time.Minute {
		t.Fatalf("ReclaimMinIdle = %s, want 1m", opts.ReclaimMinIdle)
	}
}

func gaugeValue(t *testing.T, name string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metric.GetGauge() != nil {
				return metric.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("gauge %q not found", name)
	return 0
}
