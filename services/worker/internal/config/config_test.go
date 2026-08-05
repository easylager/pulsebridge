package config

import (
	"testing"
	"time"
)

func TestFromEnvDefaults(t *testing.T) {
	cfg := FromEnv()

	if cfg.StreamKey != "pulsebridge.events" {
		t.Fatalf("StreamKey = %q", cfg.StreamKey)
	}
	if cfg.DLQStreamKey != "pulsebridge.events.dlq" {
		t.Fatalf("DLQStreamKey = %q", cfg.DLQStreamKey)
	}
	if cfg.MaxAttempts != 5 {
		t.Fatalf("MaxAttempts = %d, want 5", cfg.MaxAttempts)
	}
	if cfg.ProcessTimeout != 5*time.Second {
		t.Fatalf("ProcessTimeout = %s, want 5s", cfg.ProcessTimeout)
	}
}

// Each replica needs a distinct consumer name so the pending entries list shows
// whose messages are stuck and another replica can reclaim them.
func TestConsumerNameDefaultsToSomethingUnique(t *testing.T) {
	if cfg := FromEnv(); cfg.Consumer == "" {
		t.Fatal("Consumer must never be empty")
	}
}

func TestFromEnvReadsOverrides(t *testing.T) {
	t.Setenv("PULSEBRIDGE_STREAM_KEY", "custom.events")
	t.Setenv("PULSEBRIDGE_CONSUMER", "worker-7")
	t.Setenv("PULSEBRIDGE_CONCURRENCY", "128")
	t.Setenv("PULSEBRIDGE_MAX_ATTEMPTS", "3")
	t.Setenv("PULSEBRIDGE_PROCESS_TIMEOUT", "250ms")
	t.Setenv("PULSEBRIDGE_RECLAIM_MIN_IDLE", "1m")

	cfg := FromEnv()

	if cfg.StreamKey != "custom.events" || cfg.Consumer != "worker-7" {
		t.Fatalf("identity overrides ignored: %+v", cfg)
	}
	if cfg.Concurrency != 128 || cfg.MaxAttempts != 3 {
		t.Fatalf("limit overrides ignored: %+v", cfg)
	}
	if cfg.ProcessTimeout != 250*time.Millisecond || cfg.ReclaimMinIdle != time.Minute {
		t.Fatalf("duration overrides ignored: %+v", cfg)
	}
}

// A typo in an environment variable should not take a consumer offline during
// an incident, so unparsable values fall back to the default.
func TestInvalidValuesFallBackToDefaults(t *testing.T) {
	t.Setenv("PULSEBRIDGE_CONCURRENCY", "not-a-number")
	t.Setenv("PULSEBRIDGE_MAX_ATTEMPTS", "-4")
	t.Setenv("PULSEBRIDGE_PROCESS_TIMEOUT", "soon")

	cfg := FromEnv()

	if cfg.Concurrency != 32 {
		t.Fatalf("Concurrency = %d, want the default 32", cfg.Concurrency)
	}
	if cfg.MaxAttempts != 5 {
		t.Fatalf("MaxAttempts = %d, want the default 5", cfg.MaxAttempts)
	}
	if cfg.ProcessTimeout != 5*time.Second {
		t.Fatalf("ProcessTimeout = %s, want the default 5s", cfg.ProcessTimeout)
	}
}
