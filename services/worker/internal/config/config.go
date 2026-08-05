package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the worker's full runtime surface. Everything is environment-driven
// so the same image runs locally, in compose and in Kubernetes without a build.
type Config struct {
	RedisURL     string
	StreamKey    string
	DLQStreamKey string
	Group        string
	Consumer     string
	Concurrency  int
	MetricsAddr  string
	Namespace    string
	DedupeTTL    time.Duration

	MaxAttempts     int64
	ProcessTimeout  time.Duration
	ReclaimMinIdle  time.Duration
	ReclaimInterval time.Duration
	SampleInterval  time.Duration
	DLQMaxLen       int64

	ShutdownTimeout time.Duration
}

// FromEnv reads configuration, falling back to defaults that are safe for a
// single-node local run. Invalid values fall back rather than crash: a typo in
// an env var should not take a consumer offline during an incident.
func FromEnv() Config {
	return Config{
		RedisURL:     getenv("PULSEBRIDGE_REDIS_URL", "redis://localhost:6379/0"),
		StreamKey:    getenv("PULSEBRIDGE_STREAM_KEY", "pulsebridge.events"),
		DLQStreamKey: getenv("PULSEBRIDGE_DLQ_STREAM_KEY", "pulsebridge.events.dlq"),
		Group:        getenv("PULSEBRIDGE_GROUP", "pulsebridge-workers"),
		// Defaulting the consumer name to the hostname gives each Kubernetes pod
		// a distinct entry in the pending list, which is what lets a surviving
		// replica tell whose messages it is reclaiming.
		Consumer:    getenv("PULSEBRIDGE_CONSUMER", defaultConsumerName()),
		Concurrency: getenvInt("PULSEBRIDGE_CONCURRENCY", 32),
		MetricsAddr: getenv("PULSEBRIDGE_METRICS_ADDR", ":9100"),
		Namespace:   getenv("PULSEBRIDGE_NAMESPACE", "pb"),
		// The dedupe window must outlive the retry budget by a wide margin,
		// otherwise a claim can expire between attempts and the same event is
		// applied twice.
		DedupeTTL: getenvDuration("PULSEBRIDGE_DEDUPE_TTL", 24*time.Hour),

		MaxAttempts:     int64(getenvInt("PULSEBRIDGE_MAX_ATTEMPTS", 5)),
		ProcessTimeout:  getenvDuration("PULSEBRIDGE_PROCESS_TIMEOUT", 5*time.Second),
		ReclaimMinIdle:  getenvDuration("PULSEBRIDGE_RECLAIM_MIN_IDLE", 30*time.Second),
		ReclaimInterval: getenvDuration("PULSEBRIDGE_RECLAIM_INTERVAL", 10*time.Second),
		SampleInterval:  getenvDuration("PULSEBRIDGE_SAMPLE_INTERVAL", 5*time.Second),
		DLQMaxLen:       int64(getenvInt("PULSEBRIDGE_DLQ_MAXLEN", 100_000)),

		ShutdownTimeout: getenvDuration("PULSEBRIDGE_SHUTDOWN_TIMEOUT", 15*time.Second),
	}
}

func defaultConsumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "worker-1"
	}
	return fmt.Sprintf("worker-%s", host)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}
