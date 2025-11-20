package config

import (
	"os"
	"strconv"
)

type Config struct {
	RedisURL    string
	StreamKey   string
	Group       string
	Consumer    string
	Concurrency int
	MetricsAddr string
}

func FromEnv() Config {
	return Config{
		RedisURL:    getenv("PULSEBRIDGE_REDIS_URL", "redis://localhost:6379/0"),
		StreamKey:   getenv("PULSEBRIDGE_STREAM_KEY", "pulsebridge.events"),
		Group:       getenv("PULSEBRIDGE_GROUP", "pulsebridge-workers"),
		Consumer:    getenv("PULSEBRIDGE_CONSUMER", "worker-1"),
		Concurrency: getenvInt("PULSEBRIDGE_CONCURRENCY", 32),
		MetricsAddr: getenv("PULSEBRIDGE_METRICS_ADDR", ":9100"),
	}
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
