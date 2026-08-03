package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/maksimvasilenko/pulsebridge/worker/internal/config"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/consumer"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/dlq"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/metrics"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/processor"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/telemetry"
	"github.com/redis/go-redis/v9"
)

const version = "0.2.0"

func main() {
	if err := run(); err != nil {
		slog.Default().Error("worker exited with error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.FromEnv()
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.Setup(ctx, "pulsebridge-worker", version)
	if err != nil {
		// Tracing is an observability aid, not a correctness dependency: log the
		// failure and keep consuming rather than refusing to start.
		log.Warn("tracing disabled", "err", err)
	}

	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(opt)
	defer func() { _ = rdb.Close() }()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return err
	}

	deadLetter := dlq.New(rdb, cfg.DLQStreamKey, cfg.DLQMaxLen)
	proc := processor.New(processor.NewRedisStore(rdb, cfg.Namespace), cfg.DedupeTTL)

	c := consumer.New(rdb, deadLetter, telemetry.Traced(proc.Process), consumer.Options{
		Stream:          cfg.StreamKey,
		Group:           cfg.Group,
		Consumer:        cfg.Consumer,
		Concurrency:     cfg.Concurrency,
		MaxAttempts:     cfg.MaxAttempts,
		ProcessTimeout:  cfg.ProcessTimeout,
		ReclaimMinIdle:  cfg.ReclaimMinIdle,
		ReclaimInterval: cfg.ReclaimInterval,
		SampleInterval:  cfg.SampleInterval,
	}, log)

	if err := c.EnsureGroup(ctx); err != nil {
		return err
	}

	// Readiness flips to false the moment a shutdown signal arrives, so the load
	// balancer and any orchestrator stop routing to this pod while it drains.
	var ready atomic.Bool
	ready.Store(true)
	srv := metricsServer(cfg.MetricsAddr, &ready)

	go func() {
		log.Info("metrics listening", "addr", cfg.MetricsAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "err", err)
			stop()
		}
	}()

	effective := c.Options()
	log.Info("worker started",
		"version", version,
		"stream", effective.Stream,
		"dlq_stream", deadLetter.Stream(),
		"group", effective.Group,
		"consumer", effective.Consumer,
		"concurrency", effective.Concurrency,
		"max_attempts", effective.MaxAttempts,
		"process_timeout", effective.ProcessTimeout.String(),
		"reclaim_min_idle", effective.ReclaimMinIdle.String(),
	)

	// Run returns only after in-flight handlers have drained.
	if err := c.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("consumer stopped", "err", err)
	}
	ready.Store(false)
	log.Info("consumer drained")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	if shutdownTracing != nil {
		_ = shutdownTracing(shutdownCtx)
	}

	log.Info("worker shutdown complete")
	return nil
}

func metricsServer(addr string, ready *atomic.Bool) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("draining"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}
