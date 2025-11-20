package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/maksimvasilenko/pulsebridge/worker/internal/config"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/consumer"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/metrics"
	"github.com/maksimvasilenko/pulsebridge/worker/internal/processor"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.FromEnv()
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Error("invalid redis url", "err", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(opt)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis ping failed", "err", err)
		os.Exit(1)
	}

	proc := processor.New()
	c := consumer.New(rdb, cfg.StreamKey, cfg.Group, cfg.Consumer, cfg.Concurrency, proc.Process, log)
	if err := c.EnsureGroup(ctx); err != nil {
		log.Error("ensure group failed", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux}

	go func() {
		log.Info("metrics listening", "addr", cfg.MetricsAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server failed", "err", err)
			stop()
		}
	}()

	log.Info(
		"worker started",
		"stream", cfg.StreamKey,
		"group", cfg.Group,
		"consumer", cfg.Consumer,
		"concurrency", cfg.Concurrency,
	)

	err = c.Run(ctx)
	if err != nil && !strings.Contains(err.Error(), "context canceled") {
		log.Error("consumer stopped", "err", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = rdb.Close()
	log.Info("worker shutdown complete")
}
