package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/maksimvasilenko/pulsebridge/worker/internal/domain"
	"github.com/redis/go-redis/v9"
)

type Handler func(context.Context, domain.Event) error

type StreamConsumer struct {
	rdb         *redis.Client
	stream      string
	group       string
	consumer    string
	concurrency int
	handler     Handler
	log         *slog.Logger
}

func New(rdb *redis.Client, stream, group, consumer string, concurrency int, handler Handler, log *slog.Logger) *StreamConsumer {
	if concurrency < 1 {
		concurrency = 1
	}
	return &StreamConsumer{
		rdb:         rdb,
		stream:      stream,
		group:       group,
		consumer:    consumer,
		concurrency: concurrency,
		handler:     handler,
		log:         log,
	}
}

func (c *StreamConsumer) EnsureGroup(ctx context.Context) error {
	err := c.rdb.XGroupCreateMkStream(ctx, c.stream, c.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (c *StreamConsumer) Run(ctx context.Context) error {
	sem := make(chan struct{}, c.concurrency)
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		default:
		}

		streams, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.consumer,
			Streams:  []string{c.stream, ">"},
			Count:    int64(c.concurrency),
			Block:    2 * time.Second,
		}).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				wg.Wait()
				return ctx.Err()
			}
			c.log.Error("xreadgroup failed", "err", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		for _, st := range streams {
			for _, msg := range st.Messages {
				event, err := mapEvent(msg)
				if err != nil {
					c.log.Error("bad message", "id", msg.ID, "err", err)
					_ = c.rdb.XAck(ctx, c.stream, c.group, msg.ID).Err()
					continue
				}

				sem <- struct{}{}
				wg.Add(1)
				go func(m redis.XMessage, e domain.Event) {
					defer wg.Done()
					defer func() { <-sem }()

					pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					if err := c.handler(pctx, e); err != nil {
						c.log.Error("process failed", "event_id", e.EventID, "err", err)
						return
					}
					if err := c.rdb.XAck(ctx, c.stream, c.group, m.ID).Err(); err != nil {
						c.log.Error("ack failed", "id", m.ID, "err", err)
					}
				}(msg, event)
			}
		}
	}
}

func mapEvent(msg redis.XMessage) (domain.Event, error) {
	get := func(k string) string {
		if v, ok := msg.Values[k]; ok {
			return fmt.Sprint(v)
		}
		return ""
	}
	e := domain.Event{
		StreamID:      msg.ID,
		EventID:       get("event_id"),
		TenantID:      get("tenant_id"),
		EventType:     get("event_type"),
		PayloadJSON:   get("payload"),
		OccurredAt:    get("occurred_at"),
		CorrelationID: get("correlation_id"),
	}
	if e.EventID == "" {
		return e, fmt.Errorf("missing event_id")
	}
	return e, nil
}
