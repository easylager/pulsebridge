// Package telemetry wires OpenTelemetry tracing into the worker.
//
// The gateway writes a W3C traceparent into every stream entry, so a span
// started here can be linked to the HTTP request that produced the event. That
// link is the difference between "the queue is slow" and "tenant X's oversized
// payloads are slow", which is exactly the question that is hard to answer from
// metrics alone once producer and consumer are different processes in different
// languages.
package telemetry

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/maksimvasilenko/pulsebridge/worker/internal/domain"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/maksimvasilenko/pulsebridge/worker"

// Shutdown flushes buffered spans. Always non-nil, so callers can defer it
// unconditionally.
type Shutdown func(context.Context) error

// Setup installs a global tracer provider and the W3C propagator.
//
// With no OTLP endpoint configured it installs propagation only and leaves the
// no-op tracer in place: the repository must stay runnable with `go run` and in
// CI without anyone standing up a collector first.
func Setup(ctx context.Context, serviceName, version string) (Shutdown, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return func(context.Context) error { return nil }, fmt.Errorf("otlp exporter: %w", err)
	}

	// NewSchemaless rather than NewWithAttributes: the default resource carries
	// whatever schema URL the SDK ships with, and merging it with a pinned
	// semconv version fails outright on a mismatch. Leaving these attributes
	// schemaless makes the merge survive an SDK upgrade.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion(version),
	))
	if err != nil {
		return func(context.Context) error { return nil }, fmt.Errorf("otel resource: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)

	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return provider.Shutdown(ctx)
	}, nil
}

// ContextFromTraceParent rebuilds the producer's trace context from the
// traceparent header carried in the stream entry. An empty or malformed value
// yields the parent context unchanged, so a producer without tracing degrades
// to an unlinked span rather than an error.
func ContextFromTraceParent(ctx context.Context, traceParent string) context.Context {
	if traceParent == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{"traceparent": traceParent}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// StartConsumeSpan opens a span for one event delivery, following the
// OpenTelemetry messaging semantic conventions so it renders correctly in
// Jaeger, Tempo and vendor backends without custom mapping.
func StartConsumeSpan(ctx context.Context, event domain.Event) (context.Context, trace.Span) {
	return otel.Tracer(tracerName).Start(ctx,
		fmt.Sprintf("%s process", event.SourceStream),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("redis_streams"),
			semconv.MessagingDestinationName(event.SourceStream),
			semconv.MessagingMessageID(event.EventID),
			attribute.String("pulsebridge.tenant_id", event.TenantID),
			attribute.String("pulsebridge.event_type", event.EventType),
			attribute.String("pulsebridge.correlation_id", event.CorrelationID),
			attribute.Int64("pulsebridge.delivery_attempt", event.Attempt),
		),
	)
}

// Traced wraps a handler so every delivery produces a span that continues the
// producer's trace.
func Traced(next func(context.Context, domain.Event) error) func(context.Context, domain.Event) error {
	return func(ctx context.Context, event domain.Event) error {
		ctx = ContextFromTraceParent(ctx, event.TraceParent)
		ctx, span := StartConsumeSpan(ctx, event)
		defer span.End()

		err := next(ctx, event)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}
}
