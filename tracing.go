package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// initTracing configures an OTLP trace exporter when
// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT (or DUCKGRES_TRACE_ENDPOINT) is set.
// When no endpoint is configured, the global TracerProvider remains the
// default no-op, adding zero overhead.
// Returns a shutdown function that flushes the batch span processor.
func initTracing() func() {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("DUCKGRES_TRACE_ENDPOINT")
	}
	if endpoint == "" {
		return func() {}
	}

	ctx := context.Background()

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create trace exporter: %v\n", err)
		return func() {}
	}

	res := otelResource()

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	fmt.Fprintf(os.Stderr, "OTEL tracing enabled, exporting to %s\n", endpoint)

	var once sync.Once
	return func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tp.Shutdown(ctx)
		})
	}
}
