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
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// proxyTracer is the package-level tracer for cache-proxy spans. Until
// initTracing installs a real TracerProvider it resolves to the global no-op,
// so span calls are zero-cost when tracing is disabled.
var proxyTracer = otel.Tracer("duckgres/cache-proxy")

// initTracing configures an OTLP trace exporter when
// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT (or DUCKGRES_TRACE_ENDPOINT) is set,
// mirroring internal/cliboot.InitTracing. We replicate it here rather than
// import cliboot because cliboot transitively pulls in the DuckDB CGO runtime
// (via posthog/duckgres/server), which this standalone proxy must not link.
//
// Cache-proxy spans are emitted as their own root traces (DuckDB httpfs sends
// no traceparent), tagged with service.name=duckgres-cache-proxy so they're
// distinguishable from query traces in Tempo. They are NOT stitched into the
// query trace — cross-reference by time + s3 path + client.address (worker pod
// IP) + node. Returns a shutdown func that flushes the batch processor.
func initTracing() func() {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("DUCKGRES_TRACE_ENDPOINT")
	}
	if endpoint == "" {
		return func() {}
	}

	ctx := context.Background()

	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	}
	// VictoriaTraces uses /insert/opentelemetry/v1/traces instead of the
	// default /v1/traces. Allow overriding via OTEL_EXPORTER_OTLP_TRACES_PATH,
	// matching the main duckgres binary.
	if path := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PATH"); path != "" {
		opts = append(opts, otlptracehttp.WithURLPath(path))
	}
	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create trace exporter: %v\n", err)
		return func() {}
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("duckgres-cache-proxy"),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	proxyTracer = tp.Tracer("duckgres/cache-proxy")

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
