package server

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// tracer is the package-level tracer for the server package.
var tracer = otel.Tracer("duckgres/server")

// Tracer returns the server package tracer for use by other packages
// that need to create spans linked to server operations.
func Tracer() trace.Tracer {
	return tracer
}

// traceIDFromContext returns the hex trace ID from the span context, or "".
func traceIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}

// spanIDFromContext returns the hex span ID from the span context, or "".
func spanIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasSpanID() {
		return sc.SpanID().String()
	}
	return ""
}
