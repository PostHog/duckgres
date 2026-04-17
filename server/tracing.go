package server

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// tracer is the package-level tracer for the server package.
var tracer = otel.Tracer("duckgres/server")

// Tracer returns the server package tracer for use by other packages
// that need to create spans linked to server operations.
func Tracer() trace.Tracer {
	return tracer
}

// truncateForSpan truncates a query string for use as a span attribute.
func truncateForSpan(q string) string {
	const maxLen = 256
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen] + "..."
}

// profilingMetrics holds key metrics extracted from DuckDB's JSON profiling output.
type profilingMetrics struct {
	Latency              float64 // total wall-clock time (seconds)
	CPUTime              float64 // cumulative operator execution time (seconds)
	RowsReturned         uint64  // number of rows in the result
	ResultSetSize        uint64  // result size in bytes
	TotalMemoryAllocated uint64  // total buffer manager allocation in bytes
	PeakBufferMemory     uint64  // peak memory usage in bytes
}

// parseProfilingOutput extracts key metrics from DuckDB's JSON profiling output.
func parseProfilingOutput(jsonStr string) (profilingMetrics, bool) {
	if jsonStr == "" {
		return profilingMetrics{}, false
	}
	var root struct {
		Latency              float64 `json:"latency"`
		CPUTime              float64 `json:"cpu_time"`
		RowsReturned         uint64  `json:"rows_returned"`
		ResultSetSize        uint64  `json:"result_set_size"`
		TotalMemoryAllocated uint64  `json:"total_memory_allocated"`
		PeakBufferMemory     uint64  `json:"system_peak_buffer_memory"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &root); err != nil {
		return profilingMetrics{}, false
	}
	return profilingMetrics{
		Latency:              root.Latency,
		CPUTime:              root.CPUTime,
		RowsReturned:         root.RowsReturned,
		ResultSetSize:        root.ResultSetSize,
		TotalMemoryAllocated: root.TotalMemoryAllocated,
		PeakBufferMemory:     root.PeakBufferMemory,
	}, true
}

// enrichSpanWithProfiling adds DuckDB profiling metrics as attributes on the span.
func enrichSpanWithProfiling(span trace.Span, executor QueryExecutor) {
	output := executor.LastProfilingOutput()
	if output == "" {
		return
	}
	m, ok := parseProfilingOutput(output)
	if !ok {
		return
	}
	span.SetAttributes(
		attribute.Float64("duckdb.latency_s", m.Latency),
		attribute.Float64("duckdb.cpu_time_s", m.CPUTime),
		attribute.Float64("duckdb.overhead_s", m.Latency-m.CPUTime),
		attribute.Int64("duckdb.rows_returned", int64(m.RowsReturned)),
		attribute.Int64("duckdb.result_set_size", int64(m.ResultSetSize)),
		attribute.Int64("duckdb.total_memory_allocated", int64(m.TotalMemoryAllocated)),
		attribute.Int64("duckdb.peak_buffer_memory", int64(m.PeakBufferMemory)),
	)
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
