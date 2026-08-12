// Package observe holds duckgres' OpenTelemetry tracing helpers, the
// connection-count gauge, and the per-query Prometheus metrics emitted from
// the trace path. The package has no dependency on github.com/duckdb/duckdb-go,
// so the control plane and other duckdb-free callers can use it without
// linking libduckdb.
package observe

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/posthog/duckgres/server/sqlcore"
)

// tracer is the package-level OTEL tracer. Exposed via Tracer() so callers
// outside this package can start spans linked to server operations.
var tracer = otel.Tracer("duckgres/server")

// Tracer returns the package-level tracer.
func Tracer() trace.Tracer { return tracer }

// TruncateForSpan truncates a query string for use as a span attribute.
func TruncateForSpan(q string) string {
	const maxLen = 256
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen] + "..."
}

// ProfilingRoot represents the top-level DuckDB JSON profiling output.
type ProfilingRoot = profilingRoot

type profilingRoot struct {
	Latency                   float64             `json:"latency"`
	CPUTime                   float64             `json:"cpu_time"`
	RowsReturned              uint64              `json:"rows_returned"`
	ResultSetSize             uint64              `json:"result_set_size"`
	TotalMemoryAllocated      uint64              `json:"total_memory_allocated"`
	PeakBufferMemory          uint64              `json:"system_peak_buffer_memory"`
	TotalBytesRead            uint64              `json:"total_bytes_read"`
	Planner                   float64             `json:"planner"`
	PlannerBinding            float64             `json:"planner_binding"`
	CumulativeOptimizerTiming float64             `json:"cumulative_optimizer_timing"`
	PhysicalPlanner           float64             `json:"physical_planner"`
	Children                  []profilingOperator `json:"children"`
}

type profilingOperator struct {
	OperatorName        string              `json:"operator_name"`
	OperatorTiming      float64             `json:"operator_timing"`
	OperatorCardinality uint64              `json:"operator_cardinality"`
	OperatorRowsScanned uint64              `json:"operator_rows_scanned"`
	Children            []profilingOperator `json:"children"`
}

// ParseProfilingOutput extracts the full profiling tree from DuckDB's JSON
// output. Exposed for the integration test in package server, which captures
// real DuckDB profiling JSON and verifies our parser handles it.
func ParseProfilingOutput(jsonStr string) (profilingRoot, bool) {
	return parseProfilingOutput(jsonStr)
}

// parseProfilingOutput is the internal worker — kept private so the rest of
// observe can switch on the lowercase type without exposing it.
func parseProfilingOutput(jsonStr string) (profilingRoot, bool) {
	if jsonStr == "" {
		return profilingRoot{}, false
	}
	var root profilingRoot
	if err := json.Unmarshal([]byte(jsonStr), &root); err != nil {
		return profilingRoot{}, false
	}
	return root, true
}

// isScanOperator returns true for operators that represent data source access
// (metadata lookup + S3/file I/O + decode).
func isScanOperator(name string) bool {
	upper := strings.ToUpper(name)
	return strings.HasSuffix(upper, "_SCAN") || strings.Contains(upper, "SCAN")
}

// isPostgresScanOperator returns true for postgres_scanner operators —
// roundtrips into the DuckLake metadata Postgres. DuckDB emits these as
// POSTGRES_SCAN / POSTGRES_SCAN_PUSHDOWN under the postgres_scanner
// extension; both are caught by the POSTGRES_ prefix. Tracked separately
// from S3/parquet scans so we can attribute metadata-DB latency.
func isPostgresScanOperator(name string) bool {
	return strings.HasPrefix(strings.ToUpper(name), "POSTGRES_")
}

// collectOperatorTimings walks the operator tree and sums timings by category.
// pgScanTime/pgScanRows are a strict subset of scanTime/scanRows: postgres_scan
// counts as a scan operator AND is broken out separately so we can attribute
// metadata-DB roundtrip time without losing total scan accounting.
func collectOperatorTimings(ops []profilingOperator) (scanTime, scanRows float64, computeTime float64, pgScanTime, pgScanRows float64) {
	for _, op := range ops {
		if isScanOperator(op.OperatorName) {
			scanTime += op.OperatorTiming
			scanRows += float64(op.OperatorRowsScanned)
			if isPostgresScanOperator(op.OperatorName) {
				pgScanTime += op.OperatorTiming
				pgScanRows += float64(op.OperatorRowsScanned)
			}
		} else {
			computeTime += op.OperatorTiming
		}
		childScan, childScanRows, childCompute, childPgScan, childPgScanRows := collectOperatorTimings(op.Children)
		scanTime += childScan
		scanRows += childScanRows
		computeTime += childCompute
		pgScanTime += childPgScan
		pgScanRows += childPgScanRows
	}
	return
}

// QueryProfilingSummary is the per-query profiling rollup that
// EnrichSpanWithProfiling computes from DuckDB's profiling JSON. Callers
// (e.g. query_log) use it to persist a few high-signal fields without
// re-parsing the JSON.
type QueryProfilingSummary struct {
	// CPUTimeSeconds is DuckDB's cumulative query CPU/thread time in seconds.
	// Zero means either no profiling output, or DuckDB reported no CPU time.
	CPUTimeSeconds float64
	// PeakBufferMemoryBytes is DuckDB's system_peak_buffer_memory value.
	// It is DuckDB buffer memory, not process RSS or cgroup memory.
	PeakBufferMemoryBytes int64
	// PostgresScanSeconds is the thread-time spent inside postgres_scan
	// operators — DuckLake metadata DB roundtrips. Zero means either no
	// metadata access on this query, or no profiling output.
	PostgresScanSeconds float64
}

// EnrichSpanWithProfiling attaches DuckDB's completed profiling summary to the
// execution span. Operator timings are cumulative thread time, so they are
// recorded as aggregate attributes rather than fabricated sequential spans.
// It also emits Prometheus metrics for baseline measurement.
//
// Returns a per-query rollup so callers (e.g. query log) can persist key
// metrics without re-parsing the profiling JSON.
func EnrichSpanWithProfiling(_ context.Context, span trace.Span, _ time.Time, executor sqlcore.QueryExecutor, orgID string) QueryProfilingSummary {
	output := executor.LastProfilingOutput()
	if output == "" {
		return QueryProfilingSummary{}
	}
	m, ok := parseProfilingOutput(output)
	if !ok {
		return QueryProfilingSummary{}
	}

	span.SetAttributes(
		attribute.Float64("duckdb.latency_s", m.Latency),
		attribute.Float64("duckdb.cpu_time_s", m.CPUTime),
		attribute.Int64("duckdb.rows_returned", int64(m.RowsReturned)),
		attribute.Int64("duckdb.result_set_size", int64(m.ResultSetSize)),
		attribute.Int64("duckdb.total_memory_allocated", int64(m.TotalMemoryAllocated)),
		attribute.Int64("duckdb.peak_buffer_memory", int64(m.PeakBufferMemory)),
		attribute.Int64("duckdb.total_bytes_read", int64(m.TotalBytesRead)),
	)

	scanTime, scanRows, computeTime, pgScanTime, pgScanRows := collectOperatorTimings(m.Children)
	span.SetAttributes(
		attribute.Float64("duckdb.planner_s", m.Planner),
		attribute.Float64("duckdb.planner_binding_s", m.PlannerBinding),
		attribute.Float64("duckdb.optimizer_s", m.CumulativeOptimizerTiming),
		attribute.Float64("duckdb.physical_planner_s", m.PhysicalPlanner),
		attribute.Float64("duckdb.scan_thread_s", scanTime),
		attribute.Float64("duckdb.compute_thread_s", computeTime),
		attribute.Float64("duckdb.postgres_scan_thread_s", pgScanTime),
		attribute.Float64("duckdb.scan_rows_cumulative", scanRows),
		attribute.Float64("duckdb.postgres_scan_rows_cumulative", pgScanRows),
		attribute.String("duckdb.timing_kind", "cumulative_thread_time"),
	)

	S3BytesReadTotal.WithLabelValues(orgID).Add(float64(m.TotalBytesRead))
	PostgresScanSecondsHistogram.WithLabelValues(orgID).Observe(pgScanTime)

	return QueryProfilingSummary{
		CPUTimeSeconds:        m.CPUTime,
		PeakBufferMemoryBytes: int64(m.PeakBufferMemory),
		PostgresScanSeconds:   pgScanTime,
	}
}

// TraceIDFromContext returns the hex trace ID from the span context, or "".
func TraceIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}

// SpanIDFromContext returns the hex span ID from the span context, or "".
func SpanIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasSpanID() {
		return sc.SpanID().String()
	}
	return ""
}
