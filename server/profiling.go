package server

import (
	"context"
	"database/sql"
	"log/slog"
)

// ProfilingOutputPath is where DuckDB writes the per-query profile JSON.
// The duckdbservice worker reads this file after each query and forwards
// the contents to the control plane via gRPC trailer (see
// duckdbservice.sendProfilingMetadata) where EnrichSpanWithProfiling turns
// it into OTEL child spans.
const ProfilingOutputPath = "/tmp/duckgres-profiling.json"

// ProfilingSetupSQL returns the SQL statements that configure DuckDB
// profiling so the output is written to outputPath. These are
// session-scoped — DuckDB rejects `SET GLOBAL` for each one — so any
// caller that hands out fresh connections (e.g. after evictConnFromPool)
// must re-run them per connection.
func ProfilingSetupSQL(outputPath string) []string {
	return []string{
		// 'json' enables profiling and selects JSON output (the format the
		// control-plane parser expects).
		"SET enable_profiling = 'json'",
		// 'detailed' adds operator-level timings on top of the standard
		// query-level latency / row counts.
		"SET profiling_mode = 'detailed'",
		// Default coverage is 'SELECT', which silently skips writing the
		// profile file for INSERT / bare CREATE TABLE / etc. — see
		// TestProfilingCoverageAllCoversNonSelect.
		"SET profiling_coverage = 'ALL'",
		"SET profiling_output = '" + outputPath + "'",
	}
}

// ApplyProfilingSettings runs ProfilingSetupSQL against the given
// connection, writing profiles to ProfilingOutputPath. Use this for
// connections that bypass ConfigureMainDB — primarily fresh per-session
// connections in the cluster-mode worker, where eviction between sessions
// discards whatever settings ConfigureMainDB applied.
func ApplyProfilingSettings(ctx context.Context, conn *sql.Conn) {
	for _, stmt := range ProfilingSetupSQL(ProfilingOutputPath) {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			slog.Warn("Failed to apply DuckDB profiling setting on session conn.", "stmt", stmt, "error", err)
		}
	}
}
