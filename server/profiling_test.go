package server

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/posthog/duckgres/server/observe"
	"github.com/posthog/duckgres/server/sqlcore"
)

func TestProfilingOutputToFile(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	tmpFile := t.TempDir() + "/profiling.json"

	for _, stmt := range []string{
		"SET enable_profiling = 'json'",
		"SET profiling_mode = 'detailed'",
		"SET profiling_output = '" + tmpFile + "'",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// Run a query that produces profiling output
	if _, err := db.Exec("CREATE TABLE test_prof (id INT, name VARCHAR)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO test_prof VALUES (1, 'alice'), (2, 'bob')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	rows, err := db.Query("SELECT * FROM test_prof WHERE id > 0")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	_ = rows.Close()

	// Read profiling output from file
	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("read profiling file: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("profiling output file is empty")
	}

	// Parse and verify key fields
	m, ok := observe.ParseProfilingOutput(string(data))
	if !ok {
		t.Fatalf("failed to parse profiling output: %s", data[:min(200, len(data))])
	}
	if m.Latency <= 0 {
		t.Errorf("expected positive latency, got %f", m.Latency)
	}
	if m.RowsReturned != 2 {
		t.Errorf("expected 2 rows_returned, got %d", m.RowsReturned)
	}

	// Verify it's valid JSON with expected fields
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("profiling output is not valid JSON: %v", err)
	}
	for _, key := range []string{"latency", "cpu_time", "rows_returned", "result_set_size", "total_memory_allocated", "system_peak_buffer_memory"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("missing expected field %q in profiling output", key)
		}
	}
}

// TestProfilingCoverageAllCoversNonSelect pins the behavior that motivates
// `SET profiling_coverage = 'ALL'` in ConfigureMainDB: with the default
// coverage ('SELECT') DuckDB silently skips writing the profile file for
// some non-SELECT statements (notably INSERT and bare CREATE TABLE), so
// the worker's gRPC trailer comes back empty and EnrichSpanWithProfiling
// no-ops on the control plane. INSERT is used here because in DuckDB
// v1.5.3 it's the cleanest reproduction — CTAS already gets profiled
// under the default because of its embedded SELECT.
func TestProfilingCoverageAllCoversNonSelect(t *testing.T) {
	mustExec := func(t *testing.T, db *sql.DB, stmts ...string) {
		t.Helper()
		for _, s := range stmts {
			if _, err := db.Exec(s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
	}

	// Default coverage ('SELECT'): INSERT must NOT produce profile JSON.
	// If DuckDB ever changes the default to cover INSERT this will catch it
	// and we can re-evaluate whether the explicit 'ALL' is still needed.
	t.Run("default coverage skips INSERT", func(t *testing.T) {
		db, err := sql.Open("duckdb", ":memory:")
		if err != nil {
			t.Fatalf("open duckdb: %v", err)
		}
		defer func() { _ = db.Close() }()

		tmpFile := t.TempDir() + "/profiling.json"
		mustExec(t, db,
			"CREATE TABLE t (x INT)",
			"SET enable_profiling = 'json'",
			"SET profiling_mode = 'detailed'",
			"SET profiling_output = '"+tmpFile+"'",
		)
		mustExec(t, db, "INSERT INTO t VALUES (1), (2), (3)")

		data, err := os.ReadFile(tmpFile)
		if err == nil && len(data) > 0 {
			t.Fatalf("expected no profile JSON for INSERT under default coverage, got %d bytes", len(data))
		}
	})

	// coverage = 'ALL': INSERT must produce a parseable profile.
	t.Run("coverage=ALL covers INSERT", func(t *testing.T) {
		db, err := sql.Open("duckdb", ":memory:")
		if err != nil {
			t.Fatalf("open duckdb: %v", err)
		}
		defer func() { _ = db.Close() }()

		tmpFile := t.TempDir() + "/profiling.json"
		mustExec(t, db,
			"CREATE TABLE t (x INT)",
			"SET enable_profiling = 'json'",
			"SET profiling_mode = 'detailed'",
			"SET profiling_coverage = 'ALL'",
			"SET profiling_output = '"+tmpFile+"'",
		)
		mustExec(t, db, "INSERT INTO t VALUES (1), (2), (3)")

		data, err := os.ReadFile(tmpFile)
		if err != nil {
			t.Fatalf("read profiling file: %v", err)
		}
		if len(data) == 0 {
			t.Fatal("profiling output file is empty after INSERT with coverage='ALL'")
		}
		m, ok := observe.ParseProfilingOutput(string(data))
		if !ok {
			t.Fatalf("failed to parse profiling output: %s", data[:min(200, len(data))])
		}
		if m.Latency <= 0 {
			t.Errorf("expected positive latency, got %f", m.Latency)
		}
	})
}

type profilingOutputExecutor struct {
	output string
}

func (e profilingOutputExecutor) QueryContext(context.Context, string, ...any) (sqlcore.RowSet, error) {
	return nil, nil
}

func (e profilingOutputExecutor) ExecContext(context.Context, string, ...any) (sqlcore.ExecResult, error) {
	return nil, nil
}

func (e profilingOutputExecutor) Query(string, ...any) (sqlcore.RowSet, error) {
	return nil, nil
}

func (e profilingOutputExecutor) Exec(string, ...any) (sqlcore.ExecResult, error) {
	return nil, nil
}

func (e profilingOutputExecutor) ConnContext(context.Context) (sqlcore.RawConn, error) {
	return nil, nil
}

func (e profilingOutputExecutor) PingContext(context.Context) error {
	return nil
}

func (e profilingOutputExecutor) Close() error {
	return nil
}

func (e profilingOutputExecutor) LastProfilingOutput() string {
	return e.output
}

func TestEnrichSpanWithProfilingSummaryIncludesResourceUsage(t *testing.T) {
	ctx := context.Background()
	ctx, span := observe.Tracer().Start(ctx, "test")
	defer span.End()

	summary := observe.EnrichSpanWithProfiling(ctx, span, time.Now(), profilingOutputExecutor{output: `{
		"latency": 2.5,
		"cpu_time": 4.25,
		"rows_returned": 3,
		"result_set_size": 128,
		"total_memory_allocated": 1024,
		"system_peak_buffer_memory": 2048,
		"total_bytes_read": 4096,
		"children": []
	}`}, "org-a")

	if summary.CPUTimeSeconds != 4.25 {
		t.Fatalf("expected CPUTimeSeconds 4.25, got %f", summary.CPUTimeSeconds)
	}
	if summary.PeakBufferMemoryBytes != 2048 {
		t.Fatalf("expected PeakBufferMemoryBytes 2048, got %d", summary.PeakBufferMemoryBytes)
	}
}

// TestProfilingSettingsArePerConnection reproduces the cluster-mode bug:
// DuckDB profiling settings are session-scoped (DuckDB rejects `SET GLOBAL`
// for `enable_profiling` etc.), so when the worker's evictConnFromPool
// discards a session's connection between sessions, the next fresh
// connection has no profiling configured and the profile file stays
// untouched. ProfilingSetupSQL applied per-connection is the fix.
func TestProfilingSettingsArePerConnection(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	// Two slots so we can pin two distinct underlying connections.
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)

	ctx := context.Background()
	tmpFile := t.TempDir() + "/profiling.json"
	setup := ProfilingSetupSQL(tmpFile)

	// Connection A: full setup, simulates the warmup conn that ConfigureMainDB
	// ran on. Apply the production setup statements directly so we're testing
	// the same SQL the real code path uses.
	connA, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn A: %v", err)
	}
	defer func() { _ = connA.Close() }()
	if _, err := connA.ExecContext(ctx, "CREATE TABLE t (x INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for _, s := range setup {
		if _, err := connA.ExecContext(ctx, s); err != nil {
			t.Fatalf("connA %s: %v", s, err)
		}
	}

	// Connection B: pretend we've evicted A and the pool handed out a fresh
	// conn. Without per-session re-application, this conn has no profiling
	// settings.
	connB, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn B: %v", err)
	}
	defer func() { _ = connB.Close() }()

	_ = os.Remove(tmpFile)
	if _, err := connB.ExecContext(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("connB pre-fix INSERT: %v", err)
	}
	if data, err := os.ReadFile(tmpFile); err == nil && len(data) > 0 {
		t.Fatalf("pre-fix sanity: fresh conn should not produce profile JSON, got %d bytes", len(data))
	}

	// Now apply the same setup to connB and re-run the INSERT — this is what
	// duckdbservice.CreateSession does via server.ApplyProfilingSettings.
	for _, s := range setup {
		if _, err := connB.ExecContext(ctx, s); err != nil {
			t.Fatalf("connB re-apply %s: %v", s, err)
		}
	}
	_ = os.Remove(tmpFile)
	if _, err := connB.ExecContext(ctx, "INSERT INTO t VALUES (2)"); err != nil {
		t.Fatalf("connB post-fix INSERT: %v", err)
	}
	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("read profile after re-apply: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("post-fix: profile file is empty after INSERT — re-applying setup should have re-enabled profiling")
	}
	m, ok := observe.ParseProfilingOutput(string(data))
	if !ok {
		t.Fatalf("failed to parse profile JSON: %s", data[:min(200, len(data))])
	}
	if m.Latency <= 0 {
		t.Errorf("expected positive latency, got %f", m.Latency)
	}

	// Belt-and-braces: confirm a fresh conn after eviction really has empty
	// settings — i.e. nothing about ProfilingSetupSQL silently sets state on
	// the underlying driver/connector that survives across connections.
	_ = connB.Raw(func(any) error { return driver.ErrBadConn })
	connC, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn C: %v", err)
	}
	defer func() { _ = connC.Close() }()
	rows, err := connC.QueryContext(ctx, "SELECT value FROM duckdb_settings() WHERE name = 'enable_profiling'")
	if err != nil {
		t.Fatalf("connC query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("expected one row from duckdb_settings()")
	}
	var got string
	_ = rows.Scan(&got)
	if got != "" {
		t.Fatalf("fresh conn C should have empty enable_profiling, got %q — settings unexpectedly survived", got)
	}
}
