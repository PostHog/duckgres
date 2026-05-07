package server

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	"github.com/posthog/duckgres/server/observe"
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
// v1.5.2 it's the cleanest reproduction — CTAS already gets profiled
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
