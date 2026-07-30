package duckdbservice

import (
	"database/sql"
	"testing"

	"github.com/posthog/duckgres/server"
)

// TestOpenDuckDBPairSetsLateMaterializationMaxRows is the regression guard for
// the worker rollout that caps late materialization: every worker DB opened via
// OpenDuckDBPair must have late_materialization_max_rows = 6000.
//
// The assertion runs on a *fresh* connection from the same connector — the
// same shape as a session-pool connection serving user queries — so it also
// proves the setting is visible beyond the warmup connection that applied it.
func TestOpenDuckDBPairSetsLateMaterializationMaxRows(t *testing.T) {
	cfg := server.Config{DataDir: t.TempDir()}
	pair, err := OpenDuckDBPair(cfg, "worker")
	if err != nil {
		t.Fatalf("OpenDuckDBPair: %v", err)
	}
	defer func() { _ = pair.Close() }()

	sessionDB := sql.OpenDB(pair.connector)
	defer func() { _ = sessionDB.Close() }()

	var got string
	if err := sessionDB.QueryRow("SELECT current_setting('late_materialization_max_rows')").Scan(&got); err != nil {
		t.Fatalf("read late_materialization_max_rows: %v", err)
	}
	if got != "6000" {
		t.Errorf("late_materialization_max_rows = %q, want %q", got, "6000")
	}
}
