package server

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestDuckDBAcceptsTranspiledCasts confirms the exact SQL forms emitted by the
// transpiler's fixupAST cast fixes execute against real DuckDB.
func TestDuckDBAcceptsTranspiledCasts(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	queries := []string{
		`SELECT (current_timestamp + CAST('2' || ' day' AS "interval"))`,
		`SELECT CAST(5 || ' day' AS "interval")`,
		`SELECT CAST('{}' AS "json")`,
		`SELECT json_extract(CAST('{"x":1}' AS "json"), 'x')`,
	}
	for _, q := range queries {
		var out interface{}
		if err := db.QueryRow(q).Scan(&out); err != nil {
			t.Fatalf("DuckDB rejected transpiled form %q: %v", q, err)
		}
	}
}
