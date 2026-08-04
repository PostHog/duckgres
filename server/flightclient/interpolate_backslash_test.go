package flightclient

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// Audit H5 regression test.
//
// formatArgValue doubles backslashes (`\` → `\\`) before wrapping the value
// in a single-quoted literal. DuckDB standard `'...'` literals — like
// standard-conforming PostgreSQL — treat backslash as a literal character,
// so `'a\\b'` is the four-character string `a\\b`, not `a\b`. Every bound
// string parameter containing a backslash (Windows paths, regexes, JSON with
// escapes) that flows through the Flight executor's interpolation is
// therefore silently stored/compared with doubled backslashes.
//
// The round-trip below executes the interpolated literal against a real
// DuckDB so the test encodes the engine's actual semantics, not an
// assumption about them.
func TestFormatArgValueBackslashRoundTrip(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, in := range []string{
		`C:\Users\alice`,
		`a\b`,
		`a\\b`,
		`regex \d+ with 'quote'`,
		`trailing backslash \`,
	} {
		query := interpolateArgs("SELECT ?", []any{in})
		var out string
		if err := db.QueryRow(query).Scan(&out); err != nil {
			t.Fatalf("round-trip query %q: %v", query, err)
		}
		if out != in {
			t.Errorf("bound string corrupted through interpolation:\n  in:    %q\n  query: %s\n  out:   %q", in, query, out)
		}
	}
}
