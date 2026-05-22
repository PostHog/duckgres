package transpiler

import (
	"strings"
	"testing"
)

// TestIntervalTypmodCast verifies that a field-qualified interval cast
// ('2'::interval day) is rewritten to a DuckDB-executable form rather than the
// deparser's '2'::"interval"(8), which DuckDB cannot convert.
func TestIntervalTypmodCast(t *testing.T) {
	tp := New(DefaultConfig())
	res, err := tp.TranspileAll(`SELECT 1 WHERE ts <= (current_timestamp + '2'::interval day)`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if strings.Contains(res.SQL, `"interval"(`) {
		t.Fatalf("interval typmod not rewritten: %s", res.SQL)
	}
	if !strings.Contains(res.SQL, "interval") || !strings.Contains(res.SQL, "day") {
		t.Fatalf("expected interval/day in output: %s", res.SQL)
	}
	t.Logf("interval out: %s", res.SQL)
}

// TestPgCatalogJsonStrip verifies the deparser's pg_catalog.json canonicalization
// is stripped even when the type-mapping flag would not otherwise fire.
func TestPgCatalogJsonStrip(t *testing.T) {
	// A query parsed for some unrelated reason but NOT classified as needing
	// type mapping. The "->" forces FlagOperators (so it parses+deparses) while
	// a bare ::json cast does not set FlagTypeMapping.
	tp := New(DefaultConfig())
	res, err := tp.Transpile(`SELECT (e.properties->'x')::json AS j FROM t e`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if strings.Contains(res.SQL, "pg_catalog.json") || strings.Contains(strings.ToLower(res.SQL), "pg_catalog") {
		t.Fatalf("pg_catalog not stripped: %s", res.SQL)
	}
	t.Logf("json out: %s", res.SQL)
}
