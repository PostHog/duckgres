package transpiler

import (
	"errors"
	"strings"
	"testing"
)

// TestTranspile_QuerySourceSet asserts that `SET duckgres.query_source = '...'`
// is intercepted as a duckgres-namespaced custom GUC (QuerySourceSet populated,
// not forwarded to DuckDB) and the value is extracted from the statement.
func TestTranspile_QuerySourceSet(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"set endpoints", "SET duckgres.query_source = 'endpoints'", "endpoints"},
		{"set standard", "SET duckgres.query_source = 'standard'", "standard"},
		{"set local", "SET LOCAL duckgres.query_source = 'endpoints'", "endpoints"},
		{"case-insensitive name", "SET DUCKGRES.QUERY_SOURCE = 'endpoints'", "endpoints"},
		{"case-insensitive value normalized", "SET duckgres.query_source = 'ENDPOINTS'", "endpoints"},
		{"whitespace trimmed", "SET duckgres.query_source = '  standard '", "standard"},
		{"unquoted identifier value", "SET duckgres.query_source = endpoints", "endpoints"},
		{"empty string resets to default", "SET duckgres.query_source = ''", ""},
		{"set to default clears to empty", "SET duckgres.query_source TO DEFAULT", ""},
		{"reset clears to empty", "RESET duckgres.query_source", ""},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if result.QuerySourceSet == nil {
				t.Fatalf("Transpile(%q): QuerySourceSet = nil, want non-nil", tt.input)
			}
			if got := *result.QuerySourceSet; got != tt.want {
				t.Errorf("Transpile(%q): QuerySourceSet = %q, want %q", tt.input, got, tt.want)
			}
			// Custom GUC must never be forwarded to DuckDB.
			if result.QuerySourceShow {
				t.Errorf("Transpile(%q): QuerySourceShow = true, want false", tt.input)
			}
		})
	}
}

// TestTranspile_QuerySourceSetInvalidRejected asserts that a SET with a value
// outside the closed {standard, endpoints} set surfaces Result.Error with
// SQLSTATE 22023 (invalid_parameter_value) and does NOT populate
// QuerySourceSet. The value is the billing-bucket key, so accepting arbitrary
// strings would hand clients unbounded cardinality (and junk) in the billing
// table and its exports. The error message must name the valid values but must
// NOT echo the offending value (arbitrary client input flowing into logs / the
// recent-errors ring).
func TestTranspile_QuerySourceSetInvalidRejected(t *testing.T) {
	tr := New(DefaultConfig())

	longJunk := strings.Repeat("x", 10*1024)
	inputs := map[string]string{
		"garbage":                 "SET duckgres.query_source = 'garbage'",
		"10KB string":             "SET duckgres.query_source = '" + longJunk + "'",
		"unicode + control chars": "SET duckgres.query_source = e'caf\\u00e9\\n\\ttab'",
		"integer constant":        "SET duckgres.query_source = 123",
		"boolean constant":        "SET duckgres.query_source = true",
		"multiple values":         "SET duckgres.query_source = 'standard', 'endpoints'",
		"set local garbage":       "SET LOCAL duckgres.query_source = 'garbage'",
	}
	for name, in := range inputs {
		t.Run(name, func(t *testing.T) {
			result, err := tr.Transpile(in)
			if err != nil {
				t.Fatalf("Transpile(%.80q) error: %v", in, err)
			}
			if result.Error == nil {
				got := "<nil>"
				if result.QuerySourceSet != nil {
					got = *result.QuerySourceSet
				}
				t.Fatalf("Transpile(%.80q): Error = nil, want 22023 rejection (QuerySourceSet=%.80q)", in, got)
			}
			var coded interface{ SQLState() string }
			if !errors.As(result.Error, &coded) || coded.SQLState() != "22023" {
				t.Errorf("Transpile(%.80q): Error SQLSTATE = %v, want 22023", in, result.Error)
			}
			msg := result.Error.Error()
			if !strings.Contains(msg, `"standard"`) || !strings.Contains(msg, `"endpoints"`) {
				t.Errorf("error message must name the valid values, got %q", msg)
			}
			if strings.Contains(msg, "garbage") || strings.Contains(msg, longJunk[:64]) {
				t.Errorf("error message must not echo the offending value, got %.120q", msg)
			}
			if result.QuerySourceSet != nil {
				t.Errorf("Transpile(%.80q): QuerySourceSet = %q, want nil on rejection", in, *result.QuerySourceSet)
			}
		})
	}
}

// TestTranspile_QuerySourceShow asserts `SHOW duckgres.query_source` is
// intercepted (answered session-side) rather than treated as an unrecognized
// config parameter or forwarded to DuckDB.
func TestTranspile_QuerySourceShow(t *testing.T) {
	tr := New(DefaultConfig())
	result, err := tr.Transpile("SHOW duckgres.query_source")
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}
	if !result.QuerySourceShow {
		t.Fatalf("QuerySourceShow = false, want true")
	}
	if result.Error != nil {
		t.Errorf("Error = %v, want nil (must not be treated as unrecognized param)", result.Error)
	}
	if result.QuerySourceSet != nil {
		t.Errorf("QuerySourceSet = %v, want nil", result.QuerySourceSet)
	}
}

// TestTranspile_OtherDuckgresParamNotIntercepted guards the interception scope:
// only duckgres.query_source is handled here. Another duckgres.* SET is not
// silently swallowed by the query_source path (QuerySourceSet stays nil).
func TestTranspile_OtherSetNotQuerySource(t *testing.T) {
	tr := New(DefaultConfig())
	// A normal ignored PG param must still be classified as IsIgnoredSet, not
	// query_source.
	result, err := tr.Transpile("SET application_name = 'x'")
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}
	if result.QuerySourceSet != nil {
		t.Errorf("QuerySourceSet = %v, want nil for application_name", result.QuerySourceSet)
	}
	if !result.IsIgnoredSet {
		t.Errorf("IsIgnoredSet = false, want true for application_name")
	}
}

// TestTranspile_QuerySourceMultiStatementNotIntercepted guards the fix for the
// e2e bug: transpiling a MULTI-statement batch that starts with a
// duckgres.query_source statement must NOT surface QuerySourceSet/QuerySourceShow
// on the whole-batch Result. Surfacing it would make the transpiler return early
// for the entire batch, swallowing every statement after the GUC one. The
// connection layer splits multi-statement simple queries and re-transpiles each
// statement individually — where the single-statement interception then fires.
func TestTranspile_QuerySourceMultiStatementNotIntercepted(t *testing.T) {
	tr := New(DefaultConfig())

	cases := []string{
		"SET duckgres.query_source = 'endpoints'; SHOW duckgres.query_source",
		"SET duckgres.query_source = 'endpoints'; SELECT 1",
		"SHOW duckgres.query_source; SELECT 1",
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			result, err := tr.Transpile(in)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", in, err)
			}
			if result.QuerySourceSet != nil {
				t.Errorf("Transpile(%q): QuerySourceSet = %v, want nil for a multi-statement batch (would swallow trailing statements)", in, *result.QuerySourceSet)
			}
			if result.QuerySourceShow {
				t.Errorf("Transpile(%q): QuerySourceShow = true, want false for a multi-statement batch (would swallow trailing statements)", in)
			}
		})
	}
}
