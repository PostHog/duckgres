package transpiler

import (
	"errors"
	"strings"
	"testing"
)

// TestTranspile_S3CacheSet asserts that `SET duckgres.s3_cache = ...` is
// intercepted as a duckgres-namespaced custom GUC (S3CacheSet populated, not
// forwarded to DuckDB) and the value is normalized to the canonical on/off.
func TestTranspile_S3CacheSet(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"set off", "SET duckgres.s3_cache = 'off'", "off"},
		{"set on", "SET duckgres.s3_cache = 'on'", "on"},
		{"set passthrough", "SET duckgres.s3_cache = 'passthrough'", "passthrough"},
		{"unquoted off", "SET duckgres.s3_cache = off", "off"},
		{"unquoted boolean keyword", "SET duckgres.s3_cache = false", "off"},
		{"true normalizes to on", "SET duckgres.s3_cache = 'true'", "on"},
		{"yes normalizes to on", "SET duckgres.s3_cache = 'yes'", "on"},
		{"zero normalizes to off", "SET duckgres.s3_cache = '0'", "off"},
		{"set local", "SET LOCAL duckgres.s3_cache = 'off'", "off"},
		{"case-insensitive name", "SET DUCKGRES.S3_CACHE = 'off'", "off"},
		{"case-insensitive value normalized", "SET duckgres.s3_cache = 'OFF'", "off"},
		{"whitespace trimmed", "SET duckgres.s3_cache = '  off '", "off"},
		{"empty string resets to default", "SET duckgres.s3_cache = ''", ""},
		{"set to default clears to empty", "SET duckgres.s3_cache TO DEFAULT", ""},
		{"reset clears to empty", "RESET duckgres.s3_cache", ""},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if result.S3CacheSet == nil {
				t.Fatalf("Transpile(%q): S3CacheSet = nil, want non-nil (error=%v)", tt.input, result.Error)
			}
			if got := *result.S3CacheSet; got != tt.want {
				t.Errorf("Transpile(%q): S3CacheSet = %q, want %q", tt.input, got, tt.want)
			}
			// Custom GUC must never be forwarded to DuckDB.
			if result.S3CacheShow {
				t.Errorf("Transpile(%q): S3CacheShow = true, want false", tt.input)
			}
			// Must not leak into the query_source interception.
			if result.QuerySourceSet != nil || result.QuerySourceShow {
				t.Errorf("Transpile(%q): query_source fields populated, want untouched", tt.input)
			}
		})
	}
}

// TestTranspile_S3CacheSetInvalidRejected asserts that a SET with a value
// outside the boolean spellings surfaces Result.Error with SQLSTATE 22023 and
// does NOT populate S3CacheSet. The error message must name the valid values
// but must NOT echo the offending value (arbitrary client input flowing into
// logs / the recent-errors ring).
func TestTranspile_S3CacheSetInvalidRejected(t *testing.T) {
	tr := New(DefaultConfig())

	longJunk := strings.Repeat("x", 10*1024)
	inputs := map[string]string{
		"garbage":           "SET duckgres.s3_cache = 'garbage'",
		"10KB string":       "SET duckgres.s3_cache = '" + longJunk + "'",
		"integer constant":  "SET duckgres.s3_cache = 2",
		"multiple values":   "SET duckgres.s3_cache = 'on', 'off'",
		"set local garbage": "SET LOCAL duckgres.s3_cache = 'garbage'",
	}
	for name, in := range inputs {
		t.Run(name, func(t *testing.T) {
			result, err := tr.Transpile(in)
			if err != nil {
				t.Fatalf("Transpile(%.80q) error: %v", in, err)
			}
			if result.Error == nil {
				got := "<nil>"
				if result.S3CacheSet != nil {
					got = *result.S3CacheSet
				}
				t.Fatalf("Transpile(%.80q): Error = nil, want 22023 rejection (S3CacheSet=%.80q)", in, got)
			}
			var coded interface{ SQLState() string }
			if !errors.As(result.Error, &coded) || coded.SQLState() != "22023" {
				t.Errorf("Transpile(%.80q): Error SQLSTATE = %v, want 22023", in, result.Error)
			}
			msg := result.Error.Error()
			if !strings.Contains(msg, `"on"`) || !strings.Contains(msg, `"off"`) {
				t.Errorf("error message must name the valid values, got %q", msg)
			}
			if strings.Contains(msg, "garbage") || strings.Contains(msg, longJunk[:64]) {
				t.Errorf("error message must not echo the offending value, got %.120q", msg)
			}
			if result.S3CacheSet != nil {
				t.Errorf("Transpile(%.80q): S3CacheSet = %q, want nil on rejection", in, *result.S3CacheSet)
			}
		})
	}
}

// TestTranspile_S3CacheShow asserts `SHOW duckgres.s3_cache` is intercepted
// (answered session-side) rather than treated as an unrecognized config
// parameter or forwarded to DuckDB.
func TestTranspile_S3CacheShow(t *testing.T) {
	tr := New(DefaultConfig())
	result, err := tr.Transpile("SHOW duckgres.s3_cache")
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}
	if !result.S3CacheShow {
		t.Fatalf("S3CacheShow = false, want true (error=%v)", result.Error)
	}
	if result.Error != nil {
		t.Errorf("Error = %v, want nil (must not be treated as unrecognized param)", result.Error)
	}
	if result.S3CacheSet != nil {
		t.Errorf("S3CacheSet = %v, want nil", result.S3CacheSet)
	}
}

// TestTranspile_S3CacheMultiStatementNotIntercepted mirrors the query_source
// guard: transpiling a MULTI-statement batch containing a duckgres.s3_cache
// statement must NOT surface S3CacheSet/S3CacheShow on the whole-batch Result
// (the early return would swallow every statement after the GUC one). The
// connection layer splits the batch and re-transpiles each statement
// individually — where the single-statement interception then fires.
func TestTranspile_S3CacheMultiStatementNotIntercepted(t *testing.T) {
	tr := New(DefaultConfig())

	cases := []string{
		"SET duckgres.s3_cache = 'off'; SHOW duckgres.s3_cache",
		"SET duckgres.s3_cache = 'off'; SELECT 1",
		"SHOW duckgres.s3_cache; SELECT 1",
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			result, err := tr.Transpile(in)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", in, err)
			}
			if result.S3CacheSet != nil {
				t.Errorf("Transpile(%q): S3CacheSet = %v, want nil for a multi-statement batch (would swallow trailing statements)", in, *result.S3CacheSet)
			}
			if result.S3CacheShow {
				t.Errorf("Transpile(%q): S3CacheShow = true, want false for a multi-statement batch (would swallow trailing statements)", in)
			}
		})
	}
}
