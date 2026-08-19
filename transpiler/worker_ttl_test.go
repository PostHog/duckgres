package transpiler

import (
	"errors"
	"strings"
	"testing"
)

// TestTranspile_WorkerTTLSet asserts that `SET duckgres.worker_ttl = ...` is
// intercepted as a duckgres-namespaced custom GUC (WorkerTTLSet populated, not
// forwarded to DuckDB) and the value is normalized to its canonical Go
// duration string.
func TestTranspile_WorkerTTLSet(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"set minutes", "SET duckgres.worker_ttl = '20m'", "20m0s"},
		{"set hours", "SET duckgres.worker_ttl = '24h'", "24h0m0s"},
		{"compound duration", "SET duckgres.worker_ttl = '1h30m'", "1h30m0s"},
		{"set local", "SET LOCAL duckgres.worker_ttl = '20m'", "20m0s"},
		{"case-insensitive name", "SET DUCKGRES.WORKER_TTL = '20m'", "20m0s"},
		{"whitespace trimmed", "SET duckgres.worker_ttl = ' 20m '", "20m0s"},
		{"empty string resets to default", "SET duckgres.worker_ttl = ''", ""},
		{"set to default clears to empty", "SET duckgres.worker_ttl TO DEFAULT", ""},
		{"reset clears to empty", "RESET duckgres.worker_ttl", ""},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if result.WorkerTTLSet == nil {
				t.Fatalf("Transpile(%q): WorkerTTLSet = nil, want non-nil (error=%v)", tt.input, result.Error)
			}
			if got := *result.WorkerTTLSet; got != tt.want {
				t.Errorf("Transpile(%q): WorkerTTLSet = %q, want %q", tt.input, got, tt.want)
			}
			// Custom GUC must never be forwarded to DuckDB.
			if result.WorkerTTLShow {
				t.Errorf("Transpile(%q): WorkerTTLShow = true, want false", tt.input)
			}
			// Must not leak into the sibling GUC interceptions.
			if result.S3CacheSet != nil || result.S3CacheShow || result.QuerySourceSet != nil || result.QuerySourceShow {
				t.Errorf("Transpile(%q): sibling GUC fields populated, want untouched", tt.input)
			}
		})
	}
}

// TestTranspile_WorkerTTLSetInvalidRejected asserts that a SET with a value
// that is not a valid non-negative Go duration surfaces Result.Error with
// SQLSTATE 22023 and does NOT populate WorkerTTLSet. The error message must
// describe the expected shape but must NOT echo the offending value
// (arbitrary client input flowing into logs / the recent-errors ring).
func TestTranspile_WorkerTTLSetInvalidRejected(t *testing.T) {
	tr := New(DefaultConfig())

	longJunk := strings.Repeat("x", 10*1024)
	inputs := map[string]string{
		"garbage":           "SET duckgres.worker_ttl = 'garbage'",
		"missing unit":      "SET duckgres.worker_ttl = '20'",
		"negative duration": "SET duckgres.worker_ttl = '-5m'",
		// Zero and sub-minute values are rejected: the parked TTL is
		// persisted in whole minutes, where 0 means "deployment default" —
		// accepting them would park the worker for the default while SHOW
		// reports the shorter value (SHOW must never lie).
		"zero":                  "SET duckgres.worker_ttl = '0s'",
		"sub-minute":            "SET duckgres.worker_ttl = '30s'",
		"non-whole-minute":      "SET duckgres.worker_ttl = '90s'",
		"10KB string":       "SET duckgres.worker_ttl = '" + longJunk + "'",
		"integer constant":  "SET duckgres.worker_ttl = 2",
		"multiple values":   "SET duckgres.worker_ttl = '20m', '30m'",
		"set local garbage": "SET LOCAL duckgres.worker_ttl = 'garbage'",
	}
	for name, in := range inputs {
		t.Run(name, func(t *testing.T) {
			result, err := tr.Transpile(in)
			if err != nil {
				t.Fatalf("Transpile(%.80q) error: %v", in, err)
			}
			if result.Error == nil {
				got := "<nil>"
				if result.WorkerTTLSet != nil {
					got = *result.WorkerTTLSet
				}
				t.Fatalf("Transpile(%.80q): Error = nil, want 22023 rejection (WorkerTTLSet=%.80q)", in, got)
			}
			var coded interface{ SQLState() string }
			if !errors.As(result.Error, &coded) || coded.SQLState() != "22023" {
				t.Errorf("Transpile(%.80q): Error SQLSTATE = %v, want 22023", in, result.Error)
			}
			msg := result.Error.Error()
			if !strings.Contains(msg, "whole minutes") {
				t.Errorf("error message must name the whole-minute granularity, got %q", msg)
			}
			if strings.Contains(msg, "garbage") || strings.Contains(msg, longJunk[:64]) {
				t.Errorf("error message must not echo the offending value, got %.120q", msg)
			}
			if result.WorkerTTLSet != nil {
				t.Errorf("Transpile(%.80q): WorkerTTLSet = %q, want nil on rejection", in, *result.WorkerTTLSet)
			}
		})
	}
}

// TestTranspile_WorkerTTLShow asserts `SHOW duckgres.worker_ttl` is
// intercepted (answered session-side) rather than treated as an unrecognized
// config parameter or forwarded to DuckDB.
func TestTranspile_WorkerTTLShow(t *testing.T) {
	tr := New(DefaultConfig())
	result, err := tr.Transpile("SHOW duckgres.worker_ttl")
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}
	if !result.WorkerTTLShow {
		t.Fatalf("WorkerTTLShow = false, want true (error=%v)", result.Error)
	}
	if result.Error != nil {
		t.Errorf("Error = %v, want nil (must not be treated as unrecognized param)", result.Error)
	}
	if result.WorkerTTLSet != nil {
		t.Errorf("WorkerTTLSet = %v, want nil", result.WorkerTTLSet)
	}
}

// TestTranspile_WorkerTTLMultiStatementNotIntercepted mirrors the sibling GUC
// guard: transpiling a MULTI-statement batch containing a duckgres.worker_ttl
// statement must NOT surface WorkerTTLSet/WorkerTTLShow on the whole-batch
// Result (the early return would swallow every statement after the GUC one).
// The connection layer splits the batch and re-transpiles each statement
// individually — where the single-statement interception then fires.
func TestTranspile_WorkerTTLMultiStatementNotIntercepted(t *testing.T) {
	tr := New(DefaultConfig())

	cases := []string{
		"SET duckgres.worker_ttl = '20m'; SHOW duckgres.worker_ttl",
		"SET duckgres.worker_ttl = '20m'; SELECT 1",
		"SHOW duckgres.worker_ttl; SELECT 1",
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			result, err := tr.Transpile(in)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", in, err)
			}
			if result.WorkerTTLSet != nil {
				t.Errorf("Transpile(%q): WorkerTTLSet = %v, want nil for a multi-statement batch (would swallow trailing statements)", in, *result.WorkerTTLSet)
			}
			if result.WorkerTTLShow {
				t.Errorf("Transpile(%q): WorkerTTLShow = true, want false for a multi-statement batch (would swallow trailing statements)", in)
			}
		})
	}
}
