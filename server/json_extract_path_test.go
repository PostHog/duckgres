package server

import (
	"database/sql"
	"testing"

	"github.com/posthog/duckgres/transpiler"
)

// TestJSONExtractDollarKey_RoundTrip is the end-to-end regression test for the
// production "Binder Error: JSON path error near 'ai_session_id'" failures:
// PostHog/HogQL property keys that begin with '$' ($ai_session_id, $group_0)
// must extract their value instead of being mis-parsed by DuckDB as a malformed
// JSONPath. These cases transpile through the full pipeline and execute against
// a real in-memory DuckDB seeded with initPgCatalog (which registers the
// duckgres_json_extract_path macro), asserting the actual extracted value.
func TestJSONExtractDollarKey_RoundTrip(t *testing.T) {
	runTransformCases(t, []transformCase{
		{
			// The exact broken shape: ->> with a '$'-prefixed literal key.
			name:  "dollar key via ->> extracts the value",
			query: `SELECT ('{"$ai_session_id":"sess_abc"}'::json)->>'$ai_session_id'`,
			want:  "sess_abc",
		},
		{
			// Direct json_extract_string(...) with a '$'-prefixed literal key —
			// the function-call form clients also send.
			name:  "dollar key via direct json_extract_string extracts the value",
			query: `SELECT json_extract_string('{"$group_0":"team_42"}', '$group_0')`,
			want:  "team_42",
		},
		{
			// Chained arrows with '$'-prefixed keys at every step.
			name:  "chained dollar keys extract the nested value",
			query: `SELECT ('{"$a":{"$b":"deep"}}'::json)->'$a'->>'$b'`,
			want:  "deep",
		},
		{
			// Regression guard: a plain key must still be a literal-key lookup.
			name:  "plain key is unaffected",
			query: `SELECT json_extract_string('{"plain":"ok"}', 'plain')`,
			want:  "ok",
		},
		{
			// A '$.'/'$['-prefixed argument is a valid DuckDB JSONPath, so it is
			// left to navigate. For a direct json_extract[_string] call this is
			// unambiguously correct — the client wrote a DuckDB function and a
			// DuckDB path.
			name:  "valid JSONPath in a direct call navigates",
			query: `SELECT json_extract_string('{"a":{"b":"nested"}}', '$.a.b')`,
			want:  "nested",
		},
		{
			// KNOWN, DELIBERATE DIVERGENCE: in PostgreSQL `data ->> '$.a.b'` means
			// the literal key named "$.a.b" (-> NULL here), but we keep DuckDB
			// JSONPath semantics for '$.'/'$['-prefixed arrow keys too — consistent
			// with direct calls and with the pre-existing #639 transpiler test. This
			// only affects keys that are *also* valid JSONPaths; the reported bug
			// keys ($ai_session_id, $group_0 — no dot) are unaffected and HogQL does
			// not emit dotted JSONPath property keys. See normalizeJSONPathKey.
			name:  "dotted JSONPath via arrow navigates (documented divergence)",
			query: `SELECT ('{"a":{"b":"nested"}}'::json)->>'$.a.b'`,
			want:  "nested",
		},
	})
}

// TestJSONExtractDollarKey_ParameterRoundTrip covers the parameterized form of
// the production bug, where the JSON key arrives as a bound parameter ($1) whose
// value is unknown at transpile time. The transpiler wraps the path argument in
// the duckgres_json_extract_path() macro; here we bind '$ai_session_id' at
// execute time and confirm DuckDB returns the value instead of failing at bind
// time. A normal key bound to the same statement must still work, and an
// ordinary (non-path) string parameter must be untouched.
func TestJSONExtractDollarKey_ParameterRoundTrip(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	// initPgCatalog registers initUtilityMacros, including duckgres_json_extract_path.
	if err := initPgCatalog(db, processStartTime, processStartTime, "dev", "dev"); err != nil {
		t.Fatalf("initPgCatalog: %v", err)
	}
	// ConvertPlaceholders mirrors the extended-query protocol path that carries
	// bound parameters.
	tr := transpiler.New(transpiler.Config{ConvertPlaceholders: true})

	const doc = `{"$ai_session_id":"sess_from_param","normal":"plain_value"}`
	const arrDoc = `["zero","one","two"]`

	cases := []struct {
		name  string
		query string
		arg   any
		want  string
	}{
		{
			name:  "parameter holding a dollar key extracts the value (the prod bug)",
			query: `SELECT json_extract_string('` + doc + `'::json, $1)`,
			arg:   "$ai_session_id",
			want:  "sess_from_param",
		},
		{
			name:  "same statement with a normal key still works",
			query: `SELECT json_extract_string('` + doc + `'::json, $1)`,
			arg:   "normal",
			want:  "plain_value",
		},
		{
			name:  "arrow ->> with a parameter dollar key extracts the value",
			query: `SELECT ('` + doc + `'::json)->>$1`,
			arg:   "$ai_session_id",
			want:  "sess_from_param",
		},
		{
			// P2 regression: a path param wrapped in an explicit ::text cast (added
			// by some drivers) must still resolve, not bypass normalization.
			name:  "parameter dollar key with a ::text cast extracts the value",
			query: `SELECT json_extract_string('` + doc + `'::json, $1::text)`,
			arg:   "$ai_session_id",
			want:  "sess_from_param",
		},
		{
			// P2 regression: an INTEGER-bound parameter must still index the array
			// (Postgres `json ->> int`). The type-aware macro emits a $[i] path
			// rather than mangling the integer into a string key.
			name:  "integer parameter indexes the array (arrow)",
			query: `SELECT ('` + arrDoc + `'::json)->>$1`,
			arg:   2,
			want:  "two",
		},
		{
			name:  "integer parameter indexes the array (direct call)",
			query: `SELECT json_extract_string('` + arrDoc + `'::json, $1)`,
			arg:   int64(0),
			want:  "zero",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tr.Transpile(tc.query)
			if err != nil {
				t.Fatalf("transpile %q: %v", tc.query, err)
			}
			if res.ParamCount != 1 {
				t.Fatalf("transpile %q: ParamCount = %d, want 1", tc.query, res.ParamCount)
			}
			var got sql.NullString
			if err := db.QueryRow(res.SQL, tc.arg).Scan(&got); err != nil {
				t.Fatalf("exec %q (transpiled %q) arg=%v: %v", tc.query, res.SQL, tc.arg, err)
			}
			if !got.Valid || got.String != tc.want {
				t.Fatalf("%q arg=%v = %v, want %q (transpiled %q)", tc.query, tc.arg, got, tc.want, res.SQL)
			}
		})
	}

	// An ordinary string parameter (not a JSON path argument) must be passed
	// through verbatim — the fix must not globally rewrite string parameters.
	t.Run("ordinary string parameter is not wrapped", func(t *testing.T) {
		res, err := tr.Transpile("SELECT $1::varchar")
		if err != nil {
			t.Fatalf("transpile: %v", err)
		}
		var got sql.NullString
		if err := db.QueryRow(res.SQL, "$ai_session_id").Scan(&got); err != nil {
			t.Fatalf("exec (transpiled %q): %v", res.SQL, err)
		}
		if !got.Valid || got.String != "$ai_session_id" {
			t.Fatalf("ordinary param = %v, want %q (transpiled %q)", got, "$ai_session_id", res.SQL)
		}
	})
}
