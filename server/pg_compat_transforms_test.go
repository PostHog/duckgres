package server

import (
	"database/sql"
	"testing"

	"github.com/posthog/duckgres/transpiler"
)

// transformCase transpiles a PostgreSQL query through the full pipeline and
// executes the result against an in-memory DuckDB seeded with initPgCatalog,
// asserting the single VARCHAR (or NULL) result. This exercises the Batch E
// functions.go AST transforms end-to-end.
type transformCase struct {
	name     string
	query    string
	want     string
	wantNull bool
}

func runTransformCases(t *testing.T, cases []transformCase) {
	t.Helper()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := initPgCatalog(db, processStartTime, processStartTime, "dev", "dev"); err != nil {
		t.Fatalf("initPgCatalog: %v", err)
	}
	tr := transpiler.New(transpiler.Config{})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tr.Transpile(tc.query)
			if err != nil {
				t.Fatalf("transpile %q: %v", tc.query, err)
			}
			var got sql.NullString
			if err := db.QueryRow(res.SQL).Scan(&got); err != nil {
				t.Fatalf("exec %q (transpiled %q): %v", tc.query, res.SQL, err)
			}
			if tc.wantNull {
				if got.Valid {
					t.Fatalf("%q = %q, want NULL", tc.query, got.String)
				}
				return
			}
			if !got.Valid {
				t.Fatalf("%q = NULL, want %q", tc.query, tc.want)
			}
			if got.String != tc.want {
				t.Fatalf("%q = %q, want %q (transpiled %q)", tc.query, got.String, tc.want, res.SQL)
			}
		})
	}
}

func TestCompatTransforms_BatchE(t *testing.T) {
	runTransformCases(t, []transformCase{
		// format — literal-template printf with %I (ident), %L (literal), %s, %%
		{"format_ident_literal", `SELECT format('%I = %L','foo bar','baz')`, `"foo bar" = 'baz'`, false},
		{"format_percent", `SELECT format('%s and %%','hello')`, "hello and %", false},
		{"format_two_s", `SELECT format('%s-%s','a','b')`, "a-b", false},

		// substr — PG negative/zero-start window semantics
		{"substr_neg_count", `SELECT substr('alphabet',-2,5)`, "al", false},
		{"substr_neg_nolen", `SELECT substr('alphabet',-3)`, "alphabet", false},
		{"substr_zero", `SELECT substr('alphabet',0,3)`, "al", false},
		{"substr_normal", `SELECT substr('alphabet',3,2)`, "ph", false},

		// substring — SQL regex FROM-pattern form (capture group vs whole match)
		{"substring_group", `SELECT substring('Thomas' FROM 'o(.)')`, "m", false},
		{"substring_whole", `SELECT substring('Thomas' FROM 'mas$')`, "mas", false},
		{"substring_from_for", `SELECT substring('Thomas' FROM 4 FOR 3)`, "mas", false},

		// overlay — replace a substring window
		{"overlay_from_for", `SELECT overlay('Txxxxas' PLACING 'hom' FROM 2 FOR 4)`, "Thomas", false},
		{"overlay_from", `SELECT overlay('Txxxxas' PLACING 'hom' FROM 2)`, "Thomxas", false},
		{"overlay_start", `SELECT overlay('abcdef' PLACING 'XX' FROM 1 FOR 2)`, "XXcdef", false},

		// date_trunc — 3-arg timezone-aware form (NY-local midnight = 04:00 UTC)
		{"date_trunc_tz", `SELECT (date_trunc('day', TIMESTAMPTZ '2024-06-15 12:00:00+00', 'America/New_York') = TIMESTAMPTZ '2024-06-15 04:00:00+00')::text`, "true", false},
		{"date_trunc_2arg", `SELECT (date_trunc('hour', TIMESTAMP '2024-06-15 12:34:00') = TIMESTAMP '2024-06-15 12:00:00')::text`, "true", false},

		// cardinality — total element count (flat 1-D; NULL passthrough)
		{"cardinality_flat", `SELECT cardinality(ARRAY[10,20,30])::text`, "3", false},
		{"cardinality_empty", `SELECT cardinality(ARRAY[]::int[])::text`, "0", false},
		{"cardinality_null", `SELECT cardinality(NULL::int[])::text`, "", true},

		// isfinite — interval is always finite (DuckDB lacks the INTERVAL overload)
		{"isfinite_interval", `SELECT isfinite(INTERVAL '1 day')::text`, "true", false},
	})
}
