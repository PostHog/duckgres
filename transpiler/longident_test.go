package transpiler

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// longName is 65 bytes - the exact production case that triggered the bug:
// libpg_query truncates it to 63 bytes during Parse.
const longName = "reports__cohort_user_period_activity_calendar_monthly__4119277097"

func init() {
	if len(longName) != 65 {
		panic("longName must be 65 bytes to exercise the 63-byte truncation")
	}
}

// roundTrip parses and deparses sql the way the transpiler core does, with
// long-identifier protection applied. It returns the SQL DuckDB would receive.
func roundTrip(t *testing.T, sql string) string {
	t.Helper()
	protected, repls := protectLongIdentifiers(sql)
	tree, err := pg_query.Parse(protected)
	if err != nil {
		t.Fatalf("parse %q: %v", protected, err)
	}
	out, err := pg_query.Deparse(tree)
	if err != nil {
		t.Fatalf("deparse: %v", err)
	}
	return restoreLongIdentifiers(out, repls)
}

func TestProtectPreservesLongTableName(t *testing.T) {
	out := roundTrip(t, "INSERT INTO "+longName+" (a) VALUES (1::regtype)")
	if !strings.Contains(out, longName) {
		t.Fatalf("long table name was lost; got: %s", out)
	}
}

func TestProtectVariousStatements(t *testing.T) {
	col := "an_exceedingly_verbose_column_identifier_well_past_the_sixty_three_byte_limit"
	if len(col) <= maxIdentifierBytes {
		t.Fatalf("col fixture must exceed the limit")
	}
	cases := []string{
		"SELECT * FROM " + longName + " WHERE id::regtype = 1",
		"UPDATE " + longName + " SET x = 1 WHERE y::regtype = 2",
		"DELETE FROM " + longName + " WHERE z IN (1::regtype)",
		"SELECT " + col + " FROM " + longName,
		"SELECT a FROM myschema." + longName + " WHERE b::regtype = 1",
	}
	for _, sql := range cases {
		out := roundTrip(t, sql)
		if !strings.Contains(out, longName) && !strings.Contains(out, col) {
			t.Errorf("identifier lost for %q -> %q", sql, out)
		}
	}
}

func TestProtectQuotedLongIdentifier(t *testing.T) {
	quoted := `"` + longName + `"`
	out := roundTrip(t, "SELECT * FROM "+quoted+" WHERE x::regtype = 1")
	if !strings.Contains(out, longName) {
		t.Fatalf("quoted long identifier lost; got: %s", out)
	}
}

func TestProtectSkipsStringLiterals(t *testing.T) {
	// A long sequence inside a string literal must NOT be treated as an
	// identifier (no placeholder), and must survive verbatim.
	lit := "'" + longName + "'"
	sql := "SELECT " + lit + "::regtype"
	protected, repls := protectLongIdentifiers(sql)
	if len(repls) != 0 {
		t.Fatalf("string literal content was wrongly protected: %v", repls)
	}
	if protected != sql {
		t.Fatalf("string literal SQL altered: %q", protected)
	}
}

func TestProtectSkipsDollarQuoted(t *testing.T) {
	sql := "SELECT $tag$" + longName + "$tag$"
	_, repls := protectLongIdentifiers(sql)
	if len(repls) != 0 {
		t.Fatalf("dollar-quoted content wrongly protected: %v", repls)
	}
}

func TestProtectSkipsComments(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1 -- " + longName + "\n",
		"SELECT 1 /* " + longName + " */",
	} {
		if _, repls := protectLongIdentifiers(sql); len(repls) != 0 {
			t.Errorf("comment content wrongly protected for %q: %v", sql, repls)
		}
	}
}

func TestProtectSkipsEString(t *testing.T) {
	// Backslash-escaped quote inside an E-string must not desync the scanner.
	sql := "SELECT E'foo\\'" + longName + "' , x"
	if _, repls := protectLongIdentifiers(sql); len(repls) != 0 {
		t.Errorf("E-string content wrongly protected: %v", repls)
	}
}

func TestProtectNoOpForShortQueries(t *testing.T) {
	sql := "SELECT * FROM short_table WHERE id = 1"
	out, repls := protectLongIdentifiers(sql)
	if out != sql || repls != nil {
		t.Fatalf("short query should be untouched; got %q, %v", out, repls)
	}
}

func TestProtectDedupesRepeatedIdentifier(t *testing.T) {
	sql := "SELECT * FROM " + longName + " a JOIN " + longName + " b ON a.id = b.id WHERE a.x::regtype = 1"
	protected, repls := protectLongIdentifiers(sql)
	if len(repls) != 1 {
		t.Fatalf("expected 1 unique replacement, got %d", len(repls))
	}
	// Placeholder must appear twice in the protected SQL.
	if n := strings.Count(protected, repls[0].placeholder); n != 2 {
		t.Fatalf("expected placeholder twice, got %d", n)
	}
	if out := restoreLongIdentifiers(protected, repls); strings.Count(out, longName) != 2 {
		t.Fatalf("restore should yield two occurrences; got: %s", out)
	}
}

func TestProtectMultipleDistinctIdentifiers(t *testing.T) {
	a := "first_table_name_that_is_definitely_longer_than_the_postgres_limit_aaaa"
	b := "second_table_name_that_is_definitely_longer_than_the_postgres_limit_bb"
	sql := "SELECT * FROM " + a + " JOIN " + b + " USING (id) WHERE q::regtype = 1"
	out := roundTrip(t, sql)
	if !strings.Contains(out, a) || !strings.Contains(out, b) {
		t.Fatalf("distinct long identifiers lost; got: %s", out)
	}
}

// TestPrefixCollisionAvoided ensures the placeholder prefix is bumped when the
// default prefix already appears in the input.
func TestPrefixCollisionAvoided(t *testing.T) {
	sql := "SELECT dglongident_0_ , " + longName + "::regtype"
	protected, repls := protectLongIdentifiers(sql)
	if len(repls) != 1 {
		t.Fatalf("expected 1 replacement, got %d", len(repls))
	}
	// The chosen placeholder must not be the literal that already existed,
	// and restoration must not corrupt the pre-existing token.
	out := restoreLongIdentifiers(protected, repls)
	if !strings.Contains(out, "dglongident_0_") {
		t.Fatalf("pre-existing token corrupted: %s", out)
	}
	if !strings.Contains(out, longName) {
		t.Fatalf("long identifier lost: %s", out)
	}
}

// TestRestoreLeavesEmbeddedPlaceholder verifies boundary-aware restoration: a
// placeholder embedded inside a larger identifier (as the writable-CTE transform
// does when it builds "_cte_<placeholder>_<hex>") must NOT be substituted, since
// splicing the original token there would corrupt the generated name.
func TestRestoreLeavesEmbeddedPlaceholder(t *testing.T) {
	_, repls := protectLongIdentifiers("SELECT * FROM " + longName)
	if len(repls) != 1 {
		t.Fatalf("want 1 replacement, got %d", len(repls))
	}
	ph := repls[0].placeholder
	embedded := "_cte_" + ph + "_deadbeef"
	standalone := "SELECT * FROM " + ph + " JOIN " + embedded + " USING (id)"
	got := restoreLongIdentifiers(standalone, repls)
	want := "SELECT * FROM " + longName + " JOIN _cte_" + ph + "_deadbeef USING (id)"
	if got != want {
		t.Fatalf("boundary-aware restore wrong:\n got:  %s\n want: %s", got, want)
	}
}

// TestWritableCTELongNameProducesValidSQL exercises the full path that motivated
// boundary-aware restoration: a writable CTE whose (quoted, special-char) name
// exceeds 63 bytes must still deparse to valid SQL.
func TestWritableCTELongNameProducesValidSQL(t *testing.T) {
	cte := `"My CTE With Spaces And Punctuation!! that runs well past sixty-three bytes"`
	sql := "WITH " + cte + " AS (DELETE FROM t RETURNING id) SELECT * FROM " + cte
	tr := New(Config{DuckLakeMode: true})
	res, err := tr.Transpile(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Statements) == 0 {
		t.Fatalf("expected a multi-statement rewrite; got SQL: %s", res.SQL)
	}
	for i, s := range res.Statements {
		if _, perr := pg_query.Parse(s); perr != nil {
			t.Errorf("statement %d is invalid SQL: %v\n  %s", i, perr, s)
		}
	}
}

// TestProtectNeverPanicsAndIsSafe feeds adversarial inputs (unterminated quotes,
// nested comments, multibyte runs, adjacent placeholders) to make sure the
// scanner always terminates without panicking and that, whenever the protected
// SQL re-parses, restoration brings back the original long names.
func TestProtectNeverPanicsAndIsSafe(t *testing.T) {
	long := strings.Repeat("a", 80)
	mb := strings.Repeat("é", 40) // 80 bytes, 40 runes
	inputs := []string{
		"",
		"SELECT 1",
		"SELECT * FROM " + long,
		"SELECT '" + long + "",                     // unterminated string
		`SELECT "` + long,                          // unterminated quoted ident
		"SELECT $tag$" + long,                      // unterminated dollar quote
		"SELECT /* " + long,                        // unterminated block comment
		"SELECT E'\\'" + long,                      // E-string with trailing escape
		"SELECT " + mb + " FROM t",                 // multibyte identifier
		"SELECT " + long + "$" + long,              // '$' inside identifier run
		"SELECT $$" + long + "$$, " + long,         // dollar body then real ident
		"-- " + long + "\nSELECT " + long,          // comment then ident
		"SELECT " + long + "," + long + "," + long, // many adjacent
	}
	for _, in := range inputs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic on %q: %v", in, r)
				}
			}()
			protected, repls := protectLongIdentifiers(in)
			// Restoration must always reproduce the input exactly.
			if got := restoreLongIdentifiers(protected, repls); got != in {
				t.Errorf("round-trip mismatch for %q:\n got %q", in, got)
			}
		}()
	}
}

// TestTranspilerEndToEndLongName verifies the fix through the public Transpile
// entry point using a query that forces Tier-1 parsing (the ::regtype cast).
func TestTranspilerEndToEndLongName(t *testing.T) {
	tr := New(Config{})
	res, err := tr.Transpile("INSERT INTO " + longName + " (a) VALUES (1::regtype)")
	if err != nil {
		t.Fatal(err)
	}
	if res.FallbackToNative {
		t.Fatalf("unexpected fallback; SQL: %s", res.SQL)
	}
	if !strings.Contains(res.SQL, longName) {
		t.Fatalf("Transpile truncated the table name; got: %s", res.SQL)
	}
}
