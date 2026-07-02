package transpiler

import (
	"strings"
	"testing"
)

// Regression tests for Classify wiring gaps: queries whose only PG-ism used a
// spelling the classifier's substring tokens missed (CAST vs ::, newline/tab
// separated keywords, space before '(' ...) and therefore never reached the
// transform that already handled them.

func wantFlags(t *testing.T, cfg Config, input string, want TransformFlags) {
	t.Helper()
	cls := Classify(input, cfg)
	if cls.Direct {
		t.Fatalf("Classify(%q) = Direct, want flags containing %d", input, want)
	}
	if cls.Flags&want != want {
		t.Errorf("Classify(%q) flags = %d, want flags to contain %d (missing: %d)",
			input, cls.Flags, want, want&^cls.Flags)
	}
}

func wantDirect(t *testing.T, cfg Config, input string) {
	t.Helper()
	cls := Classify(input, cfg)
	if !cls.Direct {
		t.Errorf("Classify(%q) = {Direct: false, Flags: %d}, want Direct", input, cls.Flags)
	}
}

func transpileSQL(t *testing.T, tr *Transpiler, input string) string {
	t.Helper()
	res, err := tr.Transpile(input)
	if err != nil {
		t.Fatalf("Transpile(%q) error: %v", input, err)
	}
	if res.FallbackToNative {
		t.Fatalf("Transpile(%q) fell back to native, want transpiled SQL", input)
	}
	return res.SQL
}

// Fix 1: CAST(x AS regtype/regclass/...) and ::regoper/::regconfig/::regdictionary
// must reach TypeCastTransform, not just the ::regtype-style spellings.
func TestClassifyWiring_RegCastSpellings(t *testing.T) {
	cfg := DefaultConfig()
	wantFlags(t, cfg, "SELECT CAST('pg_class' AS regclass)", FlagTypeCast)
	wantFlags(t, cfg, "SELECT CAST('pg_class' AS pg_catalog.regclass)", FlagTypeCast)
	wantFlags(t, cfg, "SELECT CAST(typname AS regtype) FROM pg_type", FlagTypeCast)
	wantFlags(t, cfg, "SELECT 'english'::regconfig", FlagTypeCast)
	wantFlags(t, cfg, "SELECT '=(integer,integer)'::regoperator", FlagTypeCast)
	wantFlags(t, cfg, "SELECT 'simple'::regdictionary", FlagTypeCast)

	tr := New(cfg)

	// CAST spellings must produce identical output to the :: spellings.
	castOut := transpileSQL(t, tr, "SELECT CAST('pg_class' AS regclass)")
	colonOut := transpileSQL(t, tr, "SELECT 'pg_class'::regclass")
	if castOut != colonOut {
		t.Errorf("CAST spelling = %q, :: spelling = %q; want identical", castOut, colonOut)
	}
	if strings.Contains(strings.ToLower(castOut), "regclass") {
		t.Errorf("CAST(... AS regclass) output still references regclass: %q", castOut)
	}

	qualCastOut := transpileSQL(t, tr, "SELECT CAST('pg_class' AS pg_catalog.regclass)")
	qualColonOut := transpileSQL(t, tr, "SELECT 'pg_class'::pg_catalog.regclass")
	if qualCastOut != qualColonOut {
		t.Errorf("CAST pg_catalog spelling = %q, :: spelling = %q; want identical", qualCastOut, qualColonOut)
	}

	for _, tc := range []struct{ input, gone string }{
		{"SELECT 'english'::regconfig", "regconfig"},
		{"SELECT '=(integer,integer)'::regoperator", "regoperator"},
		{"SELECT 'simple'::regdictionary", "regdictionary"},
	} {
		out := strings.ToLower(transpileSQL(t, tr, tc.input))
		if strings.Contains(out, tc.gone) {
			t.Errorf("Transpile(%q) = %q, should not contain %q", tc.input, out, tc.gone)
		}
		if !strings.Contains(out, "varchar") {
			t.Errorf("Transpile(%q) = %q, should cast to varchar", tc.input, out)
		}
	}
}

// Fix 2: E-string and dollar-quoted bytea hex literals must set FlagLiterals —
// the old `'\X` signature only matched the plain-quoted spelling, silently
// storing wrong bytes for E'\\x..' / $$\x..$$.
func TestClassifyWiring_ByteaHexLiteralSpellings(t *testing.T) {
	cfg := DefaultConfig()
	wantFlags(t, cfg, `SELECT octet_length(E'\\xDEADBEEF'::bytea)`, FlagLiterals)
	wantFlags(t, cfg, `SELECT octet_length($$\xDEADBEEF$$::bytea)`, FlagLiterals)

	tr := New(cfg)
	for _, input := range []string{
		`SELECT octet_length(E'\\xDEADBEEF'::bytea)`,
		`SELECT octet_length($$\xDEADBEEF$$::bytea)`,
		`SELECT octet_length('\xDEADBEEF'::bytea)`, // plain spelling keeps working
	} {
		out := transpileSQL(t, tr, input)
		if !strings.Contains(out, "unhex('DEADBEEF')") {
			t.Errorf("Transpile(%q) = %q, want unhex('DEADBEEF')", input, out)
		}
	}
}

// Fix 10: geometric/range/multirange/timestampntz/rowversion types mapped in
// types.go need Classify tokens so they ever reach TypeMappingTransform.
func TestClassifyWiring_GeometricRangeTypeTokens(t *testing.T) {
	cfg := DefaultConfig()
	for _, input := range []string{
		"SELECT '(1,2)'::point",
		"SELECT NULL::lseg",
		"SELECT NULL::polygon",
		"SELECT NULL::circle",
		"SELECT NULL::int4range",
		"SELECT NULL::int8range",
		"SELECT NULL::numrange",
		"SELECT NULL::tsrange",
		"SELECT NULL::tstzrange",
		"SELECT NULL::daterange",
		"SELECT NULL::int4multirange",
		"SELECT now()::timestampntz",
		"SELECT 'ab'::rowversion",
		"CREATE TABLE geo_t (p point, r int4range)",
	} {
		wantFlags(t, cfg, input, FlagTypeMapping)
	}

	tr := New(cfg)
	for _, tc := range []struct{ input, want, gone string }{
		{"SELECT NULL::daterange", "text", "daterange"},
		{"SELECT '(1,2)'::point", "text", "point"},
		{"SELECT NULL::int4multirange", "text", "int4multirange"},
		{"SELECT now()::timestampntz", "timestamp", "timestampntz"},
		{"SELECT 'ab'::rowversion", "blob", "rowversion"},
		{"CREATE TABLE geo_t (p point, r int4range)", "text", "int4range"},
	} {
		out := strings.ToLower(transpileSQL(t, tr, tc.input))
		if !strings.Contains(out, tc.want) {
			t.Errorf("Transpile(%q) = %q, should contain %q", tc.input, out, tc.want)
		}
		if strings.Contains(out, tc.gone) {
			t.Errorf("Transpile(%q) = %q, should NOT contain %q", tc.input, out, tc.gone)
		}
	}

	// Window-frame RANGE BETWEEN must stay Tier-0 (no bare RANGE token).
	wantDirect(t, cfg, "SELECT sum(x) OVER (ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t")
}

// Fix 11: CAST('{1,2,3}' AS int[]) must trigger the array-literal rewrite like
// the '{1,2,3}'::int[] spelling does.
func TestClassifyWiring_BraceLiteralCastSpelling(t *testing.T) {
	cfg := DefaultConfig()
	wantFlags(t, cfg, "SELECT CAST('{1,2,3}' AS int[])", FlagTypeCast)
	wantFlags(t, cfg, "SELECT CAST('{a,b}' AS text[])", FlagTypeCast)

	tr := New(cfg)
	for _, typ := range []string{"int", "integer", "text"} {
		castOut := transpileSQL(t, tr, "SELECT CAST('{1,2,3}' AS "+typ+"[])")
		colonOut := transpileSQL(t, tr, "SELECT '{1,2,3}'::"+typ+"[]")
		if castOut != colonOut {
			t.Errorf("CAST spelling (%s[]) = %q, :: spelling = %q; want identical", typ, castOut, colonOut)
		}
		if !strings.Contains(castOut, "ARRAY[") {
			t.Errorf("Transpile CAST('{1,2,3}' AS %s[]) = %q, want ARRAY[...] rewrite", typ, castOut)
		}
	}

	// Brace-literal column alias must stay Tier-0 (no rewrite needed).
	wantDirect(t, cfg, `SELECT '{"a":1}' AS j`)
}

// Fix 12: multi-word and trailing-space tokens must match newline/tab/multi-space
// spellings via whitespace normalization, per token (an unrelated flag rescuing
// the query from Tier-0 is not enough — the query's own transform must run).
func TestClassifyWiring_WhitespaceNormalizedTokens(t *testing.T) {
	cfg := DefaultConfig()

	wantFlags(t, cfg, "SELECT 1 AS x FOR\nUPDATE", FlagLocking)
	wantFlags(t, cfg, "SELECT 1 AS x FOR\n\tKEY   SHARE", FlagLocking)
	wantFlags(t, cfg, "SELECT 'hello' SIMILAR\nTO 'h%'", FlagOperators)
	wantFlags(t, cfg, "START\nTRANSACTION", FlagSetShow)
	wantFlags(t, cfg, "START TRANSACTION\nISOLATION LEVEL READ COMMITTED", FlagSetShow)

	dlCfg := Config{DuckLakeMode: true}
	wantFlags(t, dlCfg, "ALTER\nTABLE t RENAME TO t2", FlagDDL)
	wantFlags(t, dlCfg, "CREATE TABLE t (a int DEFAULT\n5)", FlagDDL)
	wantFlags(t, dlCfg, "CREATE\nINDEX idx ON t (a)", FlagDDL)

	tr := New(cfg)
	for _, input := range []string{
		"SELECT 1 AS x FOR\nUPDATE",
		"SELECT 1 AS x FOR\n\tKEY   SHARE",
	} {
		out := transpileSQL(t, tr, input)
		if out != "SELECT 1 AS x" {
			t.Errorf("Transpile(%q) = %q, want locking clause stripped (SELECT 1 AS x)", input, out)
		}
	}

	// Per-token check: FlagOperators (||) already pulls this query out of
	// Tier-0, but the locking clause must still be stripped.
	out := transpileSQL(t, tr, "SELECT '1' || '' AS x FOR\nUPDATE")
	if strings.Contains(strings.ToUpper(out), "FOR UPDATE") {
		t.Errorf("Transpile(operator-rescued FOR\\nUPDATE) = %q, locking clause not stripped", out)
	}
}

// Fix 13: SET/SHOW/RESET/DISCARD/BEGIN/START prefix detection must use word
// boundaries, not a literal trailing space.
func TestClassifyWiring_PrefixWordBoundary(t *testing.T) {
	cfg := DefaultConfig()
	wantFlags(t, cfg, "SET\nstatement_timeout = 0", FlagSetShow)
	wantFlags(t, cfg, "SHOW\nstatement_timeout", FlagSetShow)
	wantFlags(t, cfg, "RESET\nstatement_timeout", FlagSetShow)
	wantFlags(t, cfg, "DISCARD\nALL", FlagSetShow)
	wantFlags(t, cfg, "SET\tapplication_name = 'x'", FlagSetShow)

	// Word boundary: a prefix-sharing identifier is not a SET/BEGIN statement.
	wantDirect(t, cfg, "BEGINNING")
	wantDirect(t, cfg, "SETTINGS_DUMP")

	tr := New(cfg)
	res, err := tr.Transpile("SET\nstatement_timeout = 0")
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}
	if !res.IsIgnoredSet {
		t.Errorf("Transpile(SET\\nstatement_timeout = 0).IsIgnoredSet = false, want true")
	}

	out := transpileSQL(t, tr, "SHOW\nstatement_timeout")
	if !strings.HasPrefix(strings.ToUpper(out), "SELECT") || !strings.Contains(out, "statement_timeout") {
		t.Errorf("Transpile(SHOW\\nstatement_timeout) = %q, want SELECT ... AS statement_timeout rewrite", out)
	}
}

// Fix 18: writable-CTE gate tokens must be bare keywords — DELETE\nFROM inside
// a CTE and minified WITH"d"AS(...) have no space after the keyword.
func TestClassifyWiring_WritableCTEBareTokens(t *testing.T) {
	cfg := DefaultConfig()
	wantFlags(t, cfg, "WITH d AS (DELETE\nFROM t RETURNING a) SELECT * FROM d", FlagWritableCTE)
	wantFlags(t, cfg, "WITH\nins AS (INSERT INTO probe_t VALUES (1) RETURNING i) SELECT i FROM ins", FlagWritableCTE)
	wantFlags(t, cfg, `WITH"d"AS(DELETE FROM t RETURNING a)SELECT * FROM "d"`, FlagWritableCTE)

	tr := New(cfg)
	for _, input := range []string{
		"WITH d AS (DELETE\nFROM probe_t RETURNING a) SELECT * FROM d",
		"WITH\nins AS (INSERT INTO probe_t VALUES (1) RETURNING i) SELECT i FROM ins",
		`WITH"d"AS(DELETE FROM probe_t RETURNING a)SELECT * FROM "d"`,
	} {
		res, err := tr.Transpile(input)
		if err != nil {
			t.Fatalf("Transpile(%q) error: %v", input, err)
		}
		if len(res.Statements) < 2 {
			t.Errorf("Transpile(%q) Statements = %v, want multi-statement writable-CTE rewrite", input, res.Statements)
		}
	}
}

// Fix 20: ON\nCONFLICT must flag FlagOnConflict (bare CONFLICT token).
func TestClassifyWiring_OnConflictWhitespace(t *testing.T) {
	cfg := DefaultConfig()
	wantFlags(t, cfg, "INSERT INTO t (a, b) VALUES (1, 2) ON\nCONFLICT (a) DO NOTHING", FlagOnConflict)
	wantFlags(t, cfg, "INSERT INTO t (a, b) VALUES (1, 2) ON\tCONFLICT (a) DO NOTHING", FlagOnConflict)

	dlTr := New(Config{DuckLakeMode: true})
	res, err := dlTr.Transpile("INSERT INTO t (a, b) VALUES (1, 2) ON\nCONFLICT (a) DO NOTHING")
	if err != nil {
		t.Fatalf("DuckLake Transpile(ON\\nCONFLICT) error: %v", err)
	}
	if res.Error == nil {
		t.Fatalf("DuckLake Transpile(ON\\nCONFLICT) should reject ON CONFLICT, got SQL=%q", res.SQL)
	}

	// False positive (CONFLICT in a string literal) must be a harmless no-op.
	tr := New(cfg)
	out := transpileSQL(t, tr, "SELECT note FROM audit WHERE note = 'ON CONFLICT'")
	if strings.Contains(strings.ToUpper(out), "MERGE") {
		t.Errorf("Transpile(literal 'ON CONFLICT') = %q, must not rewrite", out)
	}
}

// Fix 21: whitespace between a function name and '(' must not defeat
// paren-suffixed tokens (Tier-0 escape; regexp_matches silently diverges).
func TestClassifyWiring_SpaceBeforeParen(t *testing.T) {
	cfg := DefaultConfig()
	wantFlags(t, cfg, "SELECT every (true)", FlagFunctions)
	wantFlags(t, cfg, "SELECT every\n(true)", FlagFunctions)
	wantFlags(t, cfg, "SELECT regexp_matches ('xyz', 'y')", FlagFunctions)
	wantFlags(t, cfg, "SELECT pg_size_pretty (123)", FlagPgCatalog)
	wantFlags(t, cfg, "SELECT current_database ()", FlagFuncAlias)

	tr := New(cfg)
	for _, tc := range []struct{ input, want string }{
		{"SELECT every (true)", "bool_and"},
		{"SELECT every\n(true)", "bool_and"},
		{"SELECT btrim ('  x  ')", "trim"},
		{"SELECT regexp_matches ('xyz', 'y')", "regexp_extract_all"},
	} {
		out := transpileSQL(t, tr, tc.input)
		if !strings.Contains(out, tc.want) {
			t.Errorf("Transpile(%q) = %q, should contain %q", tc.input, out, tc.want)
		}
	}
}

// Fix 23: keyword (paren-less) CURRENT_CATALOG / CURRENT_SCHEMA must be flagged
// so the deparse pass lowercases them (PG-compatible result column names).
func TestClassifyWiring_CurrentCatalogSchemaKeyword(t *testing.T) {
	cfg := DefaultConfig()
	wantFlags(t, cfg, "SELECT CURRENT_CATALOG", FlagFuncAlias)
	wantFlags(t, cfg, "SELECT CURRENT_SCHEMA", FlagFuncAlias)

	tr := New(cfg)
	for _, tc := range []struct{ input, want string }{
		{"SELECT CURRENT_CATALOG", "SELECT current_catalog"},
		{"SELECT CURRENT_SCHEMA", "SELECT current_schema"},
	} {
		out := transpileSQL(t, tr, tc.input)
		if out != tc.want {
			t.Errorf("Transpile(%q) = %q, want %q", tc.input, out, tc.want)
		}
	}
}

// Fix 22: the boolean-predicate gate must match parenthesized literals,
// including interleaved parens and spaces.
func TestClassifyWiring_BooleanPredicateParens(t *testing.T) {
	positives := []string{
		"SELECT * FROM t WHERE flag = (true)",
		"SELECT * FROM t WHERE (true) = flag",
		"SELECT * FROM t WHERE flag != (false)",
		"SELECT * FROM t WHERE flag <> (FALSE)",
		"SELECT * FROM t WHERE flag = ((true))",
		"SELECT * FROM t WHERE flag = ( true )",
		"SELECT * FROM t WHERE flag = ( ( true ) )",
	}
	for _, input := range positives {
		if !NeedsBooleanPredicateRewrite(input) {
			t.Errorf("NeedsBooleanPredicateRewrite(%q) = false, want true", input)
		}
	}

	negatives := []string{
		"SELECT * FROM t WHERE truex = 1",
		"SELECT truely FROM t WHERE a = 1",
		"SELECT istrue(x) FROM t",
	}
	for _, input := range negatives {
		if NeedsBooleanPredicateRewrite(input) {
			t.Errorf("NeedsBooleanPredicateRewrite(%q) = true, want false", input)
		}
	}

	tr := New(DefaultConfig())
	for _, input := range []string{
		"SELECT * FROM t WHERE flag = (true)",
		"SELECT * FROM t WHERE flag = ( ( true ) )",
		"SELECT * FROM t WHERE (true) = flag",
	} {
		out := transpileSQL(t, tr, input)
		if !strings.Contains(out, "IS TRUE") {
			t.Errorf("Transpile(%q) = %q, want IS TRUE rewrite", input, out)
		}
	}
}
