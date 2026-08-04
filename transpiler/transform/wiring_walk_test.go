package transform

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// Fix: walkSelectStmt skipped ValuesLists, so WalkFunc-based transforms
// (LiteralTransform, TypeCastTransform, placeholder counting) never visited
// INSERT ... VALUES expressions.

func TestWalkSelectStmt_InsertValuesByteaHex(t *testing.T) {
	tr := NewLiteralTransform()
	out := deparseAfter(t, tr, `INSERT INTO byt VALUES ('\xDEAD'::bytea)`)
	if !strings.Contains(strings.ToLower(out), "unhex('dead')") {
		t.Errorf("expected unhex('DEAD') in VALUES, got %q", out)
	}
	if strings.Contains(out, `\x`) {
		t.Errorf("hex prefix should be gone, got %q", out)
	}
}

func TestWalkSelectStmt_InsertValuesBitString(t *testing.T) {
	tr := NewLiteralTransform()
	out := deparseAfter(t, tr, `INSERT INTO flags VALUES (B'101')`)
	low := strings.ToLower(out)
	if strings.Contains(low, "b'101'") {
		t.Errorf("raw bit literal should have been rewritten to a '101'::BIT cast, got %q", out)
	}
	if !strings.Contains(low, "'101'") || !strings.Contains(low, "bit") {
		t.Errorf("expected '101'::BIT in VALUES, got %q", out)
	}
}

func TestWalkSelectStmt_InsertValuesMultiRow(t *testing.T) {
	tr := NewLiteralTransform()
	out := deparseAfter(t, tr, `INSERT INTO byt VALUES ('\xDE'::bytea), ('\xAD'::bytea)`)
	low := strings.ToLower(out)
	if !strings.Contains(low, "unhex('de')") || !strings.Contains(low, "unhex('ad')") {
		t.Errorf("expected both rows rewritten, got %q", out)
	}
}

func TestWalkSelectStmt_BareValuesByteaHex(t *testing.T) {
	tr := NewLiteralTransform()
	out := deparseAfter(t, tr, `VALUES ('\xDEAD'::bytea)`)
	if !strings.Contains(strings.ToLower(out), "unhex('dead')") {
		t.Errorf("expected unhex('DEAD') in bare VALUES, got %q", out)
	}
}

// Fix: pg_table_is_visible was missing from CustomMacros, so it was never
// memory.main-qualified in DuckLake mode (unlike sibling macros such as
// pg_get_userbyid).

func TestPgCatalogTransform_PgTableIsVisibleQualifiedInDuckLakeMode(t *testing.T) {
	tr := NewPgCatalogTransformWithConfig(true)
	for _, sql := range []string{
		`SELECT pg_table_is_visible(1)`,
		`SELECT pg_catalog.pg_table_is_visible(1)`,
	} {
		out := deparseAfter(t, tr, sql)
		if !strings.Contains(strings.ToLower(out), "memory.main.pg_table_is_visible") {
			t.Errorf("expected memory.main.pg_table_is_visible for %q, got %q", sql, out)
		}
	}
}

func TestPgCatalogTransform_PgTableIsVisibleInPgClassFilter(t *testing.T) {
	tr := NewPgCatalogTransformWithConfig(true)
	out := deparseAfter(t, tr,
		`SELECT c.relname FROM pg_catalog.pg_class c WHERE pg_catalog.pg_table_is_visible(c.oid)`)
	low := strings.ToLower(out)
	if !strings.Contains(low, "memory.main.pg_table_is_visible") {
		t.Errorf("expected qualified macro in WHERE, got %q", out)
	}
}

// customMacroExemptions lists macros registered in server/catalog.go
// initPgCatalog that intentionally do NOT appear in CustomMacros.
var customMacroExemptions = map[string]string{
	// "overlaps" is a quoted SQL keyword: pg_query parses x OVERLAPS y into a
	// dedicated expression node, never a FuncCall named overlaps, so the
	// qualification path can't apply.
	"overlaps": "quoted keyword macro, unreachable as a FuncCall",
}

// TestCustomMacrosCoverCatalogMacros cross-checks CustomMacros against the
// macros actually registered in server/catalog.go so the next macro added
// there can't silently miss memory.main qualification in DuckLake mode.
func TestCustomMacrosCoverCatalogMacros(t *testing.T) {
	src, err := os.ReadFile("../../server/catalog.go")
	if err != nil {
		t.Fatalf("read server/catalog.go: %v", err)
	}
	re := regexp.MustCompile(`CREATE OR REPLACE (?:TEMP )?MACRO\s+"?([A-Za-z_][A-Za-z_0-9]*)"?`)
	matches := re.FindAllStringSubmatch(string(src), -1)
	if len(matches) == 0 {
		t.Fatal("no CREATE OR REPLACE MACRO statements found in server/catalog.go; regex stale?")
	}
	tr := NewPgCatalogTransformWithConfig(true)
	for _, m := range matches {
		name := strings.ToLower(m[1])
		if _, exempt := customMacroExemptions[name]; exempt {
			continue
		}
		if !tr.CustomMacros[name] {
			t.Errorf("macro %q is registered in server/catalog.go but missing from CustomMacros; "+
				"it will not be memory.main-qualified in DuckLake mode", name)
		}
	}
}

// Fix: nummultirange/tsmultirange/tstzmultirange/datemultirange were missing
// from typeMapping (the int4/int8 multirange entries already existed).

func TestTypeMappingTransform_MultirangeCasts(t *testing.T) {
	for _, typ := range []string{
		"nummultirange", "tsmultirange", "tstzmultirange", "datemultirange",
		// control: pre-existing entries
		"int4multirange", "int8multirange",
	} {
		tr := NewTypeMappingTransform()
		out := deparseAfter(t, tr, "SELECT NULL::"+typ)
		low := strings.ToLower(out)
		if !strings.Contains(low, "::text") || strings.Contains(low, "multirange") {
			t.Errorf("expected NULL::%s to rewrite to ::text, got %q", typ, out)
		}
	}
}

// Fix: information_schema.sequences and .routines passed through to DuckDB,
// which has neither — route them to compat views like columns/tables/etc.

func TestInformationSchemaTransform_SequencesAndRoutines(t *testing.T) {
	cases := map[string]string{
		`SELECT count(*) FROM information_schema.sequences`: "memory.main.information_schema_sequences_compat",
		`SELECT count(*) FROM information_schema.routines`:  "memory.main.information_schema_routines_compat",
	}
	for sql, want := range cases {
		tr := NewInformationSchemaTransformWithConfig(false)
		out := deparseAfter(t, tr, sql)
		if !strings.Contains(strings.ToLower(out), want) {
			t.Errorf("expected %s for %q, got %q", want, sql, out)
		}
	}
}
