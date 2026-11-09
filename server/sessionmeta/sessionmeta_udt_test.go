package sessionmeta

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// Metabase's column sync compares udt_schema to strings. A bare NULL in the
// session view is typed INTEGER, which makes that predicate fail with a
// conversion error.
func TestSessionInformationSchemaColumnsCompatUDTPredicateBinds(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(buildSessionMetadataSQL("memory")); err != nil {
		t.Fatalf("create session metadata: %v", err)
	}
	var n int
	err = db.QueryRow(`SELECT count(*) FROM main.information_schema_columns_compat c
		WHERE c.udt_schema IN ('public', 'pg_catalog')`).Scan(&n)
	if err != nil {
		t.Fatalf("udt_schema string predicate failed: %v", err)
	}
}
