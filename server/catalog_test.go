package server

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestPgDatabaseView(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := initPgCatalog(db, processStartTime, processStartTime, "dev", "dev"); err != nil {
		t.Fatalf("Failed to init pg_catalog: %v", err)
	}

	// Test that pg_database view has all expected columns (PostgreSQL 16 compatible)
	expectedColumns := []string{
		"oid", "datname", "datdba", "encoding", "datlocprovider",
		"datistemplate", "datallowconn", "datconnlimit", "datfrozenxid",
		"datminmxid", "dattablespace", "datcollate", "datctype",
		"daticulocale", "daticurules", "datcollversion", "datacl",
	}

	rows, err := db.Query("SELECT * FROM pg_database LIMIT 1")
	if err != nil {
		t.Fatalf("Failed to query pg_database: %v", err)
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	if len(columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(columns))
	}

	for i, expected := range expectedColumns {
		if i >= len(columns) {
			t.Errorf("Missing column %d: %s", i, expected)
			continue
		}
		if columns[i] != expected {
			t.Errorf("Column %d: expected %q, got %q", i, expected, columns[i])
		}
	}
}

func TestPgDatabaseViewContent(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := initPgCatalog(db, processStartTime, processStartTime, "dev", "dev"); err != nil {
		t.Fatalf("Failed to init pg_catalog: %v", err)
	}

	// Test that we have the expected databases
	rows, err := db.Query("SELECT datname, datlocprovider, datistemplate, datallowconn FROM pg_database ORDER BY oid")
	if err != nil {
		t.Fatalf("Failed to query pg_database: %v", err)
	}
	defer func() { _ = rows.Close() }()

	expected := []struct {
		datname        string
		datlocprovider string
		datistemplate  bool
		datallowconn   bool
	}{
		{"postgres", "c", false, true},
		{"template0", "c", true, false},
		{"template1", "c", true, true},
		{"testdb", "c", false, true}, // Hardcoded to match integration test PostgreSQL container
	}

	i := 0
	for rows.Next() {
		var datname, datlocprovider string
		var datistemplate, datallowconn bool
		if err := rows.Scan(&datname, &datlocprovider, &datistemplate, &datallowconn); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if i >= len(expected) {
			t.Errorf("Unexpected extra row: %s", datname)
			continue
		}

		if datname != expected[i].datname {
			t.Errorf("Row %d datname: expected %q, got %q", i, expected[i].datname, datname)
		}
		if datlocprovider != expected[i].datlocprovider {
			t.Errorf("Row %d datlocprovider: expected %q, got %q", i, expected[i].datlocprovider, datlocprovider)
		}
		if datistemplate != expected[i].datistemplate {
			t.Errorf("Row %d datistemplate: expected %v, got %v", i, expected[i].datistemplate, datistemplate)
		}
		if datallowconn != expected[i].datallowconn {
			t.Errorf("Row %d datallowconn: expected %v, got %v", i, expected[i].datallowconn, datallowconn)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), i)
	}
}

func TestPgStatioUserTablesViewColumns(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := initPgCatalog(db, processStartTime, processStartTime, "dev", "dev"); err != nil {
		t.Fatalf("Failed to init pg_catalog: %v", err)
	}

	expectedColumns := []string{
		"relid", "schemaname", "relname",
		"heap_blks_read", "heap_blks_hit",
		"idx_blks_read", "idx_blks_hit",
		"toast_blks_read", "toast_blks_hit",
		"tidx_blks_read", "tidx_blks_hit",
	}

	rows, err := db.Query("SELECT * FROM pg_statio_user_tables LIMIT 1")
	if err != nil {
		t.Fatalf("Failed to query pg_statio_user_tables: %v", err)
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	if len(columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(columns))
	}

	for i, expected := range expectedColumns {
		if i >= len(columns) {
			t.Errorf("Missing column %d: %s", i, expected)
			continue
		}
		if columns[i] != expected {
			t.Errorf("Column %d: expected %q, got %q", i, expected, columns[i])
		}
	}
}

// TestPgTypeHasComplexTypeOIDs verifies that the pg_type view includes
// synthetic entries for PostgreSQL OIDs used by pg_attribute. Without these,
// JOIN pg_attribute ON atttypid = pg_type.oid silently drops columns with
// complex types (JSON, arrays, structs), making tables appear to have fewer
// columns than they actually do.
func TestPgTypeHasComplexTypeOIDs(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := initPgCatalog(db, processStartTime, processStartTime, "dev", "dev"); err != nil {
		t.Fatalf("Failed to init pg_catalog: %v", err)
	}

	// These OIDs are assigned by the pg_attribute view for complex types.
	// Each must have a matching row in pg_type or the JOIN drops the column.
	required := []struct {
		oid  int
		name string
	}{
		{114, "json"},
		{3802, "jsonb"},
		{25, "text"},
		{1042, "bpchar"},
		{2249, "record"},
		// Array types
		{1000, "_bool"},
		{1007, "_int4"},
		{1015, "_varchar"},
		{1016, "_int8"},
		{1022, "_float8"},
	}

	for _, r := range required {
		var count int
		err := db.QueryRow("SELECT count(*) FROM pg_type WHERE oid = ?", r.oid).Scan(&count)
		if err != nil {
			t.Errorf("Failed to query pg_type for OID %d (%s): %v", r.oid, r.name, err)
			continue
		}
		if count == 0 {
			t.Errorf("pg_type missing OID %d (%s) — columns with this type will be invisible", r.oid, r.name)
		}
	}
}

// TestPgAttributeJoinPgTypeComplexColumns verifies end-to-end that creating a
// table with JSON and array columns produces rows in the pg_attribute JOIN
// pg_type result. This is the exact query postgres_scanner uses to read table
// schemas — if it returns fewer columns than the table has, the bug is present.
func TestPgAttributeJoinPgTypeComplexColumns(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Load json extension for JSON type support
	if _, err := db.Exec("SET autoinstall_known_extensions=1; SET autoload_known_extensions=1"); err != nil {
		t.Fatalf("Failed to set extension settings: %v", err)
	}

	if err := initPgCatalog(db, processStartTime, processStartTime, "dev", "dev"); err != nil {
		t.Fatalf("Failed to init pg_catalog: %v", err)
	}

	// Create a table with complex types
	if _, err := db.Exec("CREATE TABLE test_complex(id INTEGER, data JSON, tags VARCHAR[], name VARCHAR)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// This is the query postgres_scanner uses to discover columns (simplified).
	// The JOIN on pg_type is what caused columns to be silently dropped.
	query := `
		SELECT attname, pg_type.typname
		FROM pg_class
		JOIN pg_namespace ON relnamespace = pg_namespace.oid
		JOIN pg_attribute ON pg_class.oid = pg_attribute.attrelid
		JOIN pg_type ON atttypid = pg_type.oid
		WHERE attnum > 0 AND relname = 'test_complex'
		ORDER BY attnum
	`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Failed to query pg_attribute JOIN pg_type: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var columns []string
	for rows.Next() {
		var attname, typname string
		if err := rows.Scan(&attname, &typname); err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}
		columns = append(columns, attname)
	}

	expected := []string{"id", "data", "tags", "name"}
	if len(columns) != len(expected) {
		t.Fatalf("pg_attribute JOIN pg_type returned %d columns %v, want %d %v", len(columns), columns, len(expected), expected)
	}
	for i, col := range columns {
		if col != expected[i] {
			t.Errorf("column %d: got %q, want %q", i, col, expected[i])
		}
	}
}

func TestUptimeMacros(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	start := time.Now()
	if err := initPgCatalog(db, start, start, "dev", "dev"); err != nil {
		t.Fatalf("Failed to init pg_catalog: %v", err)
	}

	// Verify both macros return INTERVAL type and a non-negative value
	for _, fn := range []string{"uptime", "worker_uptime"} {
		var typeName string
		if err := db.QueryRow(fmt.Sprintf("SELECT pg_typeof(%s())", fn)).Scan(&typeName); err != nil {
			t.Fatalf("%s() query failed: %v", fn, err)
		}
		if typeName != "interval" {
			t.Errorf("%s() returned type %q, want interval", fn, typeName)
		}

		// Verify the interval is non-negative (catches timezone bugs where
		// UTC vs local mismatch produces a negative offset like -08:00:00)
		var isNonNeg bool
		if err := db.QueryRow(fmt.Sprintf("SELECT %s() >= INTERVAL '0 seconds'", fn)).Scan(&isNonNeg); err != nil {
			t.Fatalf("%s() >= 0 query failed: %v", fn, err)
		}
		if !isNonNeg {
			t.Errorf("%s() returned a negative interval — possible timezone bug", fn)
		}
	}
}
