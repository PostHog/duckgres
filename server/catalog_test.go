package server

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestPgDatabaseView(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := initPgCatalog(db); err != nil {
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

	if err := initPgCatalog(db); err != nil {
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
