package integration

import "testing"

func TestPgwireLogicalCatalogMapping(t *testing.T) {
	if testHarness == nil || testHarness.DuckgresDB == nil {
		t.Fatal("test harness is not initialized")
	}
	if !testHarness.useDuckLake {
		t.Skip("logical catalog mapping requires DuckLake mode")
	}

	db := testHarness.DuckgresDB

	mustExec(t, db, "DROP TABLE IF EXISTS ducklake.main.logical_catalog_mapping_test")
	t.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS ducklake.main.logical_catalog_mapping_test")
	})

	var currentDB string
	if err := db.QueryRow("SELECT current_database()").Scan(&currentDB); err != nil {
		t.Fatalf("query current_database(): %v", err)
	}
	if currentDB != "test" {
		t.Fatalf("expected current_database() = %q, got %q", "test", currentDB)
	}

	var datname string
	if err := db.QueryRow("SELECT datname FROM pg_database WHERE datname = current_database()").Scan(&datname); err != nil {
		t.Fatalf("query pg_database/current_database: %v", err)
	}
	if datname != "test" {
		t.Fatalf("expected pg_database row %q, got %q", "test", datname)
	}

	mustExec(t, db, "CREATE TABLE test.public.logical_catalog_mapping_test (id INTEGER)")
	mustExec(t, db, `INSERT INTO "test"."public".logical_catalog_mapping_test VALUES (7)`)

	var legacyCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM ducklake.main.logical_catalog_mapping_test").Scan(&legacyCount); err != nil {
		t.Fatalf("query ducklake compatibility table: %v", err)
	}
	if legacyCount != 1 {
		t.Fatalf("expected ducklake compatibility count 1, got %d", legacyCount)
	}

	var tableCatalog string
	if err := db.QueryRow(`
		SELECT table_catalog
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = 'logical_catalog_mapping_test'
	`).Scan(&tableCatalog); err != nil {
		t.Fatalf("query information_schema.tables: %v", err)
	}
	if tableCatalog != "test" {
		t.Fatalf("expected information_schema.tables.table_catalog = %q, got %q", "test", tableCatalog)
	}

	var schemaCatalog string
	if err := db.QueryRow(`
		SELECT catalog_name
		FROM information_schema.schemata
		WHERE schema_name = 'public'
		ORDER BY catalog_name
		LIMIT 1
	`).Scan(&schemaCatalog); err != nil {
		t.Fatalf("query information_schema.schemata: %v", err)
	}
	if schemaCatalog != "test" {
		t.Fatalf("expected information_schema.schemata.catalog_name = %q, got %q", "test", schemaCatalog)
	}
}
