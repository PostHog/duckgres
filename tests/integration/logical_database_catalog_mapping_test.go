package integration

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq"
)

func TestPgwireLogicalDatabaseCatalogMapping(t *testing.T) {
	if testHarness == nil || testHarness.DuckgresDB == nil {
		t.Skip("integration harness is not available")
	}
	if !testHarness.useDuckLake {
		t.Skip("PR1 logical catalog mapping requires DuckLake mode")
	}

	db := openLogicalDatabaseConnection(t, "duckgres")
	t.Cleanup(func() { _ = db.Close() })

	const (
		logicalSchema      = "bill"
		logicalTable       = "pr1_logical_catalog_users"
		quotedLogicalTable = "QuotedLogicalUsers"
		legacyTable        = "pr1_ducklake_legacy_users"
		metadataProbeTable = "pr1_metadata_probe"
	)

	_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS ducklake.%s.%s`, logicalSchema, logicalTable))
	_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS ducklake.main.%q`, quotedLogicalTable))
	_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS ducklake.main.%s`, legacyTable))
	_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS ducklake.main.%s`, metadataProbeTable))
	_, _ = db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS ducklake.%s CASCADE`, logicalSchema))

	if _, err := db.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS ducklake.%s`, logicalSchema)); err != nil {
		t.Fatalf("create logical schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS ducklake.%s.%s`, logicalSchema, logicalTable))
		_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS ducklake.main.%q`, quotedLogicalTable))
		_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS ducklake.main.%s`, legacyTable))
		_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS ducklake.main.%s`, metadataProbeTable))
		_, _ = db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS ducklake.%s CASCADE`, logicalSchema))
	})

	t.Run("metadata reports logical database", func(t *testing.T) {
		var currentDB string
		if err := db.QueryRow("SELECT current_database()").Scan(&currentDB); err != nil {
			t.Fatalf("query current_database(): %v", err)
		}
		if currentDB != "duckgres" {
			t.Fatalf("current_database() = %q, want %q", currentDB, "duckgres")
		}

		var datname string
		if err := db.QueryRow("SELECT datname FROM pg_catalog.pg_database WHERE datname = current_database()").Scan(&datname); err != nil {
			t.Fatalf("query pg_database/current_database: %v", err)
		}
		if datname != "duckgres" {
			t.Fatalf("pg_database datname = %q, want %q", datname, "duckgres")
		}

		if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE duckgres.public.%s (id INTEGER)`, metadataProbeTable)); err != nil {
			t.Fatalf("create metadata probe table: %v", err)
		}

		var tableCatalog string
		if err := db.QueryRow(fmt.Sprintf(
			"SELECT table_catalog FROM information_schema.tables WHERE table_name = '%s' LIMIT 1",
			metadataProbeTable,
		)).Scan(&tableCatalog); err != nil {
			t.Fatalf("query information_schema.tables: %v", err)
		}
		if tableCatalog != "duckgres" {
			t.Fatalf("information_schema.tables table_catalog = %q, want %q", tableCatalog, "duckgres")
		}

		var columnCatalog string
		if err := db.QueryRow(fmt.Sprintf(
			"SELECT table_catalog FROM information_schema.columns WHERE table_name = '%s' AND column_name = 'id' LIMIT 1",
			metadataProbeTable,
		)).Scan(&columnCatalog); err != nil {
			t.Fatalf("query information_schema.columns: %v", err)
		}
		if columnCatalog != "duckgres" {
			t.Fatalf("information_schema.columns table_catalog = %q, want %q", columnCatalog, "duckgres")
		}

		var schemaCatalog string
		if err := db.QueryRow("SELECT catalog_name FROM information_schema.schemata WHERE schema_name = 'public' LIMIT 1").Scan(&schemaCatalog); err != nil {
			t.Fatalf("query information_schema.schemata: %v", err)
		}
		if schemaCatalog != "duckgres" {
			t.Fatalf("information_schema.schemata catalog_name = %q, want %q", schemaCatalog, "duckgres")
		}
	})

	t.Run("logical catalog unquoted execution works", func(t *testing.T) {
		if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE duckgres.%s.%s (id INTEGER, name TEXT)`, logicalSchema, logicalTable)); err != nil {
			t.Fatalf("create logical catalog table: %v", err)
		}
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO duckgres.%s.%s VALUES (1, 'alice')`, logicalSchema, logicalTable)); err != nil {
			t.Fatalf("insert logical catalog table: %v", err)
		}

		var name string
		if err := db.QueryRow(fmt.Sprintf(`SELECT name FROM duckgres.%s.%s WHERE id = 1`, logicalSchema, logicalTable)).Scan(&name); err != nil {
			t.Fatalf("select logical catalog table: %v", err)
		}
		if name != "alice" {
			t.Fatalf("logical catalog select returned %q, want %q", name, "alice")
		}
	})

	t.Run("logical catalog quoted execution works", func(t *testing.T) {
		if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE "duckgres"."public"."%s" (id INTEGER)`, quotedLogicalTable)); err != nil {
			t.Fatalf("create quoted logical catalog table: %v", err)
		}
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO "duckgres"."public"."%s" VALUES (7)`, quotedLogicalTable)); err != nil {
			t.Fatalf("insert quoted logical catalog table: %v", err)
		}

		var count int
		if err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM "duckgres"."public"."%s"`, quotedLogicalTable)).Scan(&count); err != nil {
			t.Fatalf("select quoted logical catalog table: %v", err)
		}
		if count != 1 {
			t.Fatalf("quoted logical catalog row count = %d, want %d", count, 1)
		}
	})

	t.Run("physical ducklake references remain valid", func(t *testing.T) {
		if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE ducklake.main.%s (id INTEGER)`, legacyTable)); err != nil {
			t.Fatalf("create legacy ducklake table: %v", err)
		}
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO ducklake.main.%s VALUES (9)`, legacyTable)); err != nil {
			t.Fatalf("insert legacy ducklake table: %v", err)
		}

		var count int
		if err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM ducklake.main.%s`, legacyTable)).Scan(&count); err != nil {
			t.Fatalf("select legacy ducklake table: %v", err)
		}
		if count != 1 {
			t.Fatalf("legacy ducklake row count = %d, want %d", count, 1)
		}
	})
}

func openLogicalDatabaseConnection(t *testing.T, database string) *sql.DB {
	t.Helper()

	connStr := fmt.Sprintf(
		"host=127.0.0.1 port=%d user=testuser password=testpass dbname=%s sslmode=require",
		testHarness.dgPort,
		database,
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("open logical database connection: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatalf("ping logical database connection: %v", err)
	}
	return db
}
