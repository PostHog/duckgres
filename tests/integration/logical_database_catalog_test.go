package integration

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq"
)

func openLogicalDatabaseConn(t *testing.T, dbName string) *sql.DB {
	t.Helper()

	connStr := fmt.Sprintf(
		"host=127.0.0.1 port=%d user=testuser password=testpass dbname=%s sslmode=require",
		testHarness.dgPort,
		dbName,
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("sql.Open(%q): %v", dbName, err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatalf("Ping(%q): %v", dbName, err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return db
}

func TestLogicalDatabaseCatalogMetadata(t *testing.T) {
	if !testHarness.useDuckLake {
		t.Skip("logical catalog mapping is only relevant in DuckLake mode")
	}

	db := openLogicalDatabaseConn(t, "duckgres_catalog")
	mustExec(t, db, "CREATE SCHEMA IF NOT EXISTS bill")
	mustExec(t, db, "CREATE OR REPLACE VIEW bill.logical_catalog_view AS SELECT 1 AS id")

	t.Run("current_database_reports_logical_name", func(t *testing.T) {
		var current string
		if err := db.QueryRow("SELECT current_database()").Scan(&current); err != nil {
			t.Fatalf("SELECT current_database(): %v", err)
		}
		if current != "duckgres_catalog" {
			t.Fatalf("current_database() = %q, want %q", current, "duckgres_catalog")
		}
	})

	t.Run("pg_database_reports_logical_name", func(t *testing.T) {
		var datname string
		if err := db.QueryRow(
			"SELECT datname FROM pg_catalog.pg_database WHERE datname = current_database()",
		).Scan(&datname); err != nil {
			t.Fatalf("pg_database lookup: %v", err)
		}
		if datname != "duckgres_catalog" {
			t.Fatalf("datname = %q, want %q", datname, "duckgres_catalog")
		}
	})

	t.Run("information_schema_catalog_columns_report_logical_name", func(t *testing.T) {
		checks := []struct {
			name  string
			query string
		}{
			{
				name: "tables",
				query: `
					SELECT DISTINCT table_catalog
					FROM information_schema.tables
					WHERE table_schema = 'public' AND table_name = 'users'
				`,
			},
			{
				name: "columns",
				query: `
					SELECT DISTINCT table_catalog
					FROM information_schema.columns
					WHERE table_schema = 'public' AND table_name = 'users'
				`,
			},
			{
				name: "views",
				query: `
					SELECT DISTINCT table_catalog
					FROM information_schema.views
					WHERE table_schema = 'bill' AND table_name = 'logical_catalog_view'
				`,
			},
			{
				name: "schemata",
				query: `
					SELECT DISTINCT catalog_name
					FROM information_schema.schemata
					WHERE schema_name = 'public'
				`,
			},
		}

		for _, tc := range checks {
			t.Run(tc.name, func(t *testing.T) {
				var catalog string
				if err := db.QueryRow(tc.query).Scan(&catalog); err != nil {
					t.Fatalf("%s query: %v", tc.name, err)
				}
				if catalog != "duckgres_catalog" {
					t.Fatalf("%s catalog = %q, want %q", tc.name, catalog, "duckgres_catalog")
				}
			})
		}
	})

	t.Run("information_schema_schemata_hides_internal_ducklake_catalogs", func(t *testing.T) {
		var leaked int
		if err := db.QueryRow(`
			SELECT COUNT(*)
			FROM information_schema.schemata
			WHERE catalog_name LIKE '__ducklake_metadata_%'
		`).Scan(&leaked); err != nil {
			t.Fatalf("count internal schemata rows: %v", err)
		}
		if leaked != 0 {
			t.Fatalf("information_schema.schemata leaked %d internal DuckLake metadata rows", leaked)
		}
	})

	t.Run("show_databases_reports_only_logical_name", func(t *testing.T) {
		rows, err := db.Query("SHOW DATABASES")
		if err != nil {
			t.Fatalf("SHOW DATABASES: %v", err)
		}
		defer func() {
			if err := rows.Close(); err != nil {
				t.Fatalf("close SHOW DATABASES rows: %v", err)
			}
		}()

		var got []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scan SHOW DATABASES row: %v", err)
			}
			got = append(got, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate SHOW DATABASES rows: %v", err)
		}

		if len(got) != 1 || got[0] != "duckgres_catalog" {
			t.Fatalf("SHOW DATABASES = %v, want [duckgres_catalog]", got)
		}
	})
}

func TestLogicalDatabaseCatalogQualifiedNames(t *testing.T) {
	if !testHarness.useDuckLake {
		t.Skip("logical catalog mapping is only relevant in DuckLake mode")
	}

	t.Run("unquoted_logical_catalog_executes_and_ducklake_still_works", func(t *testing.T) {
		db := openLogicalDatabaseConn(t, "duckgres_catalog")
		mustExec(t, db, "DROP SCHEMA IF EXISTS bill CASCADE")
		mustExec(t, db, "CREATE SCHEMA bill")
		mustExec(t, db, "CREATE TABLE duckgres_catalog.bill.logical_catalog_table (id INTEGER)")
		mustExec(t, db, "INSERT INTO duckgres_catalog.bill.logical_catalog_table VALUES (1), (2)")

		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM duckgres_catalog.bill.logical_catalog_table").Scan(&count); err != nil {
			t.Fatalf("logical catalog read: %v", err)
		}
		if count != 2 {
			t.Fatalf("logical catalog row count = %d, want 2", count)
		}

		if err := db.QueryRow("SELECT COUNT(*) FROM ducklake.bill.logical_catalog_table").Scan(&count); err != nil {
			t.Fatalf("ducklake compatibility read: %v", err)
		}
		if count != 2 {
			t.Fatalf("ducklake compatibility row count = %d, want 2", count)
		}
	})

	t.Run("quoted_logical_catalog_executes", func(t *testing.T) {
		db := openLogicalDatabaseConn(t, "DuckgresCatalog")
		mustExec(t, db, "DROP SCHEMA IF EXISTS bill CASCADE")
		mustExec(t, db, "CREATE SCHEMA bill")
		mustExec(t, db, `CREATE TABLE "DuckgresCatalog"."bill"."quoted_catalog_table" (id INTEGER)`)
		mustExec(t, db, `INSERT INTO "DuckgresCatalog"."bill"."quoted_catalog_table" VALUES (7)`)

		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM "DuckgresCatalog"."bill"."quoted_catalog_table"`).Scan(&count); err != nil {
			t.Fatalf("quoted logical catalog read: %v", err)
		}
		if count != 1 {
			t.Fatalf("quoted logical catalog row count = %d, want 1", count)
		}

		if err := db.QueryRow("SELECT COUNT(*) FROM ducklake.bill.quoted_catalog_table").Scan(&count); err != nil {
			t.Fatalf("ducklake quoted compatibility read: %v", err)
		}
		if count != 1 {
			t.Fatalf("ducklake quoted compatibility row count = %d, want 1", count)
		}
	})

	t.Run("dbt_style_temp_view_rename_uses_physical_catalog", func(t *testing.T) {
		db := openLogicalDatabaseConn(t, "duckgres_catalog")
		mustExec(t, db, "DROP SCHEMA IF EXISTS bill CASCADE")
		mustExec(t, db, "CREATE SCHEMA bill")
		mustExec(t, db, "CREATE TABLE duckgres_catalog.bill.customers (id INTEGER, name TEXT)")
		mustExec(t, db, "INSERT INTO duckgres_catalog.bill.customers VALUES (1, 'alice')")

		mustExec(t, db, `
			CREATE VIEW "duckgres_catalog"."bill"."stg_customers__dbt_tmp"
			AS (
				SELECT id, name
				FROM "duckgres_catalog"."bill"."customers"
			)
		`)
		mustExec(t, db, `ALTER TABLE "duckgres_catalog"."bill"."stg_customers__dbt_tmp" RENAME TO "stg_customers"`)

		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM ducklake.bill.stg_customers").Scan(&count); err != nil {
			t.Fatalf("query renamed dbt-style view through ducklake: %v", err)
		}
		if count != 1 {
			t.Fatalf("renamed dbt-style view row count = %d, want 1", count)
		}
	})

	t.Run("dbt_style_create_table_as_uses_physical_catalog", func(t *testing.T) {
		db := openLogicalDatabaseConn(t, "duckgres_catalog")
		mustExec(t, db, "DROP SCHEMA IF EXISTS bill CASCADE")
		mustExec(t, db, "CREATE SCHEMA bill")
		mustExec(t, db, "CREATE TABLE duckgres_catalog.bill.stg_customers (id INTEGER, name TEXT)")
		mustExec(t, db, "INSERT INTO duckgres_catalog.bill.stg_customers VALUES (1, 'alice')")

		mustExec(t, db, `
			CREATE TABLE "duckgres_catalog"."bill"."customer_lifetime_value__dbt_tmp"
			AS (
				SELECT *
				FROM "duckgres_catalog"."bill"."stg_customers"
			)
		`)

		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM ducklake.bill.customer_lifetime_value__dbt_tmp").Scan(&count); err != nil {
			t.Fatalf("query dbt-style ctas table through ducklake: %v", err)
		}
		if count != 1 {
			t.Fatalf("dbt-style ctas row count = %d, want 1", count)
		}
	})
}
