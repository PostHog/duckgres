package integration

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq"
)

// openCatalogConn opens a connection with an arbitrary startup `database` name.
// duckgres no longer masks that name onto a physical catalog — in standalone
// DuckLake mode the session lands in the real `ducklake` catalog regardless of
// the name, and current_database() reports the truth.
func openCatalogConn(t *testing.T, dbName string) *sql.DB {
	t.Helper()
	connStr := fmt.Sprintf(
		"host=127.0.0.1 port=%d user=testuser password=testpass dbname=%s sslmode=require",
		testHarness.dgPort, dbName,
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
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestCatalogIsNotMasked verifies the de-masking contract: the connection
// database name no longer drives current_database()/pg_catalog surfaces; they
// reflect the real attached catalog (`ducklake`).
func TestCatalogIsNotMasked(t *testing.T) {
	if !testHarness.useDuckLake {
		t.Skip("catalog de-masking is only observable in DuckLake mode")
	}

	// Connect with a name that is NOT a real catalog — it must still de-mask to
	// the real ducklake catalog rather than being honored as a logical name.
	db := openCatalogConn(t, "anything_goes")

	t.Run("current_database reports the real catalog", func(t *testing.T) {
		var current string
		if err := db.QueryRow("SELECT current_database()").Scan(&current); err != nil {
			t.Fatalf("SELECT current_database(): %v", err)
		}
		if current != "ducklake" {
			t.Fatalf("current_database() = %q, want %q", current, "ducklake")
		}
	})

	t.Run("pg_database reports the real catalog", func(t *testing.T) {
		var datname string
		if err := db.QueryRow(
			"SELECT datname FROM pg_catalog.pg_database WHERE datname = current_database()",
		).Scan(&datname); err != nil {
			t.Fatalf("pg_database lookup: %v", err)
		}
		if datname != "ducklake" {
			t.Fatalf("datname = %q, want %q", datname, "ducklake")
		}
	})

	t.Run("information_schema reports the real catalog", func(t *testing.T) {
		// Use a dedicated, uniquely-named schema and drop it after the test so it
		// doesn't pollute the shared DuckLake catalog that later catalog tests
		// compare against (e.g. \dn schema-count parity).
		mustExec(t, db, "DROP SCHEMA IF EXISTS demask_ns CASCADE")
		mustExec(t, db, "CREATE SCHEMA demask_ns")
		t.Cleanup(func() { _, _ = db.Exec("DROP SCHEMA IF EXISTS demask_ns CASCADE") })
		mustExec(t, db, "CREATE OR REPLACE VIEW demask_ns.demask_view AS SELECT 1 AS id")
		mustExec(t, db, "CREATE TABLE demask_ns.demask_tbl (id INTEGER)")

		checks := []struct {
			name, query string
		}{
			{"tables", `SELECT DISTINCT table_catalog FROM information_schema.tables WHERE table_schema='demask_ns' AND table_name='demask_tbl'`},
			{"columns", `SELECT DISTINCT table_catalog FROM information_schema.columns WHERE table_schema='demask_ns' AND table_name='demask_tbl'`},
			{"views", `SELECT DISTINCT table_catalog FROM information_schema.views WHERE table_schema='demask_ns' AND table_name='demask_view'`},
			{"schemata", `SELECT DISTINCT catalog_name FROM information_schema.schemata WHERE schema_name='public'`},
		}
		for _, tc := range checks {
			t.Run(tc.name, func(t *testing.T) {
				var catalog string
				if err := db.QueryRow(tc.query).Scan(&catalog); err != nil {
					t.Fatalf("%s query: %v", tc.name, err)
				}
				if catalog != "ducklake" {
					t.Fatalf("%s catalog = %q, want %q", tc.name, catalog, "ducklake")
				}
			})
		}
	})

	t.Run("internal ducklake metadata catalogs stay hidden", func(t *testing.T) {
		var leaked int
		if err := db.QueryRow(`
			SELECT COUNT(*) FROM information_schema.schemata
			WHERE catalog_name LIKE '__ducklake_metadata_%'
		`).Scan(&leaked); err != nil {
			t.Fatalf("count internal schemata rows: %v", err)
		}
		if leaked != 0 {
			t.Fatalf("information_schema.schemata leaked %d internal DuckLake metadata rows", leaked)
		}
	})
}

// TestCatalogQualifiedNames verifies real-catalog three-part references work
// (with `public` mapping to DuckLake's `main` schema), and that a connection
// made with dbname=ducklake behaves identically.
func TestCatalogQualifiedNames(t *testing.T) {
	if !testHarness.useDuckLake {
		t.Skip("catalog mapping is only relevant in DuckLake mode")
	}

	db := openCatalogConn(t, "ducklake")
	// Dedicated schema + table names, dropped after the test so they don't
	// pollute the shared DuckLake catalog later catalog tests compare against.
	mustExec(t, db, "DROP SCHEMA IF EXISTS demask_ns CASCADE")
	mustExec(t, db, "CREATE SCHEMA demask_ns")
	t.Cleanup(func() {
		_, _ = db.Exec("DROP SCHEMA IF EXISTS demask_ns CASCADE")
		_, _ = db.Exec("DROP TABLE IF EXISTS ducklake.main.demask_qualified_pub")
	})

	// public -> main mapping for the real ducklake catalog.
	mustExec(t, db, "CREATE TABLE ducklake.public.demask_qualified_pub (id INTEGER)")
	mustExec(t, db, "INSERT INTO ducklake.public.demask_qualified_pub VALUES (1), (2)")
	mustExec(t, db, "CREATE TABLE ducklake.demask_ns.qualified_bill (id INTEGER)")
	mustExec(t, db, "INSERT INTO ducklake.demask_ns.qualified_bill VALUES (7)")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM ducklake.public.demask_qualified_pub").Scan(&count); err != nil {
		t.Fatalf("read ducklake.public table: %v", err)
	}
	if count != 2 {
		t.Fatalf("ducklake.public row count = %d, want 2", count)
	}
	// public maps to main, so the same table is reachable via ducklake.main.
	if err := db.QueryRow("SELECT COUNT(*) FROM ducklake.main.demask_qualified_pub").Scan(&count); err != nil {
		t.Fatalf("read ducklake.main table: %v", err)
	}
	if count != 2 {
		t.Fatalf("ducklake.main row count = %d, want 2", count)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM ducklake.demask_ns.qualified_bill").Scan(&count); err != nil {
		t.Fatalf("read ducklake.demask_ns table: %v", err)
	}
	if count != 1 {
		t.Fatalf("ducklake.demask_ns row count = %d, want 1", count)
	}
}
