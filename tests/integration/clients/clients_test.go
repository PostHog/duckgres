package clients

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/posthog/duckgres/server"
	_ "github.com/lib/pq"
)

var (
	testDB   *sql.DB
	dgServer *server.Server
	tmpDir   string
)

func TestMain(m *testing.M) {
	var err error
	tmpDir, err = os.MkdirTemp("", "duckgres-clients-*")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}

	// Generate certs
	certFile := filepath.Join(tmpDir, "server.crt")
	keyFile := filepath.Join(tmpDir, "server.key")
	if err := server.EnsureCertificates(certFile, keyFile); err != nil {
		fmt.Printf("Failed to generate certificates: %v\n", err)
		os.Exit(1)
	}

	// Start Duckgres server
	cfg := server.Config{
		Host:        "127.0.0.1",
		Port:        35499, // Use a fixed port for client tests
		DataDir:     tmpDir,
		TLSCertFile: certFile,
		TLSKeyFile:  keyFile,
		Users:       map[string]string{"testuser": "testpass"},
	}

	dgServer, err = server.New(cfg)
	if err != nil {
		fmt.Printf("Failed to create server: %v\n", err)
		os.Exit(1)
	}

	go func() { _ = dgServer.ListenAndServe() }()
	time.Sleep(200 * time.Millisecond)

	// Connect
	connStr := "host=127.0.0.1 port=35499 user=testuser password=testpass dbname=test sslmode=require"
	testDB, err = sql.Open("postgres", connStr)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}

	// Force single connection - Duckgres uses per-connection in-memory databases
	testDB.SetMaxOpenConns(1)
	testDB.SetMaxIdleConns(1)

	// Setup test data
	setupTestData()

	code := m.Run()

	_ = testDB.Close()
	_ = dgServer.Close()
	_ = os.RemoveAll(tmpDir)

	os.Exit(code)
}

func setupTestData() {
	_, _ = testDB.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, active BOOLEAN)`)
	_, _ = testDB.Exec(`INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', true), (2, 'Bob', 'bob@example.com', false)`)
	_, _ = testDB.Exec(`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price NUMERIC(10,2))`)
	_, _ = testDB.Exec(`INSERT INTO products VALUES (1, 'Widget', 'Hardware', 9.99), (2, 'Gadget', 'Electronics', 29.99)`)
}

// TestMetabaseQueries tests queries commonly issued by Metabase
func TestMetabaseQueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name: "get_schemas",
			query: `
				SELECT nspname FROM pg_namespace
				WHERE nspname NOT IN ('information_schema', 'pg_catalog')
				AND nspname NOT LIKE 'pg_%'
			`,
		},
		{
			name: "get_tables",
			query: `
				SELECT c.relname, n.nspname
				FROM pg_class c
				JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE c.relkind = 'r'
				AND n.nspname NOT IN ('information_schema', 'pg_catalog')
			`,
		},
		{
			name: "get_columns",
			query: `
				SELECT attname, format_type(atttypid, atttypmod) as type, attnotnull
				FROM pg_attribute
				WHERE attrelid = 'users'::regclass
				AND attnum > 0
				AND NOT attisdropped
			`,
		},
		{
			name:  "connection_test",
			query: "SELECT 1",
		},
		{
			name:  "version_check",
			query: "SELECT version()",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := testDB.Query(tt.query)
			if err != nil {
				t.Errorf("Query failed: %v", err)
				return
			}
			_ = rows.Close()
		})
	}
}

// TestGrafanaQueries tests queries commonly issued by Grafana
func TestGrafanaQueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name: "time_column_detection",
			query: `
				SELECT column_name
				FROM information_schema.columns
				WHERE table_name = 'users'
				AND data_type IN ('timestamp without time zone', 'timestamp with time zone', 'date')
			`,
		},
		{
			name: "table_list",
			query: `
				SELECT table_name FROM information_schema.tables
				WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
				AND table_type = 'BASE TABLE'
			`,
		},
		{
			name: "column_list",
			query: `
				SELECT column_name, data_type
				FROM information_schema.columns
				WHERE table_name = 'users'
				ORDER BY ordinal_position
			`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := testDB.Query(tt.query)
			if err != nil {
				t.Errorf("Query failed: %v", err)
				return
			}
			_ = rows.Close()
		})
	}
}

// TestSupersetQueries tests queries commonly issued by Apache Superset
func TestSupersetQueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name: "get_all_tables",
			query: `
				SELECT table_name, table_schema
				FROM information_schema.tables
				WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
				ORDER BY table_name
			`,
		},
		{
			name: "get_columns_with_types",
			query: `
				SELECT column_name, data_type, is_nullable
				FROM information_schema.columns
				WHERE table_name = 'users'
				ORDER BY ordinal_position
			`,
		},
		{
			name:  "database_version",
			query: "SELECT version()",
		},
		{
			name:  "current_database",
			query: "SELECT current_database()",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := testDB.Query(tt.query)
			if err != nil {
				t.Errorf("Query failed: %v", err)
				return
			}
			_ = rows.Close()
		})
	}
}

// TestTableauQueries tests queries commonly issued by Tableau
func TestTableauQueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "connection_test",
			query: "SELECT 1",
		},
		{
			name: "schema_discovery",
			query: `
				SELECT DISTINCT table_schema
				FROM information_schema.tables
				WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
			`,
		},
		{
			name: "table_discovery",
			query: `
				SELECT table_name, table_type
				FROM information_schema.tables
				WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
			`,
		},
		{
			name: "column_discovery",
			query: `
				SELECT
					column_name,
					ordinal_position,
					column_default,
					is_nullable,
					data_type,
					character_maximum_length,
					numeric_precision,
					numeric_scale
				FROM information_schema.columns
				WHERE table_name = 'users'
			`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := testDB.Query(tt.query)
			if err != nil {
				t.Errorf("Query failed: %v", err)
				return
			}
			_ = rows.Close()
		})
	}
}

// TestDBeaverQueries tests queries commonly issued by DBeaver
func TestDBeaverQueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "catalog_info",
			query: "SELECT current_database(), current_schema(), session_user, current_user",
		},
		{
			name: "table_metadata",
			query: `
				SELECT
					c.oid,
					c.relname,
					c.relkind,
					n.nspname,
					pg_get_userbyid(c.relowner) as owner,
					obj_description(c.oid, 'pg_class') as description
				FROM pg_class c
				JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE c.relkind IN ('r', 'v')
				AND n.nspname NOT IN ('pg_catalog', 'information_schema')
				LIMIT 10
			`,
		},
		{
			name: "column_metadata",
			query: `
				SELECT
					a.attnum,
					a.attname,
					t.typname,
					a.atttypmod,
					a.attnotnull,
					a.atthasdef
				FROM pg_attribute a
				JOIN pg_type t ON t.oid = a.atttypid
				JOIN pg_class c ON c.oid = a.attrelid
				WHERE c.relname = 'users'
				AND a.attnum > 0
				AND NOT a.attisdropped
				ORDER BY a.attnum
			`,
		},
		{
			name:  "server_version",
			query: "SHOW server_version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := testDB.Query(tt.query)
			if err != nil {
				t.Errorf("Query failed: %v", err)
				return
			}
			_ = rows.Close()
		})
	}
}

// TestFivetranQueries tests queries commonly issued by Fivetran
func TestFivetranQueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name: "schema_sync",
			query: `
				SELECT
					table_schema,
					table_name,
					column_name,
					ordinal_position,
					data_type,
					is_nullable,
					column_default
				FROM information_schema.columns
				WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
				ORDER BY table_schema, table_name, ordinal_position
			`,
		},
		{
			name: "primary_key_detection",
			query: `
				SELECT
					tc.table_schema,
					tc.table_name,
					kcu.column_name
				FROM information_schema.table_constraints tc
				JOIN information_schema.key_column_usage kcu
					ON tc.constraint_name = kcu.constraint_name
					AND tc.table_schema = kcu.table_schema
				WHERE tc.constraint_type = 'PRIMARY KEY'
			`,
		},
		{
			name:  "commented_select",
			query: "/* fivetran_sync:abc123 */ SELECT * FROM users LIMIT 10",
		},
		{
			name:  "commented_insert",
			query: "/* fivetran_sync:abc123 */ INSERT INTO users (id, name, email, active) VALUES (100, 'Fivetran', 'fivetran@test.com', true)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := testDB.Exec(tt.query)
			if err != nil {
				t.Errorf("Query failed: %v", err)
			}
		})
	}
}

// TestAirbyteQueries tests queries commonly issued by Airbyte
func TestAirbyteQueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name: "discover_tables",
			query: `
				SELECT table_name, table_schema
				FROM information_schema.tables
				WHERE table_type = 'BASE TABLE'
				AND table_schema NOT IN ('pg_catalog', 'information_schema')
			`,
		},
		{
			name: "discover_columns",
			query: `
				SELECT
					column_name,
					data_type,
					is_nullable,
					column_default
				FROM information_schema.columns
				WHERE table_name = 'users'
			`,
		},
		{
			name:  "test_connection",
			query: "SELECT 1 as connection_test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := testDB.Query(tt.query)
			if err != nil {
				t.Errorf("Query failed: %v", err)
				return
			}
			_ = rows.Close()
		})
	}
}

// TestDbtQueries tests queries commonly issued by dbt
func TestDbtQueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name: "relation_existence",
			query: `
				SELECT count(*) FROM pg_class c
				JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE c.relname = 'users'
				AND n.nspname NOT IN ('pg_catalog', 'information_schema')
			`,
		},
		{
			name: "get_columns",
			query: `
				SELECT column_name, data_type
				FROM information_schema.columns
				WHERE table_name = 'users'
				ORDER BY ordinal_position
			`,
		},
		{
			name:  "schema_exists",
			query: "SELECT count(*) FROM pg_namespace WHERE nspname = 'main'",
		},
		{
			name:  "create_schema_if_not_exists",
			query: "CREATE SCHEMA IF NOT EXISTS dbt_test",
		},
		{
			name:  "drop_schema",
			query: "DROP SCHEMA IF EXISTS dbt_test CASCADE",
		},
		{
			name:  "transaction_begin",
			query: "BEGIN",
		},
		{
			name:  "transaction_commit",
			query: "COMMIT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := testDB.Exec(tt.query)
			if err != nil {
				t.Errorf("Query failed: %v", err)
			}
		})
	}
}

// TestPreparedStatements tests that prepared statements work correctly
func TestPreparedStatements(t *testing.T) {
	t.Run("simple_prepared", func(t *testing.T) {
		stmt, err := testDB.Prepare("SELECT * FROM users WHERE id = $1")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer func() { _ = stmt.Close() }()

		rows, err := stmt.Query(1)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		if !rows.Next() {
			t.Error("Expected 1 row")
		}
	})

	t.Run("multi_param_prepared", func(t *testing.T) {
		stmt, err := testDB.Prepare("SELECT * FROM users WHERE name = $1 AND active = $2")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer func() { _ = stmt.Close() }()

		rows, err := stmt.Query("Alice", true)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		if !rows.Next() {
			t.Error("Expected 1 row")
		}
	})

	t.Run("prepared_insert", func(t *testing.T) {
		stmt, err := testDB.Prepare("INSERT INTO users (id, name, email, active) VALUES ($1, $2, $3, $4)")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer func() { _ = stmt.Close() }()

		_, err = stmt.Exec(999, "PreparedUser", "prepared@test.com", true)
		if err != nil {
			t.Fatalf("Exec failed: %v", err)
		}

		// Cleanup
		_, _ = testDB.Exec("DELETE FROM users WHERE id = 999")
	})
}

// TestExtendedQueryErrorHandling verifies that errors during the extended query
// protocol (prepared statements) are properly reported to the client and don't
// corrupt the connection state.
func TestExtendedQueryErrorHandling(t *testing.T) {
	t.Run("error_during_prepared_execution", func(t *testing.T) {
		// Use a single connection so we can verify the SAME connection survives errors.
		conn, err := testDB.Conn(context.Background())
		if err != nil {
			t.Fatalf("Conn failed: %v", err)
		}
		defer func() { _ = conn.Close() }()

		// CAST($1 AS INTEGER) with a non-numeric string uses the extended query
		// protocol (Parse/Bind/Execute) and produces a conversion error at execution time.
		var val int
		err = conn.QueryRowContext(context.Background(), "SELECT CAST($1 AS INTEGER)", "not-a-number").Scan(&val)
		if err == nil {
			t.Fatal("Expected conversion error, got none")
		}

		// The SAME connection should still be usable after the error.
		// If handleExecute didn't properly handle the error (e.g., protocol desync),
		// this would fail or hang.
		var result int
		err = conn.QueryRowContext(context.Background(), "SELECT $1::int", 42).Scan(&result)
		if err != nil {
			t.Fatalf("Connection unusable after error: %v", err)
		}
		if result != 42 {
			t.Errorf("Expected 42, got %d", result)
		}
	})

	t.Run("error_recovery_multi_cycle", func(t *testing.T) {
		// Run multiple error/success cycles on the same connection.
		conn, err := testDB.Conn(context.Background())
		if err != nil {
			t.Fatalf("Conn failed: %v", err)
		}
		defer func() { _ = conn.Close() }()

		for i := 0; i < 3; i++ {
			// Failing query via extended protocol
			var badVal int
			err := conn.QueryRowContext(context.Background(), "SELECT CAST($1 AS INTEGER)", "bad").Scan(&badVal)
			if err == nil {
				t.Fatalf("Expected error on iteration %d", i)
			}

			// Successful query via extended protocol on the same connection
			var val int
			err = conn.QueryRowContext(context.Background(), "SELECT $1::int + 1", i).Scan(&val)
			if err != nil {
				t.Fatalf("Success query failed on iteration %d: %v", i, err)
			}
			if val != i+1 {
				t.Errorf("Expected %d, got %d on iteration %d", i+1, val, i)
			}
		}
	})
}

// TestTransactions tests transaction handling
func TestTransactions(t *testing.T) {
	t.Run("commit", func(t *testing.T) {
		tx, err := testDB.Begin()
		if err != nil {
			t.Fatalf("Begin failed: %v", err)
		}

		_, err = tx.Exec("INSERT INTO users (id, name, email, active) VALUES (1000, 'TxUser', 'tx@test.com', true)")
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("Insert failed: %v", err)
		}

		err = tx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify
		var count int
		if err := testDB.QueryRow("SELECT COUNT(*) FROM users WHERE id = 1000").Scan(&count); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row after commit, got %d", count)
		}

		// Cleanup
		_, _ = testDB.Exec("DELETE FROM users WHERE id = 1000")
	})

	t.Run("rollback", func(t *testing.T) {
		tx, err := testDB.Begin()
		if err != nil {
			t.Fatalf("Begin failed: %v", err)
		}

		_, err = tx.Exec("INSERT INTO users (id, name, email, active) VALUES (1001, 'RollbackUser', 'rb@test.com', true)")
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("Insert failed: %v", err)
		}

		err = tx.Rollback()
		if err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}

		// Verify rollback worked
		var count int
		if err := testDB.QueryRow("SELECT COUNT(*) FROM users WHERE id = 1001").Scan(&count); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected 0 rows after rollback, got %d", count)
		}
	})
}
