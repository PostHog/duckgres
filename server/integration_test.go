package server

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// TestIntegrationETLWorkflow tests a complete ETL workflow against a running duckgres server.
// This test starts a real server and connects via the PostgreSQL wire protocol.
func TestIntegrationETLWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create temp directory for test data
	tmpDir, err := os.MkdirTemp("", "duckgres-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Find an available port
	port := findAvailablePort(t)

	// Generate certs
	certFile := filepath.Join(tmpDir, "server.crt")
	keyFile := filepath.Join(tmpDir, "server.key")
	if err := EnsureCertificates(certFile, keyFile); err != nil {
		t.Fatalf("Failed to generate certificates: %v", err)
	}

	// Create server config
	cfg := Config{
		Host:        "127.0.0.1",
		Port:        port,
		DataDir:     tmpDir,
		TLSCertFile: certFile,
		TLSKeyFile:  keyFile,
		Users: map[string]string{
			"testuser": "testpass",
		},
	}

	// Start server
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Run server in background
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Connect to server (sslmode=require skips cert verification for self-signed certs in lib/pq)
	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=testuser password=testpass dbname=test sslmode=require", port)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		srv.Close()
		t.Fatalf("Failed to open connection: %v", err)
	}

	// Disable connection pooling to avoid connection reuse issues
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	db.SetConnMaxLifetime(0)

	// Close and reopen to get fresh connection
	db.Close()
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		srv.Close()
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	// Configure connection pool
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Test connection with retry
	var pingErr error
	for i := 0; i < 10; i++ {
		pingErr = db.Ping()
		if pingErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pingErr != nil {
		srv.Close()
		t.Fatalf("Failed to ping server after retries: %v", pingErr)
	}

	t.Log("Connected to duckgres server")

	// Run ETL workflow tests
	t.Run("SchemaOperations", func(t *testing.T) {
		testSchemaOperations(t, db)
	})

	t.Run("TableOperations", func(t *testing.T) {
		testTableOperations(t, db)
	})

	t.Run("DataOperations", func(t *testing.T) {
		testDataOperations(t, db)
	})

	t.Run("MetadataQueries", func(t *testing.T) {
		testMetadataQueries(t, db)
	})

	t.Run("TransactionHandling", func(t *testing.T) {
		testTransactionHandling(t, db)
	})

	t.Run("CommentedQueries", func(t *testing.T) {
		testCommentedQueries(t, db)
	})

	// Cleanup
	db.Close()
	srv.Close()

	// Check for server errors
	select {
	case err := <-serverErr:
		if err != nil && err.Error() != "accept tcp 127.0.0.1:"+fmt.Sprint(port)+": use of closed network connection" {
			t.Logf("Server error (expected on close): %v", err)
		}
	default:
	}
}

func testSchemaOperations(t *testing.T, db *sql.DB) {
	// Create schema
	_, err := db.Exec("CREATE SCHEMA IF NOT EXISTS etl_test")
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	t.Log("Created schema etl_test")

	// Verify schema exists
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'etl_test'").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query schema: %v", err)
	}
	if count != 1 {
		t.Errorf("Schema not found, expected 1 got %d", count)
	}

	// Drop and recreate (idempotent)
	_, err = db.Exec("DROP SCHEMA IF EXISTS etl_test CASCADE")
	if err != nil {
		t.Fatalf("Failed to drop schema: %v", err)
	}

	_, err = db.Exec("CREATE SCHEMA etl_test")
	if err != nil {
		t.Fatalf("Failed to recreate schema: %v", err)
	}
	t.Log("Schema operations completed successfully")
}

func testTableOperations(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS etl_test.users (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(255),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	t.Log("Created table etl_test.users")

	// Verify table exists
	var count int
	err = db.QueryRow(`
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = 'etl_test' AND table_name = 'users'
	`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query table: %v", err)
	}
	if count != 1 {
		t.Errorf("Table not found, expected 1 got %d", count)
	}

	// Check columns
	rows, err := db.Query(`
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = 'etl_test' AND table_name = 'users'
		ORDER BY ordinal_position
	`)
	if err != nil {
		t.Fatalf("Failed to query columns: %v", err)
	}
	defer rows.Close()

	columnCount := 0
	for rows.Next() {
		var colName, dataType string
		if err := rows.Scan(&colName, &dataType); err != nil {
			t.Fatalf("Failed to scan column: %v", err)
		}
		t.Logf("Column: %s (%s)", colName, dataType)
		columnCount++
	}
	if columnCount != 4 {
		t.Errorf("Expected 4 columns, got %d", columnCount)
	}

	t.Log("Table operations completed successfully")
}

func testDataOperations(t *testing.T, db *sql.DB) {
	// Clear any existing data
	_, err := db.Exec("DELETE FROM etl_test.users")
	if err != nil {
		t.Fatalf("Failed to clear table: %v", err)
	}

	// Insert single row
	result, err := db.Exec("INSERT INTO etl_test.users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", affected)
	}

	// Batch insert
	result, err = db.Exec(`
		INSERT INTO etl_test.users (id, name, email) VALUES
		(2, 'Bob', 'bob@example.com'),
		(3, 'Charlie', 'charlie@example.com'),
		(4, 'Diana', 'diana@example.com')
	`)
	if err != nil {
		t.Fatalf("Failed to batch insert: %v", err)
	}
	affected, _ = result.RowsAffected()
	if affected != 3 {
		t.Errorf("Expected 3 rows affected, got %d", affected)
	}

	// Query data
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM etl_test.users").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 4 {
		t.Errorf("Expected 4 rows, got %d", count)
	}

	// Update data
	result, err = db.Exec("UPDATE etl_test.users SET name = 'Alice Updated' WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	affected, _ = result.RowsAffected()
	if affected != 1 {
		t.Errorf("Expected 1 row updated, got %d", affected)
	}

	// Verify update
	var name string
	err = db.QueryRow("SELECT name FROM etl_test.users WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("Failed to query updated row: %v", err)
	}
	if name != "Alice Updated" {
		t.Errorf("Expected 'Alice Updated', got '%s'", name)
	}

	// Delete data
	result, err = db.Exec("DELETE FROM etl_test.users WHERE id = 4")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	affected, _ = result.RowsAffected()
	if affected != 1 {
		t.Errorf("Expected 1 row deleted, got %d", affected)
	}

	// Verify delete
	err = db.QueryRow("SELECT COUNT(*) FROM etl_test.users").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count after delete: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 rows after delete, got %d", count)
	}

	t.Log("Data operations completed successfully")
}

func testMetadataQueries(t *testing.T, db *sql.DB) {
	// Query information_schema.tables
	rows, err := db.Query("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'etl_test'")
	if err != nil {
		t.Fatalf("Failed to query information_schema.tables: %v", err)
	}
	rows.Close()
	t.Log("Queried information_schema.tables")

	// Query information_schema.columns
	rows, err = db.Query("SELECT column_name FROM information_schema.columns WHERE table_name = 'users' LIMIT 10")
	if err != nil {
		t.Fatalf("Failed to query information_schema.columns: %v", err)
	}
	rows.Close()
	t.Log("Queried information_schema.columns")

	// Query pg_catalog.pg_namespace (schemas)
	rows, err = db.Query("SELECT nspname FROM pg_catalog.pg_namespace LIMIT 10")
	if err != nil {
		t.Fatalf("Failed to query pg_namespace: %v", err)
	}
	rows.Close()
	t.Log("Queried pg_catalog.pg_namespace")

	// Query current_database()
	var dbName string
	err = db.QueryRow("SELECT current_database()").Scan(&dbName)
	if err != nil {
		t.Fatalf("Failed to query current_database(): %v", err)
	}
	t.Logf("Current database: %s", dbName)

	t.Log("Metadata queries completed successfully")
}

func testTransactionHandling(t *testing.T, db *sql.DB) {
	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert in transaction
	_, err = tx.Exec("INSERT INTO etl_test.users (id, name, email) VALUES (100, 'TxUser', 'tx@example.com')")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	// Verify within transaction
	var count int
	err = tx.QueryRow("SELECT COUNT(*) FROM etl_test.users WHERE id = 100").Scan(&count)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to query in transaction: %v", err)
	}
	if count != 1 {
		tx.Rollback()
		t.Errorf("Expected 1 row in transaction, got %d", count)
	}

	// Rollback
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify rollback (row should not exist)
	err = db.QueryRow("SELECT COUNT(*) FROM etl_test.users WHERE id = 100").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query after rollback: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 rows after rollback, got %d", count)
	}

	// Test commit
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction for commit test: %v", err)
	}

	_, err = tx.Exec("INSERT INTO etl_test.users (id, name, email) VALUES (101, 'CommitUser', 'commit@example.com')")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert for commit test: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify commit
	err = db.QueryRow("SELECT COUNT(*) FROM etl_test.users WHERE id = 101").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query after commit: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 row after commit, got %d", count)
	}

	t.Log("Transaction handling completed successfully")
}

func testCommentedQueries(t *testing.T, db *sql.DB) {
	// This tests the fix for "Received resultset tuples, but no field structure for them"
	// ETL tools often prefix queries with tracking comments

	// SELECT with comment
	rows, err := db.Query("/* sync_id:test123 */ SELECT * FROM etl_test.users LIMIT 5")
	if err != nil {
		t.Fatalf("Failed to execute commented SELECT: %v", err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	t.Logf("Commented SELECT returned %d rows", count)

	// INSERT with comment
	_, err = db.Exec("/* sync_id:test123 */ INSERT INTO etl_test.users (id, name, email) VALUES (200, 'CommentTest', 'comment@test.com')")
	if err != nil {
		t.Fatalf("Failed to execute commented INSERT: %v", err)
	}
	t.Log("Commented INSERT succeeded")

	// UPDATE with comment
	_, err = db.Exec("/* sync_id:test123 */ UPDATE etl_test.users SET name = 'CommentUpdated' WHERE id = 200")
	if err != nil {
		t.Fatalf("Failed to execute commented UPDATE: %v", err)
	}
	t.Log("Commented UPDATE succeeded")

	// DELETE with comment
	_, err = db.Exec("/* sync_id:test123 */ DELETE FROM etl_test.users WHERE id = 200")
	if err != nil {
		t.Fatalf("Failed to execute commented DELETE: %v", err)
	}
	t.Log("Commented DELETE succeeded")

	// CTE with comment
	rows, err = db.Query(`
		/* sync_id:test123 */
		WITH active_users AS (
			SELECT * FROM etl_test.users WHERE id < 100
		)
		SELECT COUNT(*) FROM active_users
	`)
	if err != nil {
		t.Fatalf("Failed to execute commented CTE: %v", err)
	}
	rows.Close()
	t.Log("Commented CTE succeeded")

	t.Log("Commented queries completed successfully")
}

// findAvailablePort finds an available TCP port
func findAvailablePort(t *testing.T) int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find available port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return port
}

// init configures the postgres driver to skip certificate verification for tests
func init() {
	// This allows lib/pq to connect with sslmode=require to self-signed certs
	// In production, you'd use proper certificate verification
}
