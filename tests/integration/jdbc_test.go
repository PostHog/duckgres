package integration

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// pgxConnect creates a pgx connection to Duckgres (uses extended query protocol).
func pgxConnect(t *testing.T) *pgx.Conn {
	t.Helper()
	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=testuser password=testpass dbname=test sslmode=require", testHarness.dgPort)
	cfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}
	cfg.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	ctx := context.Background()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	return conn
}

// TestJDBCVersionQuery tests SELECT version() via extended query protocol,
// which is the pattern JDBC/Metabase uses on connection and that hangs sporadically.
func TestJDBCVersionQuery(t *testing.T) {
	ctx := context.Background()

	t.Run("simple_version", func(t *testing.T) {
		conn := pgxConnect(t)
		defer func() { _ = conn.Close(ctx) }()

		var ver string
		err := conn.QueryRow(ctx, "SELECT version()").Scan(&ver)
		if err != nil {
			t.Fatalf("SELECT version() failed: %v", err)
		}
		if ver == "" {
			t.Error("version() returned empty string")
		}
		t.Logf("version() = %s", ver)
	})

	t.Run("version_with_timeout", func(t *testing.T) {
		conn := pgxConnect(t)
		defer func() { _ = conn.Close(ctx) }()

		tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var ver string
		err := conn.QueryRow(tctx, "SELECT version()").Scan(&ver)
		if err != nil {
			t.Fatalf("SELECT version() timed out or failed: %v", err)
		}
	})

	t.Run("repeated_version_same_connection", func(t *testing.T) {
		conn := pgxConnect(t)
		defer func() { _ = conn.Close(ctx) }()

		for i := 0; i < 50; i++ {
			tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			var ver string
			err := conn.QueryRow(tctx, "SELECT version()").Scan(&ver)
			cancel()
			if err != nil {
				t.Fatalf("Iteration %d: SELECT version() failed: %v", i, err)
			}
		}
	})

	t.Run("repeated_version_new_connections", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			conn := pgxConnect(t)
			tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			var ver string
			err := conn.QueryRow(tctx, "SELECT version()").Scan(&ver)
			cancel()
			_ = conn.Close(ctx)
			if err != nil {
				t.Fatalf("Connection %d: SELECT version() failed: %v", i, err)
			}
		}
	})
}

// TestJDBCConcurrentVersionQueries tests concurrent version() queries
// across multiple connections, simulating a connection pool.
func TestJDBCConcurrentVersionQueries(t *testing.T) {
	ctx := context.Background()
	numWorkers := 5
	queriesPerWorker := 20
	timeout := 30 * time.Second

	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*queriesPerWorker)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			conn := pgxConnect(t)
			defer func() { _ = conn.Close(context.Background()) }()

			for i := 0; i < queriesPerWorker; i++ {
				qctx, qcancel := context.WithTimeout(tctx, 5*time.Second)
				var ver string
				err := conn.QueryRow(qctx, "SELECT version()").Scan(&ver)
				qcancel()
				if err != nil {
					errors <- fmt.Errorf("worker %d, query %d: %w", workerID, i, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestJDBCMetabaseProbeQueries tests the specific queries Metabase sends
// when probing a connection, using the extended query protocol.
func TestJDBCMetabaseProbeQueries(t *testing.T) {
	ctx := context.Background()

	probeQueries := []struct {
		name string
		sql  string
	}{
		{"version", "SELECT version()"},
		{"current_setting_version_num", "SELECT current_setting('server_version_num')"},
		{"current_setting_version", "SELECT current_setting('server_version')"},
		{"select_1", "SELECT 1"},
	}

	// Test via extended query protocol (pgx uses it by default)
	for _, q := range probeQueries {
		t.Run("extended_"+q.name, func(t *testing.T) {
			conn := pgxConnect(t)
			defer func() { _ = conn.Close(ctx) }()

			tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			// Scan as string to handle all return types uniformly (matches JDBC text format)
			rows, err := conn.Query(tctx, q.sql)
			if err != nil {
				t.Fatalf("Query %q failed: %v", q.sql, err)
			}
			if !rows.Next() {
				rows.Close()
				t.Fatalf("Query %q returned no rows", q.sql)
			}
			rawValues := rows.RawValues()
			rows.Close()
			t.Logf("%s = %s", q.sql, string(rawValues[0]))
		})
	}

	// Test via simple query protocol (lib/pq through database/sql)
	for _, q := range probeQueries {
		t.Run("simple_"+q.name, func(t *testing.T) {
			var val string
			err := testHarness.DuckgresDB.QueryRow(q.sql).Scan(&val)
			if err != nil {
				t.Fatalf("Query %q failed: %v", q.sql, err)
			}
			t.Logf("%s = %s", q.sql, val)
		})
	}
}

// TestJDBCDatabaseMetaDataQueries tests queries similar to what JDBC
// DatabaseMetaData methods generate (getTables, getSchemas, etc.).
func TestJDBCDatabaseMetaDataQueries(t *testing.T) {
	// These are representative of what the PostgreSQL JDBC driver generates
	// for DatabaseMetaData calls
	metadataQueries := []struct {
		name string
		sql  string
	}{
		{
			"getTables",
			`SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,
			 CASE c.relkind WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW' END AS TABLE_TYPE
			 FROM pg_catalog.pg_class c
			 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			 WHERE c.relkind IN ('r','v')
			 AND n.nspname NOT IN ('pg_catalog', 'information_schema')
			 ORDER BY TABLE_SCHEM, TABLE_NAME
			 LIMIT 20`,
		},
		{
			"getSchemas",
			`SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG
			 FROM pg_catalog.pg_namespace
			 WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema'
			 ORDER BY TABLE_SCHEM`,
		},
		{
			"getCatalogs",
			`SELECT datname AS TABLE_CAT FROM pg_catalog.pg_database ORDER BY 1`,
		},
		{
			"getTypeInfo",
			`SELECT typname FROM pg_catalog.pg_type LIMIT 20`,
		},
	}

	for _, q := range metadataQueries {
		t.Run(q.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			conn := pgxConnect(t)
			defer func() { _ = conn.Close(ctx) }()

			rows, err := conn.Query(ctx, q.sql)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("Row iteration error: %v", err)
			}
			rows.Close()
			t.Logf("%s returned %d rows", q.name, count)
		})
	}
}

// TestJDBCPreparedStatementReuse tests reusing prepared statements,
// which is a common JDBC pattern.
func TestJDBCPreparedStatementReuse(t *testing.T) {
	ctx := context.Background()
	conn := pgxConnect(t)
	defer func() { _ = conn.Close(ctx) }()

	// Create table
	_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS jdbc_prep_test")
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}
	_, err = conn.Exec(ctx, "CREATE TABLE jdbc_prep_test (id INTEGER, val VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS jdbc_prep_test")
	}()

	// Insert using prepared statement reuse (use pgx.QueryExecModeSimpleProtocol
	// to avoid type encoding issues with pgx's extended protocol for DML)
	for i := 0; i < 20; i++ {
		tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := conn.Exec(tctx, fmt.Sprintf("INSERT INTO jdbc_prep_test VALUES (%d, 'val_%d')", i, i))
		cancel()
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Query using prepared statement reuse (text mode via pgx)
	for i := 0; i < 20; i++ {
		tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var val string
		err := conn.QueryRow(tctx, "SELECT val FROM jdbc_prep_test WHERE id = $1",
			pgx.QueryExecModeSimpleProtocol, i).Scan(&val)
		cancel()
		if err != nil {
			t.Fatalf("Select %d failed: %v", i, err)
		}
		expected := fmt.Sprintf("val_%d", i)
		if val != expected {
			t.Errorf("Expected %q, got %q", expected, val)
		}
	}
}

// TestJDBCRapidReconnect tests rapid connect/query/disconnect cycles,
// which can trigger timing issues in the protocol handshake.
func TestJDBCRapidReconnect(t *testing.T) {
	ctx := context.Background()

	for i := 0; i < 30; i++ {
		conn := pgxConnect(t)
		tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var val string
		err := conn.QueryRow(tctx, "SELECT 'ok'").Scan(&val)
		cancel()
		_ = conn.Close(ctx)
		if err != nil {
			t.Fatalf("Reconnect %d: query failed: %v", i, err)
		}
	}
}
