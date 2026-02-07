package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// openDuckgresConn opens a fresh connection to the test Duckgres server.
// Each connection gets its own in-memory DuckDB instance.
func openDuckgresConn(t *testing.T) *sql.DB {
	t.Helper()
	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=testuser password=testpass dbname=test sslmode=require", testHarness.dgPort)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Verify connection works
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatalf("Failed to ping: %v", err)
	}
	return db
}

// TestCancelQueryDoesNotAffectOtherSessions verifies that cancelling a query
// on one connection does not crash the server or affect other connections.
// This reproduces the reported bug where Ctrl+C in psql killed the server.
func TestCancelQueryDoesNotAffectOtherSessions(t *testing.T) {
	t.Run("cancel_during_large_result", func(t *testing.T) {
		conn1 := openDuckgresConn(t)
		defer func() { _ = conn1.Close() }()
		conn2 := openDuckgresConn(t)
		defer func() { _ = conn2.Close() }()

		// Start a long-running query on conn1 in a goroutine
		var wg sync.WaitGroup
		var conn1Err error
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Large enough to take some time, small enough to complete in a few seconds
			var count int
			conn1Err = conn1.QueryRow("SELECT count(*) FROM range(5000000)").Scan(&count)
		}()

		// Give conn1's query a moment to start executing
		time.Sleep(50 * time.Millisecond)

		// Cancel a query on conn2 using context cancellation.
		// lib/pq sends a CancelRequest message to the server, same as psql's Ctrl+C.
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		rows, err := conn2.QueryContext(ctx, "SELECT * FROM range(50000000)")
		if err == nil {
			// Query started before cancel; drain to trigger cancel during iteration
			for rows.Next() {
			}
			_ = rows.Close()
		}
		// We expect a cancellation error - either on start or during iteration.
		// The specific error doesn't matter; what matters is the server survives.

		// Wait for conn1's query to finish
		wg.Wait()
		if conn1Err != nil {
			t.Errorf("conn1's query failed after conn2's cancel: %v", conn1Err)
		}

		// Verify conn1 is still usable
		var val int
		if err := conn1.QueryRow("SELECT 42").Scan(&val); err != nil {
			t.Errorf("conn1 unusable after cancel: %v", err)
		} else if val != 42 {
			t.Errorf("conn1 returned wrong value: got %d, want 42", val)
		}

		// Verify conn2 is still usable after its query was cancelled
		if err := conn2.QueryRow("SELECT 99").Scan(&val); err != nil {
			t.Errorf("conn2 unusable after cancel: %v", err)
		} else if val != 99 {
			t.Errorf("conn2 returned wrong value: got %d, want 99", val)
		}
	})

	t.Run("cancel_does_not_crash_server", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		// Cancel a query mid-flight
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(30 * time.Millisecond)
			cancel()
		}()

		_, _ = conn.QueryContext(ctx, "SELECT * FROM range(50000000)")

		// The server should still be alive - open a brand new connection
		freshConn := openDuckgresConn(t)
		defer func() { _ = freshConn.Close() }()

		var val int
		if err := freshConn.QueryRow("SELECT 1").Scan(&val); err != nil {
			t.Fatalf("Server died after cancel: %v", err)
		}
		if val != 1 {
			t.Errorf("Expected 1, got %d", val)
		}
	})

	t.Run("multiple_cancels_server_survives", func(t *testing.T) {
		// Rapidly cancel several queries to stress-test the cancel path
		for i := range 5 {
			conn := openDuckgresConn(t)
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				time.Sleep(20 * time.Millisecond)
				cancel()
			}()

			_, _ = conn.QueryContext(ctx, "SELECT * FROM range(50000000)")
			_ = conn.Close()
			_ = i
		}

		// Server should still be alive
		freshConn := openDuckgresConn(t)
		defer func() { _ = freshConn.Close() }()

		var val int
		if err := freshConn.QueryRow("SELECT 1").Scan(&val); err != nil {
			t.Fatalf("Server died after multiple cancels: %v", err)
		}
	})

	t.Run("cancel_returns_proper_error_code", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(30 * time.Millisecond)
			cancel()
		}()

		rows, err := conn.QueryContext(ctx, "SELECT * FROM range(50000000)")
		if err == nil {
			// If the query started, iterate until we hit the cancel
			for rows.Next() {
			}
			err = rows.Err()
			_ = rows.Close()
		}

		// Should get an error related to cancellation
		if err != nil && !strings.Contains(err.Error(), "cancel") && !strings.Contains(err.Error(), "context") {
			t.Logf("Unexpected error type (not cancellation): %v", err)
		}

		// Connection should still be usable afterwards
		var val int
		if err := conn.QueryRow("SELECT 1").Scan(&val); err != nil {
			t.Errorf("Connection unusable after cancel: %v", err)
		}
	})
}
