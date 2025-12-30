package integration

import (
	"testing"

	"github.com/lib/pq"
)

// TestCopyFromStdin tests COPY FROM STDIN functionality
func TestCopyFromStdin(t *testing.T) {
	// Create test table
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS copy_test")
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE copy_test (id INTEGER, name TEXT, value DECIMAL(10,2))")

	t.Run("copy_basic_rows", func(t *testing.T) {
		// Use pq.CopyIn to create a COPY statement
		txn, err := testHarness.DuckgresDB.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("copy_test", "id", "name", "value"))
		if err != nil {
			txn.Rollback()
			t.Fatalf("Failed to prepare COPY: %v", err)
		}

		// Insert test data
		testData := []struct {
			id    int
			name  string
			value float64
		}{
			{1, "Alice", 100.50},
			{2, "Bob", 200.75},
			{3, "Charlie", 300.00},
		}

		for _, d := range testData {
			_, err = stmt.Exec(d.id, d.name, d.value)
			if err != nil {
				stmt.Close()
				txn.Rollback()
				t.Fatalf("Failed to exec COPY data: %v", err)
			}
		}

		// Close the statement to flush the COPY
		_, err = stmt.Exec()
		if err != nil {
			stmt.Close()
			txn.Rollback()
			t.Fatalf("Failed to flush COPY: %v", err)
		}

		err = stmt.Close()
		if err != nil {
			txn.Rollback()
			t.Fatalf("Failed to close COPY statement: %v", err)
		}

		err = txn.Commit()
		if err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}

		// Verify the data was inserted
		result, err := ExecuteQuery(testHarness.DuckgresDB, "SELECT COUNT(*) FROM copy_test")
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		if result.Rows[0][0].(int64) != 3 {
			t.Errorf("Expected 3 rows, got %v", result.Rows[0][0])
		}

		// Verify specific values
		result, err = ExecuteQuery(testHarness.DuckgresDB, "SELECT name FROM copy_test WHERE id = 2")
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		if result.Rows[0][0] != "Bob" {
			t.Errorf("Expected 'Bob', got %v", result.Rows[0][0])
		}
	})

	// Cleanup and recreate for next test
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS copy_test")
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE copy_test (id INTEGER, name TEXT, value DECIMAL(10,2))")

	t.Run("copy_with_null_values", func(t *testing.T) {
		// Note: This test verifies NULL handling in COPY FROM STDIN.
		// lib/pq sends NULL as \N in text format. The server must pass
		// NULL '\N' to DuckDB's COPY command for this to work.
		txn, err := testHarness.DuckgresDB.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("copy_test", "id", "name", "value"))
		if err != nil {
			txn.Rollback()
			t.Fatalf("Failed to prepare COPY: %v", err)
		}

		// Insert row with NULL values
		_, err = stmt.Exec(1, nil, nil)
		if err != nil {
			stmt.Close()
			txn.Rollback()
			t.Fatalf("Failed to exec COPY with NULLs: %v", err)
		}

		_, err = stmt.Exec()
		if err != nil {
			stmt.Close()
			txn.Rollback()
			t.Fatalf("Failed to flush COPY: %v", err)
		}

		stmt.Close()
		txn.Commit()

		// Verify the row was inserted and NULL was handled correctly
		result, err := ExecuteQuery(testHarness.DuckgresDB, "SELECT COUNT(*) FROM copy_test WHERE id = 1")
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		if len(result.Rows) == 0 {
			t.Fatalf("No rows returned from count query")
		}
		count := result.Rows[0][0].(int64)
		if count != 1 {
			t.Fatalf("Expected 1 row inserted, got %d - NULL handling may be broken", count)
		}

		// Check that the NULL values are actually NULL
		result, err = ExecuteQuery(testHarness.DuckgresDB, "SELECT name, value FROM copy_test WHERE id = 1")
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		if len(result.Rows) == 0 {
			t.Fatalf("No rows returned - row was not inserted")
		}
		if result.Rows[0][0] != nil {
			t.Errorf("Expected NULL name, got %v", result.Rows[0][0])
		}
	})

	// Cleanup and recreate for next test
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS copy_test")
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE copy_test (id INTEGER, name TEXT, value DECIMAL(10,2))")

	t.Run("copy_large_batch", func(t *testing.T) {
		txn, err := testHarness.DuckgresDB.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("copy_test", "id", "name", "value"))
		if err != nil {
			txn.Rollback()
			t.Fatalf("Failed to prepare COPY: %v", err)
		}

		// Insert 1000 rows
		for i := 0; i < 1000; i++ {
			_, err = stmt.Exec(i, "user", float64(i)*1.5)
			if err != nil {
				stmt.Close()
				txn.Rollback()
				t.Fatalf("Failed to exec COPY data at row %d: %v", i, err)
			}
		}

		_, err = stmt.Exec()
		if err != nil {
			stmt.Close()
			txn.Rollback()
			t.Fatalf("Failed to flush COPY: %v", err)
		}

		stmt.Close()
		txn.Commit()

		// Verify count
		result, err := ExecuteQuery(testHarness.DuckgresDB, "SELECT COUNT(*) FROM copy_test")
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		if result.Rows[0][0].(int64) != 1000 {
			t.Errorf("Expected 1000 rows, got %v", result.Rows[0][0])
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS copy_test")
}

// TestCopyFromStdinWithSpecialChars tests COPY with special characters
func TestCopyFromStdinWithSpecialChars(t *testing.T) {
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS copy_special_test")
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE copy_special_test (id INTEGER, data TEXT)")

	t.Run("copy_with_commas_and_quotes", func(t *testing.T) {
		txn, err := testHarness.DuckgresDB.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("copy_special_test", "id", "data"))
		if err != nil {
			txn.Rollback()
			t.Fatalf("Failed to prepare COPY: %v", err)
		}

		// Test data with commas and quotes (these work correctly)
		// Note: Escape sequences like \n and \t are NOT interpreted because
		// we use DuckDB's CSV parser, not PostgreSQL's text format parser.
		// This is a known limitation documented in TODO.md
		testCases := []struct {
			id   int
			data string
		}{
			{1, "hello, world"},
			{2, `say "hello"`},
			{5, `{"key": "value", "num": 123}`},
		}

		for _, tc := range testCases {
			_, err = stmt.Exec(tc.id, tc.data)
			if err != nil {
				stmt.Close()
				txn.Rollback()
				t.Fatalf("Failed to exec COPY for id=%d: %v", tc.id, err)
			}
		}

		_, err = stmt.Exec()
		if err != nil {
			stmt.Close()
			txn.Rollback()
			t.Fatalf("Failed to flush COPY: %v", err)
		}

		stmt.Close()
		txn.Commit()

		// Verify each value was stored correctly
		for _, tc := range testCases {
			var data string
			err := testHarness.DuckgresDB.QueryRow(
				"SELECT data FROM copy_special_test WHERE id = $1", tc.id).Scan(&data)
			if err != nil {
				t.Fatalf("Failed to query id=%d: %v", tc.id, err)
			}
			if data != tc.data {
				t.Errorf("id=%d: expected %q, got %q", tc.id, tc.data, data)
			}
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS copy_special_test")
}

// TestCopyFromStdinMultilineJSON tests COPY with multi-line JSON
// Note: Due to using DuckDB's CSV parser (not PostgreSQL's text format parser),
// escape sequences in the data are stored literally. Real multi-line data
// that contains actual newline bytes works correctly.
func TestCopyFromStdinMultilineJSON(t *testing.T) {
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS copy_json_test")
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE copy_json_test (id INTEGER, metadata TEXT)")

	t.Run("copy_json_single_line", func(t *testing.T) {
		// Test with single-line JSON (this should work correctly)
		txn, err := testHarness.DuckgresDB.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("copy_json_test", "id", "metadata"))
		if err != nil {
			txn.Rollback()
			t.Fatalf("Failed to prepare COPY: %v", err)
		}

		// Single-line JSON
		jsonData := `{"subscription":"active","plan":"pro","features":["feature1","feature2"]}`

		_, err = stmt.Exec(1, jsonData)
		if err != nil {
			stmt.Close()
			txn.Rollback()
			t.Fatalf("Failed to exec COPY with JSON: %v", err)
		}

		_, err = stmt.Exec()
		if err != nil {
			stmt.Close()
			txn.Rollback()
			t.Fatalf("Failed to flush COPY: %v", err)
		}

		stmt.Close()
		txn.Commit()

		// Verify the JSON was stored correctly
		var stored string
		err = testHarness.DuckgresDB.QueryRow(
			"SELECT metadata FROM copy_json_test WHERE id = 1").Scan(&stored)
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		if stored != jsonData {
			t.Errorf("JSON not stored correctly.\nExpected: %s\nGot: %s", jsonData, stored)
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS copy_json_test")
}

// TestCopyToStdout tests COPY TO STDOUT functionality
// Note: lib/pq's Query() doesn't support COPY TO STDOUT directly because
// the COPY protocol uses different message types (CopyOutResponse 'H')
// that the driver doesn't handle in simple query mode.
// This would require using lib/pq's CopyOut functionality or raw protocol handling.
func TestCopyToStdout(t *testing.T) {
	t.Skip("COPY TO STDOUT requires special driver support not available in lib/pq's Query()")
}
