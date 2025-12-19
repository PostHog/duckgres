package integration

import (
	"database/sql"
	"testing"
)

// TestProtocolSimpleQuery tests the simple query protocol
func TestProtocolSimpleQuery(t *testing.T) {
	t.Run("select_literal", func(t *testing.T) {
		var val int
		err := testHarness.DuckgresDB.QueryRow("SELECT 42").Scan(&val)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if val != 42 {
			t.Errorf("Expected 42, got %d", val)
		}
	})

	t.Run("select_multiple_columns", func(t *testing.T) {
		var a, b, c int
		err := testHarness.DuckgresDB.QueryRow("SELECT 1, 2, 3").Scan(&a, &b, &c)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if a != 1 || b != 2 || c != 3 {
			t.Errorf("Expected 1,2,3 got %d,%d,%d", a, b, c)
		}
	})

	t.Run("select_string", func(t *testing.T) {
		var val string
		err := testHarness.DuckgresDB.QueryRow("SELECT 'hello world'").Scan(&val)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if val != "hello world" {
			t.Errorf("Expected 'hello world', got %q", val)
		}
	})

	t.Run("select_null", func(t *testing.T) {
		var val sql.NullString
		err := testHarness.DuckgresDB.QueryRow("SELECT NULL").Scan(&val)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if val.Valid {
			t.Errorf("Expected NULL, got valid value")
		}
	})

	t.Run("select_multiple_rows", func(t *testing.T) {
		rows, err := testHarness.DuckgresDB.Query("SELECT * FROM users ORDER BY id LIMIT 5")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Row iteration error: %v", err)
		}
		if count != 5 {
			t.Errorf("Expected 5 rows, got %d", count)
		}
	})

	t.Run("empty_result", func(t *testing.T) {
		rows, err := testHarness.DuckgresDB.Query("SELECT * FROM users WHERE id = -1")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		if rows.Next() {
			t.Error("Expected no rows")
		}
	})

	t.Run("exec_no_result", func(t *testing.T) {
		result, err := testHarness.DuckgresDB.Exec("SELECT 1")
		if err != nil {
			t.Fatalf("Exec failed: %v", err)
		}
		_, err = result.RowsAffected()
		if err != nil {
			// This is expected - SELECT doesn't have rows affected
		}
	})
}

// TestProtocolExtendedQuery tests the extended query protocol (prepared statements)
func TestProtocolExtendedQuery(t *testing.T) {
	t.Run("prepare_and_query", func(t *testing.T) {
		stmt, err := testHarness.DuckgresDB.Prepare("SELECT $1::int + $2::int")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		var result int
		err = stmt.QueryRow(10, 20).Scan(&result)
		if err != nil {
			t.Fatalf("QueryRow failed: %v", err)
		}
		if result != 30 {
			t.Errorf("Expected 30, got %d", result)
		}
	})

	t.Run("prepare_with_types", func(t *testing.T) {
		stmt, err := testHarness.DuckgresDB.Prepare("SELECT $1::text || ' ' || $2::text")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		var result string
		err = stmt.QueryRow("hello", "world").Scan(&result)
		if err != nil {
			t.Fatalf("QueryRow failed: %v", err)
		}
		if result != "hello world" {
			t.Errorf("Expected 'hello world', got %q", result)
		}
	})

	t.Run("prepare_select_where", func(t *testing.T) {
		stmt, err := testHarness.DuckgresDB.Prepare("SELECT name FROM users WHERE id = $1")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		var name string
		err = stmt.QueryRow(1).Scan(&name)
		if err != nil {
			t.Fatalf("QueryRow failed: %v", err)
		}
		if name != "Alice" {
			t.Errorf("Expected 'Alice', got %q", name)
		}
	})

	t.Run("prepare_reuse", func(t *testing.T) {
		stmt, err := testHarness.DuckgresDB.Prepare("SELECT $1::int * 2")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		for i := 1; i <= 5; i++ {
			var result int
			err = stmt.QueryRow(i).Scan(&result)
			if err != nil {
				t.Fatalf("QueryRow failed for i=%d: %v", i, err)
			}
			if result != i*2 {
				t.Errorf("Expected %d, got %d", i*2, result)
			}
		}
	})

	t.Run("prepare_insert", func(t *testing.T) {
		mustExec(t, testHarness.DuckgresDB, "CREATE TABLE protocol_insert_test (id INTEGER, name TEXT)")

		stmt, err := testHarness.DuckgresDB.Prepare("INSERT INTO protocol_insert_test (id, name) VALUES ($1, $2)")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		result, err := stmt.Exec(1, "test")
		if err != nil {
			t.Fatalf("Exec failed: %v", err)
		}

		affected, _ := result.RowsAffected()
		if affected != 1 {
			t.Errorf("Expected 1 row affected, got %d", affected)
		}

		mustExec(t, testHarness.DuckgresDB, "DROP TABLE protocol_insert_test")
	})
}

// TestProtocolTransactions tests transaction handling
func TestProtocolTransactions(t *testing.T) {
	t.Run("begin_commit", func(t *testing.T) {
		mustExec(t, testHarness.DuckgresDB, "CREATE TABLE tx_test (id INTEGER, val TEXT)")

		tx, err := testHarness.DuckgresDB.Begin()
		if err != nil {
			t.Fatalf("Begin failed: %v", err)
		}

		_, err = tx.Exec("INSERT INTO tx_test VALUES (1, 'committed')")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Insert failed: %v", err)
		}

		err = tx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		var val string
		err = testHarness.DuckgresDB.QueryRow("SELECT val FROM tx_test WHERE id = 1").Scan(&val)
		if err != nil {
			t.Fatalf("Query after commit failed: %v", err)
		}
		if val != "committed" {
			t.Errorf("Expected 'committed', got %q", val)
		}

		mustExec(t, testHarness.DuckgresDB, "DROP TABLE tx_test")
	})

	t.Run("begin_rollback", func(t *testing.T) {
		mustExec(t, testHarness.DuckgresDB, "CREATE TABLE tx_rollback_test (id INTEGER)")

		tx, err := testHarness.DuckgresDB.Begin()
		if err != nil {
			t.Fatalf("Begin failed: %v", err)
		}

		_, err = tx.Exec("INSERT INTO tx_rollback_test VALUES (1)")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Insert failed: %v", err)
		}

		err = tx.Rollback()
		if err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}

		var count int
		testHarness.DuckgresDB.QueryRow("SELECT COUNT(*) FROM tx_rollback_test").Scan(&count)
		if count != 0 {
			t.Errorf("Expected 0 rows after rollback, got %d", count)
		}

		mustExec(t, testHarness.DuckgresDB, "DROP TABLE tx_rollback_test")
	})

	t.Run("transaction_isolation", func(t *testing.T) {
		// Test that changes in a transaction are visible within that transaction
		mustExec(t, testHarness.DuckgresDB, "CREATE TABLE tx_isolation_test (id INTEGER)")

		tx, err := testHarness.DuckgresDB.Begin()
		if err != nil {
			t.Fatalf("Begin failed: %v", err)
		}
		defer tx.Rollback()

		_, err = tx.Exec("INSERT INTO tx_isolation_test VALUES (1), (2), (3)")
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		// Should see our own changes
		var count int
		err = tx.QueryRow("SELECT COUNT(*) FROM tx_isolation_test").Scan(&count)
		if err != nil {
			t.Fatalf("Query in transaction failed: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected 3 rows in transaction, got %d", count)
		}

		tx.Rollback()
		mustExec(t, testHarness.DuckgresDB, "DROP TABLE tx_isolation_test")
	})
}

// TestProtocolErrors tests error handling
func TestProtocolErrors(t *testing.T) {
	t.Run("syntax_error", func(t *testing.T) {
		_, err := testHarness.DuckgresDB.Query("SELEC 1") // typo
		if err == nil {
			t.Error("Expected syntax error")
		}
	})

	t.Run("table_not_found", func(t *testing.T) {
		_, err := testHarness.DuckgresDB.Query("SELECT * FROM nonexistent_table_xyz")
		if err == nil {
			t.Error("Expected table not found error")
		}
	})

	t.Run("column_not_found", func(t *testing.T) {
		_, err := testHarness.DuckgresDB.Query("SELECT nonexistent_column FROM users")
		if err == nil {
			t.Error("Expected column not found error")
		}
	})

	t.Run("type_error", func(t *testing.T) {
		_, err := testHarness.DuckgresDB.Query("SELECT 'not a number'::INTEGER")
		if err == nil {
			t.Error("Expected type error")
		}
	})

	t.Run("error_recovery", func(t *testing.T) {
		// After an error, connection should still be usable
		testHarness.DuckgresDB.Query("SELECT * FROM nonexistent") // This should error

		// But this should work
		var val int
		err := testHarness.DuckgresDB.QueryRow("SELECT 1").Scan(&val)
		if err != nil {
			t.Errorf("Query after error failed: %v", err)
		}
		if val != 1 {
			t.Errorf("Expected 1, got %d", val)
		}
	})
}

// TestProtocolDataTypes tests that various data types are correctly transmitted
func TestProtocolDataTypes(t *testing.T) {
	t.Run("integer", func(t *testing.T) {
		var val int64
		err := testHarness.DuckgresDB.QueryRow("SELECT 9223372036854775807").Scan(&val)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if val != 9223372036854775807 {
			t.Errorf("Expected 9223372036854775807, got %d", val)
		}
	})

	t.Run("float", func(t *testing.T) {
		var val float64
		err := testHarness.DuckgresDB.QueryRow("SELECT 3.14159265358979::DOUBLE PRECISION").Scan(&val)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if val < 3.14 || val > 3.15 {
			t.Errorf("Expected ~3.14, got %f", val)
		}
	})

	t.Run("boolean_true", func(t *testing.T) {
		var val bool
		err := testHarness.DuckgresDB.QueryRow("SELECT TRUE").Scan(&val)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if !val {
			t.Error("Expected true")
		}
	})

	t.Run("boolean_false", func(t *testing.T) {
		var val bool
		err := testHarness.DuckgresDB.QueryRow("SELECT FALSE").Scan(&val)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if val {
			t.Error("Expected false")
		}
	})

	t.Run("text", func(t *testing.T) {
		var val string
		err := testHarness.DuckgresDB.QueryRow("SELECT 'hello, world!'::TEXT").Scan(&val)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if val != "hello, world!" {
			t.Errorf("Expected 'hello, world!', got %q", val)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		var val []byte
		err := testHarness.DuckgresDB.QueryRow("SELECT '\\x48656c6c6f'::BYTEA").Scan(&val)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if string(val) != "Hello" {
			t.Errorf("Expected 'Hello', got %q", string(val))
		}
	})

	t.Run("null_values", func(t *testing.T) {
		var intVal sql.NullInt64
		var strVal sql.NullString
		var boolVal sql.NullBool

		row := testHarness.DuckgresDB.QueryRow("SELECT NULL::INTEGER, NULL::TEXT, NULL::BOOLEAN")
		err := row.Scan(&intVal, &strVal, &boolVal)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if intVal.Valid || strVal.Valid || boolVal.Valid {
			t.Error("Expected all NULL values")
		}
	})
}

// TestProtocolRowDescription tests that column metadata is correctly returned
func TestProtocolRowDescription(t *testing.T) {
	t.Run("column_names", func(t *testing.T) {
		rows, err := testHarness.DuckgresDB.Query("SELECT 1 AS col_a, 2 AS col_b, 3 AS col_c")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			t.Fatalf("Columns failed: %v", err)
		}

		expected := []string{"col_a", "col_b", "col_c"}
		if len(columns) != len(expected) {
			t.Fatalf("Expected %d columns, got %d", len(expected), len(columns))
		}
		for i, col := range columns {
			if col != expected[i] {
				t.Errorf("Column %d: expected %q, got %q", i, expected[i], col)
			}
		}
	})

	t.Run("column_types", func(t *testing.T) {
		rows, err := testHarness.DuckgresDB.Query("SELECT 1::INTEGER, 'text'::TEXT, TRUE::BOOLEAN")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		types, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("ColumnTypes failed: %v", err)
		}

		if len(types) != 3 {
			t.Fatalf("Expected 3 column types, got %d", len(types))
		}

		// Just verify we can access type info
		for i, ct := range types {
			name := ct.Name()
			dbType := ct.DatabaseTypeName()
			t.Logf("Column %d: name=%q, dbType=%q", i, name, dbType)
		}
	})
}

// TestProtocolMultipleStatements tests handling of multiple statements
func TestProtocolMultipleStatements(t *testing.T) {
	t.Run("sequential_queries", func(t *testing.T) {
		// Execute multiple queries in sequence on the same connection
		queries := []string{
			"SELECT 1",
			"SELECT 2",
			"SELECT 3",
		}

		for i, q := range queries {
			var val int
			err := testHarness.DuckgresDB.QueryRow(q).Scan(&val)
			if err != nil {
				t.Fatalf("Query %d failed: %v", i, err)
			}
			if val != i+1 {
				t.Errorf("Query %d: expected %d, got %d", i, i+1, val)
			}
		}
	})

	t.Run("interleaved_query_and_exec", func(t *testing.T) {
		mustExec(t, testHarness.DuckgresDB, "CREATE TABLE interleave_test (id INTEGER)")

		// Mix queries and execs
		_, err := testHarness.DuckgresDB.Exec("INSERT INTO interleave_test VALUES (1)")
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		var count int
		err = testHarness.DuckgresDB.QueryRow("SELECT COUNT(*) FROM interleave_test").Scan(&count)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1, got %d", count)
		}

		_, err = testHarness.DuckgresDB.Exec("INSERT INTO interleave_test VALUES (2)")
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		err = testHarness.DuckgresDB.QueryRow("SELECT COUNT(*) FROM interleave_test").Scan(&count)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2, got %d", count)
		}

		mustExec(t, testHarness.DuckgresDB, "DROP TABLE interleave_test")
	})
}

// TestProtocolLargeResults tests handling of large result sets
func TestProtocolLargeResults(t *testing.T) {
	t.Run("many_rows", func(t *testing.T) {
		rows, err := testHarness.DuckgresDB.Query("SELECT * FROM generate_series(1, 1000)")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Row iteration error: %v", err)
		}
		if count != 1000 {
			t.Errorf("Expected 1000 rows, got %d", count)
		}
	})

	t.Run("many_columns", func(t *testing.T) {
		// Generate a query with many columns
		rows, err := testHarness.DuckgresDB.Query(`
			SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			       11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
			       21, 22, 23, 24, 25, 26, 27, 28, 29, 30
		`)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		columns, _ := rows.Columns()
		if len(columns) != 30 {
			t.Errorf("Expected 30 columns, got %d", len(columns))
		}
	})
}
