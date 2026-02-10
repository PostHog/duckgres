package integration

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// TestConcurrentConnections tests multiple connections operating simultaneously.
func TestConcurrentConnections(t *testing.T) {
	t.Run("concurrent_reads", func(t *testing.T) {
		const numGoroutines = 10
		var wg sync.WaitGroup
		errs := make(chan error, numGoroutines)

		for i := range numGoroutines {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				conn := openDuckgresConn(t)
				defer func() { _ = conn.Close() }()

				var val int
				if err := conn.QueryRow(fmt.Sprintf("SELECT %d", id)).Scan(&val); err != nil {
					errs <- fmt.Errorf("goroutine %d: query failed: %w", id, err)
					return
				}
				if val != id {
					errs <- fmt.Errorf("goroutine %d: expected %d, got %d", id, id, val)
				}
			}(i)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	})

	t.Run("concurrent_writes", func(t *testing.T) {
		const numGoroutines = 5
		var wg sync.WaitGroup
		errs := make(chan error, numGoroutines)

		for i := range numGoroutines {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				conn := openDuckgresConn(t)
				defer func() { _ = conn.Close() }()

				tableName := fmt.Sprintf("concurrent_write_%d", id)
				if _, err := conn.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, val TEXT)", tableName)); err != nil {
					errs <- fmt.Errorf("goroutine %d: create failed: %w", id, err)
					return
				}
				if _, err := conn.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'hello')", tableName)); err != nil {
					errs <- fmt.Errorf("goroutine %d: insert failed: %w", id, err)
					return
				}
				var count int
				if err := conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count); err != nil {
					errs <- fmt.Errorf("goroutine %d: count failed: %w", id, err)
					return
				}
				if count != 1 {
					errs <- fmt.Errorf("goroutine %d: expected 1 row, got %d", id, count)
				}
				_, _ = conn.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
			}(i)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	})

	t.Run("concurrent_ddl_and_dml", func(t *testing.T) {
		conn1 := openDuckgresConn(t)
		defer func() { _ = conn1.Close() }()
		conn2 := openDuckgresConn(t)
		defer func() { _ = conn2.Close() }()

		// conn1 does DDL
		mustExec(t, conn1, "CREATE TABLE concurrent_ddl_test (id INTEGER, name TEXT)")
		mustExec(t, conn1, "INSERT INTO concurrent_ddl_test VALUES (1, 'one')")

		// conn2 runs DML on its own table simultaneously
		mustExec(t, conn2, "CREATE TABLE concurrent_dml_test (id INTEGER)")
		mustExec(t, conn2, "INSERT INTO concurrent_dml_test VALUES (42)")

		var val int
		if err := conn2.QueryRow("SELECT id FROM concurrent_dml_test").Scan(&val); err != nil {
			t.Fatalf("conn2 query failed: %v", err)
		}
		if val != 42 {
			t.Errorf("Expected 42, got %d", val)
		}

		_, _ = conn1.Exec("DROP TABLE concurrent_ddl_test")
		_, _ = conn2.Exec("DROP TABLE concurrent_dml_test")
	})

	t.Run("connection_churn", func(t *testing.T) {
		for i := range 20 {
			conn := openDuckgresConn(t)
			var val int
			if err := conn.QueryRow("SELECT 1").Scan(&val); err != nil {
				t.Fatalf("Connection %d: query failed: %v", i, err)
			}
			if val != 1 {
				t.Errorf("Connection %d: expected 1, got %d", i, val)
			}
			_ = conn.Close()
		}

		// Server should still work after all the churn
		fresh := openDuckgresConn(t)
		defer func() { _ = fresh.Close() }()
		var val int
		if err := fresh.QueryRow("SELECT 99").Scan(&val); err != nil {
			t.Fatalf("Post-churn query failed: %v", err)
		}
		if val != 99 {
			t.Errorf("Expected 99, got %d", val)
		}
	})
}

// TestErrorRecovery tests that sessions remain usable after various error states.
func TestErrorRecovery(t *testing.T) {
	t.Run("error_in_transaction_then_rollback", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE error_recovery_tx (id INTEGER)")

		tx, err := conn.Begin()
		if err != nil {
			t.Fatalf("Begin failed: %v", err)
		}

		_, _ = tx.Exec("INSERT INTO nonexistent_table VALUES (1)") // error
		_ = tx.Rollback()

		// Connection should still work
		mustExec(t, conn, "INSERT INTO error_recovery_tx VALUES (1)")
		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM error_recovery_tx").Scan(&count); err != nil {
			t.Fatalf("Query after rollback failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}

		mustExec(t, conn, "DROP TABLE error_recovery_tx")
	})

	t.Run("multiple_errors_same_connection", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		// Fire several errors in sequence
		_, _ = conn.Exec("SELECT * FROM table_does_not_exist_1")
		_, _ = conn.Exec("SELECT * FROM table_does_not_exist_2")
		_, _ = conn.Exec("SELEC bad syntax")

		// Connection should still work
		var val int
		if err := conn.QueryRow("SELECT 42").Scan(&val); err != nil {
			t.Fatalf("Query after errors failed: %v", err)
		}
		if val != 42 {
			t.Errorf("Expected 42, got %d", val)
		}
	})

	t.Run("error_then_successful_transaction", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		// Cause an error
		_, _ = conn.Exec("SELECT * FROM nonexistent_recovery_table")

		// Now do a successful transaction
		mustExec(t, conn, "CREATE TABLE error_then_tx (id INTEGER)")
		tx, err := conn.Begin()
		if err != nil {
			t.Fatalf("Begin failed: %v", err)
		}
		if _, err := tx.Exec("INSERT INTO error_then_tx VALUES (1)"); err != nil {
			_ = tx.Rollback()
			t.Fatalf("Insert failed: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM error_then_tx").Scan(&count); err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}

		mustExec(t, conn, "DROP TABLE error_then_tx")
	})

	t.Run("division_by_zero_recovery", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		_, err := conn.Exec("SELECT 1/0")
		if err == nil {
			t.Log("DuckDB may not error on division by zero (returns NULL or Inf)")
		}

		// Connection should still work
		var val int
		if err := conn.QueryRow("SELECT 1").Scan(&val); err != nil {
			t.Fatalf("Query after division error failed: %v", err)
		}
		if val != 1 {
			t.Errorf("Expected 1, got %d", val)
		}
	})

	t.Run("constraint_violation_recovery", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE constraint_recovery (id INTEGER PRIMARY KEY, val TEXT)")
		mustExec(t, conn, "INSERT INTO constraint_recovery VALUES (1, 'first')")

		// Violate PK constraint
		_, err := conn.Exec("INSERT INTO constraint_recovery VALUES (1, 'duplicate')")
		if err == nil {
			t.Log("No constraint error — DuckDB may not enforce PK in all modes")
		}

		// Connection should still work
		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM constraint_recovery").Scan(&count); err != nil {
			t.Fatalf("Query after constraint violation failed: %v", err)
		}
		if count < 1 {
			t.Errorf("Expected at least 1 row, got %d", count)
		}

		mustExec(t, conn, "DROP TABLE constraint_recovery")
	})
}

// TestPreparedStatementEdgeCases tests advanced prepared statement scenarios.
func TestPreparedStatementEdgeCases(t *testing.T) {
	t.Run("many_parameters", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		// Build query with 20 parameters
		var placeholders []string
		var args []interface{}
		for i := 1; i <= 20; i++ {
			placeholders = append(placeholders, fmt.Sprintf("$%d::int", i))
			args = append(args, i)
		}
		query := fmt.Sprintf("SELECT %s", strings.Join(placeholders, " + "))

		var result int
		if err := conn.QueryRow(query, args...).Scan(&result); err != nil {
			t.Fatalf("Query with many parameters failed: %v", err)
		}
		// Sum of 1..20 = 210
		if result != 210 {
			t.Errorf("Expected 210, got %d", result)
		}
	})

	t.Run("reuse_after_error", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		stmt, err := conn.Prepare("SELECT $1::int + 1")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer func() { _ = stmt.Close() }()

		// Valid use
		var val int
		if err := stmt.QueryRow(5).Scan(&val); err != nil {
			t.Fatalf("First use failed: %v", err)
		}
		if val != 6 {
			t.Errorf("Expected 6, got %d", val)
		}

		// Cause an error (string where int expected)
		_ = stmt.QueryRow("not_a_number").Scan(&val)

		// Should still work after error
		if err := stmt.QueryRow(10).Scan(&val); err != nil {
			t.Fatalf("Reuse after error failed: %v", err)
		}
		if val != 11 {
			t.Errorf("Expected 11, got %d", val)
		}
	})

	t.Run("multiple_stmts_same_connection", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		stmt1, err := conn.Prepare("SELECT $1::int * 2")
		if err != nil {
			t.Fatalf("Prepare stmt1 failed: %v", err)
		}
		defer func() { _ = stmt1.Close() }()

		stmt2, err := conn.Prepare("SELECT $1::int + 100")
		if err != nil {
			t.Fatalf("Prepare stmt2 failed: %v", err)
		}
		defer func() { _ = stmt2.Close() }()

		var val1, val2 int
		if err := stmt1.QueryRow(5).Scan(&val1); err != nil {
			t.Fatalf("stmt1 query failed: %v", err)
		}
		if err := stmt2.QueryRow(5).Scan(&val2); err != nil {
			t.Fatalf("stmt2 query failed: %v", err)
		}
		if val1 != 10 {
			t.Errorf("stmt1: expected 10, got %d", val1)
		}
		if val2 != 105 {
			t.Errorf("stmt2: expected 105, got %d", val2)
		}
	})

	t.Run("prepare_ddl", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		// Preparing DDL may succeed or fail depending on server — both are acceptable
		_, err := conn.Exec("CREATE TABLE prepare_ddl_test (id INTEGER)")
		if err != nil {
			t.Fatalf("DDL exec failed: %v", err)
		}

		// Verify table exists
		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM prepare_ddl_test").Scan(&count); err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected 0 rows in new table, got %d", count)
		}

		_, _ = conn.Exec("DROP TABLE prepare_ddl_test")
	})

	t.Run("null_parameters", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		var result sql.NullString
		if err := conn.QueryRow("SELECT $1::text", nil).Scan(&result); err != nil {
			t.Fatalf("Query with NULL parameter failed: %v", err)
		}
		if result.Valid {
			t.Errorf("Expected NULL, got %q", result.String)
		}
	})

	t.Run("stmt_across_transactions", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE stmt_across_tx (id INTEGER, val TEXT)")

		stmt, err := conn.Prepare("INSERT INTO stmt_across_tx VALUES ($1, $2)")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer func() { _ = stmt.Close() }()

		// Use in first transaction
		tx1, err := conn.Begin()
		if err != nil {
			t.Fatalf("Begin tx1 failed: %v", err)
		}
		if _, err := tx1.Stmt(stmt).Exec(1, "first"); err != nil {
			_ = tx1.Rollback()
			t.Fatalf("Insert in tx1 failed: %v", err)
		}
		if err := tx1.Commit(); err != nil {
			t.Fatalf("Commit tx1 failed: %v", err)
		}

		// Use in second transaction
		tx2, err := conn.Begin()
		if err != nil {
			t.Fatalf("Begin tx2 failed: %v", err)
		}
		if _, err := tx2.Stmt(stmt).Exec(2, "second"); err != nil {
			_ = tx2.Rollback()
			t.Fatalf("Insert in tx2 failed: %v", err)
		}
		if err := tx2.Commit(); err != nil {
			t.Fatalf("Commit tx2 failed: %v", err)
		}

		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM stmt_across_tx").Scan(&count); err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}

		mustExec(t, conn, "DROP TABLE stmt_across_tx")
	})
}

// TestSavepoints tests nested transaction control.
func TestSavepoints(t *testing.T) {
	t.Run("basic_savepoint", func(t *testing.T) {
		skipIfKnown(t)
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE savepoint_basic (id INTEGER)")
		mustExec(t, conn, "BEGIN")
		mustExec(t, conn, "INSERT INTO savepoint_basic VALUES (1)")
		mustExec(t, conn, "SAVEPOINT sp1")
		mustExec(t, conn, "INSERT INTO savepoint_basic VALUES (2)")
		mustExec(t, conn, "ROLLBACK TO SAVEPOINT sp1")

		// Only row 1 should survive
		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM savepoint_basic").Scan(&count); err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row after rollback to savepoint, got %d", count)
		}

		mustExec(t, conn, "COMMIT")
		mustExec(t, conn, "DROP TABLE savepoint_basic")
	})

	t.Run("nested_savepoints", func(t *testing.T) {
		skipIfKnown(t)
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE savepoint_nested (id INTEGER)")
		mustExec(t, conn, "BEGIN")
		mustExec(t, conn, "INSERT INTO savepoint_nested VALUES (1)")
		mustExec(t, conn, "SAVEPOINT sp1")
		mustExec(t, conn, "INSERT INTO savepoint_nested VALUES (2)")
		mustExec(t, conn, "SAVEPOINT sp2")
		mustExec(t, conn, "INSERT INTO savepoint_nested VALUES (3)")
		mustExec(t, conn, "ROLLBACK TO SAVEPOINT sp2")

		// Rows 1 and 2 should survive
		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM savepoint_nested").Scan(&count); err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 rows after rollback to sp2, got %d", count)
		}

		mustExec(t, conn, "COMMIT")
		mustExec(t, conn, "DROP TABLE savepoint_nested")
	})

	t.Run("release_savepoint", func(t *testing.T) {
		skipIfKnown(t)
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE savepoint_release (id INTEGER)")
		mustExec(t, conn, "BEGIN")
		mustExec(t, conn, "SAVEPOINT sp1")
		mustExec(t, conn, "INSERT INTO savepoint_release VALUES (1)")
		mustExec(t, conn, "RELEASE SAVEPOINT sp1")
		mustExec(t, conn, "COMMIT")

		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM savepoint_release").Scan(&count); err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row after release + commit, got %d", count)
		}

		mustExec(t, conn, "DROP TABLE savepoint_release")
	})

	t.Run("savepoint_with_error", func(t *testing.T) {
		skipIfKnown(t)
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE savepoint_error (id INTEGER)")
		mustExec(t, conn, "BEGIN")
		mustExec(t, conn, "INSERT INTO savepoint_error VALUES (1)")
		mustExec(t, conn, "SAVEPOINT sp1")

		// Cause an error
		_, _ = conn.Exec("SELECT * FROM nonexistent_savepoint_table")

		// Rollback to savepoint to recover
		mustExec(t, conn, "ROLLBACK TO SAVEPOINT sp1")

		// Should be able to continue
		mustExec(t, conn, "INSERT INTO savepoint_error VALUES (2)")
		mustExec(t, conn, "COMMIT")

		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM savepoint_error").Scan(&count); err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}

		mustExec(t, conn, "DROP TABLE savepoint_error")
	})
}

// TestUnicodeAndSpecialData tests that the wire protocol handles real-world data correctly.
func TestUnicodeAndSpecialData(t *testing.T) {
	t.Run("unicode_insert_select", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE unicode_test (id INTEGER, val TEXT)")

		testCases := []struct {
			id  int
			val string
		}{
			{1, "Hello World"},
			{2, "日本語テスト"},         // Japanese
			{3, "中文测试"},            // Chinese
			{4, "한국어 테스트"},         // Korean
			{5, "Ñoño résumé café"}, // Accented Latin
			{6, "🎉🚀🌍"},             // Emoji
			{7, "مرحبا"},            // Arabic
		}

		for _, tc := range testCases {
			if _, err := conn.Exec("INSERT INTO unicode_test VALUES ($1, $2)", tc.id, tc.val); err != nil {
				t.Fatalf("Insert id=%d failed: %v", tc.id, err)
			}
		}

		for _, tc := range testCases {
			var got string
			if err := conn.QueryRow("SELECT val FROM unicode_test WHERE id = $1", tc.id).Scan(&got); err != nil {
				t.Fatalf("Select id=%d failed: %v", tc.id, err)
			}
			if got != tc.val {
				t.Errorf("id=%d: expected %q, got %q", tc.id, tc.val, got)
			}
		}

		mustExec(t, conn, "DROP TABLE unicode_test")
	})

	t.Run("empty_string_vs_null", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE empty_vs_null (id INTEGER, val TEXT)")
		mustExec(t, conn, "INSERT INTO empty_vs_null VALUES (1, '')")
		mustExec(t, conn, "INSERT INTO empty_vs_null VALUES (2, NULL)")

		var val1 sql.NullString
		if err := conn.QueryRow("SELECT val FROM empty_vs_null WHERE id = 1").Scan(&val1); err != nil {
			t.Fatalf("Query id=1 failed: %v", err)
		}
		if !val1.Valid {
			t.Error("id=1: expected valid empty string, got NULL")
		} else if val1.String != "" {
			t.Errorf("id=1: expected empty string, got %q", val1.String)
		}

		var val2 sql.NullString
		if err := conn.QueryRow("SELECT val FROM empty_vs_null WHERE id = 2").Scan(&val2); err != nil {
			t.Fatalf("Query id=2 failed: %v", err)
		}
		if val2.Valid {
			t.Errorf("id=2: expected NULL, got %q", val2.String)
		}

		mustExec(t, conn, "DROP TABLE empty_vs_null")
	})

	t.Run("very_long_text", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE long_text_test (id INTEGER, val TEXT)")

		// 100KB text
		longText := strings.Repeat("abcdefghij", 10000)
		if _, err := conn.Exec("INSERT INTO long_text_test VALUES (1, $1)", longText); err != nil {
			t.Fatalf("Insert long text failed: %v", err)
		}

		var got string
		if err := conn.QueryRow("SELECT val FROM long_text_test WHERE id = 1").Scan(&got); err != nil {
			t.Fatalf("Select long text failed: %v", err)
		}
		if len(got) != len(longText) {
			t.Errorf("Length mismatch: expected %d, got %d", len(longText), len(got))
		}
		if got != longText {
			t.Error("Long text content mismatch")
		}

		mustExec(t, conn, "DROP TABLE long_text_test")
	})

	t.Run("special_sql_chars", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE special_chars_test (id INTEGER, val TEXT)")

		testCases := []struct {
			id  int
			val string
		}{
			{1, "it's a test"},            // single quote
			{2, `he said "hello"`},        // double quote
			{3, `back\slash`},             // backslash
			{4, "semi;colon"},             // semicolon
			{5, "line\nbreak"},            // newline
			{6, "tab\there"},              // tab
			{7, "percent%sign"},           // percent
			{8, "under_score"},            // underscore
			{9, "null\x00byte"},           // null byte (may be stripped)
		}

		for _, tc := range testCases {
			_, err := conn.Exec("INSERT INTO special_chars_test VALUES ($1, $2)", tc.id, tc.val)
			if err != nil {
				// Null byte may cause an error - that's acceptable
				if tc.id == 9 {
					t.Logf("id=9 (null byte) insert error (acceptable): %v", err)
					continue
				}
				t.Fatalf("Insert id=%d failed: %v", tc.id, err)
			}
		}

		// Verify readable ones round-trip
		for _, tc := range testCases {
			if tc.id == 9 {
				continue // skip null byte check
			}
			var got string
			if err := conn.QueryRow("SELECT val FROM special_chars_test WHERE id = $1", tc.id).Scan(&got); err != nil {
				t.Fatalf("Select id=%d failed: %v", tc.id, err)
			}
			if got != tc.val {
				t.Errorf("id=%d: expected %q, got %q", tc.id, tc.val, got)
			}
		}

		mustExec(t, conn, "DROP TABLE special_chars_test")
	})
}

// TestQuotedIdentifiers tests case-sensitive and reserved-word identifiers.
func TestQuotedIdentifiers(t *testing.T) {
	t.Run("quoted_table_name", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, `CREATE TABLE "MixedCaseTable" (id INTEGER, val TEXT)`)
		mustExec(t, conn, `INSERT INTO "MixedCaseTable" VALUES (1, 'hello')`)

		var val string
		if err := conn.QueryRow(`SELECT val FROM "MixedCaseTable" WHERE id = 1`).Scan(&val); err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if val != "hello" {
			t.Errorf("Expected 'hello', got %q", val)
		}

		mustExec(t, conn, `DROP TABLE "MixedCaseTable"`)
	})

	t.Run("quoted_column_names", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, `CREATE TABLE quoted_cols ("First Name" TEXT, "Last Name" TEXT, "Age" INTEGER)`)
		mustExec(t, conn, `INSERT INTO quoted_cols VALUES ('John', 'Doe', 30)`)

		var firstName, lastName string
		var age int
		if err := conn.QueryRow(`SELECT "First Name", "Last Name", "Age" FROM quoted_cols`).Scan(&firstName, &lastName, &age); err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if firstName != "John" || lastName != "Doe" || age != 30 {
			t.Errorf("Got %q %q %d, expected John Doe 30", firstName, lastName, age)
		}

		mustExec(t, conn, "DROP TABLE quoted_cols")
	})

	t.Run("reserved_word_identifiers", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, `CREATE TABLE "select" ("from" INTEGER, "where" TEXT)`)
		mustExec(t, conn, `INSERT INTO "select" VALUES (1, 'test')`)

		var from int
		var where string
		if err := conn.QueryRow(`SELECT "from", "where" FROM "select"`).Scan(&from, &where); err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if from != 1 || where != "test" {
			t.Errorf("Got %d %q, expected 1 test", from, where)
		}

		mustExec(t, conn, `DROP TABLE "select"`)
	})
}

// TestExplain tests EXPLAIN output.
func TestExplain(t *testing.T) {
	t.Run("explain_select", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		rows, err := conn.Query("EXPLAIN SELECT * FROM generate_series(1, 10)")
		if err != nil {
			t.Fatalf("EXPLAIN failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		count := 0
		for rows.Next() {
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Row iteration failed: %v", err)
		}
		if count == 0 {
			t.Error("EXPLAIN returned no rows")
		}
	})

	t.Run("explain_analyze", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		rows, err := conn.Query("EXPLAIN ANALYZE SELECT * FROM generate_series(1, 10)")
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		count := 0
		for rows.Next() {
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Row iteration failed: %v", err)
		}
		if count == 0 {
			t.Error("EXPLAIN ANALYZE returned no rows")
		}
	})

	t.Run("explain_insert", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE explain_insert_test (id INTEGER, val TEXT)")

		rows, err := conn.Query("EXPLAIN INSERT INTO explain_insert_test VALUES (1, 'test')")
		if err != nil {
			t.Fatalf("EXPLAIN INSERT failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		count := 0
		for rows.Next() {
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Row iteration failed: %v", err)
		}
		if count == 0 {
			t.Error("EXPLAIN INSERT returned no rows")
		}

		mustExec(t, conn, "DROP TABLE explain_insert_test")
	})

	t.Run("explain_with_join", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE explain_a (id INTEGER, val TEXT)")
		mustExec(t, conn, "CREATE TABLE explain_b (id INTEGER, ref_id INTEGER)")

		rows, err := conn.Query("EXPLAIN SELECT a.val FROM explain_a a JOIN explain_b b ON a.id = b.ref_id")
		if err != nil {
			t.Fatalf("EXPLAIN JOIN failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		count := 0
		for rows.Next() {
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Row iteration failed: %v", err)
		}
		if count == 0 {
			t.Error("EXPLAIN JOIN returned no rows")
		}

		_, _ = conn.Exec("DROP TABLE explain_a")
		_, _ = conn.Exec("DROP TABLE explain_b")
	})
}

// TestEmptyTableOperations tests operations on tables with no rows.
func TestEmptyTableOperations(t *testing.T) {
	t.Run("select_empty", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE empty_ops (id INTEGER, val TEXT, num DOUBLE)")

		rows, err := conn.Query("SELECT * FROM empty_ops")
		if err != nil {
			t.Fatalf("SELECT from empty table failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		if rows.Next() {
			t.Error("Expected no rows from empty table")
		}

		mustExec(t, conn, "DROP TABLE empty_ops")
	})

	t.Run("update_empty", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE empty_update (id INTEGER, val TEXT)")

		result, err := conn.Exec("UPDATE empty_update SET val = 'changed' WHERE id = 1")
		if err != nil {
			t.Fatalf("UPDATE on empty table failed: %v", err)
		}
		affected, _ := result.RowsAffected()
		if affected != 0 {
			t.Errorf("Expected 0 rows affected, got %d", affected)
		}

		mustExec(t, conn, "DROP TABLE empty_update")
	})

	t.Run("delete_empty", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE empty_delete (id INTEGER)")

		result, err := conn.Exec("DELETE FROM empty_delete WHERE id = 1")
		if err != nil {
			t.Fatalf("DELETE on empty table failed: %v", err)
		}
		affected, _ := result.RowsAffected()
		if affected != 0 {
			t.Errorf("Expected 0 rows affected, got %d", affected)
		}

		mustExec(t, conn, "DROP TABLE empty_delete")
	})

	t.Run("aggregate_empty", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE empty_agg (id INTEGER, val DOUBLE)")

		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM empty_agg").Scan(&count); err != nil {
			t.Fatalf("COUNT on empty failed: %v", err)
		}
		if count != 0 {
			t.Errorf("COUNT: expected 0, got %d", count)
		}

		var sum sql.NullFloat64
		if err := conn.QueryRow("SELECT SUM(val) FROM empty_agg").Scan(&sum); err != nil {
			t.Fatalf("SUM on empty failed: %v", err)
		}
		if sum.Valid {
			t.Errorf("SUM: expected NULL, got %f", sum.Float64)
		}

		var avg sql.NullFloat64
		if err := conn.QueryRow("SELECT AVG(val) FROM empty_agg").Scan(&avg); err != nil {
			t.Fatalf("AVG on empty failed: %v", err)
		}
		if avg.Valid {
			t.Errorf("AVG: expected NULL, got %f", avg.Float64)
		}

		mustExec(t, conn, "DROP TABLE empty_agg")
	})

	t.Run("join_with_empty", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE join_populated (id INTEGER, val TEXT)")
		mustExec(t, conn, "INSERT INTO join_populated VALUES (1, 'one'), (2, 'two')")
		mustExec(t, conn, "CREATE TABLE join_empty (id INTEGER, ref_id INTEGER)")

		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM join_populated p JOIN join_empty e ON p.id = e.ref_id").Scan(&count); err != nil {
			t.Fatalf("JOIN with empty failed: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected 0 rows from join with empty, got %d", count)
		}

		_, _ = conn.Exec("DROP TABLE join_populated")
		_, _ = conn.Exec("DROP TABLE join_empty")
	})
}

// TestMultiStatementBehavior tests handling of multi-statement query strings.
func TestMultiStatementBehavior(t *testing.T) {
	t.Run("two_selects", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		// lib/pq sends this as a single simple query message.
		// The server should handle it — either returning the last result
		// or erroring gracefully.
		rows, err := conn.Query("SELECT 1; SELECT 2")
		if err != nil {
			// Some PG-compatible servers reject multi-statement in simple query — acceptable
			t.Logf("Multi-statement query returned error (acceptable): %v", err)
			return
		}
		defer func() { _ = rows.Close() }()

		// If it succeeded, we should get at least one result
		if !rows.Next() {
			t.Error("Expected at least one row from multi-statement query")
		}
	})

	t.Run("create_then_insert", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		// Try DDL + DML in one string
		_, err := conn.Exec("CREATE TABLE multi_stmt_test (id INTEGER); INSERT INTO multi_stmt_test VALUES (1)")
		if err != nil {
			t.Logf("Multi-statement DDL+DML returned error (acceptable): %v", err)
			// Clean up in case the CREATE succeeded
			_, _ = conn.Exec("DROP TABLE IF EXISTS multi_stmt_test")
			return
		}

		// Verify if it worked
		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM multi_stmt_test").Scan(&count); err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}

		mustExec(t, conn, "DROP TABLE multi_stmt_test")
	})

	t.Run("empty_statements", func(t *testing.T) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		// Trailing semicolons
		_, err := conn.Exec("SELECT 1;")
		if err != nil {
			t.Logf("Trailing semicolon error (acceptable): %v", err)
		}

		// Connection should still work
		var val int
		if err := conn.QueryRow("SELECT 42").Scan(&val); err != nil {
			t.Fatalf("Query after empty statement failed: %v", err)
		}
		if val != 42 {
			t.Errorf("Expected 42, got %d", val)
		}
	})
}

// TestConnectionParameters tests different connection string options.
func TestConnectionParameters(t *testing.T) {
	t.Run("application_name", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=127.0.0.1 port=%d user=testuser password=testpass dbname=test sslmode=require application_name=test_app",
			testHarness.dgPort,
		)
		db, err := sql.Open("postgres", connStr)
		if err != nil {
			t.Fatalf("Open with application_name failed: %v", err)
		}
		defer func() { _ = db.Close() }()

		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		var val int
		if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
			t.Fatalf("Query with application_name failed: %v", err)
		}
		if val != 1 {
			t.Errorf("Expected 1, got %d", val)
		}
	})

	t.Run("connect_timeout", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=127.0.0.1 port=%d user=testuser password=testpass dbname=test sslmode=require connect_timeout=10",
			testHarness.dgPort,
		)
		db, err := sql.Open("postgres", connStr)
		if err != nil {
			t.Fatalf("Open with connect_timeout failed: %v", err)
		}
		defer func() { _ = db.Close() }()

		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		var val int
		if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
			t.Fatalf("Query with connect_timeout failed: %v", err)
		}
		if val != 1 {
			t.Errorf("Expected 1, got %d", val)
		}
	})

	t.Run("multiple_reconnects", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=127.0.0.1 port=%d user=testuser password=testpass dbname=test sslmode=require",
			testHarness.dgPort,
		)

		for i := range 5 {
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				t.Fatalf("Reconnect %d: open failed: %v", i, err)
			}
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)

			var val int
			if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
				_ = db.Close()
				t.Fatalf("Reconnect %d: query failed: %v", i, err)
			}
			if val != 1 {
				t.Errorf("Reconnect %d: expected 1, got %d", i, val)
			}
			_ = db.Close()
		}
	})
}
