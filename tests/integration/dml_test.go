package integration

import (
	"testing"
)

// TestDMLInsert tests INSERT variations
func TestDMLInsert(t *testing.T) {
	// Create test table
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_insert_test (id INTEGER, name TEXT, value NUMERIC(10,2))")

	t.Run("insert_single_row", func(t *testing.T) {
		result, err := testHarness.DuckgresDB.Exec("INSERT INTO dml_insert_test (id, name, value) VALUES (1, 'Alice', 100.50)")
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		affected, _ := result.RowsAffected()
		if affected != 1 {
			t.Errorf("Expected 1 row affected, got %d", affected)
		}
	})

	t.Run("insert_without_columns", func(t *testing.T) {
		_, err := testHarness.DuckgresDB.Exec("INSERT INTO dml_insert_test VALUES (2, 'Bob', 200.75)")
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	})

	t.Run("insert_multiple_rows", func(t *testing.T) {
		result, err := testHarness.DuckgresDB.Exec(`
			INSERT INTO dml_insert_test (id, name, value) VALUES
			(3, 'Charlie', 300.00),
			(4, 'Diana', 400.25),
			(5, 'Eve', 500.50)
		`)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		affected, _ := result.RowsAffected()
		if affected != 3 {
			t.Errorf("Expected 3 rows affected, got %d", affected)
		}
	})

	t.Run("insert_with_select", func(t *testing.T) {
		mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_insert_target (id INTEGER, name TEXT, value NUMERIC(10,2))")
		result, err := testHarness.DuckgresDB.Exec("INSERT INTO dml_insert_target SELECT * FROM dml_insert_test WHERE id <= 3")
		if err != nil {
			t.Fatalf("Insert with SELECT failed: %v", err)
		}
		affected, _ := result.RowsAffected()
		if affected != 3 {
			t.Errorf("Expected 3 rows affected, got %d", affected)
		}
		mustExec(t, testHarness.DuckgresDB, "DROP TABLE dml_insert_target")
	})

	t.Run("insert_with_default", func(t *testing.T) {
		mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_insert_default (id INTEGER, status TEXT DEFAULT 'pending')")
		_, err := testHarness.DuckgresDB.Exec("INSERT INTO dml_insert_default (id) VALUES (1)")
		if err != nil {
			t.Fatalf("Insert with default failed: %v", err)
		}
		// Verify default was applied
		result, _ := ExecuteQuery(testHarness.DuckgresDB, "SELECT status FROM dml_insert_default WHERE id = 1")
		if len(result.Rows) != 1 || result.Rows[0][0] != "pending" {
			t.Errorf("Default value not applied correctly")
		}
		mustExec(t, testHarness.DuckgresDB, "DROP TABLE dml_insert_default")
	})

	t.Run("insert_null", func(t *testing.T) {
		_, err := testHarness.DuckgresDB.Exec("INSERT INTO dml_insert_test (id, name, value) VALUES (6, NULL, NULL)")
		if err != nil {
			t.Fatalf("Insert NULL failed: %v", err)
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_insert_test")
}

// TestDMLInsertReturning tests INSERT ... RETURNING
func TestDMLInsertReturning(t *testing.T) {
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_returning_test (id INTEGER, name TEXT)")

	t.Run("insert_returning_star", func(t *testing.T) {
		skipIfKnown(t)
		rows, err := testHarness.DuckgresDB.Query("INSERT INTO dml_returning_test (id, name) VALUES (1, 'Alice') RETURNING *")
		if err != nil {
			t.Fatalf("Insert RETURNING failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		if !rows.Next() {
			t.Error("Expected 1 row returned")
		}
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if id != 1 || name != "Alice" {
			t.Errorf("Unexpected values: id=%d, name=%s", id, name)
		}
	})

	t.Run("insert_returning_columns", func(t *testing.T) {
		skipIfKnown(t)
		rows, err := testHarness.DuckgresDB.Query("INSERT INTO dml_returning_test (id, name) VALUES (2, 'Bob') RETURNING id")
		if err != nil {
			t.Fatalf("Insert RETURNING failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		if !rows.Next() {
			t.Error("Expected 1 row returned")
		}
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if id != 2 {
			t.Errorf("Unexpected id: %d", id)
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_returning_test")
}

// TestDMLInsertOnConflict tests INSERT ... ON CONFLICT (UPSERT)
func TestDMLInsertOnConflict(t *testing.T) {
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_upsert_test (id INTEGER PRIMARY KEY, name TEXT, counter INTEGER)")
	mustExec(t, testHarness.DuckgresDB, "INSERT INTO dml_upsert_test VALUES (1, 'Alice', 10)")

	t.Run("on_conflict_do_nothing", func(t *testing.T) {
		skipIfKnown(t)
		_, err := testHarness.DuckgresDB.Exec("INSERT INTO dml_upsert_test (id, name, counter) VALUES (1, 'Duplicate', 99) ON CONFLICT DO NOTHING")
		if err != nil {
			t.Fatalf("ON CONFLICT DO NOTHING failed: %v", err)
		}
		// Verify original row unchanged
		result, _ := ExecuteQuery(testHarness.DuckgresDB, "SELECT name FROM dml_upsert_test WHERE id = 1")
		if result.Rows[0][0] != "Alice" {
			t.Errorf("Row was modified when it shouldn't have been")
		}
	})

	t.Run("on_conflict_do_update", func(t *testing.T) {
		_, err := testHarness.DuckgresDB.Exec(`
			INSERT INTO dml_upsert_test (id, name, counter) VALUES (1, 'Updated', 20)
			ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, counter = dml_upsert_test.counter + EXCLUDED.counter
		`)
		if err != nil {
			t.Fatalf("ON CONFLICT DO UPDATE failed: %v", err)
		}
		// Verify row was updated
		result, _ := ExecuteQuery(testHarness.DuckgresDB, "SELECT name, counter FROM dml_upsert_test WHERE id = 1")
		if result.Rows[0][0] != "Updated" {
			t.Errorf("Name not updated: got %v", result.Rows[0][0])
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_upsert_test")
}

// TestDMLUpdate tests UPDATE variations
func TestDMLUpdate(t *testing.T) {
	// Create and populate test table
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_update_test (id INTEGER, name TEXT, status TEXT, counter INTEGER)")
	mustExec(t, testHarness.DuckgresDB, `
		INSERT INTO dml_update_test VALUES
		(1, 'Alice', 'active', 10),
		(2, 'Bob', 'active', 20),
		(3, 'Charlie', 'inactive', 30),
		(4, 'Diana', 'active', 40),
		(5, 'Eve', 'inactive', 50)
	`)

	t.Run("update_single_column", func(t *testing.T) {
		result, err := testHarness.DuckgresDB.Exec("UPDATE dml_update_test SET status = 'updated' WHERE id = 1")
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
		affected, _ := result.RowsAffected()
		if affected != 1 {
			t.Errorf("Expected 1 row affected, got %d", affected)
		}
	})

	t.Run("update_multiple_columns", func(t *testing.T) {
		result, err := testHarness.DuckgresDB.Exec("UPDATE dml_update_test SET status = 'modified', counter = 100 WHERE id = 2")
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
		affected, _ := result.RowsAffected()
		if affected != 1 {
			t.Errorf("Expected 1 row affected, got %d", affected)
		}
	})

	t.Run("update_with_expression", func(t *testing.T) {
		_, err := testHarness.DuckgresDB.Exec("UPDATE dml_update_test SET counter = counter + 5 WHERE id = 3")
		if err != nil {
			t.Fatalf("Update with expression failed: %v", err)
		}
		// Verify
		result, _ := ExecuteQuery(testHarness.DuckgresDB, "SELECT counter FROM dml_update_test WHERE id = 3")
		if result.Rows[0][0].(int64) != 35 {
			t.Errorf("Expected counter=35, got %v", result.Rows[0][0])
		}
	})

	t.Run("update_multiple_rows", func(t *testing.T) {
		result, err := testHarness.DuckgresDB.Exec("UPDATE dml_update_test SET status = 'batch_updated' WHERE status = 'inactive'")
		if err != nil {
			t.Fatalf("Batch update failed: %v", err)
		}
		affected, _ := result.RowsAffected()
		if affected < 1 {
			t.Errorf("Expected at least 1 row affected, got %d", affected)
		}
	})

	t.Run("update_all_rows", func(t *testing.T) {
		result, err := testHarness.DuckgresDB.Exec("UPDATE dml_update_test SET counter = 0")
		if err != nil {
			t.Fatalf("Update all rows failed: %v", err)
		}
		affected, _ := result.RowsAffected()
		if affected != 5 {
			t.Errorf("Expected 5 rows affected, got %d", affected)
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_update_test")
}

// TestDMLUpdateReturning tests UPDATE ... RETURNING
func TestDMLUpdateReturning(t *testing.T) {
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_update_returning (id INTEGER, name TEXT, version INTEGER)")
	mustExec(t, testHarness.DuckgresDB, "INSERT INTO dml_update_returning VALUES (1, 'Test', 1)")

	t.Run("update_returning_star", func(t *testing.T) {
		skipIfKnown(t)
		rows, err := testHarness.DuckgresDB.Query("UPDATE dml_update_returning SET version = version + 1 WHERE id = 1 RETURNING *")
		if err != nil {
			t.Fatalf("Update RETURNING failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		if !rows.Next() {
			t.Error("Expected 1 row returned")
		}
		var id int
		var name string
		var version int
		if err := rows.Scan(&id, &name, &version); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if version != 2 {
			t.Errorf("Expected version=2, got %d", version)
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_update_returning")
}

// TestDMLUpdateFromJoin tests UPDATE with FROM clause (join)
func TestDMLUpdateFromJoin(t *testing.T) {
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_update_target (id INTEGER, value INTEGER)")
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_update_source (id INTEGER, new_value INTEGER)")
	mustExec(t, testHarness.DuckgresDB, "INSERT INTO dml_update_target VALUES (1, 10), (2, 20), (3, 30)")
	mustExec(t, testHarness.DuckgresDB, "INSERT INTO dml_update_source VALUES (1, 100), (2, 200)")

	t.Run("update_from_join", func(t *testing.T) {
		_, err := testHarness.DuckgresDB.Exec(`
			UPDATE dml_update_target
			SET value = dml_update_source.new_value
			FROM dml_update_source
			WHERE dml_update_target.id = dml_update_source.id
		`)
		if err != nil {
			t.Fatalf("Update from join failed: %v", err)
		}
		// Verify
		result, _ := ExecuteQuery(testHarness.DuckgresDB, "SELECT value FROM dml_update_target WHERE id = 1")
		if result.Rows[0][0].(int64) != 100 {
			t.Errorf("Expected value=100, got %v", result.Rows[0][0])
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_update_target")
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_update_source")
}

// TestDMLDelete tests DELETE variations
func TestDMLDelete(t *testing.T) {
	// Create and populate test table
	setupDeleteTable := func() {
		mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_delete_test")
		mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_delete_test (id INTEGER, name TEXT, status TEXT)")
		mustExec(t, testHarness.DuckgresDB, `
			INSERT INTO dml_delete_test VALUES
			(1, 'Alice', 'active'),
			(2, 'Bob', 'active'),
			(3, 'Charlie', 'inactive'),
			(4, 'Diana', 'active'),
			(5, 'Eve', 'inactive')
		`)
	}

	t.Run("delete_single_row", func(t *testing.T) {
		setupDeleteTable()
		result, err := testHarness.DuckgresDB.Exec("DELETE FROM dml_delete_test WHERE id = 1")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		affected, _ := result.RowsAffected()
		if affected != 1 {
			t.Errorf("Expected 1 row affected, got %d", affected)
		}
	})

	t.Run("delete_multiple_rows", func(t *testing.T) {
		setupDeleteTable()
		result, err := testHarness.DuckgresDB.Exec("DELETE FROM dml_delete_test WHERE status = 'inactive'")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		affected, _ := result.RowsAffected()
		if affected != 2 {
			t.Errorf("Expected 2 rows affected, got %d", affected)
		}
	})

	t.Run("delete_with_subquery", func(t *testing.T) {
		setupDeleteTable()
		_, err := testHarness.DuckgresDB.Exec("DELETE FROM dml_delete_test WHERE id IN (SELECT id FROM dml_delete_test WHERE status = 'inactive')")
		if err != nil {
			t.Fatalf("Delete with subquery failed: %v", err)
		}
	})

	t.Run("delete_all", func(t *testing.T) {
		setupDeleteTable()
		result, err := testHarness.DuckgresDB.Exec("DELETE FROM dml_delete_test")
		if err != nil {
			t.Fatalf("Delete all failed: %v", err)
		}
		affected, _ := result.RowsAffected()
		if affected != 5 {
			t.Errorf("Expected 5 rows affected, got %d", affected)
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_delete_test")
}

// TestDMLDeleteReturning tests DELETE ... RETURNING
func TestDMLDeleteReturning(t *testing.T) {
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_delete_returning (id INTEGER, name TEXT)")
	mustExec(t, testHarness.DuckgresDB, "INSERT INTO dml_delete_returning VALUES (1, 'Test'), (2, 'Test2')")

	t.Run("delete_returning_star", func(t *testing.T) {
		skipIfKnown(t)
		rows, err := testHarness.DuckgresDB.Query("DELETE FROM dml_delete_returning WHERE id = 1 RETURNING *")
		if err != nil {
			t.Fatalf("Delete RETURNING failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		if !rows.Next() {
			t.Error("Expected 1 row returned")
		}
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if id != 1 || name != "Test" {
			t.Errorf("Unexpected values: id=%d, name=%s", id, name)
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_delete_returning")
}

// TestDMLDeleteUsing tests DELETE ... USING (join-style delete)
func TestDMLDeleteUsing(t *testing.T) {
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_delete_main (id INTEGER, status TEXT)")
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_delete_filter (id INTEGER)")
	mustExec(t, testHarness.DuckgresDB, "INSERT INTO dml_delete_main VALUES (1, 'keep'), (2, 'delete'), (3, 'keep'), (4, 'delete')")
	mustExec(t, testHarness.DuckgresDB, "INSERT INTO dml_delete_filter VALUES (2), (4)")

	t.Run("delete_using", func(t *testing.T) {
		_, err := testHarness.DuckgresDB.Exec(`
			DELETE FROM dml_delete_main
			USING dml_delete_filter
			WHERE dml_delete_main.id = dml_delete_filter.id
		`)
		if err != nil {
			t.Fatalf("Delete USING failed: %v", err)
		}
		// Verify
		result, _ := ExecuteQuery(testHarness.DuckgresDB, "SELECT COUNT(*) FROM dml_delete_main")
		if result.Rows[0][0].(int64) != 2 {
			t.Errorf("Expected 2 rows remaining, got %v", result.Rows[0][0])
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_delete_main")
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_delete_filter")
}

// TestDMLWithCTE tests DML with CTEs
func TestDMLWithCTE(t *testing.T) {
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_cte_source (id INTEGER, value INTEGER)")
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE dml_cte_target (id INTEGER, value INTEGER)")
	mustExec(t, testHarness.DuckgresDB, "INSERT INTO dml_cte_source VALUES (1, 100), (2, 200), (3, 300)")

	t.Run("insert_with_cte", func(t *testing.T) {
		_, err := testHarness.DuckgresDB.Exec(`
			WITH high_values AS (
				SELECT * FROM dml_cte_source WHERE value > 150
			)
			INSERT INTO dml_cte_target SELECT * FROM high_values
		`)
		if err != nil {
			t.Fatalf("Insert with CTE failed: %v", err)
		}
		// Verify
		result, _ := ExecuteQuery(testHarness.DuckgresDB, "SELECT COUNT(*) FROM dml_cte_target")
		if result.Rows[0][0].(int64) != 2 {
			t.Errorf("Expected 2 rows inserted, got %v", result.Rows[0][0])
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_cte_source")
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS dml_cte_target")
}
