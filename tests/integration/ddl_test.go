package integration

import (
	"testing"
)

// TestDDLCreateTable tests CREATE TABLE variations
func TestDDLCreateTable(t *testing.T) {
	tests := []QueryTest{
		// Basic table creation
		{
			Name:         "create_table_basic",
			Query:        "CREATE TABLE ddl_test_basic (id INTEGER, name TEXT)",
			DuckgresOnly: true,
		},
		{
			Name:         "create_table_if_not_exists",
			Query:        "CREATE TABLE IF NOT EXISTS ddl_test_basic (id INTEGER, name TEXT)",
			DuckgresOnly: true,
		},

		// With data types
		{
			Name: "create_table_types",
			Query: `CREATE TABLE ddl_test_types (
				bool_col BOOLEAN,
				int2_col SMALLINT,
				int4_col INTEGER,
				int8_col BIGINT,
				float4_col REAL,
				float8_col DOUBLE PRECISION,
				numeric_col NUMERIC(10, 2),
				text_col TEXT,
				varchar_col VARCHAR(255),
				date_col DATE,
				time_col TIME,
				timestamp_col TIMESTAMP,
				uuid_col UUID,
				json_col JSON
			)`,
			DuckgresOnly: true,
		},

		// With constraints (note: some may be no-ops in DuckLake mode)
		{
			Name: "create_table_primary_key",
			Query: `CREATE TABLE ddl_test_pk (
				id INTEGER PRIMARY KEY,
				name TEXT
			)`,
			DuckgresOnly: true,
		},
		{
			Name: "create_table_not_null",
			Query: `CREATE TABLE ddl_test_notnull (
				id INTEGER NOT NULL,
				name TEXT NOT NULL
			)`,
			DuckgresOnly: true,
		},
		{
			Name: "create_table_default",
			Query: `CREATE TABLE ddl_test_default (
				id INTEGER,
				status TEXT DEFAULT 'pending',
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			)`,
			DuckgresOnly: true,
		},
		{
			Name: "create_table_unique",
			Query: `CREATE TABLE ddl_test_unique (
				id INTEGER,
				email TEXT UNIQUE
			)`,
			DuckgresOnly: true,
		},

		// CREATE TABLE AS
		{
			Name:         "create_table_as_select",
			Query:        "CREATE TABLE ddl_test_as AS SELECT id, name, active FROM users WHERE active = true",
			DuckgresOnly: true,
		},

		// Temporary tables
		{
			Name:         "create_temp_table",
			Query:        "CREATE TEMP TABLE ddl_test_temp (id INTEGER, name TEXT)",
			DuckgresOnly: true,
		},
		{
			Name:         "create_temporary_table",
			Query:        "CREATE TEMPORARY TABLE ddl_test_temporary (id INTEGER, name TEXT)",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)

	// Cleanup
	cleanupDDLTables(t)
}

// TestDDLAlterTable tests ALTER TABLE variations
func TestDDLAlterTable(t *testing.T) {
	// Create a table to alter
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE ddl_alter_test (id INTEGER, name TEXT, old_col TEXT)")

	tests := []QueryTest{
		// Add column
		{
			Name:         "alter_add_column",
			Query:        "ALTER TABLE ddl_alter_test ADD COLUMN new_col TEXT",
			DuckgresOnly: true,
		},
		{
			Name:         "alter_add_column_default",
			Query:        "ALTER TABLE ddl_alter_test ADD COLUMN with_default INTEGER DEFAULT 0",
			DuckgresOnly: true,
		},

		// Drop column
		{
			Name:         "alter_drop_column",
			Query:        "ALTER TABLE ddl_alter_test DROP COLUMN old_col",
			DuckgresOnly: true,
		},
		{
			Name:         "alter_drop_column_if_exists",
			Query:        "ALTER TABLE ddl_alter_test DROP COLUMN IF EXISTS nonexistent",
			DuckgresOnly: true,
		},

		// Rename column
		{
			Name:         "alter_rename_column",
			Query:        "ALTER TABLE ddl_alter_test RENAME COLUMN name TO full_name",
			DuckgresOnly: true,
		},

		// Rename table
		{
			Name:         "alter_rename_table",
			Query:        "ALTER TABLE ddl_alter_test RENAME TO ddl_alter_renamed",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS ddl_alter_renamed")
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS ddl_alter_test")
}

// TestDDLDropTable tests DROP TABLE variations
func TestDDLDropTable(t *testing.T) {
	// Create tables to drop
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE ddl_drop_test1 (id INTEGER)")
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE ddl_drop_test2 (id INTEGER)")
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE ddl_drop_test3 (id INTEGER)")

	tests := []QueryTest{
		{
			Name:         "drop_table",
			Query:        "DROP TABLE ddl_drop_test1",
			DuckgresOnly: true,
		},
		{
			Name:         "drop_table_if_exists",
			Query:        "DROP TABLE IF EXISTS ddl_drop_test1",
			DuckgresOnly: true,
		},
		{
			Name:         "drop_table_cascade",
			Query:        "DROP TABLE ddl_drop_test2 CASCADE",
			DuckgresOnly: true,
		},
		{
			Name:         "drop_table_restrict",
			Query:        "DROP TABLE ddl_drop_test3 RESTRICT",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestDDLViews tests CREATE/DROP VIEW
func TestDDLViews(t *testing.T) {
	tests := []QueryTest{
		// Create view
		{
			Name:         "create_view",
			Query:        "CREATE VIEW ddl_view_test AS SELECT id, name FROM users WHERE active = true",
			DuckgresOnly: true,
		},
		{
			Name:         "create_or_replace_view",
			Query:        "CREATE OR REPLACE VIEW ddl_view_test AS SELECT id, name, email FROM users WHERE active = true",
			DuckgresOnly: true,
		},
		{
			Name:         "create_view_with_columns",
			Query:        "CREATE VIEW ddl_view_cols (user_id, user_name) AS SELECT id, name FROM users",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)

	// Test that views are queryable
	t.Run("query_created_view", func(t *testing.T) {
		result, err := ExecuteQuery(testHarness.DuckgresDB, "SELECT * FROM ddl_view_test LIMIT 5")
		if err != nil {
			t.Fatalf("Failed to query view: %v", err)
		}
		if result.Error != nil {
			t.Fatalf("Query error: %v", result.Error)
		}
		if len(result.Columns) < 2 {
			t.Errorf("Expected at least 2 columns, got %d", len(result.Columns))
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP VIEW IF EXISTS ddl_view_test")
	mustExec(t, testHarness.DuckgresDB, "DROP VIEW IF EXISTS ddl_view_cols")
}

// TestDDLIndexes tests CREATE/DROP INDEX (may be no-ops)
func TestDDLIndexes(t *testing.T) {
	// Create a test table
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE ddl_index_test (id INTEGER, name TEXT, email TEXT)")

	tests := []QueryTest{
		{
			Name:         "create_index",
			Query:        "CREATE INDEX ddl_idx_name ON ddl_index_test (name)",
			DuckgresOnly: true,
		},
		{
			Name:         "create_index_multiple_cols",
			Query:        "CREATE INDEX ddl_idx_multi ON ddl_index_test (name, email)",
			DuckgresOnly: true,
		},
		{
			Name:         "create_unique_index",
			Query:        "CREATE UNIQUE INDEX ddl_idx_unique ON ddl_index_test (email)",
			DuckgresOnly: true,
		},
		{
			Name:         "create_index_if_not_exists",
			Query:        "CREATE INDEX IF NOT EXISTS ddl_idx_name ON ddl_index_test (name)",
			DuckgresOnly: true,
		},
		{
			Name:         "drop_index",
			Query:        "DROP INDEX IF EXISTS ddl_idx_name",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS ddl_index_test CASCADE")
}

// TestDDLSchemas tests CREATE/DROP SCHEMA
func TestDDLSchemas(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "create_schema",
			Query:        "CREATE SCHEMA ddl_schema_test",
			DuckgresOnly: true,
		},
		{
			Name:         "create_schema_if_not_exists",
			Query:        "CREATE SCHEMA IF NOT EXISTS ddl_schema_test",
			DuckgresOnly: true,
		},
		{
			Name:         "drop_schema",
			Query:        "DROP SCHEMA ddl_schema_test",
			DuckgresOnly: true,
		},
		{
			Name:         "drop_schema_if_exists",
			Query:        "DROP SCHEMA IF EXISTS ddl_schema_test",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestDDLTruncate tests TRUNCATE
func TestDDLTruncate(t *testing.T) {
	// Create and populate a test table
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE ddl_truncate_test (id INTEGER, name TEXT)")
	mustExec(t, testHarness.DuckgresDB, "INSERT INTO ddl_truncate_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	// Verify data exists
	t.Run("verify_data_before_truncate", func(t *testing.T) {
		result, _ := ExecuteQuery(testHarness.DuckgresDB, "SELECT COUNT(*) FROM ddl_truncate_test")
		if len(result.Rows) != 1 || result.Rows[0][0].(int64) != 3 {
			t.Errorf("Expected 3 rows before truncate")
		}
	})

	tests := []QueryTest{
		{
			Name:         "truncate",
			Query:        "TRUNCATE TABLE ddl_truncate_test",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)

	// Verify data is gone
	t.Run("verify_data_after_truncate", func(t *testing.T) {
		result, _ := ExecuteQuery(testHarness.DuckgresDB, "SELECT COUNT(*) FROM ddl_truncate_test")
		if len(result.Rows) != 1 || result.Rows[0][0].(int64) != 0 {
			t.Errorf("Expected 0 rows after truncate")
		}
	})

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS ddl_truncate_test")
}

// TestDDLComment tests COMMENT ON
func TestDDLComment(t *testing.T) {
	// Create test objects
	mustExec(t, testHarness.DuckgresDB, "CREATE TABLE ddl_comment_test (id INTEGER, name TEXT)")

	tests := []QueryTest{
		{
			Name:         "comment_on_table",
			Query:        "COMMENT ON TABLE ddl_comment_test IS 'Test table for comments'",
			DuckgresOnly: true,
		},
		{
			Name:         "comment_on_column",
			Query:        "COMMENT ON COLUMN ddl_comment_test.name IS 'User name'",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS ddl_comment_test")
}

// TestDDLConstraints tests constraint-related DDL
func TestDDLConstraints(t *testing.T) {
	tests := []QueryTest{
		// Table-level constraints
		{
			Name: "create_table_pk_constraint",
			Query: `CREATE TABLE ddl_constraint_pk (
				id INTEGER,
				name TEXT,
				CONSTRAINT pk_id PRIMARY KEY (id)
			)`,
			DuckgresOnly: true,
		},
		{
			Name: "create_table_unique_constraint",
			Query: `CREATE TABLE ddl_constraint_unique (
				id INTEGER,
				email TEXT,
				CONSTRAINT uq_email UNIQUE (email)
			)`,
			DuckgresOnly: true,
		},
		{
			Name: "create_table_check_constraint",
			Query: `CREATE TABLE ddl_constraint_check (
				id INTEGER,
				age INTEGER,
				CONSTRAINT chk_age CHECK (age >= 0)
			)`,
			DuckgresOnly: true,
		},
		{
			Name: "create_table_foreign_key",
			Query: `CREATE TABLE ddl_constraint_fk (
				id INTEGER PRIMARY KEY,
				user_id INTEGER,
				CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)
			)`,
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)

	// Cleanup
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS ddl_constraint_fk")
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS ddl_constraint_check")
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS ddl_constraint_unique")
	mustExec(t, testHarness.DuckgresDB, "DROP TABLE IF EXISTS ddl_constraint_pk")
}

// Helper to cleanup DDL test tables
func cleanupDDLTables(t *testing.T) {
	tables := []string{
		"ddl_test_basic",
		"ddl_test_types",
		"ddl_test_pk",
		"ddl_test_notnull",
		"ddl_test_default",
		"ddl_test_unique",
		"ddl_test_as",
		"ddl_test_temp",
		"ddl_test_temporary",
	}
	for _, table := range tables {
		_, _ = testHarness.DuckgresDB.Exec("DROP TABLE IF EXISTS " + table)
	}
}
