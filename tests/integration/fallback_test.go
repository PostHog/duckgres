package integration

import (
	"testing"
)

// TestFallbackToNativeDuckDB tests that DuckDB-specific syntax that isn't valid
// PostgreSQL is automatically detected and executed via the native DuckDB fallback.
// This is the "two-tier query pathway" feature.
func TestFallbackToNativeDuckDB(t *testing.T) {
	// These tests only run on Duckgres since the syntax is DuckDB-specific
	// and won't work on PostgreSQL
	tests := []QueryTest{
		// DuckDB DESCRIBE statement
		{
			Name:         "describe_select",
			Query:        "DESCRIBE SELECT 1 AS num",
			DuckgresOnly: true,
		},
		// DuckDB SUMMARIZE statement
		{
			Name:         "summarize_users",
			Query:        "SUMMARIZE SELECT * FROM users",
			DuckgresOnly: true,
		},
		// DuckDB FROM-first syntax
		{
			Name:         "from_first_select",
			Query:        "FROM users SELECT name LIMIT 3",
			DuckgresOnly: true,
		},
		// DuckDB database functions
		{
			Name:         "duckdb_databases",
			Query:        "SELECT * FROM duckdb_databases()",
			DuckgresOnly: true,
		},
		// DuckDB pragma as table-function (some pragmas support this)
		{
			Name:         "pragma_table_info",
			Query:        "SELECT * FROM pragma_table_info('users')",
			DuckgresOnly: true,
		},
		// DuckDB tables/views function
		{
			Name:         "duckdb_tables",
			Query:        "SELECT * FROM duckdb_tables() LIMIT 3",
			DuckgresOnly: true,
		},
		// DuckDB SELECT with EXCLUDE
		{
			Name:         "select_exclude",
			Query:        "SELECT * EXCLUDE (email) FROM users LIMIT 3",
			DuckgresOnly: true,
		},
		// DuckDB SELECT with REPLACE
		{
			Name:         "select_replace",
			Query:        "SELECT * REPLACE (upper(name) AS name) FROM users LIMIT 3",
			DuckgresOnly: true,
		},
		// DuckDB QUALIFY clause
		{
			Name:         "qualify_row_number",
			Query:        "SELECT id, name FROM users QUALIFY row_number() OVER (ORDER BY id) <= 3",
			DuckgresOnly: true,
		},
		// DuckDB struct syntax
		{
			Name:         "struct_pack",
			Query:        "SELECT struct_pack(a := 1, b := 'hello') AS s",
			DuckgresOnly: true,
		},
		// DuckDB list comprehension
		{
			Name:         "list_transform",
			Query:        "SELECT list_transform([1, 2, 3], x -> x * 2) AS doubled",
			DuckgresOnly: true,
		},
		// DuckDB list filter
		{
			Name:         "list_filter",
			Query:        "SELECT list_filter([1, 2, 3, 4, 5], x -> x > 2) AS filtered",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestFallbackExtensionCommands tests DuckDB extension management commands
// that aren't valid PostgreSQL syntax.
func TestFallbackExtensionCommands(t *testing.T) {
	tests := []QueryTest{
		// Note: INSTALL and LOAD may fail if extension isn't available,
		// but the point is that the syntax is accepted and passed to DuckDB
		{
			Name:         "select_installed_extensions",
			Query:        "SELECT extension_name, installed FROM duckdb_extensions() WHERE installed = true LIMIT 5",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestFallbackComplexDuckDBSyntax tests more complex DuckDB-specific syntax
func TestFallbackComplexDuckDBSyntax(t *testing.T) {
	tests := []QueryTest{
		// DuckDB positional join
		{
			Name:         "positional_join",
			Query:        "SELECT * FROM (SELECT 1 AS a, 2 AS b) t1 POSITIONAL JOIN (SELECT 3 AS c, 4 AS d) t2",
			DuckgresOnly: true,
		},
		// DuckDB ASOF join (requires proper timestamp/ordering columns)
		{
			Name:         "asof_join_basic",
			Query:        "SELECT * FROM (SELECT 1 AS id, 100 AS ts) t1 ASOF JOIN (SELECT 1 AS id, 50 AS ts) t2 ON t1.id = t2.id AND t1.ts >= t2.ts",
			DuckgresOnly: true,
		},
		// DuckDB COLUMNS expression
		{
			Name:         "columns_expression",
			Query:        "SELECT COLUMNS('id|name') FROM users LIMIT 1",
			DuckgresOnly: true,
		},
		// DuckDB sample
		{
			Name:         "sample_rows",
			Query:        "SELECT * FROM users USING SAMPLE 3 ROWS",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestFallbackInvalidSyntax tests that truly invalid syntax still returns errors
func TestFallbackInvalidSyntax(t *testing.T) {
	tests := []QueryTest{
		// Completely invalid syntax should fail
		{
			Name:         "gibberish",
			Query:        "BLAHBLAH NOTASYNTAX 123",
			DuckgresOnly: true,
			ExpectError:  true,
		},
		{
			Name:         "incomplete_select",
			Query:        "SELECT FROM WHERE",
			DuckgresOnly: true,
			ExpectError:  true,
		},
		{
			Name:         "invalid_keyword_order",
			Query:        "FROM ORDER BY GROUP",
			DuckgresOnly: true,
			ExpectError:  true,
		},
	}
	runQueryTests(t, tests)
}

// TestFallbackWithValidPostgreSQL ensures valid PostgreSQL still goes through
// the normal transpilation path (no fallback)
func TestFallbackWithValidPostgreSQL(t *testing.T) {
	// These should work on both PostgreSQL and Duckgres via transpilation
	tests := []QueryTest{
		{
			Name:  "select_simple",
			Query: "SELECT 1 AS num",
		},
		{
			Name:  "select_from_users",
			Query: "SELECT id, name FROM users LIMIT 5",
		},
		{
			Name:  "select_with_where",
			Query: "SELECT * FROM users WHERE id = 1",
		},
		{
			Name:  "insert_select",
			Query: "SELECT 'test'::text AS name",
		},
		{
			Name:  "cte_query",
			Query: "WITH nums AS (SELECT generate_series AS n FROM generate_series(1,3)) SELECT * FROM nums",
		},
	}
	runQueryTests(t, tests)
}

// TestFallbackPreparedStatement tests that DuckDB-specific syntax works
// with the extended query protocol (prepared statements)
func TestFallbackPreparedStatement(t *testing.T) {
	if testHarness == nil || testHarness.DuckgresDB == nil {
		t.Skip("Test harness not available")
	}

	db := testHarness.DuckgresDB

	t.Run("describe_simple", func(t *testing.T) {
		skipIfKnown(t)
		// Execute a DESCRIBE statement (no parameters)
		rows, err := db.Query("DESCRIBE SELECT 1 AS num, 'hello' AS str")
		if err != nil {
			t.Fatalf("Failed to execute DESCRIBE: %v", err)
		}
		defer func() { _ = rows.Close() }()

		// Just verify we got some results
		if !rows.Next() {
			t.Error("Expected at least one row from DESCRIBE")
		}
	})

	t.Run("pragma_table_info", func(t *testing.T) {
		skipIfKnown(t)
		// Use function-style PRAGMA which returns a table
		rows, err := db.Query("SELECT * FROM pragma_table_info('users')")
		if err != nil {
			t.Fatalf("Failed to execute pragma_table_info: %v", err)
		}
		defer func() { _ = rows.Close() }()

		if !rows.Next() {
			t.Error("Expected at least one row from pragma_table_info")
		}
	})

	t.Run("from_first_syntax_no_params", func(t *testing.T) {
		skipIfKnown(t)
		// Test FROM-first syntax without parameters
		// Note: Prepared statements with parameters don't work with fallback
		// because EXPLAIN validation can't handle unbound parameters
		rows, err := db.Query("FROM users SELECT name LIMIT 1")
		if err != nil {
			t.Fatalf("Failed to execute FROM-first query: %v", err)
		}
		defer func() { _ = rows.Close() }()
		// Query executed successfully, that's the main test
	})
}

// TestFallbackUtilityCommands tests DuckDB utility commands that don't support EXPLAIN
func TestFallbackUtilityCommands(t *testing.T) {
	if testHarness == nil || testHarness.DuckgresDB == nil {
		t.Skip("Test harness not available")
	}

	db := testHarness.DuckgresDB

	t.Run("attach_detach", func(t *testing.T) {
		skipIfKnown(t)

		// ATTACH a memory database
		_, err := db.Exec("ATTACH ':memory:' AS util_test_db")
		if err != nil {
			t.Fatalf("ATTACH failed: %v", err)
		}

		// Create a table in the attached database
		_, err = db.Exec("CREATE TABLE util_test_db.test_tbl (id INT)")
		if err != nil {
			t.Fatalf("CREATE TABLE in attached db failed: %v", err)
		}

		// Query the table
		var count int
		err = db.QueryRow("SELECT count(*) FROM util_test_db.test_tbl").Scan(&count)
		if err != nil {
			t.Fatalf("Query attached db failed: %v", err)
		}

		// DETACH the database
		_, err = db.Exec("DETACH util_test_db")
		if err != nil {
			t.Fatalf("DETACH failed: %v", err)
		}
	})

	// Note: CREATE/DROP SECRET tests are skipped in integration tests
	// because DuckLake mode creates its own S3 secret which can cause conflicts.
	// These commands work correctly - verified manually with:
	//   CREATE SECRET test (TYPE S3, KEY_ID 'x', SECRET 'y', REGION 'us-east-1')
	//   DROP SECRET test

	t.Run("use_database", func(t *testing.T) {
		skipIfKnown(t)

		// Get current database
		var currentDb string
		err := db.QueryRow("SELECT current_database()").Scan(&currentDb)
		if err != nil {
			t.Fatalf("Get current database failed: %v", err)
		}

		// Switch to memory database (always available)
		_, err = db.Exec("USE memory")
		if err != nil {
			t.Fatalf("USE memory failed: %v", err)
		}

		// Verify we switched
		var newDb string
		err = db.QueryRow("SELECT current_database()").Scan(&newDb)
		if err != nil {
			t.Fatalf("Get new database failed: %v", err)
		}

		if newDb != "memory" {
			t.Errorf("Expected database 'memory', got %q", newDb)
		}

		// Switch back to original database
		_, err = db.Exec("USE " + currentDb)
		if err != nil {
			t.Fatalf("USE original failed: %v", err)
		}
	})

	t.Run("set_reset", func(t *testing.T) {
		skipIfKnown(t)

		// DuckDB SET command - use a safe setting
		_, err := db.Exec("SET memory_limit = '2GB'")
		if err != nil {
			t.Fatalf("SET failed: %v", err)
		}

		// RESET command
		_, err = db.Exec("RESET memory_limit")
		if err != nil {
			t.Fatalf("RESET failed: %v", err)
		}
	})
}

// TestFallbackWithComments tests that DuckDB-specific syntax works
// when queries have leading comments
func TestFallbackWithComments(t *testing.T) {
	tests := []QueryTest{
		// Block comment before DESCRIBE
		{
			Name:         "block_comment_describe",
			Query:        "/* comment */ DESCRIBE SELECT 1 AS num",
			DuckgresOnly: true,
		},
		// Line comment before FROM-first
		{
			Name:         "line_comment_from_first",
			Query:        "-- get users\nFROM users SELECT name LIMIT 1",
			DuckgresOnly: true,
		},
		// Multi-line block comment before SUMMARIZE
		{
			Name:         "multiline_comment_summarize",
			Query:        "/* multi\n   line\n   comment */ SUMMARIZE SELECT 1, 2, 3",
			DuckgresOnly: true,
		},
		// Multiple comments before utility command
		{
			Name:         "multiple_comments",
			Query:        "/* first */ -- second\n/* third */ DESCRIBE SELECT 'test'::text",
			DuckgresOnly: true,
		},
		// Block comment before SELECT EXCLUDE
		{
			Name:         "comment_select_exclude",
			Query:        "/* exclude email */ SELECT * EXCLUDE (email) FROM users LIMIT 1",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestFallbackUtilityWithComments tests utility commands with leading comments
func TestFallbackUtilityWithComments(t *testing.T) {
	if testHarness == nil || testHarness.DuckgresDB == nil {
		t.Skip("Test harness not available")
	}

	db := testHarness.DuckgresDB

	t.Run("attach_with_comment", func(t *testing.T) {
		skipIfKnown(t)

		// ATTACH with block comment
		_, err := db.Exec("/* attach test db */ ATTACH ':memory:' AS comment_attach_test")
		if err != nil {
			t.Fatalf("ATTACH with comment failed: %v", err)
		}

		// USE with line comment
		_, err = db.Exec("-- switch db\nUSE comment_attach_test")
		if err != nil {
			t.Fatalf("USE with comment failed: %v", err)
		}

		// Verify we switched
		var currentDb string
		err = db.QueryRow("SELECT current_database()").Scan(&currentDb)
		if err != nil {
			t.Fatalf("Get current database failed: %v", err)
		}
		if currentDb != "comment_attach_test" {
			t.Errorf("Expected 'comment_attach_test', got %q", currentDb)
		}

		// Switch back and detach
		_, err = db.Exec("/* back to ducklake */ USE ducklake")
		if err != nil {
			t.Fatalf("USE ducklake failed: %v", err)
		}

		_, err = db.Exec("/* cleanup */ DETACH comment_attach_test")
		if err != nil {
			t.Fatalf("DETACH with comment failed: %v", err)
		}
	})

	t.Run("set_with_multiline_comment", func(t *testing.T) {
		skipIfKnown(t)

		// SET with multi-line comment
		_, err := db.Exec("/* configure\n   memory\n   limit */ SET memory_limit = '1GB'")
		if err != nil {
			t.Fatalf("SET with multiline comment failed: %v", err)
		}

		// RESET with comment
		_, err = db.Exec("-- reset to default\nRESET memory_limit")
		if err != nil {
			t.Fatalf("RESET with comment failed: %v", err)
		}
	})
}

// TestFallbackMixedSession tests that switching between PostgreSQL-valid
// and DuckDB-specific queries in the same session works correctly
func TestFallbackMixedSession(t *testing.T) {
	if testHarness == nil || testHarness.DuckgresDB == nil {
		t.Skip("Test harness not available")
	}

	db := testHarness.DuckgresDB

	t.Run("alternate_pg_and_duckdb_syntax", func(t *testing.T) {
		skipIfKnown(t)

		// 1. Standard PostgreSQL query
		_, err := db.Exec("SELECT 1")
		if err != nil {
			t.Fatalf("Standard SELECT failed: %v", err)
		}

		// 2. DuckDB-specific DESCRIBE
		rows, err := db.Query("DESCRIBE SELECT 1 AS num")
		if err != nil {
			t.Fatalf("DESCRIBE failed: %v", err)
		}
		rows.Close()

		// 3. Another standard PostgreSQL query
		_, err = db.Exec("SELECT * FROM users LIMIT 1")
		if err != nil {
			t.Fatalf("Second standard SELECT failed: %v", err)
		}

		// 4. DuckDB-specific FROM-first syntax
		rows, err = db.Query("FROM users SELECT name LIMIT 1")
		if err != nil {
			t.Fatalf("FROM-first query failed: %v", err)
		}
		rows.Close()

		// 5. PostgreSQL-style type cast
		var result string
		err = db.QueryRow("SELECT 'hello'::text").Scan(&result)
		if err != nil {
			t.Fatalf("Type cast query failed: %v", err)
		}
		if result != "hello" {
			t.Errorf("Expected 'hello', got %q", result)
		}

		// 6. Another DuckDB-specific query (SUMMARIZE)
		rows, err = db.Query("SUMMARIZE SELECT * FROM users LIMIT 1")
		if err != nil {
			t.Fatalf("SUMMARIZE failed: %v", err)
		}
		rows.Close()
	})
}
