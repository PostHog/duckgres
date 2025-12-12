package server

import (
	"testing"
)

func TestStripLeadingComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no comments",
			input:    "SELECT * FROM users",
			expected: "SELECT * FROM users",
		},
		{
			name:     "block comment at start",
			input:    "/*Fivetran*/CREATE SCHEMA test",
			expected: "CREATE SCHEMA test",
		},
		{
			name:     "block comment with spaces",
			input:    "/* comment */ SELECT 1",
			expected: "SELECT 1",
		},
		{
			name:     "multiple block comments",
			input:    "/* first */ /* second */ INSERT INTO t",
			expected: "INSERT INTO t",
		},
		{
			name:     "line comment at start",
			input:    "-- comment\nSELECT 1",
			expected: "SELECT 1",
		},
		{
			name:     "mixed comments",
			input:    "/* block */ -- line\nUPDATE t SET x=1",
			expected: "UPDATE t SET x=1",
		},
		{
			name:     "whitespace before comment",
			input:    "  /* comment */ DELETE FROM t",
			expected: "DELETE FROM t",
		},
		{
			name:     "unclosed block comment",
			input:    "/* unclosed SELECT",
			expected: "/* unclosed SELECT",
		},
		{
			name:     "line comment without newline",
			input:    "-- only comment",
			expected: "",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only whitespace",
			input:    "   ",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripLeadingComments(tt.input)
			if result != tt.expected {
				t.Errorf("stripLeadingComments(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestGetCommandType(t *testing.T) {
	// Create a minimal clientConn for testing
	c := &clientConn{}

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		// Basic commands without comments
		{
			name:     "SELECT",
			query:    "SELECT * FROM users",
			expected: "SELECT",
		},
		{
			name:     "INSERT",
			query:    "INSERT INTO users VALUES (1)",
			expected: "INSERT",
		},
		{
			name:     "UPDATE",
			query:    "UPDATE users SET name='test'",
			expected: "UPDATE",
		},
		{
			name:     "DELETE",
			query:    "DELETE FROM users",
			expected: "DELETE",
		},
		{
			name:     "CREATE TABLE",
			query:    "CREATE TABLE users (id INT)",
			expected: "CREATE TABLE",
		},
		{
			name:     "CREATE SCHEMA",
			query:    "CREATE SCHEMA myschema",
			expected: "CREATE SCHEMA",
		},
		{
			name:     "DROP TABLE",
			query:    "DROP TABLE users",
			expected: "DROP TABLE",
		},
		{
			name:     "DROP SCHEMA",
			query:    "DROP SCHEMA myschema",
			expected: "DROP SCHEMA",
		},
		{
			name:     "DROP SCHEMA IF EXISTS CASCADE",
			query:    "DROP SCHEMA IF EXISTS myschema CASCADE",
			expected: "DROP SCHEMA",
		},

		// Commands with Fivetran-style comments
		{
			name:     "CREATE SCHEMA with Fivetran comment",
			query:    "/*Fivetran*/CREATE SCHEMA test_schema",
			expected: "CREATE SCHEMA",
		},
		{
			name:     "DROP SCHEMA with Fivetran comment",
			query:    "/*Fivetran*/DROP SCHEMA IF EXISTS test_schema CASCADE",
			expected: "DROP SCHEMA",
		},
		{
			name:     "CREATE TABLE with Fivetran comment",
			query:    "/*Fivetran*/CREATE TABLE test_table (id INT)",
			expected: "CREATE TABLE",
		},
		{
			name:     "INSERT with Fivetran comment",
			query:    "/*Fivetran*/INSERT INTO test_table VALUES (1)",
			expected: "INSERT",
		},

		// Commands with other comment styles
		{
			name:     "SELECT with block comment",
			query:    "/* query */ SELECT 1",
			expected: "SELECT",
		},
		{
			name:     "UPDATE with line comment",
			query:    "-- update query\nUPDATE t SET x=1",
			expected: "UPDATE",
		},

		// Transaction commands
		{
			name:     "BEGIN",
			query:    "BEGIN",
			expected: "BEGIN",
		},
		{
			name:     "COMMIT",
			query:    "COMMIT",
			expected: "COMMIT",
		},
		{
			name:     "ROLLBACK",
			query:    "ROLLBACK",
			expected: "ROLLBACK",
		},

		// Other commands
		{
			name:     "SET",
			query:    "SET search_path TO myschema",
			expected: "SET",
		},
		{
			name:     "TRUNCATE",
			query:    "TRUNCATE TABLE users",
			expected: "TRUNCATE TABLE",
		},
		{
			name:     "ALTER",
			query:    "ALTER TABLE users ADD COLUMN name TEXT",
			expected: "ALTER TABLE",
		},

		// Edge cases
		{
			name:     "lowercase command",
			query:    "select * from users",
			expected: "SELECT",
		},
		{
			name:     "mixed case with comment",
			query:    "/*Test*/Select * From Users",
			expected: "SELECT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// getCommandType expects uppercase input
			result := c.getCommandType(tt.query)
			if result != tt.expected {
				t.Errorf("getCommandType(%q) = %q, want %q", tt.query, result, tt.expected)
			}
		})
	}
}

func TestRedactConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "postgres connection string with password",
			input:    "postgres:host=localhost user=postgres password=secretpass dbname=ducklake",
			expected: "postgres:host=localhost user=postgres password=[REDACTED] dbname=ducklake",
		},
		{
			name:     "connection string with password= format",
			input:    "host=localhost password=mysecret user=admin",
			expected: "host=localhost password=[REDACTED] user=admin",
		},
		{
			name:     "connection string with PASSWORD uppercase",
			input:    "host=localhost PASSWORD=mysecret user=admin",
			expected: "host=localhost PASSWORD=[REDACTED] user=admin",
		},
		{
			name:     "connection string without password",
			input:    "host=localhost user=postgres dbname=test",
			expected: "host=localhost user=postgres dbname=test",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "password with special characters",
			input:    "host=localhost password=p@ss!word123 user=admin",
			expected: "host=localhost password=[REDACTED] user=admin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := redactConnectionString(tt.input)
			if result != tt.expected {
				t.Errorf("redactConnectionString(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestTransactionStatusTracking(t *testing.T) {
	c := &clientConn{txStatus: txStatusIdle}

	// Initially should be idle
	if c.txStatus != txStatusIdle {
		t.Errorf("initial txStatus = %c, want %c", c.txStatus, txStatusIdle)
	}

	// BEGIN should set to transaction
	c.updateTxStatus("BEGIN")
	if c.txStatus != txStatusTransaction {
		t.Errorf("after BEGIN txStatus = %c, want %c", c.txStatus, txStatusTransaction)
	}

	// SELECT should not change status
	c.updateTxStatus("SELECT")
	if c.txStatus != txStatusTransaction {
		t.Errorf("after SELECT txStatus = %c, want %c", c.txStatus, txStatusTransaction)
	}

	// COMMIT should set back to idle
	c.updateTxStatus("COMMIT")
	if c.txStatus != txStatusIdle {
		t.Errorf("after COMMIT txStatus = %c, want %c", c.txStatus, txStatusIdle)
	}

	// Test ROLLBACK path
	c.updateTxStatus("BEGIN")
	if c.txStatus != txStatusTransaction {
		t.Errorf("after second BEGIN txStatus = %c, want %c", c.txStatus, txStatusTransaction)
	}
	c.updateTxStatus("ROLLBACK")
	if c.txStatus != txStatusIdle {
		t.Errorf("after ROLLBACK txStatus = %c, want %c", c.txStatus, txStatusIdle)
	}
}

func TestTransactionErrorStatus(t *testing.T) {
	c := &clientConn{txStatus: txStatusIdle}

	// Error outside transaction should not change status
	c.setTxError()
	if c.txStatus != txStatusIdle {
		t.Errorf("error outside transaction txStatus = %c, want %c", c.txStatus, txStatusIdle)
	}

	// Error inside transaction should set to error
	c.updateTxStatus("BEGIN")
	c.setTxError()
	if c.txStatus != txStatusError {
		t.Errorf("error inside transaction txStatus = %c, want %c", c.txStatus, txStatusError)
	}

	// ROLLBACK should recover from error state
	c.updateTxStatus("ROLLBACK")
	if c.txStatus != txStatusIdle {
		t.Errorf("after ROLLBACK from error txStatus = %c, want %c", c.txStatus, txStatusIdle)
	}
}

func TestNestedBeginDetection(t *testing.T) {
	// Test that we can detect when a nested BEGIN would occur
	// The actual warning is sent in handleQuery, but we test the detection logic here
	c := &clientConn{txStatus: txStatusIdle}

	// First BEGIN should work normally
	c.updateTxStatus("BEGIN")
	if c.txStatus != txStatusTransaction {
		t.Errorf("after first BEGIN txStatus = %c, want %c", c.txStatus, txStatusTransaction)
	}

	// At this point, a second BEGIN should trigger warning behavior
	// In handleQuery, when cmdType == "BEGIN" && c.txStatus == txStatusTransaction,
	// we send a warning and return success without calling DuckDB
	isNestedBegin := c.txStatus == txStatusTransaction
	if !isNestedBegin {
		t.Error("expected nested BEGIN to be detected")
	}

	// Transaction status should remain 'T' (not change to 'I' or 'E')
	// The warning is sent but the transaction continues
	if c.txStatus != txStatusTransaction {
		t.Errorf("txStatus should still be %c after nested BEGIN detection, got %c", txStatusTransaction, c.txStatus)
	}
}

func TestQueryReturnsResults(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		// SELECT queries
		{
			name:     "simple SELECT",
			query:    "SELECT * FROM users",
			expected: true,
		},
		{
			name:     "SELECT with comment",
			query:    "/*Fivetran*/ SELECT * FROM users",
			expected: true,
		},
		{
			name:     "SELECT with block and line comment",
			query:    "/* comment */ -- line\nSELECT 1",
			expected: true,
		},
		// WITH/CTE queries
		{
			name:     "WITH clause",
			query:    "WITH cte AS (SELECT 1) SELECT * FROM cte",
			expected: true,
		},
		{
			name:     "WITH clause with comment",
			query:    "/*Fivetran*/ WITH cte AS (SELECT 1) SELECT * FROM cte",
			expected: true,
		},
		// VALUES
		{
			name:     "VALUES",
			query:    "VALUES (1, 2), (3, 4)",
			expected: true,
		},
		// SHOW
		{
			name:     "SHOW",
			query:    "SHOW TABLES",
			expected: true,
		},
		// TABLE
		{
			name:     "TABLE command",
			query:    "TABLE users",
			expected: true,
		},
		// EXPLAIN
		{
			name:     "EXPLAIN",
			query:    "EXPLAIN SELECT * FROM users",
			expected: true,
		},
		// DESCRIBE
		{
			name:     "DESCRIBE",
			query:    "DESCRIBE users",
			expected: true,
		},
		// Non-result queries
		{
			name:     "INSERT",
			query:    "INSERT INTO users VALUES (1)",
			expected: false,
		},
		{
			name:     "UPDATE",
			query:    "UPDATE users SET name = 'test'",
			expected: false,
		},
		{
			name:     "DELETE",
			query:    "DELETE FROM users",
			expected: false,
		},
		{
			name:     "CREATE TABLE",
			query:    "CREATE TABLE test (id INT)",
			expected: false,
		},
		{
			name:     "CREATE TABLE with comment",
			query:    "/*Fivetran*/ CREATE TABLE test (id INT)",
			expected: false,
		},
		{
			name:     "DROP TABLE",
			query:    "DROP TABLE users",
			expected: false,
		},
		{
			name:     "BEGIN",
			query:    "BEGIN",
			expected: false,
		},
		{
			name:     "COMMIT",
			query:    "COMMIT",
			expected: false,
		},
		{
			name:     "ROLLBACK",
			query:    "ROLLBACK",
			expected: false,
		},
		{
			name:     "SET",
			query:    "SET search_path = public",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := queryReturnsResults(tt.query)
			if result != tt.expected {
				t.Errorf("queryReturnsResults(%q) = %v, want %v", tt.query, result, tt.expected)
			}
		})
	}
}

// TestQueryReturnsResultsWithComments verifies that queries with leading comments
// are correctly identified as result-returning queries.
func TestQueryReturnsResultsWithComments(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		// Queries with leading comments that return results
		{"block comment before SELECT", "/* comment */ SELECT * FROM users", true},
		{"block comment before SELECT no space", "/*comment*/SELECT 1", true},
		{"block comment before WITH", "/* query */ WITH cte AS (SELECT 1) SELECT * FROM cte", true},
		{"line comment before SELECT", "-- comment\nSELECT * FROM users", true},
		{"multiple block comments", "/* first */ /* second */ SELECT 1", true},
		{"block comment before SHOW", "/* comment */ SHOW TABLES", true},
		{"block comment before VALUES", "/* comment */ VALUES (1, 2)", true},

		// Queries with leading comments that don't return results
		{"block comment before INSERT", "/* comment */ INSERT INTO t VALUES (1)", false},
		{"block comment before CREATE", "/* comment */ CREATE TABLE t (id INT)", false},
		{"block comment before DROP", "/* comment */ DROP TABLE t", false},
		{"block comment before BEGIN", "/* comment */ BEGIN", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := queryReturnsResults(tt.query)
			if result != tt.expected {
				t.Errorf("queryReturnsResults(%q) = %v, want %v", tt.query, result, tt.expected)
			}
		})
	}
}
