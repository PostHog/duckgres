package server

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func TestIsEmptyQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{"empty string", "", true},
		{"single semicolon", ";", true},
		{"multiple semicolons", ";;;", true},
		{"semicolons with spaces", "; ; ;", true},
		{"semicolons with tabs", ";\t;\t;", true},
		{"semicolons with newlines", ";\n;\n;", true},
		{"only whitespace", "   \t\n", true},
		{"line comment only", "-- ping", true},
		{"line comment with newline", "-- ping\n", true},
		{"block comment only", "/* comment */", true},
		{"block comment then semicolons", "/* comment */;", true},
		{"comment then query", "-- comment\nSELECT 1", false},
		{"block comment then query", "/* comment */SELECT 1", false},
		{"SELECT query", "SELECT 1", false},
		{"SELECT with semicolon", "SELECT 1;", false},
		{"semicolon then query", ";SELECT 1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isEmptyQuery(tt.query)
			if result != tt.expected {
				t.Errorf("isEmptyQuery(%q) = %v, want %v", tt.query, result, tt.expected)
			}
		})
	}
}

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

func TestStripLeadingNoise(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"no noise", "SELECT 1", "SELECT 1"},
		{"leading comment", "/* comment */ SELECT 1", "SELECT 1"},
		{"leading paren", "(SELECT 1)", "SELECT 1)"},
		{"paren then comment", "(/* comment */ SELECT 1)", "SELECT 1)"},
		{"comment then paren", "/* comment */ (SELECT 1)", "SELECT 1)"},
		{"nested parens and comments", "( ( /* comment */ SELECT 1 ) )", "SELECT 1 ) )"},
		{"paren comment paren", "(/* c1 */(/* c2 */ SELECT 1))", "SELECT 1))"},
		{"line comment then paren", "-- comment\n(SELECT 1)", "SELECT 1)"},
		{"paren then line comment", "( -- comment\nSELECT 1)", "SELECT 1)"},
		{"only noise", "( /* comment */ )", ")"},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripLeadingNoise(tt.input)
			if result != tt.expected {
				t.Errorf("stripLeadingNoise(%q) = %q, want %q", tt.input, result, tt.expected)
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
			name:     "CREATE TEMPORARY TABLE",
			query:    "CREATE TEMPORARY TABLE temp_users (id INT)",
			expected: "CREATE TABLE",
		},
		{
			name:     "CREATE TEMP TABLE",
			query:    "CREATE TEMP TABLE temp_users (id INT)",
			expected: "CREATE TABLE",
		},
		{
			name:     "CREATE TEMPORARY TABLE with Fivetran comment",
			query:    "/*Fivetran*/CREATE TEMPORARY TABLE temp_users (id INT)",
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

func TestIsConnectionBroken(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "SSL connection closed",
			err:      fmt.Errorf("SSL connection has been closed unexpectedly"),
			expected: true,
		},
		{
			name:     "connection refused",
			err:      fmt.Errorf("dial tcp: connection refused"),
			expected: true,
		},
		{
			name:     "broken pipe",
			err:      fmt.Errorf("write: broken pipe"),
			expected: true,
		},
		{
			name:     "connection reset",
			err:      fmt.Errorf("read: connection reset by peer"),
			expected: true,
		},
		{
			name:     "network unreachable",
			err:      fmt.Errorf("network is unreachable"),
			expected: true,
		},
		{
			name:     "no route to host",
			err:      fmt.Errorf("no route to host"),
			expected: true,
		},
		{
			name:     "regular query error",
			err:      fmt.Errorf("table does not exist"),
			expected: false,
		},
		{
			name:     "syntax error",
			err:      fmt.Errorf("Parser Error: syntax error"),
			expected: false,
		},
		{
			name:     "DuckLake SSL error from logs",
			err:      fmt.Errorf(`Failed to execute query "ROLLBACK": SSL connection has been closed unexpectedly`),
			expected: true,
		},
		{
			name:     "i/o timeout",
			err:      fmt.Errorf("read tcp: i/o timeout"),
			expected: true,
		},
		{
			name:     "use of closed network connection",
			err:      fmt.Errorf("use of closed network connection"),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isConnectionBroken(tt.err)
			if result != tt.expected {
				t.Errorf("isConnectionBroken(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
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
		// DML with RETURNING clause
		{
			name:     "INSERT RETURNING star",
			query:    "INSERT INTO t VALUES (1) RETURNING *",
			expected: true,
		},
		{
			name:     "INSERT RETURNING columns",
			query:    "INSERT INTO t VALUES (1) RETURNING id, name",
			expected: true,
		},
		{
			name:     "UPDATE RETURNING star",
			query:    "UPDATE t SET x = 1 RETURNING *",
			expected: true,
		},
		{
			name:     "DELETE RETURNING star",
			query:    "DELETE FROM t WHERE id = 1 RETURNING *",
			expected: true,
		},
		{
			name:     "comment before INSERT RETURNING",
			query:    "/* comment */ INSERT INTO t VALUES (1) RETURNING *",
			expected: true,
		},
		// Parenthesized queries
		{
			name:     "parenthesized SELECT",
			query:    "(SELECT 1 UNION SELECT 2)",
			expected: true,
		},
		{
			name:     "nested parenthesized SELECT",
			query:    "((SELECT 1))",
			expected: true,
		},
		{
			name:     "parenthesized SELECT with spaces",
			query:    "( SELECT 1 )",
			expected: true,
		},
		// Case sensitivity
		{
			name:     "lowercase insert returning",
			query:    "insert into t values (1) returning *",
			expected: true,
		},
		{
			name:     "mixed case Insert Returning",
			query:    "Insert Into t Values (1) Returning *",
			expected: true,
		},
		{
			name:     "lowercase select",
			query:    "select 1",
			expected: true,
		},
		// Multi-line DML RETURNING
		{
			name:     "multi-line INSERT RETURNING",
			query:    "INSERT INTO t\nVALUES (1)\nRETURNING *",
			expected: true,
		},
		// RETURNING in non-clause positions — correctly rejected
		{
			name:     "RETURNING in string literal (not false positive)",
			query:    "INSERT INTO t (col) VALUES ('RETURNING')",
			expected: false, // inside string literal, skipped by scanner
		},
		{
			name:     "RETURNING as column name (not false positive)",
			query:    "INSERT INTO t (returning) VALUES (1)",
			expected: false, // inside parentheses (depth > 0)
		},
		// Queries that should NOT match RETURNING
		{
			name:     "INSERT with RETURNING-like substring no space",
			query:    "INSERT INTO treturning VALUES (1)",
			expected: false, // no space before RETURNING
		},
		{
			name:     "plain CREATE TABLE",
			query:    "CREATE TABLE returning_results (id INT)",
			expected: false, // not INSERT/UPDATE/DELETE prefix
		},
		// Edge cases
		{
			name:     "empty string",
			query:    "",
			expected: false,
		},
		{
			name:     "only whitespace",
			query:    "   ",
			expected: false,
		},
		{
			name:     "only comment",
			query:    "-- just a comment",
			expected: false,
		},
		{
			name:     "SUMMARIZE (DuckDB-specific)",
			query:    "SUMMARIZE users",
			expected: true,
		},
		{
			name:     "FROM-first syntax",
			query:    "FROM users SELECT *",
			expected: true,
		},
		{
			name:     "EXECUTE",
			query:    "EXECUTE my_stmt",
			expected: true,
		},
		// WITH + DML RETURNING (WITH prefix matches first, correct)
		{
			name:     "CTE with INSERT RETURNING",
			query:    "WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte RETURNING *",
			expected: true,
		},
		// ALTER, TRUNCATE — should not return results
		{
			name:     "ALTER TABLE",
			query:    "ALTER TABLE t ADD COLUMN x INT",
			expected: false,
		},
		{
			name:     "TRUNCATE",
			query:    "TRUNCATE t",
			expected: false,
		},
		// DELETE with RETURNING in subquery — correctly rejected (depth > 0)
		{
			name:     "DELETE with RETURNING in subquery",
			query:    "DELETE FROM t WHERE id IN (SELECT returning FROM s)",
			expected: false,
		},
		// Tab before RETURNING
		{
			name:     "INSERT with tab before RETURNING",
			query:    "INSERT INTO t VALUES (1)\tRETURNING *",
			expected: true,
		},
		// Carriage return before RETURNING
		{
			name:     "INSERT with CR before RETURNING",
			query:    "INSERT INTO t VALUES (1)\rRETURNING *",
			expected: true,
		},
		// RETURNING with no preceding whitespace — not a clause
		{
			name:     "INSERT NORETURNING (no space)",
			query:    "INSERT INTO tRETURNING VALUES (1)",
			expected: false,
		},
		// Multiple RETURNING occurrences — first in parens, second real
		{
			name:     "INSERT with RETURNING in subselect and real RETURNING",
			query:    "INSERT INTO t SELECT (RETURNING) FROM s RETURNING *",
			expected: true,
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

		// Queries with leading comments that return results (DML RETURNING)
		{"block comment before INSERT RETURNING", "/* comment */ INSERT INTO t VALUES (1) RETURNING *", true},
		{"block comment before UPDATE RETURNING", "/* comment */ UPDATE t SET x = 1 RETURNING *", true},
		{"block comment before DELETE RETURNING", "/* comment */ DELETE FROM t RETURNING *", true},

		// Parentheses interleaved with comments
		{"paren then comment then SELECT", "(/* comment */ SELECT 1)", true},
		{"comment then paren then SELECT", "/* comment */ (SELECT 1)", true},
		{"nested parens and comments", "( ( /* comment */ SELECT 1 ) )", true},
		{"paren then line comment then SELECT", "( -- comment\nSELECT 1)", true},
		{"paren comment paren SELECT", "(/* c1 */(/* c2 */ SELECT 1))", true},
		{"paren then comment then INSERT RETURNING", "(/* comment */ INSERT INTO t VALUES (1) RETURNING *)", true},

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

// TestContainsReturning tests the low-level scanner directly.
// Input must be pre-uppercased (matching how callers invoke it).
func TestContainsReturning(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		// --- Basic positive cases ---
		{"simple", "INSERT INTO T VALUES (1) RETURNING *", true},
		{"tab before", "DELETE FROM T\tRETURNING *", true},
		{"newline before", "INSERT INTO T VALUES (1)\nRETURNING *", true},
		{"cr before", "UPDATE T SET X = 1\rRETURNING *", true},
		{"at end of string", "DELETE FROM T RETURNING", true},
		{"semicolon after", "INSERT INTO T VALUES (1) RETURNING *;", true},

		// --- Trailing character variations ---
		{"RETURNING*", "DELETE FROM T RETURNING*", true},
		{"RETURNING(expr)", "DELETE FROM T RETURNING(ID)", true},
		{"RETURNING,col", "INSERT INTO T VALUES (1) RETURNING ID,NAME", true},

		// --- Basic negative cases ---
		{"no RETURNING", "INSERT INTO T VALUES (1)", false},
		{"empty string", "", false},
		{"bare RETURNING no prefix", "RETURNING", false},
		{"RETURNING as prefix of word", "INSERT INTO T VALUES (1) RETURNING_ID", false},
		{"RETURNINGS", "INSERT INTO T VALUES (1) RETURNINGS", false},
		{"no space before", "INSERT INTO TRETURNING VALUES (1)", false},

		// --- Parenthesis depth ---
		{"RETURNING in subquery", "INSERT INTO T SELECT * FROM (SELECT RETURNING FROM S)", false},
		{"RETURNING in nested subquery", "INSERT INTO T SELECT * FROM (SELECT * FROM (SELECT RETURNING FROM A))", false},
		{"RETURNING in subquery + real RETURNING", "INSERT INTO T SELECT (RETURNING) FROM S RETURNING *", true},
		{"only in depth-1 parens", "DELETE FROM T WHERE ID IN (SELECT RETURNING FROM S)", false},
		{"real RETURNING after subquery", "DELETE FROM T WHERE ID IN (SELECT X FROM S) RETURNING *", true},
		{"ON CONFLICT with parens", "INSERT INTO T VALUES (1) ON CONFLICT (ID) DO UPDATE SET X = EXCLUDED.X RETURNING *", true},
		{"subquery in ON CONFLICT", "INSERT INTO T VALUES (1) ON CONFLICT (ID) DO UPDATE SET X = (SELECT MAX(X) FROM T2) RETURNING *", true},

		// --- Single-quoted string literals ---
		{"RETURNING in string literal", "INSERT INTO T VALUES (' RETURNING ') ", false},
		{"parens in string literal", "INSERT INTO T VALUES ('(') RETURNING *", true},
		{"close paren in string literal", "INSERT INTO T VALUES (')') RETURNING *", true},
		{"unbalanced parens in string literal", "INSERT INTO T VALUES ('(((') RETURNING *", true},
		{"escaped quote in string", "INSERT INTO T VALUES ('IT''S RETURNING')", false},
		{"escaped quote then real RETURNING", "INSERT INTO T VALUES ('IT''S') RETURNING *", true},

		// --- E-string backslash escapes ---
		{"E-string with backslash quote", "UPDATE T SET X = E'FOO\\'S RETURNING BAR' WHERE ID = 1", false},
		{"E-string then real RETURNING", "UPDATE T SET X = E'FOO\\'S' RETURNING *", true},
		{"E-string with backslash-n", "INSERT INTO T VALUES (E'LINE1\\NLINE2') RETURNING *", true},

		// --- Double-quoted identifiers ---
		{"RETURNING as quoted identifier", `INSERT INTO T ("RETURNING") VALUES (1)`, false},
		{"quoted identifier then real RETURNING", `INSERT INTO T ("RETURNING") VALUES (1) RETURNING *`, true},
		{"escaped quote in identifier", `INSERT INTO T ("COL""RETURNING") VALUES (1)`, false},

		// --- Dollar-quoted strings ---
		{"RETURNING in $$", "UPDATE T SET BODY = $$ RETURNING $$ WHERE ID = 1", false},
		{"RETURNING in $tag$", "UPDATE T SET BODY = $TAG$ RETURNING $TAG$ WHERE ID = 1", false},
		{"dollar-quoted then real RETURNING", "UPDATE T SET BODY = $$ X $$ RETURNING *", true},
		{"$$ with parens inside", "UPDATE T SET BODY = $$(RETURNING)$$ RETURNING *", true},
		{"incomplete dollar tag (not a tag)", "UPDATE T SET X = $5 RETURNING *", true},

		// --- Block comments ---
		{"RETURNING in block comment", "DELETE FROM T /* RETURNING * */ WHERE ID = 1", false},
		{"block comment then real RETURNING", "DELETE FROM T /* comment */ RETURNING *", true},
		{"nested-ish block comment content", "INSERT INTO T /* RETURNING * /* nested */ */ VALUES (1)", false},

		// --- Line comments ---
		{"RETURNING in line comment", "UPDATE T SET X = 1 -- RETURNING *\nWHERE ID = 1", false},
		{"line comment then real RETURNING", "UPDATE T SET X = 1 -- comment\nRETURNING *", true},
		{"line comment at end (no newline)", "INSERT INTO T VALUES (1) -- RETURNING *", false},

		// --- Combined edge cases ---
		{"string + subquery + real RETURNING", "INSERT INTO T SELECT 'RETURNING', (SELECT RETURNING FROM S) RETURNING *", true},
		{"comment + string + RETURNING", "DELETE FROM T /* skip */ WHERE X = ' RETURNING ' RETURNING *", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := containsReturning(tt.input)
			if result != tt.expected {
				t.Errorf("containsReturning(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsDMLReturning(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		// DML with RETURNING → true
		{"INSERT RETURNING", "INSERT INTO t VALUES (1) RETURNING *", true},
		{"UPDATE RETURNING", "UPDATE t SET x = 1 RETURNING id", true},
		{"DELETE RETURNING", "DELETE FROM t WHERE id = 1 RETURNING *", true},
		{"INSERT RETURNING with comment", "/* comment */ INSERT INTO t VALUES (1) RETURNING *", true},
		{"INSERT RETURNING multiline", "INSERT INTO t\nVALUES (1)\nRETURNING *", true},

		// DML without RETURNING → false
		{"plain INSERT", "INSERT INTO t VALUES (1)", false},
		{"plain UPDATE", "UPDATE t SET x = 1", false},
		{"plain DELETE", "DELETE FROM t WHERE id = 1", false},

		// Non-DML → false
		{"SELECT", "SELECT * FROM t", false},
		{"CREATE TABLE", "CREATE TABLE t (id INT)", false},
		{"WITH CTE", "WITH cte AS (SELECT 1) SELECT * FROM cte", false},

		// Edge cases: RETURNING in non-keyword positions → false
		{"column named returning", "INSERT INTO t (returning_col) VALUES (1)", false},
		{"RETURNING in subquery only", "DELETE FROM t WHERE id IN (SELECT returning FROM s)", false},
		{"RETURNING in string literal only", "INSERT INTO t VALUES ('returning')", false},
		{"RETURNING in block comment only", "DELETE FROM t /* returning * */ WHERE id = 1", false},

		// Edge cases: real RETURNING with noise → true
		{"subquery + real RETURNING", "DELETE FROM t WHERE id IN (SELECT x FROM s) RETURNING *", true},
		{"RETURNING*", "DELETE FROM t RETURNING*", true},
		{"RETURNING(id)", "DELETE FROM t RETURNING(id)", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDMLReturning(tt.query)
			if result != tt.expected {
				t.Errorf("isDMLReturning(%q) = %v, want %v", tt.query, result, tt.expected)
			}
		})
	}
}

// Note: isIgnoredSetParameter tests have been moved to transpiler/transpiler_test.go.
// The transpiler package now handles SET parameter filtering via AST transformation.

func TestBuildCommandTagFromRowCount(t *testing.T) {
	tests := []struct {
		cmdType  string
		rowCount int64
		expected string
	}{
		{"INSERT", 0, "INSERT 0 0"},
		{"INSERT", 1, "INSERT 0 1"},
		{"INSERT", 5, "INSERT 0 5"},
		{"UPDATE", 0, "UPDATE 0"},
		{"UPDATE", 1, "UPDATE 1"},
		{"UPDATE", 3, "UPDATE 3"},
		{"DELETE", 0, "DELETE 0"},
		{"DELETE", 1, "DELETE 1"},
		{"DELETE", 3, "DELETE 3"},
		{"SELECT", 0, "SELECT 0"},
		{"SELECT", 5, "SELECT 5"},
		// Large row counts
		{"INSERT", 1000000, "INSERT 0 1000000"},
		{"DELETE", 999999, "DELETE 999999"},
		// Unknown command types fall through to SELECT-style
		{"CREATE TABLE", 0, "SELECT 0"},
		{"", 0, "SELECT 0"},
		{"COPY", 0, "SELECT 0"},
		// Verify INSERT always has the OID 0 prefix (PostgreSQL compat)
		{"INSERT", 42, "INSERT 0 42"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%d", tt.cmdType, tt.rowCount), func(t *testing.T) {
			got := buildCommandTagFromRowCount(tt.cmdType, tt.rowCount)
			if got != tt.expected {
				t.Errorf("buildCommandTagFromRowCount(%q, %d) = %q, want %q", tt.cmdType, tt.rowCount, got, tt.expected)
			}
		})
	}
}

// TestBuildCommandTagConsistency verifies that buildCommandTagFromRowCount produces
// the same tags as buildCommandTag for DML types. This ensures the Query path
// (used for DML RETURNING) and the Exec path produce identical command tags.
func TestBuildCommandTagConsistency(t *testing.T) {
	c := &clientConn{}
	for _, cmdType := range []string{"INSERT", "UPDATE", "DELETE"} {
		for _, count := range []int64{0, 1, 5, 100} {
			t.Run(fmt.Sprintf("%s_%d", cmdType, count), func(t *testing.T) {
				fromRowCount := buildCommandTagFromRowCount(cmdType, count)
				fromExecResult := c.buildCommandTag(cmdType, &fakeExecResult{rowsAffected: count})
				if fromRowCount != fromExecResult {
					t.Errorf("tag mismatch for %s/%d: fromRowCount=%q, fromExecResult=%q",
						cmdType, count, fromRowCount, fromExecResult)
				}
			})
		}
	}
}

type fakeExecResult struct {
	rowsAffected int64
}

func (f *fakeExecResult) RowsAffected() (int64, error) {
	return f.rowsAffected, nil
}

func TestCopyToStdoutRegex(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		shouldMatch  bool
		expectedPart string // Expected captured group (source table/query)
	}{
		{
			name:         "simple COPY table TO STDOUT",
			query:        "COPY users TO STDOUT",
			shouldMatch:  true,
			expectedPart: "users",
		},
		{
			name:         "COPY with schema",
			query:        "COPY public.users TO STDOUT",
			shouldMatch:  true,
			expectedPart: "public.users",
		},
		{
			name:         "COPY with query",
			query:        "COPY (SELECT * FROM users WHERE id > 10) TO STDOUT",
			shouldMatch:  true,
			expectedPart: "(SELECT * FROM users WHERE id > 10)",
		},
		{
			name:         "COPY with options",
			query:        "COPY users TO STDOUT WITH (FORMAT CSV)",
			shouldMatch:  true,
			expectedPart: "users",
		},
		{
			name:         "case insensitive",
			query:        "copy users to stdout",
			shouldMatch:  true,
			expectedPart: "users",
		},
		{
			name:        "COPY FROM should not match",
			query:       "COPY users FROM STDIN",
			shouldMatch: false,
		},
		{
			name:        "COPY TO file should not match",
			query:       "COPY users TO '/tmp/file.csv'",
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := copyToStdoutRegex.FindStringSubmatch(tt.query)
			matched := len(matches) > 0
			if matched != tt.shouldMatch {
				t.Errorf("copyToStdoutRegex.Match(%q) = %v, want %v", tt.query, matched, tt.shouldMatch)
				return
			}
			if tt.shouldMatch && len(matches) > 1 && matches[1] != tt.expectedPart {
				t.Errorf("copyToStdoutRegex captured %q, want %q", matches[1], tt.expectedPart)
			}
		})
	}
}

func TestCopyFromStdinRegex(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		shouldMatch     bool
		expectedTable   string
		expectedColumns string
	}{
		{
			name:          "simple COPY table FROM STDIN",
			query:         "COPY users FROM STDIN",
			shouldMatch:   true,
			expectedTable: "users",
		},
		{
			name:          "COPY with schema",
			query:         "COPY public.users FROM STDIN",
			shouldMatch:   true,
			expectedTable: "public.users",
		},
		{
			name:            "COPY with columns",
			query:           "COPY users (id, name, email) FROM STDIN",
			shouldMatch:     true,
			expectedTable:   "users",
			expectedColumns: "id, name, email",
		},
		{
			name:          "COPY with options",
			query:         "COPY users FROM STDIN WITH (FORMAT CSV)",
			shouldMatch:   true,
			expectedTable: "users",
		},
		{
			name:            "COPY with columns and options",
			query:           "COPY users (id, name) FROM STDIN CSV HEADER",
			shouldMatch:     true,
			expectedTable:   "users",
			expectedColumns: "id, name",
		},
		{
			name:          "case insensitive",
			query:         "copy users from stdin",
			shouldMatch:   true,
			expectedTable: "users",
		},
		{
			name:        "COPY TO should not match",
			query:       "COPY users TO STDOUT",
			shouldMatch: false,
		},
		{
			name:        "COPY FROM file should not match",
			query:       "COPY users FROM '/tmp/file.csv'",
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := copyFromStdinRegex.FindStringSubmatch(tt.query)
			matched := len(matches) > 0
			if matched != tt.shouldMatch {
				t.Errorf("copyFromStdinRegex.Match(%q) = %v, want %v", tt.query, matched, tt.shouldMatch)
				return
			}
			if tt.shouldMatch {
				if len(matches) < 2 {
					t.Errorf("copyFromStdinRegex expected at least 2 captures, got %d", len(matches))
					return
				}
				if matches[1] != tt.expectedTable {
					t.Errorf("table: got %q, want %q", matches[1], tt.expectedTable)
				}
				if len(matches) > 2 && matches[2] != tt.expectedColumns {
					t.Errorf("columns: got %q, want %q", matches[2], tt.expectedColumns)
				}
			}
		})
	}
}

func TestCopyOptionRegexes(t *testing.T) {
	// Tests for CSV and HEADER detection
	t.Run("CSV and HEADER detection", func(t *testing.T) {
		tests := []struct {
			name      string
			query     string
			isCSV     bool
			hasHeader bool
		}{
			{
				name:      "plain text format",
				query:     "COPY users FROM STDIN",
				isCSV:     false,
				hasHeader: false,
			},
			{
				name:      "CSV format",
				query:     "COPY users FROM STDIN CSV",
				isCSV:     true,
				hasHeader: false,
			},
			{
				name:      "CSV with HEADER",
				query:     "COPY users FROM STDIN CSV HEADER",
				isCSV:     true,
				hasHeader: true,
			},
			{
				name:      "WITH FORMAT CSV",
				query:     "COPY users FROM STDIN WITH (FORMAT CSV)",
				isCSV:     true,
				hasHeader: false,
			},
			{
				name:      "WITH FORMAT CSV and HEADER",
				query:     "COPY users FROM STDIN WITH (FORMAT CSV, HEADER)",
				isCSV:     true,
				hasHeader: true,
			},
			{
				name:      "CSV lowercase",
				query:     "COPY users FROM STDIN csv header",
				isCSV:     true,
				hasHeader: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				upperQuery := strings.ToUpper(tt.query)

				isCSV := copyWithCSVRegex.MatchString(upperQuery)
				if isCSV != tt.isCSV {
					t.Errorf("CSV detection: got %v, want %v", isCSV, tt.isCSV)
				}

				hasHeader := copyWithHeaderRegex.MatchString(upperQuery)
				if hasHeader != tt.hasHeader {
					t.Errorf("HEADER detection: got %v, want %v", hasHeader, tt.hasHeader)
				}
			})
		}
	})

	// Tests for NULL string detection
	t.Run("NULL string detection", func(t *testing.T) {
		tests := []struct {
			name    string
			query   string
			nullStr string
		}{
			{
				name:    "custom NULL string",
				query:   "COPY users FROM STDIN WITH NULL 'N/A'",
				nullStr: "N/A",
			},
			{
				name:    "NULL empty string",
				query:   "COPY users FROM STDIN WITH NULL ''",
				nullStr: "",
			},
			{
				name:    "NULL with NONE",
				query:   "COPY users FROM STDIN CSV HEADER NULL 'NONE'",
				nullStr: "NONE",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				m := copyNullRegex.FindStringSubmatch(tt.query)
				if len(m) < 2 {
					t.Errorf("NULL regex didn't match: %q", tt.query)
					return
				}
				if m[1] != tt.nullStr {
					t.Errorf("NULL string: got %q, want %q", m[1], tt.nullStr)
				}
			})
		}
	})

	// Test delimiter regex behavior
	// Note: The current delimiter regex `\bDELIMITER\s+['"](.)['"]\b` requires a word
	// boundary after the closing quote. This works when DELIMITER is followed by another
	// option keyword, but may not work with trailing delimiters at end of query.
	t.Run("delimiter detection with following keyword", func(t *testing.T) {
		tests := []struct {
			name      string
			query     string
			delimiter string
			shouldMatch bool
		}{
			{
				name:        "delimiter followed by NULL",
				query:       "COPY users FROM STDIN CSV DELIMITER ','NULL 'test'",
				delimiter:   ",",
				shouldMatch: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				m := copyDelimiterRegex.FindStringSubmatch(tt.query)
				matched := len(m) > 1
				if matched != tt.shouldMatch {
					t.Errorf("delimiter match: got %v, want %v", matched, tt.shouldMatch)
				}
				if matched && m[1] != tt.delimiter {
					t.Errorf("delimiter: got %q, want %q", m[1], tt.delimiter)
				}
			})
		}
	})
}

func TestFormatCopyValue(t *testing.T) {
	c := &clientConn{}

	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{
			name:     "nil value",
			input:    nil,
			expected: "\\N",
		},
		{
			name:     "string value",
			input:    "hello",
			expected: "hello",
		},
		{
			name:     "integer value",
			input:    42,
			expected: "42",
		},
		{
			name:     "float value",
			input:    3.14,
			expected: "3.14",
		},
		{
			name:     "boolean true",
			input:    true,
			expected: "true",
		},
		{
			name:     "boolean false",
			input:    false,
			expected: "false",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "string with special chars",
			input:    "hello\tworld",
			expected: "hello\tworld",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := c.formatCopyValue(tt.input)
			if result != tt.expected {
				t.Errorf("formatCopyValue(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseCopyFromOptions(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		wantErr    bool
		tableName  string
		columnList string
		delimiter  string
		hasHeader  bool
		nullString string
		quote      string
		escape     string
	}{
		{
			name:       "simple COPY FROM STDIN",
			query:      "COPY users FROM STDIN",
			tableName:  "users",
			columnList: "",
			delimiter:  "\t",
			hasHeader:  false,
			nullString: "\\N",
			quote:      "",
			escape:     "",
		},
		{
			name:       "COPY with public schema stripped",
			query:      "COPY public.users FROM STDIN",
			tableName:  "users",
			columnList: "",
			delimiter:  "\t",
			hasHeader:  false,
			nullString: "\\N",
			quote:      "",
			escape:     "",
		},
		{
			name:       "COPY with quoted public schema stripped",
			query:      `COPY "public"."users" FROM STDIN`,
			tableName:  `"users"`,
			columnList: "",
			delimiter:  "\t",
			hasHeader:  false,
			nullString: "\\N",
			quote:      "",
			escape:     "",
		},
		{
			name:       "COPY with columns",
			query:      "COPY users (id, name, email) FROM STDIN",
			tableName:  "users",
			columnList: "(id, name, email)",
			delimiter:  "\t",
			hasHeader:  false,
			nullString: "\\N",
			quote:      "",
			escape:     "",
		},
		{
			name:       "COPY CSV",
			query:      "COPY users FROM STDIN CSV",
			tableName:  "users",
			columnList: "",
			delimiter:  ",",
			hasHeader:  false,
			nullString: "\\N",
			quote:      `"`,
			escape:     "",
		},
		{
			name:       "COPY CSV HEADER",
			query:      "COPY users FROM STDIN CSV HEADER",
			tableName:  "users",
			columnList: "",
			delimiter:  ",",
			hasHeader:  true,
			nullString: "\\N",
			quote:      `"`,
			escape:     "",
		},
		{
			name:       "COPY WITH FORMAT CSV HEADER",
			query:      "COPY users FROM STDIN WITH (FORMAT CSV, HEADER)",
			tableName:  "users",
			columnList: "",
			delimiter:  ",",
			hasHeader:  true,
			nullString: "\\N",
			quote:      `"`,
			escape:     "",
		},
		{
			name:       "COPY with custom NULL",
			query:      "COPY users FROM STDIN WITH NULL 'NA'",
			tableName:  "users",
			columnList: "",
			delimiter:  "\t",
			hasHeader:  false,
			nullString: "NA",
			quote:      "",
			escape:     "",
		},
		{
			name:       "COPY CSV with all options",
			query:      "COPY users (id, name) FROM STDIN CSV HEADER NULL ''",
			tableName:  "users",
			columnList: "(id, name)",
			delimiter:  ",",
			hasHeader:  true,
			nullString: "",
			quote:      `"`,
			escape:     "",
		},
		{
			name:       "COPY CSV with custom QUOTE",
			query:      `COPY users FROM STDIN CSV QUOTE "'"`,
			tableName:  "users",
			columnList: "",
			delimiter:  ",",
			hasHeader:  false,
			nullString: "\\N",
			quote:      `'`,
			escape:     "",
		},
		{
			name:       "COPY CSV with custom ESCAPE",
			query:      `COPY users FROM STDIN CSV ESCAPE "\"`,
			tableName:  "users",
			columnList: "",
			delimiter:  ",",
			hasHeader:  false,
			nullString: "\\N",
			quote:      `"`,
			escape:     `\`,
		},
		{
			name:    "invalid query - not COPY FROM STDIN",
			query:   "COPY users TO STDOUT",
			wantErr: true,
		},
		{
			name:    "invalid query - SELECT",
			query:   "SELECT * FROM users",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := ParseCopyFromOptions(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseCopyFromOptions() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseCopyFromOptions() unexpected error: %v", err)
			}
			if opts.TableName != tt.tableName {
				t.Errorf("TableName = %q, want %q", opts.TableName, tt.tableName)
			}
			if opts.ColumnList != tt.columnList {
				t.Errorf("ColumnList = %q, want %q", opts.ColumnList, tt.columnList)
			}
			if opts.Delimiter != tt.delimiter {
				t.Errorf("Delimiter = %q, want %q", opts.Delimiter, tt.delimiter)
			}
			if opts.HasHeader != tt.hasHeader {
				t.Errorf("HasHeader = %v, want %v", opts.HasHeader, tt.hasHeader)
			}
			if opts.NullString != tt.nullString {
				t.Errorf("NullString = %q, want %q", opts.NullString, tt.nullString)
			}
			if opts.Quote != tt.quote {
				t.Errorf("Quote = %q, want %q", opts.Quote, tt.quote)
			}
			if opts.Escape != tt.escape {
				t.Errorf("Escape = %q, want %q", opts.Escape, tt.escape)
			}
		})
	}
}

func TestParseCopyToOptions(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantErr   bool
		source    string
		delimiter string
		hasHeader bool
		isQuery   bool
	}{
		{
			name:      "simple COPY TO STDOUT",
			query:     "COPY users TO STDOUT",
			source:    "users",
			delimiter: "\t",
			hasHeader: false,
			isQuery:   false,
		},
		{
			name:      "COPY with schema",
			query:     "COPY public.users TO STDOUT",
			source:    "public.users",
			delimiter: "\t",
			hasHeader: false,
			isQuery:   false,
		},
		{
			name:      "COPY query TO STDOUT",
			query:     "COPY (SELECT * FROM users WHERE id > 10) TO STDOUT",
			source:    "(SELECT * FROM users WHERE id > 10)",
			delimiter: "\t",
			hasHeader: false,
			isQuery:   true,
		},
		{
			name:      "COPY CSV",
			query:     "COPY users TO STDOUT CSV",
			source:    "users",
			delimiter: ",",
			hasHeader: false,
			isQuery:   false,
		},
		{
			name:      "COPY CSV HEADER",
			query:     "COPY users TO STDOUT CSV HEADER",
			source:    "users",
			delimiter: ",",
			hasHeader: true,
			isQuery:   false,
		},
		{
			name:      "COPY WITH FORMAT CSV HEADER",
			query:     "COPY users TO STDOUT WITH (FORMAT CSV, HEADER)",
			source:    "users",
			delimiter: ",",
			hasHeader: true,
			isQuery:   false,
		},
		{
			name:    "invalid query - COPY FROM STDIN",
			query:   "COPY users FROM STDIN",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := ParseCopyToOptions(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseCopyToOptions() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseCopyToOptions() unexpected error: %v", err)
			}
			if opts.Source != tt.source {
				t.Errorf("Source = %q, want %q", opts.Source, tt.source)
			}
			if opts.Delimiter != tt.delimiter {
				t.Errorf("Delimiter = %q, want %q", opts.Delimiter, tt.delimiter)
			}
			if opts.HasHeader != tt.hasHeader {
				t.Errorf("HasHeader = %v, want %v", opts.HasHeader, tt.hasHeader)
			}
			if opts.IsQuery != tt.isQuery {
				t.Errorf("IsQuery = %v, want %v", opts.IsQuery, tt.isQuery)
			}
		})
	}
}

func TestBuildDuckDBCopyFromSQL(t *testing.T) {
	tests := []struct {
		name       string
		tableName  string
		columnList string
		filePath   string
		opts       *CopyFromOptions
		want       string
	}{
		{
			name:       "basic text format",
			tableName:  "users",
			columnList: "",
			filePath:   "/tmp/data.csv",
			opts: &CopyFromOptions{
				Delimiter:  "\t",
				HasHeader:  false,
				NullString: "\\N",
			},
			want: "COPY users  FROM '/tmp/data.csv' (FORMAT CSV, AUTO_DETECT FALSE, STRICT_MODE FALSE, PARALLEL FALSE, NULL '\\N', DELIMITER '\t')",
		},
		{
			name:       "CSV with header",
			tableName:  "users",
			columnList: "",
			filePath:   "/tmp/data.csv",
			opts: &CopyFromOptions{
				Delimiter:  ",",
				HasHeader:  true,
				NullString: "\\N",
				Quote:      `"`,
			},
			want: "COPY users  FROM '/tmp/data.csv' (FORMAT CSV, AUTO_DETECT FALSE, STRICT_MODE FALSE, PARALLEL FALSE, HEADER, NULL '\\N', DELIMITER ',', QUOTE '\"', ESCAPE '\"')",
		},
		{
			name:       "with column list",
			tableName:  "users",
			columnList: "(id, name)",
			filePath:   "/tmp/data.csv",
			opts: &CopyFromOptions{
				Delimiter:  ",",
				HasHeader:  false,
				NullString: "\\N",
				Quote:      `"`,
			},
			want: "COPY users (id, name) FROM '/tmp/data.csv' (FORMAT CSV, AUTO_DETECT FALSE, STRICT_MODE FALSE, PARALLEL FALSE, NULL '\\N', DELIMITER ',', QUOTE '\"', ESCAPE '\"')",
		},
		{
			name:       "custom NULL string",
			tableName:  "users",
			columnList: "",
			filePath:   "/tmp/data.csv",
			opts: &CopyFromOptions{
				Delimiter:  ",",
				HasHeader:  false,
				NullString: "NA",
				Quote:      `"`,
			},
			want: "COPY users  FROM '/tmp/data.csv' (FORMAT CSV, AUTO_DETECT FALSE, STRICT_MODE FALSE, PARALLEL FALSE, NULL 'NA', DELIMITER ',', QUOTE '\"', ESCAPE '\"')",
		},
		{
			name:       "empty NULL string",
			tableName:  "users",
			columnList: "",
			filePath:   "/tmp/data.csv",
			opts: &CopyFromOptions{
				Delimiter:  ",",
				HasHeader:  true,
				NullString: "",
				Quote:      `"`,
			},
			want: "COPY users  FROM '/tmp/data.csv' (FORMAT CSV, AUTO_DETECT FALSE, STRICT_MODE FALSE, PARALLEL FALSE, HEADER, NULL '', DELIMITER ',', QUOTE '\"', ESCAPE '\"')",
		},
		{
			name:       "schema qualified table",
			tableName:  "public.users",
			columnList: "(id, name, email)",
			filePath:   "/var/tmp/copy-123.csv",
			opts: &CopyFromOptions{
				Delimiter:  "\t",
				HasHeader:  true,
				NullString: "\\N",
				Quote:      `"`,
			},
			want: "COPY public.users (id, name, email) FROM '/var/tmp/copy-123.csv' (FORMAT CSV, AUTO_DETECT FALSE, STRICT_MODE FALSE, PARALLEL FALSE, HEADER, NULL '\\N', DELIMITER '\t', QUOTE '\"', ESCAPE '\"')",
		},
		{
			name:       "CSV with custom escape character",
			tableName:  "users",
			columnList: "",
			filePath:   "/tmp/data.csv",
			opts: &CopyFromOptions{
				Delimiter:  ",",
				HasHeader:  true,
				NullString: "\\N",
				Quote:      `"`,
				Escape:     `\`,
			},
			want: "COPY users  FROM '/tmp/data.csv' (FORMAT CSV, AUTO_DETECT FALSE, STRICT_MODE FALSE, PARALLEL FALSE, HEADER, NULL '\\N', DELIMITER ',', QUOTE '\"', ESCAPE '\\')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildDuckDBCopyFromSQL(tt.tableName, tt.columnList, tt.filePath, tt.opts)
			if got != tt.want {
				t.Errorf("BuildDuckDBCopyFromSQL() =\n  %q\nwant:\n  %q", got, tt.want)
			}
		})
	}
}

func TestCopyCommandTypeDetection(t *testing.T) {
	c := &clientConn{}

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "COPY TO STDOUT",
			query:    "COPY users TO STDOUT",
			expected: "COPY",
		},
		{
			name:     "COPY FROM STDIN",
			query:    "COPY users FROM STDIN",
			expected: "COPY",
		},
		{
			name:     "COPY with options",
			query:    "COPY users FROM STDIN WITH (FORMAT CSV)",
			expected: "COPY",
		},
		{
			name:     "lowercase copy",
			query:    "copy users to stdout",
			expected: "COPY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := c.getCommandType(strings.ToUpper(tt.query))
			if result != tt.expected {
				t.Errorf("getCommandType(%q) = %q, want %q", tt.query, result, tt.expected)
			}
		})
	}
}

func TestParseCopyLine(t *testing.T) {
	c := &clientConn{}

	tests := []struct {
		name      string
		line      string
		delimiter string
		expected  []string
	}{
		{
			name:      "simple CSV values",
			line:      `a,b,c`,
			delimiter: ",",
			expected:  []string{"a", "b", "c"},
		},
		{
			name:      "quoted value with embedded comma",
			line:      `"hello, world",normal,value`,
			delimiter: ",",
			expected:  []string{"hello, world", "normal", "value"},
		},
		{
			name:      "multiple quoted values with commas",
			line:      `"value, one","value, two","value, three"`,
			delimiter: ",",
			expected:  []string{"value, one", "value, two", "value, three"},
		},
		{
			name:      "mixed quoted and unquoted",
			line:      `id,"url with, comma",status`,
			delimiter: ",",
			expected:  []string{"id", "url with, comma", "status"},
		},
		{
			name:      "tab-separated values",
			line:      "a\tb\tc",
			delimiter: "\t",
			expected:  []string{"a", "b", "c"},
		},
		{
			name:      "quoted value with embedded tab",
			line:      "\"hello\tworld\"\tnormal",
			delimiter: "\t",
			expected:  []string{"hello\tworld", "normal"},
		},
		{
			name:      "empty values",
			line:      `a,,c`,
			delimiter: ",",
			expected:  []string{"a", "", "c"},
		},
		{
			name:      "URL with comma in quoted field",
			line:      `"cs_123","https://example.com/success?a=1,b=2",active`,
			delimiter: ",",
			expected:  []string{"cs_123", "https://example.com/success?a=1,b=2", "active"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := c.parseCopyLine(tt.line, tt.delimiter)
			if len(result) != len(tt.expected) {
				t.Errorf("parseCopyLine(%q, %q) returned %d values, want %d\nGot: %v\nWant: %v",
					tt.line, tt.delimiter, len(result), len(tt.expected), result, tt.expected)
				return
			}
			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("parseCopyLine(%q, %q)[%d] = %q, want %q",
						tt.line, tt.delimiter, i, v, tt.expected[i])
				}
			}
		})
	}
}

// TestPreparedStmtMultiStatement verifies that preparedStmt can store multi-statement
// results from writable CTE transforms for the extended query protocol.
func TestPreparedStmtMultiStatement(t *testing.T) {
	// This test ensures that when a transpiler produces multi-statement results
	// (e.g., writable CTE rewrites), the preparedStmt struct can store them.
	//
	// The extended query protocol (Parse/Bind/Execute) used by tools like Airbyte
	// must handle multi-statement results just like handleQuery does for simple queries.

	// Test that preparedStmt has the necessary fields for multi-statement results
	stmt := preparedStmt{
		query:             "WITH updates AS (UPDATE t SET x = 1 RETURNING *) SELECT * FROM updates",
		convertedQuery:    "", // Single SQL - would fail with writable CTE
		statements:        []string{"BEGIN", "CREATE TEMP TABLE _cte_updates AS ...", "UPDATE t SET x = 1", "SELECT * FROM _cte_updates"},
		cleanupStatements: []string{"DROP TABLE IF EXISTS _cte_updates", "COMMIT"},
	}

	// Verify that multi-statement fields exist and can be set
	if len(stmt.statements) != 4 {
		t.Errorf("expected 4 statements, got %d", len(stmt.statements))
	}
	if len(stmt.cleanupStatements) != 2 {
		t.Errorf("expected 2 cleanup statements, got %d", len(stmt.cleanupStatements))
	}

	// Verify that we can detect when to use multi-statement execution
	hasMultiStatement := len(stmt.statements) > 0
	if !hasMultiStatement {
		t.Error("expected hasMultiStatement to be true")
	}
}

func TestCopyBinaryRegex(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		shouldMatch bool
	}{
		{
			name:        "FORMAT binary unquoted",
			query:       "COPY users TO STDOUT (FORMAT binary)",
			shouldMatch: true,
		},
		{
			name:        "FORMAT BINARY uppercase",
			query:       "COPY users TO STDOUT (FORMAT BINARY)",
			shouldMatch: true,
		},
		{
			name:        `FORMAT "binary" quoted`,
			query:       `COPY users TO STDOUT (FORMAT "binary")`,
			shouldMatch: true,
		},
		{
			name:        "FORMAT csv should not match",
			query:       "COPY users TO STDOUT (FORMAT csv)",
			shouldMatch: false,
		},
		{
			name:        "no FORMAT clause",
			query:       "COPY users TO STDOUT",
			shouldMatch: false,
		},
		{
			name:        "FORMAT binary in FROM STDIN",
			query:       "COPY users FROM STDIN (FORMAT binary)",
			shouldMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched := copyBinaryRegex.MatchString(tt.query)
			if matched != tt.shouldMatch {
				t.Errorf("copyBinaryRegex.Match(%q) = %v, want %v", tt.query, matched, tt.shouldMatch)
			}
		})
	}
}

func TestParseCopyFromOptions_Binary(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		wantErr    bool
		isBinary   bool
		tableName  string
		columnList string
	}{
		{
			name:      "COPY with FORMAT binary",
			query:     "COPY users FROM STDIN (FORMAT binary)",
			isBinary:  true,
			tableName: "users",
		},
		{
			name:       "COPY with columns and FORMAT binary",
			query:      "COPY users (id, name) FROM STDIN (FORMAT binary)",
			isBinary:   true,
			tableName:  "users",
			columnList: "(id, name)",
		},
		{
			name:      "COPY CSV is not binary",
			query:     "COPY users FROM STDIN CSV",
			isBinary:  false,
			tableName: "users",
		},
		{
			name:      "COPY text format is not binary",
			query:     "COPY users FROM STDIN",
			isBinary:  false,
			tableName: "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := ParseCopyFromOptions(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseCopyFromOptions(%q) error: %v", tt.query, err)
			}
			if opts.IsBinary != tt.isBinary {
				t.Errorf("IsBinary = %v, want %v", opts.IsBinary, tt.isBinary)
			}
			if opts.TableName != tt.tableName {
				t.Errorf("TableName = %q, want %q", opts.TableName, tt.tableName)
			}
			if tt.columnList != "" && opts.ColumnList != tt.columnList {
				t.Errorf("ColumnList = %q, want %q", opts.ColumnList, tt.columnList)
			}
		})
	}
}

func TestDecodeBinaryCopy(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		oid      int32
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "nil data returns nil",
			data:     nil,
			oid:      OidText,
			expected: nil,
		},
		{
			name:     "empty data text type returns empty string",
			data:     []byte{},
			oid:      OidText,
			expected: "",
		},
		{
			name:     "empty data varchar returns empty string",
			data:     []byte{},
			oid:      OidVarchar,
			expected: "",
		},
		{
			name:     "empty data json returns empty string",
			data:     []byte{},
			oid:      OidJSON,
			expected: "",
		},
		{
			name:     "empty data int returns nil",
			data:     []byte{},
			oid:      OidInt4,
			expected: nil,
		},
		{
			name:     "bool true",
			data:     []byte{1},
			oid:      OidBool,
			expected: true,
		},
		{
			name:     "bool false",
			data:     []byte{0},
			oid:      OidBool,
			expected: false,
		},
		{
			name:     "int2 value 42",
			data:     []byte{0, 42},
			oid:      OidInt2,
			expected: int16(42),
		},
		{
			name:     "int4 value 1000",
			data:     []byte{0, 0, 3, 232},
			oid:      OidInt4,
			expected: int32(1000),
		},
		{
			name:     "int8 value 1000000",
			data:     []byte{0, 0, 0, 0, 0, 15, 66, 64},
			oid:      OidInt8,
			expected: int64(1000000),
		},
		{
			name:     "int OID with 2-byte data uses int2 decode",
			data:     []byte{0, 7},
			oid:      OidInt4,
			expected: int16(7),
		},
		{
			name:     "int OID with 8-byte data uses int8 decode",
			data:     []byte{0, 0, 0, 0, 0, 0, 0, 1},
			oid:      OidInt4,
			expected: int64(1),
		},
		{
			name:     "text data",
			data:     []byte("hello world"),
			oid:      OidText,
			expected: "hello world",
		},
		{
			name:     "varchar data",
			data:     []byte("test"),
			oid:      OidVarchar,
			expected: "test",
		},
		{
			name:     "bytea data",
			data:     []byte{0xDE, 0xAD, 0xBE, 0xEF},
			oid:      OidBytea,
			expected: []byte{0xDE, 0xAD, 0xBE, 0xEF},
		},
		{
			name:     "OidOid (UINTEGER) decodes as int4",
			data:     []byte{0, 0, 0, 42},
			oid:      OidOid,
			expected: int32(42),
		},
		{
			name:     "unknown OID returns string",
			data:     []byte("some_data"),
			oid:      99999,
			expected: "some_data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeBinaryCopy(tt.data, tt.oid)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("decodeBinaryCopy() error: %v", err)
			}
			// Compare with type-specific checks
			switch expected := tt.expected.(type) {
			case nil:
				if result != nil {
					t.Errorf("got %v (%T), want nil", result, result)
				}
			case bool:
				if v, ok := result.(bool); !ok || v != expected {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case int16:
				if v, ok := result.(int16); !ok || v != expected {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case int32:
				if v, ok := result.(int32); !ok || v != expected {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case int64:
				if v, ok := result.(int64); !ok || v != expected {
					t.Errorf("got %v (%T), want %v", result, result, expected)
				}
			case string:
				if v, ok := result.(string); !ok || v != expected {
					t.Errorf("got %v (%T), want %q", result, result, expected)
				}
			case []byte:
				if v, ok := result.([]byte); !ok {
					t.Errorf("got %v (%T), want []byte", result, result)
				} else {
					for i := range expected {
						if i >= len(v) || v[i] != expected[i] {
							t.Errorf("byte mismatch at index %d", i)
							break
						}
					}
				}
			default:
				t.Errorf("unhandled type in test: %T", expected)
			}
		})
	}
}

func TestDecodeBinaryCopy_FloatWidthMismatch(t *testing.T) {
	// DuckDB's postgres extension may send float data with mismatched width
	// e.g., float4 OID but 8-byte data, or float8 OID but 4-byte data

	// float4 OID with 8-byte data should decode as float8
	float8Data := make([]byte, 8)
	bits := math.Float64bits(3.14)
	binary.BigEndian.PutUint64(float8Data, bits)

	result, err := decodeBinaryCopy(float8Data, OidFloat4)
	if err != nil {
		t.Fatalf("decodeBinaryCopy(float8 data, OidFloat4) error: %v", err)
	}
	if v, ok := result.(float64); !ok || math.Abs(v-3.14) > 0.001 {
		t.Errorf("got %v (%T), want ~3.14", result, result)
	}

	// float8 OID with 4-byte data should decode as float4
	float4Data := make([]byte, 4)
	bits32 := math.Float32bits(2.5)
	binary.BigEndian.PutUint32(float4Data, bits32)

	result, err = decodeBinaryCopy(float4Data, OidFloat8)
	if err != nil {
		t.Fatalf("decodeBinaryCopy(float4 data, OidFloat8) error: %v", err)
	}
	if v, ok := result.(float32); !ok || math.Abs(float64(v)-2.5) > 0.001 {
		t.Errorf("got %v (%T), want ~2.5", result, result)
	}
}

func TestParseMultiLineCSV(t *testing.T) {
	// This tests the fix for COPY FROM STDIN with multi-line quoted fields.
	// Previously, we split by newlines first then parsed each line, which broke
	// when quoted fields contained embedded newlines (e.g., JSON with formatting).
	// Now we use csv.Reader on the entire buffer which handles this correctly.

	tests := []struct {
		name      string
		input     string
		delimiter string
		expected  [][]string
	}{
		{
			name:      "simple rows no newlines",
			input:     "a,b,c\n1,2,3\n",
			delimiter: ",",
			expected:  [][]string{{"a", "b", "c"}, {"1", "2", "3"}},
		},
		{
			name:      "quoted field with embedded newline",
			input:     "id,json,status\n1,\"{\"\"key\"\":\n\"\"value\"\"}\",active\n",
			delimiter: ",",
			expected:  [][]string{{"id", "json", "status"}, {"1", "{\"key\":\n\"value\"}", "active"}},
		},
		{
			name:      "multiple fields with newlines",
			input:     "a,\"line1\nline2\",b\nc,\"x\ny\nz\",d\n",
			delimiter: ",",
			expected:  [][]string{{"a", "line1\nline2", "b"}, {"c", "x\ny\nz", "d"}},
		},
		{
			name:      "JSON metadata like Fivetran sends",
			input:     "customer_id,metadata,type\ncust_123,\"{\"\"subscription\"\":\n  \"\"active\"\",\n  \"\"plan\"\": \"\"pro\"\"}\",invoice\n",
			delimiter: ",",
			expected:  [][]string{{"customer_id", "metadata", "type"}, {"cust_123", "{\"subscription\":\n  \"active\",\n  \"plan\": \"pro\"}", "invoice"}},
		},
		{
			name:      "13 columns with multiline JSON in middle",
			input:     "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13\nv1,v2,v3,v4,v5,v6,\"{\"\"a\"\":\n\"\"b\"\"}\",v8,v9,v10,v11,v12,v13\n",
			delimiter: ",",
			expected:  [][]string{{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13"}, {"v1", "v2", "v3", "v4", "v5", "v6", "{\"a\":\n\"b\"}", "v8", "v9", "v10", "v11", "v12", "v13"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate what handleCopyIn does: use csv.Reader on entire buffer
			reader := csv.NewReader(strings.NewReader(tt.input))
			reader.Comma = rune(tt.delimiter[0])
			reader.LazyQuotes = true
			reader.FieldsPerRecord = -1

			var rows [][]string
			for {
				record, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("csv.Read() error: %v", err)
				}
				rows = append(rows, record)
			}

			if len(rows) != len(tt.expected) {
				t.Errorf("got %d rows, want %d\nGot: %v\nWant: %v",
					len(rows), len(tt.expected), rows, tt.expected)
				return
			}

			for i, row := range rows {
				if len(row) != len(tt.expected[i]) {
					t.Errorf("row %d: got %d columns, want %d\nGot: %v\nWant: %v",
						i, len(row), len(tt.expected[i]), row, tt.expected[i])
					continue
				}
				for j, val := range row {
					if val != tt.expected[i][j] {
						t.Errorf("row %d col %d: got %q, want %q",
							i, j, val, tt.expected[i][j])
					}
				}
			}
		})
	}
}

func TestFormatInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval duckdb.Interval
		want     string
	}{
		// Zero
		{"zero", duckdb.Interval{}, "00:00:00"},

		// Time only
		{"seconds", duckdb.Interval{Micros: 5_000_000}, "00:00:05"},
		{"minutes and seconds", duckdb.Interval{Micros: 788_000_000}, "00:13:08"},
		{"hours minutes seconds", duckdb.Interval{Micros: 3_661_000_000}, "01:01:01"},
		{"fractional seconds", duckdb.Interval{Micros: 788_917_797}, "00:13:08.917797"},
		{"exact microseconds", duckdb.Interval{Micros: 1}, "00:00:00.000001"},

		// Days
		{"one day", duckdb.Interval{Days: 1}, "1 day"},
		{"multiple days", duckdb.Interval{Days: 5}, "5 days"},
		{"days and time", duckdb.Interval{Days: 3, Micros: 9_000_000_000}, "3 days 02:30:00"},

		// Months
		{"one month", duckdb.Interval{Months: 1}, "1 mon"},
		{"multiple months", duckdb.Interval{Months: 7}, "7 mons"},
		{"one year", duckdb.Interval{Months: 12}, "1 year"},
		{"years and months", duckdb.Interval{Months: 14}, "1 year 2 mons"},
		{"multiple years", duckdb.Interval{Months: 36}, "3 years"},

		// Mixed
		{"full interval", duckdb.Interval{Months: 14, Days: 3, Micros: 14_706_123_456}, "1 year 2 mons 3 days 04:05:06.123456"},

		// Negative
		{"negative time", duckdb.Interval{Micros: -3_600_000_000}, "-01:00:00"},
		{"negative days", duckdb.Interval{Days: -2}, "-2 days"},
		{"negative months", duckdb.Interval{Months: -1}, "-1 mon"},
		{"negative years", duckdb.Interval{Months: -14}, "-1 year -2 mons"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatInterval(tt.interval)
			if got != tt.want {
				t.Errorf("formatInterval(%+v) = %q, want %q", tt.interval, got, tt.want)
			}
		})
	}
}

func TestMatchPgCursorsQuery(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		wantName       string
		wantParam      bool
		wantOK         bool
	}{
		// Literal cursor name queries
		{
			name:      "basic pg_cursors query",
			query:     "SELECT 1 FROM pg_cursors WHERE name = 'test_cursor'",
			wantName:  "test_cursor",
			wantParam: false,
			wantOK:    true,
		},
		{
			name:      "pg_catalog prefix",
			query:     "SELECT 1 FROM pg_catalog.pg_cursors WHERE name = 'test_cursor'",
			wantName:  "test_cursor",
			wantParam: false,
			wantOK:    true,
		},
		{
			name:      "psycopg3 style cursor name with dots",
			query:     "SELECT 1 FROM pg_cursors WHERE name = 'posthog_2_data_imports_team_2.users'",
			wantName:  "posthog_2_data_imports_team_2.users",
			wantParam: false,
			wantOK:    true,
		},
		{
			name:      "leading whitespace",
			query:     "  SELECT 1 FROM pg_cursors WHERE name = 'my_cursor'",
			wantName:  "my_cursor",
			wantParam: false,
			wantOK:    true,
		},
		{
			name:      "leading newline",
			query:     "\nSELECT 1 FROM pg_cursors WHERE name = 'my_cursor'\n",
			wantName:  "my_cursor",
			wantParam: false,
			wantOK:    true,
		},
		{
			name:      "case insensitive",
			query:     "select 1 from pg_cursors where name = 'test'",
			wantName:  "test",
			wantParam: false,
			wantOK:    true,
		},
		{
			name:      "trailing semicolon",
			query:     "SELECT 1 FROM pg_cursors WHERE name = 'test_cursor';",
			wantName:  "test_cursor",
			wantParam: false,
			wantOK:    true,
		},
		{
			name:      "pg_catalog with spaces around dot",
			query:     "SELECT 1 FROM pg_catalog . pg_cursors WHERE name = 'test'",
			wantName:  "test",
			wantParam: false,
			wantOK:    true,
		},
		{
			name:      "empty cursor name",
			query:     "SELECT 1 FROM pg_cursors WHERE name = ''",
			wantName:  "",
			wantParam: false,
			wantOK:    true,
		},

		// Parameterized queries ($1)
		{
			name:      "parameterized query",
			query:     "SELECT 1 FROM pg_cursors WHERE name = $1",
			wantName:  "",
			wantParam: true,
			wantOK:    true,
		},
		{
			name:      "parameterized with pg_catalog",
			query:     "SELECT 1 FROM pg_catalog.pg_cursors WHERE name = $1",
			wantName:  "",
			wantParam: true,
			wantOK:    true,
		},

		// Non-matching queries
		{
			name:      "regular SELECT",
			query:     "SELECT * FROM users",
			wantName:  "",
			wantParam: false,
			wantOK:    false,
		},
		{
			name:      "pg_cursors in different context",
			query:     "SELECT * FROM pg_cursors",
			wantName:  "",
			wantParam: false,
			wantOK:    false,
		},
		{
			name:      "pg_cursors without WHERE name",
			query:     "SELECT count(*) FROM pg_cursors WHERE is_holdable = true",
			wantName:  "",
			wantParam: false,
			wantOK:    false,
		},
		{
			name:      "empty query",
			query:     "",
			wantName:  "",
			wantParam: false,
			wantOK:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, param, ok := matchPgCursorsQuery(tt.query)
			if ok != tt.wantOK {
				t.Errorf("matchPgCursorsQuery(%q) ok = %v, want %v", tt.query, ok, tt.wantOK)
			}
			if name != tt.wantName {
				t.Errorf("matchPgCursorsQuery(%q) name = %q, want %q", tt.query, name, tt.wantName)
			}
			if param != tt.wantParam {
				t.Errorf("matchPgCursorsQuery(%q) parameterized = %v, want %v", tt.query, param, tt.wantParam)
			}
		})
	}
}

func TestCursorCloseAllCursors(t *testing.T) {
	c := &clientConn{
		cursors: map[string]*cursorState{
			"cur1": {query: "SELECT 1"},
			"cur2": {query: "SELECT 2"},
			"cur3": {query: "SELECT 3"},
		},
	}

	if len(c.cursors) != 3 {
		t.Fatalf("expected 3 cursors, got %d", len(c.cursors))
	}

	c.closeAllCursors()

	if len(c.cursors) != 0 {
		t.Errorf("after closeAllCursors, expected 0 cursors, got %d", len(c.cursors))
	}
}

func TestCursorCloseSingle(t *testing.T) {
	c := &clientConn{
		cursors: map[string]*cursorState{
			"cur1": {query: "SELECT 1"},
			"cur2": {query: "SELECT 2"},
		},
	}

	c.closeCursor("cur1")

	if len(c.cursors) != 1 {
		t.Errorf("expected 1 cursor remaining, got %d", len(c.cursors))
	}
	if _, ok := c.cursors["cur2"]; !ok {
		t.Error("expected cur2 to still exist")
	}
	if _, ok := c.cursors["cur1"]; ok {
		t.Error("expected cur1 to be removed")
	}
}

func TestCursorCloseNonexistent(t *testing.T) {
	c := &clientConn{
		cursors: map[string]*cursorState{
			"cur1": {query: "SELECT 1"},
		},
	}

	// Should not panic
	c.closeCursor("nonexistent")

	if len(c.cursors) != 1 {
		t.Errorf("expected 1 cursor, got %d", len(c.cursors))
	}
}

func TestTransactionCommitClosesAllCursors(t *testing.T) {
	c := &clientConn{
		txStatus: txStatusTransaction,
		cursors: map[string]*cursorState{
			"cur1": {query: "SELECT 1"},
			"cur2": {query: "SELECT 2"},
		},
	}

	c.updateTxStatus("COMMIT")

	if c.txStatus != txStatusIdle {
		t.Errorf("txStatus = %c, want %c", c.txStatus, txStatusIdle)
	}
	if len(c.cursors) != 0 {
		t.Errorf("expected 0 cursors after COMMIT, got %d", len(c.cursors))
	}
}

func TestTransactionRollbackClosesAllCursors(t *testing.T) {
	c := &clientConn{
		txStatus: txStatusTransaction,
		cursors: map[string]*cursorState{
			"cur1": {query: "SELECT 1"},
		},
	}

	c.updateTxStatus("ROLLBACK")

	if c.txStatus != txStatusIdle {
		t.Errorf("txStatus = %c, want %c", c.txStatus, txStatusIdle)
	}
	if len(c.cursors) != 0 {
		t.Errorf("expected 0 cursors after ROLLBACK, got %d", len(c.cursors))
	}
}

func TestIsFetchForwardOnly(t *testing.T) {
	tests := []struct {
		name string
		dir  pg_query.FetchDirection
		want bool
	}{
		{"undefined (default)", pg_query.FetchDirection_FETCH_DIRECTION_UNDEFINED, true},
		{"forward", pg_query.FetchDirection_FETCH_FORWARD, true},
		{"backward", pg_query.FetchDirection_FETCH_BACKWARD, false},
		{"absolute", pg_query.FetchDirection_FETCH_ABSOLUTE, false},
		{"relative", pg_query.FetchDirection_FETCH_RELATIVE, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isFetchForwardOnly(tt.dir)
			if got != tt.want {
				t.Errorf("isFetchForwardOnly(%v) = %v, want %v", tt.dir, got, tt.want)
			}
		})
	}
}

func TestCountDollarParams(t *testing.T) {
	tests := []struct {
		query    string
		expected int
	}{
		{"SELECT 1", 0},
		{"SELECT $1", 1},
		{"SELECT $1, $2", 2},
		{"SELECT $2, $1", 2},
		{"SELECT $10", 10},
		{"INSERT INTO t VALUES ($1, $2, $3)", 3},
		{"SELECT * FROM t WHERE id = $1 AND name = $3", 3},
		{"", 0},
		{"SELECT '$1'", 1}, // simplified: doesn't parse string literals
	}

	for _, tt := range tests {
		got := countDollarParams(tt.query)
		if got != tt.expected {
			t.Errorf("countDollarParams(%q) = %d, want %d", tt.query, got, tt.expected)
		}
	}
}

func TestMatchPgStatActivityQuery(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		// Should match
		{"simple select", "SELECT * FROM pg_stat_activity", true},
		{"pg_catalog prefix", "SELECT * FROM pg_catalog.pg_stat_activity", true},
		{"pg_catalog with spaces", "SELECT * FROM pg_catalog . pg_stat_activity", true},
		{"case insensitive FROM", "select * from pg_stat_activity", true},
		{"uppercase FROM keyword", "SELECT * FROM pg_stat_activity", true},
		// Note: strings.Contains fast path is case-sensitive for the table name.
		// This is fine because PostgreSQL clients always send lowercase catalog names.
		{"uppercase table name skipped by fast path", "select * from PG_STAT_ACTIVITY", false},
		{"with where clause", "SELECT pid, usename FROM pg_stat_activity WHERE state = 'active'", true},
		{"with limit", "SELECT * FROM pg_stat_activity LIMIT 10", true},
		{"with count", "SELECT count(*) FROM pg_stat_activity", true},
		{"multiline", "SELECT pid\nFROM pg_stat_activity\nWHERE state = 'active'", true},
		{"tab before table", "SELECT * FROM\tpg_stat_activity", true},
		{"multiple spaces", "SELECT * FROM   pg_stat_activity", true},

		// Should not match
		{"no FROM", "SELECT 'pg_stat_activity'", false},
		{"in string literal", "SELECT 'text pg_stat_activity text'", false},
		{"different table", "SELECT * FROM pg_stat_statements", false},
		{"substring match", "SELECT * FROM pg_stat_activity_detail", false},
		{"prefix only", "SELECT * FROM my_pg_stat_activity", false},
		{"empty query", "", false},
		{"INSERT", "INSERT INTO pg_stat_activity VALUES (1)", false},
		{"CREATE VIEW", "CREATE VIEW v AS SELECT * FROM pg_stat_activity", true}, // this references FROM, so it matches
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchPgStatActivityQuery(tt.query)
			if got != tt.want {
				t.Errorf("matchPgStatActivityQuery(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

func TestPgStatActivityColumnsAndOIDs(t *testing.T) {
	// Verify that pgStatActivityColumns and pgStatActivityTypeOIDs are consistent
	if len(pgStatActivityTypeOIDs) != len(pgStatActivityColumns) {
		t.Fatalf("pgStatActivityTypeOIDs length %d != pgStatActivityColumns length %d",
			len(pgStatActivityTypeOIDs), len(pgStatActivityColumns))
	}

	for i, col := range pgStatActivityColumns {
		if pgStatActivityTypeOIDs[i] != col.oid {
			t.Errorf("pgStatActivityTypeOIDs[%d] = %d, want %d (column %s)",
				i, pgStatActivityTypeOIDs[i], col.oid, col.name)
		}
	}

	// Verify expected columns are present
	expectedColumns := map[string]bool{
		"datid": false, "datname": false, "pid": false, "usesysid": false,
		"usename": false, "application_name": false, "client_addr": false,
		"client_port": false, "backend_start": false, "state": false,
		"query": false, "backend_type": false, "worker_id": false,
	}
	for _, col := range pgStatActivityColumns {
		if _, ok := expectedColumns[col.name]; ok {
			expectedColumns[col.name] = true
		}
	}
	for name, found := range expectedColumns {
		if !found {
			t.Errorf("expected column %q not found in pgStatActivityColumns", name)
		}
	}
}

func TestConnectionRegistry(t *testing.T) {
	srv := &Server{
		conns: make(map[int32]*clientConn),
	}

	// Initially empty
	conns := srv.listConns()
	if len(conns) != 0 {
		t.Fatalf("expected 0 conns, got %d", len(conns))
	}

	// Register a connection
	c1 := &clientConn{pid: 100, username: "user1"}
	srv.registerConn(c1)
	conns = srv.listConns()
	if len(conns) != 1 {
		t.Fatalf("expected 1 conn, got %d", len(conns))
	}
	if conns[0].pid != 100 {
		t.Errorf("expected pid 100, got %d", conns[0].pid)
	}

	// Register a second connection
	c2 := &clientConn{pid: 200, username: "user2"}
	srv.registerConn(c2)
	conns = srv.listConns()
	if len(conns) != 2 {
		t.Fatalf("expected 2 conns, got %d", len(conns))
	}

	// Unregister first connection
	srv.unregisterConn(100)
	conns = srv.listConns()
	if len(conns) != 1 {
		t.Fatalf("expected 1 conn after unregister, got %d", len(conns))
	}
	if conns[0].pid != 200 {
		t.Errorf("expected remaining conn pid 200, got %d", conns[0].pid)
	}

	// Unregister non-existent pid (should not panic)
	srv.unregisterConn(999)
	conns = srv.listConns()
	if len(conns) != 1 {
		t.Fatalf("expected 1 conn after no-op unregister, got %d", len(conns))
	}

	// Unregister last connection
	srv.unregisterConn(200)
	conns = srv.listConns()
	if len(conns) != 0 {
		t.Fatalf("expected 0 conns after final unregister, got %d", len(conns))
	}
}

func TestConnectionRegistryOverwrite(t *testing.T) {
	// Verify that registering the same PID overwrites the previous entry
	srv := &Server{
		conns: make(map[int32]*clientConn),
	}

	c1 := &clientConn{pid: 100, username: "user1"}
	srv.registerConn(c1)

	c2 := &clientConn{pid: 100, username: "user2"}
	srv.registerConn(c2)

	conns := srv.listConns()
	if len(conns) != 1 {
		t.Fatalf("expected 1 conn, got %d", len(conns))
	}
	if conns[0].username != "user2" {
		t.Errorf("expected username 'user2' after overwrite, got %q", conns[0].username)
	}
}

func TestInitConnsMap(t *testing.T) {
	srv := &Server{}
	if srv.conns != nil {
		t.Fatal("expected nil conns before initConnsMap")
	}
	srv.initConnsMap()
	if srv.conns == nil {
		t.Fatal("expected non-nil conns after initConnsMap")
	}
	// Should be usable
	srv.registerConn(&clientConn{pid: 1})
	if len(srv.conns) != 1 {
		t.Fatalf("expected 1 conn, got %d", len(srv.conns))
	}
}
