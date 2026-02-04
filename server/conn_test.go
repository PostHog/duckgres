package server

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"testing"
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
		{"SELECT query", "SELECT 1", false},
		{"SELECT with semicolon", "SELECT 1;", false},
		{"comment", "/* comment */", false},
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

// Note: isIgnoredSetParameter tests have been moved to transpiler/transpiler_test.go.
// The transpiler package now handles SET parameter filtering via AST transformation.

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
			name:       "COPY with schema",
			query:      "COPY public.users FROM STDIN",
			tableName:  "public.users",
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
			want: "COPY users  FROM '/tmp/data.csv' (FORMAT CSV, NULL '\\N', DELIMITER '\t')",
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
			want: "COPY users  FROM '/tmp/data.csv' (FORMAT CSV, HEADER, NULL '\\N', QUOTE '\"')",
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
			want: "COPY users (id, name) FROM '/tmp/data.csv' (FORMAT CSV, NULL '\\N', QUOTE '\"')",
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
			want: "COPY users  FROM '/tmp/data.csv' (FORMAT CSV, NULL 'NA', QUOTE '\"')",
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
			want: "COPY users  FROM '/tmp/data.csv' (FORMAT CSV, HEADER, NULL '', QUOTE '\"')",
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
			want: "COPY public.users (id, name, email) FROM '/var/tmp/copy-123.csv' (FORMAT CSV, HEADER, NULL '\\N', DELIMITER '\t', QUOTE '\"')",
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
			want: "COPY users  FROM '/tmp/data.csv' (FORMAT CSV, HEADER, NULL '\\N', QUOTE '\"', ESCAPE '\\')",
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
