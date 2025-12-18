package server

import (
	"testing"
)

// TestETLQueryPatterns tests query patterns commonly used by ETL tools like Fivetran.
// These patterns include:
// - Queries with leading comments (e.g., /* sync_id:abc123 */)
// - Schema and table creation/management
// - Metadata queries against information_schema and pg_catalog
// - Transaction handling
// - Data manipulation (INSERT, UPDATE, DELETE)

func TestETLCommentedQueries(t *testing.T) {
	// ETL tools often prefix queries with tracking comments
	tests := []struct {
		name           string
		query          string
		returnsResults bool
		commandType    string
	}{
		// Schema operations with comments
		{
			name:           "create schema with comment",
			query:          "/* sync_id:abc123 */ CREATE SCHEMA IF NOT EXISTS etl_destination",
			returnsResults: false,
			commandType:    "CREATE SCHEMA",
		},
		{
			name:           "drop schema with comment",
			query:          "/* sync_id:abc123 */ DROP SCHEMA IF EXISTS etl_destination CASCADE",
			returnsResults: false,
			commandType:    "DROP SCHEMA",
		},

		// Table operations with comments
		{
			name:           "create table with comment",
			query:          "/* sync_id:abc123 */ CREATE TABLE etl_destination.users (id INTEGER, name VARCHAR, created_at TIMESTAMP)",
			returnsResults: false,
			commandType:    "CREATE TABLE",
		},
		{
			name:           "drop table with comment",
			query:          "/* sync_id:abc123 */ DROP TABLE IF EXISTS etl_destination.users",
			returnsResults: false,
			commandType:    "DROP TABLE",
		},

		// Select queries with comments (metadata inspection)
		{
			name:           "select with comment",
			query:          "/* sync_id:abc123 */ SELECT * FROM information_schema.tables WHERE table_schema = 'etl_destination'",
			returnsResults: true,
			commandType:    "SELECT",
		},
		{
			name:           "select columns with comment",
			query:          "/* sync_id:abc123 */ SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'users'",
			returnsResults: true,
			commandType:    "SELECT",
		},

		// Data operations with comments
		{
			name:           "insert with comment",
			query:          "/* sync_id:abc123 */ INSERT INTO etl_destination.users (id, name) VALUES (1, 'test')",
			returnsResults: false,
			commandType:    "INSERT",
		},
		{
			name:           "update with comment",
			query:          "/* sync_id:abc123 */ UPDATE etl_destination.users SET name = 'updated' WHERE id = 1",
			returnsResults: false,
			commandType:    "UPDATE",
		},
		{
			name:           "delete with comment",
			query:          "/* sync_id:abc123 */ DELETE FROM etl_destination.users WHERE id = 1",
			returnsResults: false,
			commandType:    "DELETE",
		},

		// Transaction control with comments
		{
			name:           "begin with comment",
			query:          "/* sync_id:abc123 */ BEGIN",
			returnsResults: false,
			commandType:    "BEGIN",
		},
		{
			name:           "commit with comment",
			query:          "/* sync_id:abc123 */ COMMIT",
			returnsResults: false,
			commandType:    "COMMIT",
		},
		{
			name:           "rollback with comment",
			query:          "/* sync_id:abc123 */ ROLLBACK",
			returnsResults: false,
			commandType:    "ROLLBACK",
		},
	}

	c := &clientConn{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test queryReturnsResults
			result := queryReturnsResults(tt.query)
			if result != tt.returnsResults {
				t.Errorf("queryReturnsResults(%q) = %v, want %v", tt.query, result, tt.returnsResults)
			}

			// Test getCommandType
			cmdType := c.getCommandType(tt.query)
			if cmdType != tt.commandType {
				t.Errorf("getCommandType(%q) = %q, want %q", tt.query, cmdType, tt.commandType)
			}
		})
	}
}

func TestETLMetadataQueries(t *testing.T) {
	// ETL tools query metadata to understand schema structure
	tests := []struct {
		name           string
		query          string
		returnsResults bool
	}{
		// information_schema queries
		{
			name:           "list tables",
			query:          "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
			returnsResults: true,
		},
		{
			name:           "list columns",
			query:          "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'users'",
			returnsResults: true,
		},
		{
			name:           "check table exists",
			query:          "SELECT 1 FROM information_schema.tables WHERE table_schema = 'etl' AND table_name = 'sync_state' LIMIT 1",
			returnsResults: true,
		},

		// pg_catalog queries (commonly used by JDBC drivers)
		{
			name:           "pg_database query",
			query:          "SELECT datname FROM pg_catalog.pg_database WHERE datname = current_database()",
			returnsResults: true,
		},
		{
			name:           "pg_namespace query",
			query:          "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT LIKE 'pg_%'",
			returnsResults: true,
		},
		{
			name:           "pg_class query",
			query:          "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relnamespace = 2200",
			returnsResults: true,
		},
		{
			name:           "pg_attribute query",
			query:          "SELECT attname, atttypid FROM pg_catalog.pg_attribute WHERE attrelid = 12345",
			returnsResults: true,
		},

		// Common introspection patterns
		{
			name:           "get primary keys",
			query:          "SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_constraint c ON a.attrelid = c.conrelid WHERE c.contype = 'p'",
			returnsResults: true,
		},
		{
			name:           "get foreign keys",
			query:          "SELECT conname, confrelid FROM pg_catalog.pg_constraint WHERE contype = 'f'",
			returnsResults: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := queryReturnsResults(tt.query)
			if result != tt.returnsResults {
				t.Errorf("queryReturnsResults(%q) = %v, want %v", tt.query, result, tt.returnsResults)
			}
		})
	}
}

func TestETLBatchOperations(t *testing.T) {
	// ETL tools often use batch operations
	tests := []struct {
		name           string
		query          string
		returnsResults bool
		commandType    string
	}{
		// Multi-value INSERT
		{
			name:           "batch insert",
			query:          "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com'), (3, 'Charlie', 'charlie@example.com')",
			returnsResults: false,
			commandType:    "INSERT",
		},

		// TRUNCATE for full refresh
		{
			name:           "truncate table",
			query:          "TRUNCATE TABLE etl_destination.users",
			returnsResults: false,
			commandType:    "TRUNCATE TABLE",
		},

		// DELETE with subquery
		{
			name:           "delete with subquery",
			query:          "DELETE FROM users WHERE id IN (SELECT id FROM deleted_users)",
			returnsResults: false,
			commandType:    "DELETE",
		},

		// UPDATE with join pattern
		{
			name:           "update from staging",
			query:          "UPDATE users SET name = s.name FROM staging s WHERE users.id = s.id",
			returnsResults: false,
			commandType:    "UPDATE",
		},
	}

	c := &clientConn{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := queryReturnsResults(tt.query)
			if result != tt.returnsResults {
				t.Errorf("queryReturnsResults(%q) = %v, want %v", tt.query, result, tt.returnsResults)
			}

			cmdType := c.getCommandType(tt.query)
			if cmdType != tt.commandType {
				t.Errorf("getCommandType(%q) = %q, want %q", tt.query, cmdType, tt.commandType)
			}
		})
	}
}

func TestETLSchemaEvolution(t *testing.T) {
	// ETL tools handle schema changes
	tests := []struct {
		name           string
		query          string
		returnsResults bool
		commandType    string
	}{
		// Add column
		{
			name:           "add column",
			query:          "ALTER TABLE users ADD COLUMN phone VARCHAR",
			returnsResults: false,
			commandType:    "ALTER TABLE",
		},

		// Drop column
		{
			name:           "drop column",
			query:          "ALTER TABLE users DROP COLUMN deprecated_field",
			returnsResults: false,
			commandType:    "ALTER TABLE",
		},

		// Rename column
		{
			name:           "rename column",
			query:          "ALTER TABLE users RENAME COLUMN old_name TO new_name",
			returnsResults: false,
			commandType:    "ALTER TABLE",
		},

		// Change column type
		{
			name:           "alter column type",
			query:          "ALTER TABLE users ALTER COLUMN id TYPE BIGINT",
			returnsResults: false,
			commandType:    "ALTER TABLE",
		},
	}

	c := &clientConn{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := queryReturnsResults(tt.query)
			if result != tt.returnsResults {
				t.Errorf("queryReturnsResults(%q) = %v, want %v", tt.query, result, tt.returnsResults)
			}

			cmdType := c.getCommandType(tt.query)
			if cmdType != tt.commandType {
				t.Errorf("getCommandType(%q) = %q, want %q", tt.query, cmdType, tt.commandType)
			}
		})
	}
}

func TestETLCTEQueries(t *testing.T) {
	// ETL tools sometimes use CTEs for complex transformations
	tests := []struct {
		name           string
		query          string
		returnsResults bool
	}{
		{
			name: "simple CTE",
			query: `WITH active_users AS (
				SELECT * FROM users WHERE status = 'active'
			)
			SELECT * FROM active_users`,
			returnsResults: true,
		},
		{
			name: "CTE with comment",
			query: `/* sync_id:abc123 */ WITH recent_orders AS (
				SELECT * FROM orders WHERE created_at > '2024-01-01'
			)
			SELECT customer_id, COUNT(*) FROM recent_orders GROUP BY customer_id`,
			returnsResults: true,
		},
		{
			name: "multiple CTEs",
			query: `WITH
				users_cte AS (SELECT * FROM users),
				orders_cte AS (SELECT * FROM orders)
			SELECT u.name, COUNT(o.id)
			FROM users_cte u
			LEFT JOIN orders_cte o ON u.id = o.user_id
			GROUP BY u.name`,
			returnsResults: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := queryReturnsResults(tt.query)
			if result != tt.returnsResults {
				t.Errorf("queryReturnsResults(%q) = %v, want %v", tt.query, result, tt.returnsResults)
			}
		})
	}
}

// Note: pg_catalog query rewriting tests have been moved to transpiler/transpiler_test.go
// The transpiler package now handles all SQL rewriting via AST transformation.
