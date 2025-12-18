package server

import (
	"testing"
)

func TestRewriteForDuckLake(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Basic constraint stripping
		{
			name:     "strip PRIMARY KEY inline",
			input:    "CREATE TABLE users (id INTEGER PRIMARY KEY)",
			expected: "CREATE TABLE users (id INTEGER)",
		},
		{
			name:     "strip UNIQUE inline",
			input:    "CREATE TABLE users (email VARCHAR UNIQUE)",
			expected: "CREATE TABLE users (email VARCHAR)",
		},
		{
			name:     "strip multiple inline constraints",
			input:    "CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR UNIQUE NOT NULL)",
			expected: "CREATE TABLE users (id INTEGER, email VARCHAR NOT NULL)",
		},
		{
			name:     "strip REFERENCES/FOREIGN KEY inline",
			input:    "CREATE TABLE orders (user_id INTEGER REFERENCES users(id))",
			expected: "CREATE TABLE orders (user_id INTEGER)",
		},
		{
			name:     "strip REFERENCES with ON DELETE CASCADE",
			input:    "CREATE TABLE orders (user_id INTEGER REFERENCES users(id) ON DELETE CASCADE)",
			expected: "CREATE TABLE orders (user_id INTEGER)",
		},
		{
			name:     "strip CHECK constraint",
			input:    "CREATE TABLE users (status VARCHAR CHECK (status IN ('active', 'inactive')))",
			expected: "CREATE TABLE users (status VARCHAR)",
		},
		// SERIAL types
		{
			name:     "convert SERIAL to INTEGER",
			input:    "CREATE TABLE users (id SERIAL)",
			expected: "CREATE TABLE users (id INTEGER)",
		},
		{
			name:     "convert BIGSERIAL to BIGINT",
			input:    "CREATE TABLE users (id BIGSERIAL)",
			expected: "CREATE TABLE users (id BIGINT)",
		},
		{
			name:     "convert SMALLSERIAL to SMALLINT",
			input:    "CREATE TABLE users (id SMALLSERIAL)",
			expected: "CREATE TABLE users (id SMALLINT)",
		},
		{
			name:     "SERIAL with PRIMARY KEY",
			input:    "CREATE TABLE users (id SERIAL PRIMARY KEY)",
			expected: "CREATE TABLE users (id INTEGER)",
		},
		// DEFAULT now() stripping
		{
			name:     "strip DEFAULT now()",
			input:    "CREATE TABLE users (created_at TIMESTAMP DEFAULT now())",
			expected: "CREATE TABLE users (created_at TIMESTAMP)",
		},
		{
			name:     "strip DEFAULT current_timestamp",
			input:    "CREATE TABLE users (created_at TIMESTAMP DEFAULT current_timestamp)",
			expected: "CREATE TABLE users (created_at TIMESTAMP)",
		},
		{
			name:     "strip DEFAULT CURRENT_TIMESTAMP",
			input:    "CREATE TABLE users (created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
			expected: "CREATE TABLE users (created_at TIMESTAMP)",
		},
		{
			name:     "keep literal DEFAULT",
			input:    "CREATE TABLE users (status VARCHAR DEFAULT 'active')",
			expected: "CREATE TABLE users (status VARCHAR DEFAULT 'active')",
		},
		// Table-level constraints
		{
			name:     "strip table-level PRIMARY KEY",
			input:    "CREATE TABLE users (id INTEGER, name VARCHAR, PRIMARY KEY (id))",
			expected: "CREATE TABLE users (id INTEGER, name VARCHAR)",
		},
		{
			name:     "strip table-level UNIQUE",
			input:    "CREATE TABLE users (id INTEGER, email VARCHAR, UNIQUE (email))",
			expected: "CREATE TABLE users (id INTEGER, email VARCHAR)",
		},
		{
			name:     "strip table-level FOREIGN KEY",
			input:    "CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))",
			expected: "CREATE TABLE orders (id INTEGER, user_id INTEGER)",
		},
		{
			name:     "strip named constraint",
			input:    "CREATE TABLE users (id INTEGER, CONSTRAINT users_pkey PRIMARY KEY (id))",
			expected: "CREATE TABLE users (id INTEGER)",
		},
		// TEMPORARY TABLE
		{
			name:     "handle CREATE TEMPORARY TABLE",
			input:    "CREATE TEMPORARY TABLE tmp (id SERIAL PRIMARY KEY)",
			expected: "CREATE TEMPORARY TABLE tmp (id INTEGER)",
		},
		{
			name:     "handle CREATE TEMP TABLE",
			input:    "CREATE TEMP TABLE tmp (id INTEGER PRIMARY KEY)",
			expected: "CREATE TEMP TABLE tmp (id INTEGER)",
		},
		// Non-CREATE TABLE queries should pass through unchanged
		{
			name:     "SELECT passes through",
			input:    "SELECT * FROM users WHERE id = 1",
			expected: "SELECT * FROM users WHERE id = 1",
		},
		{
			name:     "INSERT passes through",
			input:    "INSERT INTO users (name) VALUES ('test')",
			expected: "INSERT INTO users (name) VALUES ('test')",
		},
		{
			name:     "ALTER TABLE passes through",
			input:    "ALTER TABLE users ADD COLUMN age INTEGER",
			expected: "ALTER TABLE users ADD COLUMN age INTEGER",
		},
		// Complex real-world case
		{
			name: "complex table with multiple constraints",
			input: `CREATE TABLE orders (
				id BIGSERIAL PRIMARY KEY,
				user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
				email VARCHAR UNIQUE NOT NULL,
				status VARCHAR CHECK (status IN ('pending', 'complete')),
				created_at TIMESTAMP DEFAULT now(),
				CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES users(id)
			)`,
			expected: `CREATE TABLE orders (
				id BIGINT,
				user_id INTEGER,
				email VARCHAR NOT NULL,
				status VARCHAR,
				created_at TIMESTAMP
			)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rewriteForDuckLake(tt.input)
			if result != tt.expected {
				t.Errorf("rewriteForDuckLake(%q)\ngot:      %q\nexpected: %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsNoOpCommand(t *testing.T) {
	tests := []struct {
		cmdType  string
		expected bool
	}{
		// No-op commands
		{"CREATE INDEX", true},
		{"DROP INDEX", true},
		{"REINDEX", true},
		{"CLUSTER", true},
		{"VACUUM", true},
		{"ANALYZE", true},
		{"GRANT", true},
		{"REVOKE", true},
		{"COMMENT", true},
		{"REFRESH", true},
		{"ALTER TABLE ADD CONSTRAINT", true},
		// Not no-op commands
		{"SELECT", false},
		{"INSERT", false},
		{"UPDATE", false},
		{"DELETE", false},
		{"CREATE TABLE", false},
		{"ALTER TABLE", false},
		{"DROP TABLE", false},
		{"BEGIN", false},
		{"COMMIT", false},
		{"ROLLBACK", false},
	}

	for _, tt := range tests {
		t.Run(tt.cmdType, func(t *testing.T) {
			result := isNoOpCommand(tt.cmdType)
			if result != tt.expected {
				t.Errorf("isNoOpCommand(%q) = %v, expected %v", tt.cmdType, result, tt.expected)
			}
		})
	}
}

func TestGetNoOpCommandTag(t *testing.T) {
	tests := []struct {
		cmdType  string
		expected string
	}{
		{"CREATE INDEX", "CREATE INDEX"},
		{"DROP INDEX", "DROP INDEX"},
		{"ALTER TABLE ADD CONSTRAINT", "ALTER TABLE"},
		{"VACUUM", "VACUUM"},
	}

	for _, tt := range tests {
		t.Run(tt.cmdType, func(t *testing.T) {
			result := getNoOpCommandTag(tt.cmdType)
			if result != tt.expected {
				t.Errorf("getNoOpCommandTag(%q) = %q, expected %q", tt.cmdType, result, tt.expected)
			}
		})
	}
}

func TestGetCommandTypeWithConstraints(t *testing.T) {
	c := &clientConn{}
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "ALTER TABLE ADD CONSTRAINT",
			query:    "ALTER TABLE users ADD CONSTRAINT users_pkey PRIMARY KEY (id)",
			expected: "ALTER TABLE ADD CONSTRAINT",
		},
		{
			name:     "ALTER TABLE ADD PRIMARY KEY",
			query:    "ALTER TABLE users ADD PRIMARY KEY (id)",
			expected: "ALTER TABLE ADD CONSTRAINT",
		},
		{
			name:     "ALTER TABLE ADD UNIQUE",
			query:    "ALTER TABLE users ADD UNIQUE (email)",
			expected: "ALTER TABLE ADD CONSTRAINT",
		},
		{
			name:     "ALTER TABLE ADD FOREIGN KEY",
			query:    "ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id)",
			expected: "ALTER TABLE ADD CONSTRAINT",
		},
		{
			name:     "ALTER TABLE ADD CHECK",
			query:    "ALTER TABLE users ADD CHECK (age > 0)",
			expected: "ALTER TABLE ADD CONSTRAINT",
		},
		{
			name:     "ALTER TABLE ADD COLUMN (not constraint)",
			query:    "ALTER TABLE users ADD COLUMN age INTEGER",
			expected: "ALTER TABLE",
		},
		{
			name:     "ALTER TABLE DROP COLUMN",
			query:    "ALTER TABLE users DROP COLUMN age",
			expected: "ALTER TABLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := c.getCommandType(tt.query)
			if result != tt.expected {
				t.Errorf("getCommandType(%q) = %q, expected %q", tt.query, result, tt.expected)
			}
		})
	}
}
