package server

import (
	"testing"
)

// Note: DDL rewriting tests (rewriteForDuckLake, isNoOpCommand, getNoOpCommandTag)
// have been moved to transpiler/transpiler_test.go.
// The transpiler package now handles all SQL rewriting via AST transformation.

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
