package transform

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func TestOnConflictTransform_Name(t *testing.T) {
	tr := NewOnConflictTransform()
	if tr.Name() != "onconflict" {
		t.Errorf("Name() = %q, want %q", tr.Name(), "onconflict")
	}
}

func TestOnConflictTransform_NonRejectingMode(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(false)

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "DO UPDATE passes through",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
		},
		{
			name:  "DO NOTHING passes through",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO NOTHING",
		},
		{
			name:  "ON CONSTRAINT passes through",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING",
		},
		{
			name:  "targetless DO NOTHING passes through",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT DO NOTHING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := pg_query.Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			result := &Result{}
			changed, err := tr.Transform(tree, result)
			if err != nil {
				t.Fatalf("Transform error: %v", err)
			}

			if changed {
				t.Error("Transform should not change SQL when ON CONFLICT rejection is disabled")
			}
			if result.Error != nil {
				t.Fatalf("Transform returned unexpected error: %v", result.Error)
			}
			if tree.Stmts[0].Stmt.GetInsertStmt() == nil {
				t.Error("Statement should remain an INSERT")
			}
		})
	}
}

func TestOnConflictTransform_RejectingModeRejectsAllOnConflict(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "DO UPDATE",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
		},
		{
			name:  "DO NOTHING",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO NOTHING",
		},
		{
			name:  "multiple values",
			input: "INSERT INTO users (id, name) VALUES (1, 'first'), (2, 'second') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
		},
		{
			name:  "INSERT SELECT",
			input: "INSERT INTO users (id, name) SELECT id, name FROM staging_users ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
		},
		{
			name:  "ON CONSTRAINT",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING",
		},
		{
			name:  "expression target",
			input: "INSERT INTO users (id, email) VALUES (1, 'a@example.com') ON CONFLICT ((lower(email))) DO NOTHING",
		},
		{
			name:  "partial target",
			input: "INSERT INTO users (id, email, active) VALUES (1, 'a@example.com', true) ON CONFLICT (email) WHERE active DO NOTHING",
		},
		{
			name:  "targetless DO NOTHING",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT DO NOTHING",
		},
		{
			name:  "DEFAULT VALUES",
			input: "INSERT INTO users DEFAULT VALUES ON CONFLICT DO NOTHING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := pg_query.Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			result := &Result{}
			changed, err := tr.Transform(tree, result)
			if err != nil {
				t.Fatalf("Transform error: %v", err)
			}

			if changed {
				t.Error("Transform should reject without rewriting SQL")
			}
			if result.Error == nil {
				t.Fatal("expected ON CONFLICT to be rejected")
			}
			if got := transformErrSQLState(result.Error); got != "0A000" {
				t.Fatalf("SQLSTATE = %q, want 0A000", got)
			}
			if !strings.Contains(result.Error.Error(), "ON CONFLICT is not supported") {
				t.Fatalf("error = %q, want ON CONFLICT unsupported message", result.Error.Error())
			}
		})
	}
}

func TestOnConflictTransform_RejectingModeRejectsNestedOnConflict(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	tests := []struct {
		name  string
		input string
	}{
		{
			name: "writable CTE under SELECT",
			input: `WITH up AS (
				INSERT INTO users (id, name) VALUES (1, 'test')
				ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
				RETURNING id
			) SELECT * FROM up`,
		},
		{
			name: "writable CTE under INSERT",
			input: `WITH up AS (
				INSERT INTO users (id, name) VALUES (1, 'test')
				ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
				RETURNING id
			) INSERT INTO audit_users (id) SELECT id FROM up`,
		},
		{
			name: "writable CTE under UPDATE",
			input: `WITH up AS (
				INSERT INTO users (id, name) VALUES (1, 'test')
				ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
				RETURNING id
			) UPDATE audit_users SET touched = true FROM up WHERE audit_users.id = up.id`,
		},
		{
			name: "writable CTE under DELETE",
			input: `WITH up AS (
				INSERT INTO users (id, name) VALUES (1, 'test')
				ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
				RETURNING id
			) DELETE FROM audit_users USING up WHERE audit_users.id = up.id`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := pg_query.Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			result := &Result{}
			changed, err := tr.Transform(tree, result)
			if err != nil {
				t.Fatalf("Transform error: %v", err)
			}

			if changed {
				t.Error("Transform should reject without rewriting SQL")
			}
			if result.Error == nil {
				t.Fatal("expected nested ON CONFLICT to be rejected")
			}
			if got := transformErrSQLState(result.Error); got != "0A000" {
				t.Fatalf("SQLSTATE = %q, want 0A000", got)
			}
		})
	}
}

func TestOnConflictTransform_NoTransformCases(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "INSERT without ON CONFLICT",
			input: "INSERT INTO users (id, name) VALUES (1, 'test')",
		},
		{
			name:  "SELECT statement",
			input: "SELECT * FROM users",
		},
		{
			name:  "UPDATE statement",
			input: "UPDATE users SET name = 'test' WHERE id = 1",
		},
		{
			name:  "DELETE statement",
			input: "DELETE FROM users WHERE id = 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := pg_query.Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			result := &Result{}
			changed, err := tr.Transform(tree, result)
			if err != nil {
				t.Fatalf("Transform error: %v", err)
			}

			if changed {
				t.Error("Transform should not change SQL without ON CONFLICT")
			}
			if result.Error != nil {
				t.Fatalf("Transform returned unexpected error: %v", result.Error)
			}
		})
	}
}
