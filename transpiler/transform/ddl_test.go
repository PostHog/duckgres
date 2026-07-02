package transform

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/transpiler/backend"
)

// lakeDDLPolicy mirrors the lake (DuckLake) DDL policy: silent strip/rewrite.
func lakeDDLPolicy() backend.DDLPolicy {
	return backend.ForName(backend.DuckLake).DDL()
}

func runDDL(t *testing.T, policy backend.DDLPolicy, sql string) (*Result, string) {
	t.Helper()
	tree, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q) error: %v", sql, err)
	}
	tr := NewDDLTransform(policy)
	result := &Result{}
	if _, err := tr.Transform(tree, result); err != nil {
		t.Fatalf("Transform error: %v", err)
	}
	deparsed, err := pg_query.Deparse(tree)
	if err != nil {
		t.Fatalf("Deparse error: %v", err)
	}
	return result, deparsed
}

func TestDDL_ConstraintsStrippedSilently(t *testing.T) {
	policy := lakeDDLPolicy()
	cases := []struct {
		name string
		sql  string
		gone string // substring that must be stripped from the deparsed SQL
	}{
		{"primary key", "CREATE TABLE t (id int PRIMARY KEY)", "PRIMARY KEY"},
		{"unique", "CREATE TABLE t (id int UNIQUE)", "UNIQUE"},
		{"check", "CREATE TABLE t (id int CHECK (id > 0))", "CHECK"},
		{"foreign key", "CREATE TABLE t (id int REFERENCES other(id))", "REFERENCES"},
		{"table-level pk", "CREATE TABLE t (id int, PRIMARY KEY (id))", "PRIMARY KEY"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, deparsed := runDDL(t, policy, tc.sql)
			if result.Error != nil {
				t.Fatalf("expected no error, got %v", result.Error)
			}
			if strings.Contains(strings.ToUpper(deparsed), tc.gone) {
				t.Errorf("constraint not stripped: %q", deparsed)
			}
		})
	}
}

func TestDDL_SilentNullFeaturesStripped(t *testing.T) {
	policy := lakeDDLPolicy()
	cases := []struct {
		name string
		sql  string
		gone string // substring that must be gone from the deparsed SQL
	}{
		{"serial", "CREATE TABLE t (id serial)", "serial"},
		{"bigserial", "CREATE TABLE t (id bigserial, v text)", "serial"},
		{"smallserial", "CREATE TABLE t (id smallserial)", "serial"},
		{"generated stored", "CREATE TABLE t (a int, b int GENERATED ALWAYS AS (a * 2) STORED)", "generated"},
		{"default now()", "CREATE TABLE t (id int, ts timestamp DEFAULT now())", "default"},
		{"default current_timestamp", "CREATE TABLE t (id int, ts timestamp DEFAULT CURRENT_TIMESTAMP)", "default"},
		{"default expression", "CREATE TABLE t (id int, n int DEFAULT (1 + 2))", "default"},
		{"default boolean", "CREATE TABLE t (id int, b boolean DEFAULT true)", "default"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, deparsed := runDDL(t, policy, tc.sql)
			if result.Error != nil {
				t.Fatalf("expected silent strip/rewrite, got error %v", result.Error)
			}
			if strings.Contains(strings.ToLower(deparsed), tc.gone) {
				t.Errorf("%q not stripped/rewritten: %q", tc.gone, deparsed)
			}
		})
	}
}

func TestDDL_SerialRewrittenToInteger(t *testing.T) {
	policy := lakeDDLPolicy()
	cases := []struct {
		sql  string
		want string
	}{
		{"CREATE TABLE t (id serial)", "int"},
		{"CREATE TABLE t (id bigserial)", "int8"},
		{"CREATE TABLE t (id smallserial)", "int2"},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			result, deparsed := runDDL(t, policy, tc.sql)
			if result.Error != nil {
				t.Fatalf("expected no error, got %v", result.Error)
			}
			if !strings.Contains(strings.ToLower(deparsed), tc.want) {
				t.Errorf("expected serial rewritten to %q, got %q", tc.want, deparsed)
			}
		})
	}
}

func TestDDL_SafeDefaultsPreserved(t *testing.T) {
	policy := lakeDDLPolicy()
	cases := []struct {
		name string
		sql  string
	}{
		{"default null", "CREATE TABLE t (id int, n int DEFAULT NULL)"},
		{"default int literal", "CREATE TABLE t (id int, n int DEFAULT 5)"},
		{"default string literal", "CREATE TABLE t (id int, s text DEFAULT 'x')"},
		{"not null", "CREATE TABLE t (id int NOT NULL)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, _ := runDDL(t, policy, tc.sql)
			if result.Error != nil {
				t.Fatalf("expected no error for %q, got %v", tc.sql, result.Error)
			}
		})
	}
}

// TestDDL_NotNullPreserved locks the invariant that NOT NULL is not stripped on
// a lake catalog (it is the one constraint that is actually enforced).
func TestDDL_NotNullPreserved(t *testing.T) {
	policy := lakeDDLPolicy()
	result, deparsed := runDDL(t, policy, "CREATE TABLE t (id int NOT NULL)")
	if result.Error != nil {
		t.Fatal(result.Error)
	}
	if !strings.Contains(strings.ToUpper(deparsed), "NOT NULL") {
		t.Errorf("NOT NULL should be preserved, got %q", deparsed)
	}
}

func TestDDL_MemoryProfileUnaffected(t *testing.T) {
	policy := backend.ForName(backend.Memory).DDL()
	result, deparsed := runDDL(t, policy, "CREATE TABLE t (id serial PRIMARY KEY, ts timestamp DEFAULT now())")
	if result.Error != nil {
		t.Errorf("memory profile should not error on PK/serial/default now(), got %v", result.Error)
	}
	if !strings.Contains(strings.ToUpper(deparsed), "PRIMARY KEY") {
		t.Errorf("memory profile should preserve PRIMARY KEY, got %q", deparsed)
	}
}

// TestDDL_DuckLakeStripsSilently locks that DuckLake keeps the historical
// silent-strip behavior (no warn/error) so sqlmesh/dbt-issued DDL still succeeds.
func TestDDL_DuckLakeStripsSilently(t *testing.T) {
	policy := backend.ForName(backend.DuckLake).DDL()
	result, _ := runDDL(t, policy, "CREATE TABLE t (id bigserial PRIMARY KEY, ts timestamp DEFAULT now())")
	if result.Error != nil {
		t.Errorf("DuckLake should not error on serial/default now(), got %v", result.Error)
	}
}

func TestDDL_AlterAddColumnSilentNullStripped(t *testing.T) {
	policy := lakeDDLPolicy()
	cases := []struct {
		sql  string
		gone string
	}{
		{"ALTER TABLE t ADD COLUMN id serial", "serial"},
		{"ALTER TABLE t ADD COLUMN ts timestamp DEFAULT now()", "default"},
		{"ALTER TABLE t ADD COLUMN b int GENERATED ALWAYS AS (1) STORED", "generated"},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			result, deparsed := runDDL(t, policy, tc.sql)
			if result.Error != nil {
				t.Fatalf("expected silent strip/rewrite for %q, got error %v", tc.sql, result.Error)
			}
			if strings.Contains(strings.ToLower(deparsed), tc.gone) {
				t.Errorf("%q not stripped/rewritten: %q", tc.gone, deparsed)
			}
		})
	}
}

func TestDDL_AlterColumnTypeUsingRejected(t *testing.T) {
	policy := lakeDDLPolicy()
	result, _ := runDDL(t, policy, "ALTER TABLE t ALTER COLUMN c TYPE integer USING c::integer")
	if result.Error == nil {
		t.Fatal("expected a feature-not-supported error for ALTER COLUMN TYPE ... USING")
	}
	if got := transformErrSQLState(result.Error); got != "0A000" {
		t.Errorf("SQLSTATE = %q, want 0A000 (err: %v)", got, result.Error)
	}
}

func TestDDL_AlterDropColumnPassesThrough(t *testing.T) {
	policy := lakeDDLPolicy()
	result, deparsed := runDDL(t, policy, "ALTER TABLE t DROP COLUMN x")
	if result.Error != nil {
		t.Fatalf("DROP COLUMN should not error, got %v", result.Error)
	}
	// pg_query deparses "DROP COLUMN x" as "DROP x"; both are the same command.
	if !strings.Contains(strings.ToUpper(deparsed), "DROP X") {
		t.Errorf("DROP COLUMN should pass through, got %q", deparsed)
	}
}

// transformErrSQLState extracts the SQLSTATE from a transform error.
func transformErrSQLState(err error) string {
	type sqlStater interface{ SQLState() string }
	if s, ok := err.(sqlStater); ok {
		return s.SQLState()
	}
	return ""
}
