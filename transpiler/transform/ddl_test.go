package transform

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/transpiler/backend"
)

// lakeDDLPolicy mirrors the lake (DuckLake/Iceberg) DDL policy: strip + hybrid
// warn/error.
func lakeDDLPolicy() backend.DDLPolicy {
	return backend.ForName(backend.Iceberg).DDL()
}

func runDDL(t *testing.T, policy backend.DDLPolicy, sql string) (*Result, bool) {
	t.Helper()
	tree, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q) error: %v", sql, err)
	}
	tr := NewDDLTransform(policy)
	result := &Result{}
	changed, err := tr.Transform(tree, result)
	if err != nil {
		t.Fatalf("Transform error: %v", err)
	}
	return result, changed
}

func TestDDL_ConstraintsWarnNotError(t *testing.T) {
	policy := lakeDDLPolicy()
	cases := []struct {
		name string
		sql  string
		want string // substring expected in a warning
	}{
		{"primary key", "CREATE TABLE t (id int PRIMARY KEY)", "PRIMARY KEY"},
		{"unique", "CREATE TABLE t (id int UNIQUE)", "UNIQUE"},
		{"check", "CREATE TABLE t (id int CHECK (id > 0))", "CHECK"},
		{"foreign key", "CREATE TABLE t (id int REFERENCES other(id))", "FOREIGN KEY"},
		{"table-level pk", "CREATE TABLE t (id int, PRIMARY KEY (id))", "PRIMARY KEY"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, _ := runDDL(t, policy, tc.sql)
			if result.Error != nil {
				t.Fatalf("expected no error, got %v", result.Error)
			}
			if len(result.Warnings) == 0 {
				t.Fatalf("expected a warning, got none")
			}
			joined := strings.Join(result.Warnings, "\n")
			if !strings.Contains(joined, tc.want) {
				t.Errorf("warning %q does not contain %q", joined, tc.want)
			}
			if !strings.Contains(joined, "not enforced") {
				t.Errorf("warning %q should explain the constraint is not enforced", joined)
			}
		})
	}
}

func TestDDL_SilentNullFeaturesError(t *testing.T) {
	policy := lakeDDLPolicy()
	cases := []struct {
		name string
		sql  string
	}{
		{"serial", "CREATE TABLE t (id serial)"},
		{"bigserial", "CREATE TABLE t (id bigserial, v text)"},
		{"smallserial", "CREATE TABLE t (id smallserial)"},
		{"generated stored", "CREATE TABLE t (a int, b int GENERATED ALWAYS AS (a * 2) STORED)"},
		{"default now()", "CREATE TABLE t (id int, ts timestamp DEFAULT now())"},
		{"default current_timestamp", "CREATE TABLE t (id int, ts timestamp DEFAULT CURRENT_TIMESTAMP)"},
		{"default expression", "CREATE TABLE t (id int, n int DEFAULT (1 + 2))"},
		{"default boolean", "CREATE TABLE t (id int, b boolean DEFAULT true)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, _ := runDDL(t, policy, tc.sql)
			if result.Error == nil {
				t.Fatalf("expected a feature-not-supported error, got none")
			}
			if got := transformErrSQLState(result.Error); got != "0A000" {
				t.Errorf("SQLSTATE = %q, want 0A000 (err: %v)", got, result.Error)
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
	tree, err := pg_query.Parse("CREATE TABLE t (id int NOT NULL)")
	if err != nil {
		t.Fatal(err)
	}
	tr := NewDDLTransform(policy)
	result := &Result{}
	if _, err := tr.Transform(tree, result); err != nil {
		t.Fatal(err)
	}
	deparsed, err := pg_query.Deparse(tree)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToUpper(deparsed), "NOT NULL") {
		t.Errorf("NOT NULL should be preserved, got %q", deparsed)
	}
}

func TestDDL_MemoryProfileUnaffected(t *testing.T) {
	policy := backend.ForName(backend.Memory).DDL()
	result, _ := runDDL(t, policy, "CREATE TABLE t (id serial PRIMARY KEY, ts timestamp DEFAULT now())")
	if result.Error != nil {
		t.Errorf("memory profile should not error on PK/serial/default now(), got %v", result.Error)
	}
	if len(result.Warnings) != 0 {
		t.Errorf("memory profile should not warn, got %v", result.Warnings)
	}
}

// TestDDL_DuckLakeStillStripsSilently locks that DuckLake keeps the historical
// silent-strip behavior (no warn/error) so sqlmesh/dbt-issued DDL still succeeds.
func TestDDL_DuckLakeStillStripsSilently(t *testing.T) {
	policy := backend.ForName(backend.DuckLake).DDL()
	result, _ := runDDL(t, policy, "CREATE TABLE t (id bigserial PRIMARY KEY, ts timestamp DEFAULT now())")
	if result.Error != nil {
		t.Errorf("DuckLake should not error on serial/default now(), got %v", result.Error)
	}
	if len(result.Warnings) != 0 {
		t.Errorf("DuckLake should not warn, got %v", result.Warnings)
	}
}

func TestDDL_AlterAddColumnSilentNullErrors(t *testing.T) {
	policy := lakeDDLPolicy()
	cases := []string{
		"ALTER TABLE t ADD COLUMN id serial",
		"ALTER TABLE t ADD COLUMN ts timestamp DEFAULT now()",
		"ALTER TABLE t ADD COLUMN b int GENERATED ALWAYS AS (1) STORED",
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			result, _ := runDDL(t, policy, sql)
			if result.Error == nil {
				t.Fatalf("expected error for %q", sql)
			}
			if got := transformErrSQLState(result.Error); got != "0A000" {
				t.Errorf("SQLSTATE = %q, want 0A000", got)
			}
		})
	}
}

func TestDDL_AlterDropColumnWarns(t *testing.T) {
	policy := lakeDDLPolicy()
	result, _ := runDDL(t, policy, "ALTER TABLE t DROP COLUMN x")
	if result.Error != nil {
		t.Fatalf("DROP COLUMN should not error, got %v", result.Error)
	}
	if len(result.Warnings) == 0 || !strings.Contains(strings.Join(result.Warnings, "\n"), "DROP COLUMN") {
		t.Errorf("expected a DROP COLUMN warning, got %v", result.Warnings)
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
