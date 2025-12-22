package transpiler

import (
	"strings"
	"testing"
)

func TestTranspile_PassThrough(t *testing.T) {
	// Simple queries should pass through mostly unchanged
	tests := []struct {
		name  string
		input string
	}{
		{"simple select", "SELECT 1"},
		{"select from table", "SELECT * FROM users"},
		{"select with where", "SELECT * FROM users WHERE id = 1"},
		{"insert", "INSERT INTO users (name) VALUES ('test')"},
		{"update", "UPDATE users SET name = 'test' WHERE id = 1"},
		{"delete", "DELETE FROM users WHERE id = 1"},
		{"create schema", "CREATE SCHEMA test"},
		{"drop table", "DROP TABLE users"},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if result.SQL == "" {
				t.Errorf("Transpile(%q) returned empty SQL", tt.input)
			}
			if result.IsNoOp {
				t.Errorf("Transpile(%q) should not be a no-op", tt.input)
			}
		})
	}
}

func TestTranspile_PgCatalog(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "pg_catalog.pg_class -> pg_class_full",
			input:    "SELECT * FROM pg_catalog.pg_class",
			contains: "pg_class_full",
			excludes: "pg_catalog",
		},
		{
			name:     "pg_catalog.pg_database -> pg_database",
			input:    "SELECT * FROM pg_catalog.pg_database",
			contains: "pg_database",
			excludes: "pg_catalog",
		},
		{
			name:     "pg_catalog function prefix stripped",
			input:    "SELECT pg_catalog.pg_get_userbyid(1)",
			contains: "pg_get_userbyid",
			excludes: "pg_catalog.pg_get_userbyid",
		},
		{
			name:     "format_type function",
			input:    "SELECT pg_catalog.format_type(23, -1)",
			contains: "format_type",
			excludes: "pg_catalog.format_type",
		},
		{
			name:     "string literal NOT rewritten",
			input:    "SELECT 'pg_catalog.pg_class' AS name",
			contains: "pg_catalog.pg_class",
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if tt.contains != "" && !strings.Contains(result.SQL, tt.contains) {
				t.Errorf("Transpile(%q) = %q, should contain %q", tt.input, result.SQL, tt.contains)
			}
			if tt.excludes != "" && strings.Contains(result.SQL, tt.excludes) {
				t.Errorf("Transpile(%q) = %q, should NOT contain %q", tt.input, result.SQL, tt.excludes)
			}
		})
	}
}

func TestTranspile_InformationSchema(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "information_schema.columns -> compat view",
			input:    "SELECT * FROM information_schema.columns",
			contains: "information_schema_columns_compat",
			excludes: "information_schema.columns",
		},
		{
			name:     "information_schema.tables -> compat view",
			input:    "SELECT * FROM information_schema.tables",
			contains: "information_schema_tables_compat",
			excludes: "information_schema.tables",
		},
		{
			name:     "information_schema.schemata -> compat view",
			input:    "SELECT * FROM information_schema.schemata",
			contains: "information_schema_schemata_compat",
			excludes: "information_schema.schemata",
		},
		{
			name:     "INFORMATION_SCHEMA.COLUMNS uppercase -> compat view",
			input:    "SELECT * FROM INFORMATION_SCHEMA.COLUMNS",
			contains: "information_schema_columns_compat",
			excludes: "information_schema.columns",
		},
		{
			name:     "aliased information_schema query",
			input:    "SELECT c.column_name FROM information_schema.columns c WHERE c.table_name = 'test'",
			contains: "information_schema_columns_compat",
			excludes: "information_schema.columns",
		},
		{
			name:     "string literal NOT rewritten",
			input:    "SELECT 'information_schema.columns' AS name",
			contains: "information_schema.columns",
		},
		{
			name:     "unmapped information_schema table passes through",
			input:    "SELECT * FROM information_schema.routines",
			contains: "information_schema.routines",
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if tt.contains != "" && !strings.Contains(result.SQL, tt.contains) {
				t.Errorf("Transpile(%q) = %q, should contain %q", tt.input, result.SQL, tt.contains)
			}
			if tt.excludes != "" && strings.Contains(result.SQL, tt.excludes) {
				t.Errorf("Transpile(%q) = %q, should NOT contain %q", tt.input, result.SQL, tt.excludes)
			}
		})
	}
}

func TestTranspile_InformationSchema_DuckLakeMode(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{
			name:     "information_schema.columns with DuckLake -> main qualified",
			input:    "SELECT * FROM information_schema.columns",
			contains: "main.information_schema_columns_compat",
		},
		{
			name:     "information_schema.tables with DuckLake -> main qualified",
			input:    "SELECT * FROM information_schema.tables",
			contains: "main.information_schema_tables_compat",
		},
	}

	tr := New(Config{DuckLakeMode: true})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if tt.contains != "" && !strings.Contains(result.SQL, tt.contains) {
				t.Errorf("Transpile(%q) = %q, should contain %q", tt.input, result.SQL, tt.contains)
			}
		})
	}
}

func TestTranspile_TypeCast(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "regtype cast",
			input:    "SELECT typname::pg_catalog.regtype FROM pg_type",
			contains: "varchar",
			excludes: "regtype",
		},
		{
			name:     "regclass cast",
			input:    "SELECT oid::pg_catalog.regclass FROM pg_class",
			contains: "varchar",
			excludes: "regclass",
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			lowerSQL := strings.ToLower(result.SQL)
			if tt.contains != "" && !strings.Contains(lowerSQL, strings.ToLower(tt.contains)) {
				t.Errorf("Transpile(%q) = %q, should contain %q", tt.input, result.SQL, tt.contains)
			}
			if tt.excludes != "" && strings.Contains(lowerSQL, strings.ToLower(tt.excludes)) {
				t.Errorf("Transpile(%q) = %q, should NOT contain %q", tt.input, result.SQL, tt.excludes)
			}
		})
	}
}

func TestTranspile_Version(t *testing.T) {
	tr := New(DefaultConfig())

	result, err := tr.Transpile("SELECT version()")
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}

	// version() should be replaced with a literal string
	if !strings.Contains(result.SQL, "PostgreSQL") {
		t.Errorf("version() should be replaced with PostgreSQL version string, got: %q", result.SQL)
	}
}

func TestTranspile_DDL_DuckLakeMode(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "strip PRIMARY KEY inline",
			input:    "CREATE TABLE users (id INTEGER PRIMARY KEY)",
			excludes: "PRIMARY KEY",
		},
		{
			name:     "strip UNIQUE inline",
			input:    "CREATE TABLE users (email VARCHAR UNIQUE)",
			excludes: "UNIQUE",
		},
		{
			name:     "strip REFERENCES",
			input:    "CREATE TABLE orders (user_id INTEGER REFERENCES users(id))",
			excludes: "REFERENCES",
		},
		{
			name:     "convert SERIAL to INTEGER",
			input:    "CREATE TABLE users (id SERIAL)",
			excludes: "SERIAL",
		},
		{
			name:     "convert BIGSERIAL to BIGINT",
			input:    "CREATE TABLE users (id BIGSERIAL)",
			excludes: "BIGSERIAL",
		},
		{
			name:     "strip DEFAULT now()",
			input:    "CREATE TABLE users (created_at TIMESTAMP DEFAULT now())",
			excludes: "now()",
		},
		{
			name:     "keep literal DEFAULT",
			input:    "CREATE TABLE users (status VARCHAR DEFAULT 'active')",
			contains: "active",
		},
		{
			name:     "strip table-level PRIMARY KEY",
			input:    "CREATE TABLE users (id INTEGER, name VARCHAR, PRIMARY KEY (id))",
			excludes: "PRIMARY KEY",
		},
		{
			name:     "strip table-level FOREIGN KEY",
			input:    "CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))",
			excludes: "FOREIGN KEY",
		},
	}

	tr := New(Config{DuckLakeMode: true})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			upperSQL := strings.ToUpper(result.SQL)
			if tt.contains != "" && !strings.Contains(upperSQL, strings.ToUpper(tt.contains)) {
				t.Errorf("Transpile(%q) = %q, should contain %q", tt.input, result.SQL, tt.contains)
			}
			if tt.excludes != "" && strings.Contains(upperSQL, strings.ToUpper(tt.excludes)) {
				t.Errorf("Transpile(%q) = %q, should NOT contain %q", tt.input, result.SQL, tt.excludes)
			}
		})
	}
}

func TestTranspile_DDL_NoOps(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		noOpTag string
	}{
		{"CREATE INDEX", "CREATE INDEX idx_name ON users (name)", "CREATE INDEX"},
		{"DROP INDEX", "DROP INDEX idx_name", "DROP INDEX"},
		{"VACUUM", "VACUUM users", "VACUUM"},
		{"GRANT", "GRANT SELECT ON users TO public", "GRANT"},
		{"REVOKE", "REVOKE SELECT ON users FROM public", "REVOKE"},
	}

	tr := New(Config{DuckLakeMode: true})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if !result.IsNoOp {
				t.Errorf("Transpile(%q) should be a no-op", tt.input)
			}
			if result.NoOpTag != tt.noOpTag {
				t.Errorf("Transpile(%q) NoOpTag = %q, want %q", tt.input, result.NoOpTag, tt.noOpTag)
			}
		})
	}
}

func TestTranspile_Placeholders(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		paramCount int
	}{
		{"no placeholders", "SELECT * FROM users", 0},
		{"single placeholder", "SELECT * FROM users WHERE id = $1", 1},
		{"multiple placeholders", "SELECT * FROM users WHERE id = $1 AND name = $2", 2},
		{"placeholder in INSERT", "INSERT INTO users (name) VALUES ($1)", 1},
		{"placeholder in UPDATE", "UPDATE users SET name = $1 WHERE id = $2", 2},
	}

	tr := New(Config{ConvertPlaceholders: true})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if result.ParamCount != tt.paramCount {
				t.Errorf("Transpile(%q) ParamCount = %d, want %d", tt.input, result.ParamCount, tt.paramCount)
			}
		})
	}
}

func TestTranspile_SetShow(t *testing.T) {
	tr := New(DefaultConfig())

	// Test ignored SET parameters
	t.Run("ignored SET parameter", func(t *testing.T) {
		result, err := tr.Transpile("SET statement_timeout = 5000")
		if err != nil {
			t.Fatalf("Transpile error: %v", err)
		}
		if !result.IsIgnoredSet {
			t.Error("SET statement_timeout should be marked as ignored")
		}
	})

	// Test SHOW application_name returns default value
	t.Run("SHOW application_name", func(t *testing.T) {
		result, err := tr.Transpile("SHOW application_name")
		if err != nil {
			t.Fatalf("Transpile error: %v", err)
		}
		// Should return SELECT 'duckgres' AS application_name
		if !strings.Contains(result.SQL, "duckgres") {
			t.Errorf("SHOW application_name should return default value 'duckgres', got: %q", result.SQL)
		}
	})

	// Test SET application_name is ignored
	t.Run("SET application_name ignored", func(t *testing.T) {
		result, err := tr.Transpile("SET application_name = 'fivetran'")
		if err != nil {
			t.Fatalf("Transpile error: %v", err)
		}
		if !result.IsIgnoredSet {
			t.Error("SET application_name should be marked as ignored")
		}
	})
}

func TestTranspile_ComplexQueries(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "CTE query",
			input: "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users",
		},
		{
			name:  "subquery in FROM",
			input: "SELECT * FROM (SELECT id, name FROM users) AS u",
		},
		{
			name:  "JOIN query",
			input: "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
		},
		{
			name:  "UNION query",
			input: "SELECT name FROM users UNION SELECT name FROM admins",
		},
		{
			name:  "query with comment prefix",
			input: "/* sync_id:abc123 */ SELECT * FROM users",
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if result.SQL == "" {
				t.Errorf("Transpile(%q) returned empty SQL", tt.input)
			}
		})
	}
}

func TestTranspile_ETLPatterns(t *testing.T) {
	// Test patterns commonly used by ETL tools
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "CREATE TABLE with comment prefix",
			input: `/*ETL*/CREATE TABLE "schema"."table" ( "id" CHARACTER VARYING(256), "created_at" TIMESTAMP WITH TIME ZONE )`,
		},
		{
			name:  "information_schema query",
			input: "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
		},
		{
			name:  "pg_namespace query",
			input: "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT LIKE 'pg_%'",
		},
	}

	tr := New(Config{DuckLakeMode: true})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if result.SQL == "" {
				t.Errorf("Transpile(%q) returned empty SQL", tt.input)
			}
		})
	}
}

func TestTranspile_DDL_Complex(t *testing.T) {
	tr := New(Config{DuckLakeMode: true})

	input := `CREATE TABLE orders (
		id BIGSERIAL PRIMARY KEY,
		user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
		email VARCHAR UNIQUE NOT NULL,
		status VARCHAR CHECK (status IN ('pending', 'complete')),
		created_at TIMESTAMP DEFAULT now(),
		CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES users(id)
	)`

	result, err := tr.Transpile(input)
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}

	upperSQL := strings.ToUpper(result.SQL)

	// Should NOT contain constraints
	constraints := []string{"PRIMARY KEY", "UNIQUE", "REFERENCES", "CHECK", "FOREIGN KEY"}
	for _, c := range constraints {
		if strings.Contains(upperSQL, c) {
			t.Errorf("Result should NOT contain %q: %s", c, result.SQL)
		}
	}

	// Should NOT contain SERIAL
	if strings.Contains(upperSQL, "SERIAL") {
		t.Errorf("Result should NOT contain SERIAL: %s", result.SQL)
	}

	// Should NOT contain now()
	if strings.Contains(strings.ToLower(result.SQL), "now()") {
		t.Errorf("Result should NOT contain now(): %s", result.SQL)
	}

	// Should still contain NOT NULL (this is allowed)
	if !strings.Contains(upperSQL, "NOT NULL") {
		t.Errorf("Result should contain NOT NULL: %s", result.SQL)
	}
}

func TestTranspile_EmptyQuery(t *testing.T) {
	tr := New(DefaultConfig())

	result, err := tr.Transpile("")
	if err != nil {
		t.Fatalf("Transpile('') error: %v", err)
	}
	if result.SQL != "" {
		t.Errorf("Transpile('') should return empty SQL, got: %q", result.SQL)
	}
}

func TestTranspile_WhitespaceQuery(t *testing.T) {
	tr := New(DefaultConfig())

	result, err := tr.Transpile("   \n\t  ")
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}
	if result.SQL != "" {
		t.Errorf("Transpile('   ') should return empty SQL, got: %q", result.SQL)
	}
}

func BenchmarkTranspile_SimpleSelect(b *testing.B) {
	tr := New(DefaultConfig())
	query := "SELECT * FROM users WHERE id = 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tr.Transpile(query)
	}
}

func BenchmarkTranspile_PgCatalog(b *testing.B) {
	tr := New(DefaultConfig())
	query := "SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tr.Transpile(query)
	}
}

func BenchmarkTranspile_CreateTable(b *testing.B) {
	tr := New(Config{DuckLakeMode: true})
	query := "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR UNIQUE, email VARCHAR NOT NULL)"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tr.Transpile(query)
	}
}

func TestTranspile_TypeMappings(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "JSONB -> JSON",
			input:    "CREATE TABLE t (data JSONB)",
			contains: "json",
			excludes: "jsonb",
		},
		{
			name:     "BYTEA -> BLOB",
			input:    "CREATE TABLE t (data BYTEA)",
			contains: "blob",
			excludes: "bytea",
		},
		{
			name:     "INET -> TEXT",
			input:    "CREATE TABLE t (ip INET)",
			contains: "text",
			excludes: "inet",
		},
		{
			name:     "TSVECTOR -> TEXT",
			input:    "CREATE TABLE t (search TSVECTOR)",
			contains: "text",
			excludes: "tsvector",
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			lowerSQL := strings.ToLower(result.SQL)
			if tt.contains != "" && !strings.Contains(lowerSQL, tt.contains) {
				t.Errorf("Transpile(%q) = %q, should contain %q", tt.input, result.SQL, tt.contains)
			}
			if tt.excludes != "" && strings.Contains(lowerSQL, tt.excludes) {
				t.Errorf("Transpile(%q) = %q, should NOT contain %q", tt.input, result.SQL, tt.excludes)
			}
		})
	}
}

func TestTranspile_FunctionMappings(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "array_agg -> list",
			input:    "SELECT array_agg(name) FROM users",
			contains: "list",
			excludes: "array_agg",
		},
		{
			name:     "string_to_array -> string_split",
			input:    "SELECT string_to_array('a,b,c', ',')",
			contains: "string_split",
			excludes: "string_to_array",
		},
		{
			name:     "regexp_matches -> regexp_extract_all",
			input:    "SELECT regexp_matches(name, '\\w+')",
			contains: "regexp_extract_all",
			excludes: "regexp_matches",
		},
		{
			name:     "pg_typeof -> typeof",
			input:    "SELECT pg_typeof(1)",
			contains: "typeof",
			excludes: "pg_typeof",
		},
		{
			name:     "json_build_object -> json_object",
			input:    "SELECT json_build_object('a', 1, 'b', 2)",
			contains: "json_object",
			excludes: "json_build_object",
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			lowerSQL := strings.ToLower(result.SQL)
			if tt.contains != "" && !strings.Contains(lowerSQL, tt.contains) {
				t.Errorf("Transpile(%q) = %q, should contain %q", tt.input, result.SQL, tt.contains)
			}
			if tt.excludes != "" && strings.Contains(lowerSQL, tt.excludes) {
				t.Errorf("Transpile(%q) = %q, should NOT contain %q", tt.input, result.SQL, tt.excludes)
			}
		})
	}
}

func TestTranspile_OnConflict(t *testing.T) {
	// DuckDB supports ON CONFLICT syntax, so these should pass through
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "ON CONFLICT DO NOTHING",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO NOTHING",
		},
		{
			name:  "ON CONFLICT DO UPDATE",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			// ON CONFLICT should be preserved
			if !strings.Contains(strings.ToUpper(result.SQL), "ON CONFLICT") {
				t.Errorf("Transpile(%q) = %q, should contain ON CONFLICT", tt.input, result.SQL)
			}
		})
	}
}

func TestTranspile_JSONOperators(t *testing.T) {
	// DuckDB supports -> and ->> operators
	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{
			name:     "JSON arrow operator",
			input:    "SELECT data->'key' FROM t",
			contains: "->",
		},
		{
			name:     "JSON double arrow operator",
			input:    "SELECT data->>'key' FROM t",
			contains: "->>",
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if !strings.Contains(result.SQL, tt.contains) {
				t.Errorf("Transpile(%q) = %q, should contain %q", tt.input, result.SQL, tt.contains)
			}
		})
	}
}
