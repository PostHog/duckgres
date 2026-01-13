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
			contains: "memory.main.information_schema_columns_compat",
			excludes: "information_schema.columns",
		},
		{
			name:     "information_schema.tables -> compat view",
			input:    "SELECT * FROM information_schema.tables",
			contains: "memory.main.information_schema_tables_compat",
			excludes: "information_schema.tables",
		},
		{
			name:     "information_schema.schemata -> compat view",
			input:    "SELECT * FROM information_schema.schemata",
			contains: "memory.main.information_schema_schemata_compat",
			excludes: "information_schema.schemata",
		},
		{
			name:     "INFORMATION_SCHEMA.COLUMNS uppercase -> compat view",
			input:    "SELECT * FROM INFORMATION_SCHEMA.COLUMNS",
			contains: "memory.main.information_schema_columns_compat",
			excludes: "information_schema.columns",
		},
		{
			name:     "aliased information_schema query",
			input:    "SELECT c.column_name FROM information_schema.columns c WHERE c.table_name = 'test'",
			contains: "memory.main.information_schema_columns_compat",
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
			name:     "information_schema.columns with DuckLake -> memory.main qualified",
			input:    "SELECT * FROM information_schema.columns",
			contains: "memory.main.information_schema_columns_compat",
		},
		{
			name:     "information_schema.tables with DuckLake -> memory.main qualified",
			input:    "SELECT * FROM information_schema.tables",
			contains: "memory.main.information_schema_tables_compat",
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
		{"ALTER TABLE ADD CONSTRAINT", "ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id)", "ALTER TABLE"},
		{"ALTER TABLE SET NOT NULL", "ALTER TABLE users ALTER COLUMN name SET NOT NULL", "ALTER TABLE"},
		{"ALTER TABLE DROP NOT NULL", "ALTER TABLE users ALTER COLUMN name DROP NOT NULL", "ALTER TABLE"},
		{"ALTER TABLE SET DEFAULT", "ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active'", "ALTER TABLE"},
		{"ALTER TABLE DROP DEFAULT", "ALTER TABLE users ALTER COLUMN status DROP DEFAULT", "ALTER TABLE"},
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

	// Test SET SESSION CHARACTERISTICS is ignored
	t.Run("SET SESSION CHARACTERISTICS ignored", func(t *testing.T) {
		result, err := tr.Transpile("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
		if err != nil {
			t.Fatalf("Transpile error: %v", err)
		}
		if !result.IsIgnoredSet {
			t.Error("SET SESSION CHARACTERISTICS should be marked as ignored")
		}
	})

	// Test various SET SESSION CHARACTERISTICS variations
	t.Run("SET SESSION CHARACTERISTICS variations", func(t *testing.T) {
		tests := []string{
			"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
			"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ",
			"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
			"SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
			"SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE",
		}
		for _, query := range tests {
			result, err := tr.Transpile(query)
			if err != nil {
				t.Errorf("Transpile(%q) error: %v", query, err)
			}
			if !result.IsIgnoredSet {
				t.Errorf("Transpile(%q) should be marked as ignored", query)
			}
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

func TestTranspile_OnConflict_DuckLakeMode(t *testing.T) {
	// In DuckLake mode, ON CONFLICT is converted to MERGE statement
	// because PRIMARY KEY/UNIQUE constraints don't exist in DuckLake
	tests := []struct {
		name        string
		input       string
		wantMerge   bool
		contains    []string
		notContains []string
	}{
		{
			name:      "ON CONFLICT DO NOTHING becomes MERGE with only INSERT",
			input:     "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO NOTHING",
			wantMerge: true,
			contains: []string{
				"MERGE INTO users",
				"USING (SELECT 1 AS id, 'test' AS name) excluded",
				"ON excluded.id = users.id",
				"WHEN NOT MATCHED THEN INSERT",
			},
			notContains: []string{
				"ON CONFLICT",
				"WHEN MATCHED THEN UPDATE", // DO NOTHING means no update action
			},
		},
		{
			name:      "ON CONFLICT DO UPDATE becomes MERGE with UPDATE and INSERT",
			input:     "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
			wantMerge: true,
			contains: []string{
				"MERGE INTO users",
				"USING (SELECT 1 AS id, 'test' AS name) excluded",
				"ON excluded.id = users.id",
				"WHEN MATCHED THEN UPDATE SET name = excluded.name",
				"WHEN NOT MATCHED THEN INSERT",
			},
			notContains: []string{
				"ON CONFLICT",
			},
		},
		{
			name:      "Multiple values become UNION ALL",
			input:     "INSERT INTO users (id, name) VALUES (1, 'test'), (2, 'test2') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
			wantMerge: true,
			contains: []string{
				"MERGE INTO users",
				"UNION ALL",
				"WHEN MATCHED THEN UPDATE",
				"WHEN NOT MATCHED THEN INSERT",
			},
			notContains: []string{
				"ON CONFLICT",
			},
		},
		{
			name:      "Multiple conflict columns",
			input:     "INSERT INTO users (id, org_id, name) VALUES (1, 100, 'test') ON CONFLICT (id, org_id) DO UPDATE SET name = EXCLUDED.name",
			wantMerge: true,
			contains: []string{
				"MERGE INTO users",
				"excluded.id = users.id",
				"excluded.org_id = users.org_id",
				"AND", // Multiple conditions joined with AND
			},
			notContains: []string{
				"ON CONFLICT",
			},
		},
	}

	tr := New(Config{DuckLakeMode: true})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}

			sql := result.SQL

			// Check MERGE statement is generated
			if tt.wantMerge {
				if !strings.Contains(strings.ToUpper(sql), "MERGE INTO") {
					t.Errorf("Transpile(%q) = %q, should contain MERGE INTO", tt.input, sql)
				}
			}

			// Check expected contents
			for _, want := range tt.contains {
				if !strings.Contains(sql, want) {
					t.Errorf("Transpile(%q) = %q, should contain %q", tt.input, sql, want)
				}
			}

			// Check contents that should not be present
			for _, notWant := range tt.notContains {
				if strings.Contains(strings.ToUpper(sql), strings.ToUpper(notWant)) {
					t.Errorf("Transpile(%q) = %q, should NOT contain %q", tt.input, sql, notWant)
				}
			}
		})
	}
}

func TestTranspile_DropCascade_DuckLakeMode(t *testing.T) {
	// In DuckLake mode, CASCADE should be stripped from DROP statements
	// See: https://github.com/duckdb/dbt-duckdb/pull/557
	tests := []struct {
		name        string
		input       string
		notContains string
	}{
		{
			name:        "DROP TABLE CASCADE becomes DROP TABLE",
			input:       "DROP TABLE users CASCADE",
			notContains: "CASCADE",
		},
		{
			name:        "DROP TABLE IF EXISTS CASCADE",
			input:       "DROP TABLE IF EXISTS users CASCADE",
			notContains: "CASCADE",
		},
		{
			name:        "DROP VIEW CASCADE",
			input:       "DROP VIEW my_view CASCADE",
			notContains: "CASCADE",
		},
		{
			name:        "DROP SCHEMA CASCADE",
			input:       "DROP SCHEMA my_schema CASCADE",
			notContains: "CASCADE",
		},
	}

	tr := New(Config{DuckLakeMode: true})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if strings.Contains(strings.ToUpper(result.SQL), tt.notContains) {
				t.Errorf("Transpile(%q) = %q, should NOT contain %q", tt.input, result.SQL, tt.notContains)
			}
		})
	}
}

func TestTranspile_DropCascade_NonDuckLakeMode(t *testing.T) {
	// Outside DuckLake mode, CASCADE should be preserved
	tr := New(DefaultConfig())

	result, err := tr.Transpile("DROP TABLE users CASCADE")
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}
	if !strings.Contains(strings.ToUpper(result.SQL), "CASCADE") {
		t.Errorf("Non-DuckLake mode should preserve CASCADE, got: %q", result.SQL)
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

func TestTranspile_ExpandArray(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains []string
		excludes []string
	}{
		{
			name:  "_pg_expandarray with field access .n",
			input: "SELECT (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ FROM pg_index i",
			contains: []string{
				"_pg_exp_1.n",                                     // field access replaced
				"unnest(i.indkey)",                                // LATERAL join added
				"generate_subscripts(i.indkey, 1)",                // index generation added
			},
			excludes: []string{
				"_pg_expandarray", // original function removed
			},
		},
		{
			name:  "_pg_expandarray with field access .x",
			input: "SELECT (information_schema._pg_expandarray(arr)).x FROM t",
			contains: []string{
				"_pg_exp_1.x",
				"unnest",
				"generate_subscripts",
			},
			excludes: []string{
				"_pg_expandarray",
			},
		},
		{
			name:  "_pg_expandarray aliased without field access",
			input: "SELECT information_schema._pg_expandarray(arr) AS keys FROM t",
			contains: []string{
				"struct_pack", // converted to struct for field access
				"_pg_exp_1",
			},
			excludes: []string{
				"_pg_expandarray",
			},
		},
		{
			name:  "multiple _pg_expandarray with same array",
			input: "SELECT (information_schema._pg_expandarray(i.indkey)).n, (information_schema._pg_expandarray(i.indkey)).x FROM pg_index i",
			contains: []string{
				"_pg_exp_1.n",
				"_pg_exp_1.x",
			},
			excludes: []string{
				"_pg_expandarray",
			},
		},
		{
			name:  "nested subquery pattern (JDBC actual)",
			input: "SELECT result.key_seq FROM (SELECT (information_schema._pg_expandarray(i.indkey)).n AS key_seq FROM pg_index i) result",
			contains: []string{
				"_pg_exp_1.n",
				"unnest",
			},
			excludes: []string{
				"_pg_expandarray",
			},
		},
		{
			name:  "case insensitivity - uppercase",
			input: "SELECT (information_schema._PG_EXPANDARRAY(arr)).n FROM t",
			contains: []string{
				"_pg_exp_1.n",
				"unnest",
			},
			excludes: []string{
				"_PG_EXPANDARRAY",
			},
		},
		{
			name:  "without information_schema prefix",
			input: "SELECT (_pg_expandarray(arr)).x FROM t",
			contains: []string{
				"_pg_exp_1.x",
				"unnest",
			},
			excludes: []string{
				"_pg_expandarray",
			},
		},
		{
			name:  "in WHERE clause",
			input: "SELECT * FROM t WHERE col = (information_schema._pg_expandarray(arr)).x",
			contains: []string{
				"_pg_exp_1.x",
				"unnest",
			},
			excludes: []string{
				"_pg_expandarray",
			},
		},
		{
			name:     "passthrough - no _pg_expandarray",
			input:    "SELECT unnest(arr) FROM t",
			contains: []string{"unnest(arr)"},
			excludes: []string{"_pg_exp"},
		},
		{
			name:  "different array expressions - with different arrays",
			input: "SELECT (information_schema._pg_expandarray(a.arr1)).n, (information_schema._pg_expandarray(b.arr2)).x FROM t1 a, t2 b",
			contains: []string{
				"_pg_exp_1",
				"_pg_exp_2",
				"unnest(a.arr1)",
				"unnest(b.arr2)",
			},
			excludes: []string{
				"_pg_expandarray",
			},
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			for _, c := range tt.contains {
				if !strings.Contains(result.SQL, c) {
					t.Errorf("Transpile(%q) = %q, should contain %q", tt.input, result.SQL, c)
				}
			}
			for _, e := range tt.excludes {
				if strings.Contains(result.SQL, e) {
					t.Errorf("Transpile(%q) = %q, should NOT contain %q", tt.input, result.SQL, e)
				}
			}
		})
	}
}

func TestTranspile_WritableCTE(t *testing.T) {
	// Test writable CTE detection and rewriting
	tests := []struct {
		name              string
		input             string
		wantMultiStmt     bool   // expect multi-statement rewrite
		wantStmtCount     int    // expected number of statements
		wantCleanupCount  int    // expected number of cleanup statements
		firstStmtContains string // first statement should contain this
		lastStmtContains  string // last statement should contain this (before cleanup)
	}{
		{
			name:          "simple SELECT CTE - no rewrite",
			input:         "WITH x AS (SELECT 1 AS id) SELECT * FROM x",
			wantMultiStmt: false,
		},
		{
			name:              "UPDATE CTE with final SELECT",
			input:             "WITH updates AS (UPDATE users SET active = true WHERE id = 1 RETURNING *) SELECT * FROM updates",
			wantMultiStmt:     true,
			wantStmtCount:     4, // BEGIN, CREATE TEMP, UPDATE, SELECT
			wantCleanupCount:  2, // DROP, COMMIT
			firstStmtContains: "BEGIN",
		},
		{
			name:              "DELETE CTE with final SELECT",
			input:             "WITH deleted AS (DELETE FROM users WHERE active = false RETURNING *) SELECT * FROM deleted",
			wantMultiStmt:     true,
			wantStmtCount:     4,
			wantCleanupCount:  2,
			firstStmtContains: "BEGIN",
		},
		{
			name:              "INSERT CTE with final SELECT",
			input:             "WITH inserted AS (INSERT INTO users (name) VALUES ('test') RETURNING *) SELECT * FROM inserted",
			wantMultiStmt:     true,
			wantStmtCount:     4,
			wantCleanupCount:  2,
			firstStmtContains: "BEGIN",
		},
		{
			name:              "mixed read and write CTEs",
			input:             "WITH source AS (SELECT * FROM staging), updates AS (UPDATE target SET name = s.name FROM source s WHERE target.id = s.id RETURNING *) SELECT * FROM updates",
			wantMultiStmt:     true,
			wantStmtCount:     5, // BEGIN, CREATE source, CREATE updates, UPDATE, SELECT
			wantCleanupCount:  3, // DROP updates, DROP source, COMMIT
			firstStmtContains: "BEGIN",
		},
		{
			name:              "Airbyte-style upsert pattern",
			input:             `WITH deduped AS (SELECT * FROM staging WHERE rn = 1), updates AS (UPDATE target SET name = d.name FROM deduped d WHERE target.id = d.id RETURNING *) INSERT INTO target SELECT * FROM deduped WHERE NOT EXISTS (SELECT 1 FROM updates WHERE updates.id = deduped.id)`,
			wantMultiStmt:     true,
			wantStmtCount:     5, // BEGIN, CREATE deduped, CREATE updates, UPDATE, INSERT
			wantCleanupCount:  3, // DROP updates, DROP deduped, COMMIT
			firstStmtContains: "BEGIN",
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}

			if tt.wantMultiStmt {
				if len(result.Statements) == 0 {
					t.Errorf("Transpile(%q) should produce multi-statement result", tt.input)
					return
				}
				if len(result.Statements) != tt.wantStmtCount {
					t.Errorf("Transpile(%q) produced %d statements, want %d. Statements: %v",
						tt.input, len(result.Statements), tt.wantStmtCount, result.Statements)
				}
				if len(result.CleanupStatements) != tt.wantCleanupCount {
					t.Errorf("Transpile(%q) produced %d cleanup statements, want %d. Cleanup: %v",
						tt.input, len(result.CleanupStatements), tt.wantCleanupCount, result.CleanupStatements)
				}
				if tt.firstStmtContains != "" && !strings.Contains(result.Statements[0], tt.firstStmtContains) {
					t.Errorf("First statement %q should contain %q", result.Statements[0], tt.firstStmtContains)
				}
			} else {
				if len(result.Statements) > 0 {
					t.Errorf("Transpile(%q) should NOT produce multi-statement result, got: %v",
						tt.input, result.Statements)
				}
			}
		})
	}
}

func TestTranspile_WritableCTE_CTEReferences(t *testing.T) {
	// Test that CTE references are properly rewritten to temp table names
	tr := New(DefaultConfig())

	input := "WITH updates AS (UPDATE users SET active = true RETURNING *) SELECT * FROM updates"
	result, err := tr.Transpile(input)
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}

	if len(result.Statements) == 0 {
		t.Fatal("Expected multi-statement result")
	}

	// The final SELECT should reference the temp table, not "updates"
	finalStmt := result.Statements[len(result.Statements)-1]
	if strings.Contains(finalStmt, " updates") && !strings.Contains(finalStmt, "_cte_") {
		t.Errorf("Final statement should use temp table name, got: %s", finalStmt)
	}

	// Cleanup should contain DROP statements for temp tables
	hasDrops := false
	for _, cleanup := range result.CleanupStatements {
		if strings.Contains(strings.ToUpper(cleanup), "DROP TABLE") {
			hasDrops = true
			break
		}
	}
	if !hasDrops {
		t.Errorf("Cleanup should contain DROP TABLE statements: %v", result.CleanupStatements)
	}
}

func TestTranspile_WritableCTE_TempTableNaming(t *testing.T) {
	// Test that temp table names are safe identifiers
	tr := New(DefaultConfig())

	input := "WITH my_updates AS (UPDATE t SET x = 1 RETURNING *) SELECT * FROM my_updates"
	result, err := tr.Transpile(input)
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}

	if len(result.Statements) == 0 {
		t.Fatal("Expected multi-statement result")
	}

	// Find the CREATE TEMP TABLE statement
	for _, stmt := range result.Statements {
		if strings.Contains(strings.ToUpper(stmt), "CREATE TEMP TABLE") {
			// Should contain _cte_ prefix
			if !strings.Contains(stmt, "_cte_") {
				t.Errorf("Temp table name should have _cte_ prefix: %s", stmt)
			}
			// Should be quoted
			if !strings.Contains(stmt, `"`) {
				t.Errorf("Temp table name should be quoted: %s", stmt)
			}
			break
		}
	}
}

func TestConvertAlterTableToAlterView(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantSQL    string
		wantOK     bool
		wantChange bool // whether output should differ from input
	}{
		{
			name:       "ALTER TABLE RENAME to ALTER VIEW RENAME",
			input:      `ALTER TABLE "my_schema"."my_view__tmp" RENAME TO "my_view"`,
			wantOK:     true,
			wantChange: true,
		},
		{
			name:       "simple ALTER TABLE RENAME",
			input:      `ALTER TABLE myview RENAME TO newname`,
			wantOK:     true,
			wantChange: true,
		},
		{
			name:       "non-rename ALTER TABLE unchanged",
			input:      `ALTER TABLE users ADD COLUMN email TEXT`,
			wantOK:     false,
			wantChange: false,
		},
		{
			name:       "SELECT statement unchanged",
			input:      `SELECT * FROM users`,
			wantOK:     false,
			wantChange: false,
		},
		{
			name:       "CREATE TABLE unchanged",
			input:      `CREATE TABLE users (id INT)`,
			wantOK:     false,
			wantChange: false,
		},
		{
			name:       "empty string",
			input:      ``,
			wantOK:     false,
			wantChange: false,
		},
		{
			name:       "invalid SQL",
			input:      `NOT VALID SQL AT ALL`,
			wantOK:     false,
			wantChange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := ConvertAlterTableToAlterView(tt.input)
			if ok != tt.wantOK {
				t.Errorf("ConvertAlterTableToAlterView(%q) ok = %v, want %v", tt.input, ok, tt.wantOK)
			}
			if tt.wantChange {
				if result == tt.input {
					t.Errorf("ConvertAlterTableToAlterView(%q) should change the SQL", tt.input)
				}
				// Check that it contains ALTER VIEW
				if !strings.Contains(strings.ToUpper(result), "ALTER VIEW") {
					t.Errorf("ConvertAlterTableToAlterView(%q) = %q, should contain ALTER VIEW", tt.input, result)
				}
				// Check that it does NOT contain ALTER TABLE
				if strings.Contains(strings.ToUpper(result), "ALTER TABLE") {
					t.Errorf("ConvertAlterTableToAlterView(%q) = %q, should not contain ALTER TABLE", tt.input, result)
				}
			} else {
				if result != tt.input {
					t.Errorf("ConvertAlterTableToAlterView(%q) = %q, should return original", tt.input, result)
				}
			}
		})
	}
}
