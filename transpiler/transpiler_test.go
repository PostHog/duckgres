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
			name:     "pg_catalog.pg_class -> memory.main.pg_class_full",
			input:    "SELECT * FROM pg_catalog.pg_class",
			contains: "memory.main.pg_class_full",
			excludes: "pg_catalog",
		},
		{
			name:     "pg_catalog.pg_database -> memory.main.pg_database",
			input:    "SELECT * FROM pg_catalog.pg_database",
			contains: "memory.main.pg_database",
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
			name:     "pg_catalog.version() stripped (SQLAlchemy compatibility)",
			input:    "SELECT pg_catalog.version()",
			contains: "PostgreSQL", // VersionTransform inlines the version string
			excludes: "pg_catalog.version",
		},
		{
			name:     "pg_get_serial_sequence prefix stripped",
			input:    "SELECT pg_catalog.pg_get_serial_sequence('users', 'id')",
			contains: "pg_get_serial_sequence",
			excludes: "pg_catalog.pg_get_serial_sequence",
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

func TestTranspile_PgCatalog_DuckLakeMode(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{
			name:     "pg_catalog.pg_class -> memory.main.pg_class_full",
			input:    "SELECT * FROM pg_catalog.pg_class",
			contains: "memory.main.pg_class_full",
		},
		{
			name:     "pg_catalog.pg_matviews -> memory.main.pg_matviews",
			input:    "SELECT * FROM pg_catalog.pg_matviews",
			contains: "memory.main.pg_matviews",
		},
		{
			name:     "unqualified pg_matviews -> memory.main.pg_matviews",
			input:    "SELECT matviewname FROM pg_matviews WHERE schemaname = 'public'",
			contains: "memory.main.pg_matviews",
		},
		{
			name:     "unqualified pg_class -> memory.main.pg_class_full",
			input:    "SELECT * FROM pg_class",
			contains: "memory.main.pg_class_full",
		},
		{
			name:     "pg_catalog.pg_database -> memory.main.pg_database",
			input:    "SELECT * FROM pg_catalog.pg_database",
			contains: "memory.main.pg_database",
		},
		{
			name:     "pg_catalog.pg_namespace -> memory.main.pg_namespace",
			input:    "SELECT * FROM pg_catalog.pg_namespace",
			contains: "memory.main.pg_namespace",
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
			name:     "regclass cast from unqualified oid - fallback to varchar (avoids shadowing)",
			input:    "SELECT oid::pg_catalog.regclass FROM pg_class",
			contains: "varchar",                // Should fallback, NOT produce subquery
			excludes: "select relname from",    // Must NOT produce shadowing subquery
		},
		{
			name:     "regclass cast from string literal",
			input:    "SELECT 'users'::regclass",
			contains: "select oid from",
			excludes: "",
		},
		{
			name:     "regclass cast from qualified column ref - fallback to varchar",
			input:    "SELECT a.attrelid::regclass FROM pg_attribute a",
			contains: "varchar",             // Now falls back (conservative - no type info)
			excludes: "select relname from", // No subquery without explicit cast
		},
		{
			name:     "regclass to oid chain",
			input:    "SELECT 'users'::regclass::oid",
			contains: "select oid from",
			excludes: "regclass",
		},
		// Bug fix tests: TypeCast to text should use name lookup, not OID lookup
		{
			name:     "regclass cast from text typecast - name lookup",
			input:    "SELECT 'users'::text::regclass",
			contains: "where relname =", // Name-based lookup
			excludes: "where oid =",     // NOT OID-based lookup
		},
		{
			name:     "regclass cast from oid typecast - OID lookup",
			input:    "SELECT foo::oid::regclass FROM bar",
			contains: "select relname from", // OID-based lookup
			excludes: "where relname =",     // NOT name-based lookup
		},
		// Bug fix: Column refs should fall back to varchar (no type info)
		{
			name:     "regclass from column ref - fallback to varchar",
			input:    "SELECT a.some_col::regclass FROM tab a",
			contains: "varchar",             // Falls back safely
			excludes: "select relname from", // No subquery
		},
		// Users can explicitly cast to get OID lookup
		{
			name:     "regclass from column via explicit oid cast",
			input:    "SELECT a.attrelid::oid::regclass FROM pg_attribute a",
			contains: "select relname from", // OID lookup via explicit cast
			excludes: "",
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

func TestTranspile_FuncAlias(t *testing.T) {
	tr := New(DefaultConfig())

	tests := []struct {
		name          string
		input         string
		expectedAlias string // can be quoted or unquoted
	}{
		{
			name:          "current_database direct",
			input:         "SELECT current_database()",
			expectedAlias: "current_database",
		},
		{
			name:          "current_database in subquery",
			input:         "SELECT * FROM (SELECT current_database()) c",
			expectedAlias: "current_database",
		},
		{
			name:          "current_schema direct",
			input:         "SELECT current_schema()",
			expectedAlias: "current_schema",
		},
		{
			name:          "current_schema in subquery",
			input:         "SELECT * FROM (SELECT current_schema()) c",
			expectedAlias: "current_schema",
		},
		{
			name:          "nested subquery",
			input:         "SELECT * FROM (SELECT * FROM (SELECT current_database()) a) b",
			expectedAlias: "current_database",
		},
		{
			name:          "join with subqueries",
			input:         "SELECT * FROM (SELECT current_database()) a JOIN (SELECT current_schema()) b ON true",
			expectedAlias: "current_database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile error: %v", err)
			}
			// Check for alias with or without quotes
			hasAlias := strings.Contains(result.SQL, "AS "+tt.expectedAlias) ||
				strings.Contains(result.SQL, "AS \""+tt.expectedAlias+"\"")
			if !hasAlias {
				t.Errorf("Expected AS %s (quoted or unquoted) in output, got: %q", tt.expectedAlias, result.SQL)
			}
		})
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
	// In DuckLake mode, CASCADE should be stripped from DROP TABLE/VIEW statements
	// See: https://github.com/duckdb/dbt-duckdb/pull/557
	// However, DROP SCHEMA CASCADE is supported by DuckDB natively and should be preserved.
	tr := New(Config{DuckLakeMode: true})

	// Test cases where CASCADE should be stripped
	stripCascadeTests := []struct {
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
	}

	for _, tt := range stripCascadeTests {
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

	// Test that DROP SCHEMA CASCADE is preserved (DuckDB supports it natively)
	t.Run("DROP SCHEMA CASCADE is preserved", func(t *testing.T) {
		result, err := tr.Transpile("DROP SCHEMA my_schema CASCADE")
		if err != nil {
			t.Fatalf("Transpile error: %v", err)
		}
		if !strings.Contains(strings.ToUpper(result.SQL), "CASCADE") {
			t.Errorf("DROP SCHEMA CASCADE should be preserved, got: %q", result.SQL)
		}
	})
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

func TestTranspile_WritableCTE_ColumnRefRewriting(t *testing.T) {
	// Test that column references (table.column) in WHERE clauses are rewritten
	// This is the Airbyte pattern that was failing
	tr := New(DefaultConfig())

	input := `WITH deduped AS (SELECT * FROM staging), updates AS (UPDATE target SET name = d.name FROM deduped d WHERE target.id = d.id RETURNING *) INSERT INTO target SELECT * FROM deduped WHERE NOT EXISTS (SELECT 1 FROM updates WHERE updates.id = deduped.id)`
	result, err := tr.Transpile(input)
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}

	if len(result.Statements) == 0 {
		t.Fatal("Expected multi-statement result")
	}

	// Check that column references like "deduped.id" and "updates.id" are rewritten
	// to use temp table names in the generated statements
	for _, stmt := range result.Statements {
		// Skip BEGIN and simple statements
		if stmt == "BEGIN" {
			continue
		}
		// Column references should use temp table names, not original CTE names
		// Look for unqualified references that should have been rewritten
		if strings.Contains(stmt, "deduped.id") || strings.Contains(stmt, "updates.id") {
			t.Errorf("Column reference should use temp table name, found original CTE name in: %s", stmt)
		}
	}

	// The final INSERT should have rewritten column refs in the NOT EXISTS subquery
	finalStmt := result.Statements[len(result.Statements)-1]
	if strings.Contains(finalStmt, "deduped.id") {
		t.Errorf("Final statement should use temp table name for column refs, got: %s", finalStmt)
	}
}

func TestCountParameters(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected int
		wantErr  bool
	}{
		{
			name:     "no parameters",
			sql:      "SELECT * FROM users",
			expected: 0,
		},
		{
			name:     "single parameter",
			sql:      "SELECT * FROM users WHERE id = $1",
			expected: 1,
		},
		{
			name:     "multiple sequential parameters",
			sql:      "SELECT * FROM users WHERE id = $1 AND name = $2 AND age = $3",
			expected: 3,
		},
		{
			name:     "non-sequential parameters",
			sql:      "SELECT * FROM users WHERE id = $5",
			expected: 5,
		},
		{
			name:     "mixed parameter numbers",
			sql:      "SELECT * FROM users WHERE id = $3 OR id = $1",
			expected: 3,
		},
		{
			name:     "parameters in INSERT",
			sql:      "INSERT INTO users (name, email) VALUES ($1, $2)",
			expected: 2,
		},
		{
			name:     "parameters in UPDATE",
			sql:      "UPDATE users SET name = $1 WHERE id = $2",
			expected: 2,
		},
		{
			name:     "empty string",
			sql:      "",
			expected: 0,
		},
		{
			name:     "whitespace only",
			sql:      "   ",
			expected: 0,
		},
		{
			name:    "invalid SQL",
			sql:     "NOT VALID SQL AT ALL $$$",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := CountParameters(tt.sql)
			if tt.wantErr {
				if err == nil {
					t.Errorf("CountParameters(%q) expected error, got nil", tt.sql)
				}
				return
			}
			if err != nil {
				t.Fatalf("CountParameters(%q) unexpected error: %v", tt.sql, err)
			}
			if count != tt.expected {
				t.Errorf("CountParameters(%q) = %d, want %d", tt.sql, count, tt.expected)
			}
		})
	}
}

func TestTranspile_FallbackToNative(t *testing.T) {
	// Test that FallbackToNative is set correctly when PostgreSQL parsing fails
	// but the query might be valid DuckDB syntax
	tr := New(DefaultConfig())

	tests := []struct {
		name             string
		input            string
		wantFallback     bool
		wantSQL          string // expected SQL in result (original for fallback)
		wantErrNil       bool   // whether err should be nil
	}{
		{
			name:         "valid PostgreSQL - no fallback",
			input:        "SELECT * FROM users",
			wantFallback: false,
			wantErrNil:   true,
		},
		{
			name:         "valid PostgreSQL with WHERE - no fallback",
			input:        "SELECT id, name FROM users WHERE active = true",
			wantFallback: false,
			wantErrNil:   true,
		},
		{
			name:         "valid PostgreSQL INSERT - no fallback",
			input:        "INSERT INTO users (name) VALUES ('test')",
			wantFallback: false,
			wantErrNil:   true,
		},
		// Note: COPY syntax is valid PostgreSQL, so it doesn't trigger fallback
		// even if the FORMAT PARQUET is DuckDB-specific
		{
			name:         "DuckDB DESCRIBE statement - fallback",
			input:        "DESCRIBE SELECT * FROM users",
			wantFallback: true,
			wantSQL:      "DESCRIBE SELECT * FROM users",
			wantErrNil:   true,
		},
		{
			name:         "DuckDB SUMMARIZE statement - fallback",
			input:        "SUMMARIZE SELECT * FROM users",
			wantFallback: true,
			wantSQL:      "SUMMARIZE SELECT * FROM users",
			wantErrNil:   true,
		},
		{
			name:         "DuckDB PIVOT syntax - fallback",
			input:        "PIVOT cities ON year USING sum(population)",
			wantFallback: true,
			wantSQL:      "PIVOT cities ON year USING sum(population)",
			wantErrNil:   true,
		},
		{
			name:         "DuckDB UNPIVOT syntax - fallback",
			input:        "UNPIVOT monthly_sales ON jan, feb, mar INTO NAME month VALUE sales",
			wantFallback: true,
			wantSQL:      "UNPIVOT monthly_sales ON jan, feb, mar INTO NAME month VALUE sales",
			wantErrNil:   true,
		},
		{
			name:         "DuckDB FROM-first syntax - fallback",
			input:        "FROM users SELECT name, email",
			wantFallback: true,
			wantSQL:      "FROM users SELECT name, email",
			wantErrNil:   true,
		},
		// Note: read_parquet() and read_csv() are valid PostgreSQL syntax
		// (just function calls), so they don't trigger fallback.
		// They only fail at execution time if the function doesn't exist.
		{
			name:         "DuckDB INSTALL extension - fallback",
			input:        "INSTALL httpfs",
			wantFallback: true,
			wantSQL:      "INSTALL httpfs",
			wantErrNil:   true,
		},
		{
			name:         "DuckDB LOAD extension - fallback",
			input:        "LOAD httpfs",
			wantFallback: true,
			wantSQL:      "LOAD httpfs",
			wantErrNil:   true,
		},
		{
			name:         "DuckDB ATTACH database - fallback",
			input:        "ATTACH 'my.db' AS mydb",
			wantFallback: true,
			wantSQL:      "ATTACH 'my.db' AS mydb",
			wantErrNil:   true,
		},
		{
			name:         "DuckDB USE database - fallback",
			input:        "USE mydb",
			wantFallback: true,
			wantSQL:      "USE mydb",
			wantErrNil:   true,
		},
		{
			name:         "DuckDB PRAGMA statement - fallback",
			input:        "PRAGMA database_list",
			wantFallback: true,
			wantSQL:      "PRAGMA database_list",
			wantErrNil:   true,
		},
		{
			name:         "DuckDB CREATE MACRO - fallback",
			input:        "CREATE MACRO add(a, b) AS a + b",
			wantFallback: true,
			wantSQL:      "CREATE MACRO add(a, b) AS a + b",
			wantErrNil:   true,
		},
		{
			name:         "DuckDB SELECT with EXCLUDE - fallback",
			input:        "SELECT * EXCLUDE (password) FROM users",
			wantFallback: true,
			wantSQL:      "SELECT * EXCLUDE (password) FROM users",
			wantErrNil:   true,
		},
		{
			name:         "DuckDB SELECT with REPLACE - fallback",
			input:        "SELECT * REPLACE (upper(name) AS name) FROM users",
			wantFallback: true,
			wantSQL:      "SELECT * REPLACE (upper(name) AS name) FROM users",
			wantErrNil:   true,
		},
		{
			name:         "DuckDB QUALIFY clause - fallback",
			input:        "SELECT * FROM sales QUALIFY row_number() OVER (PARTITION BY region) = 1",
			wantFallback: true,
			wantSQL:      "SELECT * FROM sales QUALIFY row_number() OVER (PARTITION BY region) = 1",
			wantErrNil:   true,
		},
		{
			name:         "valid PostgreSQL CTE - no fallback",
			input:        "WITH cte AS (SELECT 1) SELECT * FROM cte",
			wantFallback: false,
			wantErrNil:   true,
		},
		{
			name:         "valid PostgreSQL subquery - no fallback",
			input:        "SELECT * FROM (SELECT id FROM users) AS sub",
			wantFallback: false,
			wantErrNil:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)

			// Check error expectation
			if tt.wantErrNil && err != nil {
				t.Fatalf("Transpile(%q) unexpected error: %v", tt.input, err)
			}
			if !tt.wantErrNil && err == nil {
				t.Fatalf("Transpile(%q) expected error, got nil", tt.input)
			}

			if err != nil {
				return
			}

			// Check FallbackToNative flag
			if result.FallbackToNative != tt.wantFallback {
				t.Errorf("Transpile(%q) FallbackToNative = %v, want %v",
					tt.input, result.FallbackToNative, tt.wantFallback)
			}

			// For fallback cases, SQL should be the original query
			if tt.wantFallback && tt.wantSQL != "" && result.SQL != tt.wantSQL {
				t.Errorf("Transpile(%q) SQL = %q, want %q",
					tt.input, result.SQL, tt.wantSQL)
			}
		})
	}
}

func TestTranspileMulti_FallbackToNative(t *testing.T) {
	// Test that TranspileMulti also handles FallbackToNative correctly
	tr := New(DefaultConfig())

	tests := []struct {
		name         string
		input        string
		wantFallback bool
		wantCount    int // expected number of results
	}{
		{
			name:         "valid PostgreSQL multi-statement - no fallback",
			input:        "SELECT 1; SELECT 2",
			wantFallback: false,
			wantCount:    2,
		},
		{
			name:         "DuckDB-specific syntax - fallback",
			input:        "DESCRIBE SELECT * FROM users",
			wantFallback: true,
			wantCount:    1,
		},
		{
			name:         "DuckDB PRAGMA - fallback",
			input:        "PRAGMA table_info('users')",
			wantFallback: true,
			wantCount:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := tr.TranspileMulti(tt.input)
			if err != nil {
				t.Fatalf("TranspileMulti(%q) error: %v", tt.input, err)
			}

			if len(results) != tt.wantCount {
				t.Errorf("TranspileMulti(%q) returned %d results, want %d",
					tt.input, len(results), tt.wantCount)
			}

			// Check first result for fallback flag
			if len(results) > 0 && results[0].FallbackToNative != tt.wantFallback {
				t.Errorf("TranspileMulti(%q) FallbackToNative = %v, want %v",
					tt.input, results[0].FallbackToNative, tt.wantFallback)
			}
		})
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
