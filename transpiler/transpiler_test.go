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

func TestCountParametersRegex(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"no parameters", "SELECT 1", 0},
		{"simple parameters", "SELECT $1, $2", 2},
		{"parameter in string", "SELECT '$1'", 0},
		{"parameter in ident", "SELECT \"$1\"", 0},
		{"parameter in line comment", "SELECT $1 -- $2", 1},
		{"parameter in block comment", "SELECT $1 /* $2 */ $3", 3},
		{"escaped quote in string", "SELECT '$1''$2'", 0},
		{"highest parameter number", "SELECT $1, $10, $2", 10},
		{"multiple occurrences", "SELECT $1, $1, $1", 1},
		{"duckdb native syntax with parameters", "INSTALL httpfs; SELECT $1", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := countParametersRegex(tt.input)
			if actual != tt.expected {
				t.Errorf("countParametersRegex(%q) = %d, expected %d", tt.input, actual, tt.expected)
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
			name:     "pg_catalog.pg_statio_user_tables -> memory.main.pg_statio_user_tables",
			input:    "SELECT * FROM pg_catalog.pg_statio_user_tables",
			contains: "memory.main.pg_statio_user_tables",
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
		{
			name:     "pg_catalog.pg_statio_user_tables -> memory.main.pg_statio_user_tables",
			input:    "SELECT * FROM pg_catalog.pg_statio_user_tables",
			contains: "memory.main.pg_statio_user_tables",
		},
		{
			name:     "unqualified pg_statio_user_tables -> memory.main.pg_statio_user_tables",
			input:    "SELECT * FROM pg_statio_user_tables",
			contains: "memory.main.pg_statio_user_tables",
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

func TestTranspile_PublicSchema(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		contains string
		excludes string
	}{
		{
			name:     "public table ref -> main",
			input:    "SELECT id, name, created_at FROM public.airflow_test",
			contains: "main.airflow_test",
			excludes: "public.airflow_test",
		},
		{
			name:     "public insert target -> main",
			input:    "INSERT INTO public.airflow_test (id) VALUES (1)",
			contains: "main.airflow_test",
			excludes: "public.airflow_test",
		},
		{
			name:     "public DDL target -> main",
			input:    "CREATE TABLE public.new_table (id INT)",
			contains: "main.new_table",
			excludes: "public.new_table",
		},
		{
			name:     "3-part catalog.public.table unchanged",
			input:    "SELECT * FROM postgres.public.users",
			expected: "SELECT * FROM postgres.public.users",
		},
		{
			name:     "3-part catalog.public.table in INSERT unchanged",
			input:    "INSERT INTO mydb.public.events (id) VALUES (1)",
			expected: "INSERT INTO mydb.public.events (id) VALUES (1)",
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if tt.expected != "" {
				if result.SQL != tt.expected {
					t.Errorf("Transpile(%q) = %q, expected %q", tt.input, result.SQL, tt.expected)
				}
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
			contains: "varchar",             // Should fallback, NOT produce subquery
			excludes: "select relname from", // Must NOT produce shadowing subquery
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
		{"REINDEX", "REINDEX TABLE users", "REINDEX"},
		{"CLUSTER", "CLUSTER users USING idx_name", "CLUSTER"},
		{"COMMENT ON", "COMMENT ON TABLE users IS 'User accounts'", "COMMENT"},
		{"REFRESH MATERIALIZED VIEW", "REFRESH MATERIALIZED VIEW my_view", "REFRESH MATERIALIZED VIEW"},
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

	// Test SET ROLE is ignored
	t.Run("SET ROLE ignored", func(t *testing.T) {
		tests := []string{
			"SET ROLE NONE",
			"SET ROLE postgres",
			"RESET ROLE",
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

	// Test SET SESSION AUTHORIZATION is ignored
	t.Run("SET SESSION AUTHORIZATION ignored", func(t *testing.T) {
		result, err := tr.Transpile("SET SESSION AUTHORIZATION DEFAULT")
		if err != nil {
			t.Fatalf("Transpile error: %v", err)
		}
		if !result.IsIgnoredSet {
			t.Error("SET SESSION AUTHORIZATION DEFAULT should be marked as ignored")
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
		{
			name:     "pg_catalog.json prefix stripped",
			input:    "CREATE TABLE t (data pg_catalog.json)",
			contains: "json",
			excludes: "pg_catalog",
		},
		{
			name:     "pg_catalog.int4 prefix stripped and mapped",
			input:    "CREATE TABLE t (id pg_catalog.int4)",
			contains: "integer",
			excludes: "pg_catalog",
		},
		{
			name:     "pg_catalog.varchar prefix stripped",
			input:    "CREATE TABLE t (name pg_catalog.varchar)",
			contains: "varchar",
			excludes: "pg_catalog",
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
		{
			name:     "json_object strips pg_catalog prefix",
			input:    "SELECT JSON_OBJECT('id', id, 'name', name) FROM test",
			contains: "json_object",
			excludes: "pg_catalog",
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
	// JSON operators -> and ->> are converted to function calls to avoid
	// DuckDB operator precedence issues where "a AND b -> 'key'" is parsed
	// as "(a AND b) -> 'key'" instead of "a AND (b -> 'key')"
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "JSON arrow operator converts to json_extract",
			input:    "SELECT data->'key' FROM t",
			expected: "SELECT json_extract(data, 'key') FROM t",
		},
		{
			name:     "JSON double arrow operator converts to json_extract_string",
			input:    "SELECT data->>'key' FROM t",
			expected: "SELECT json_extract_string(data, 'key') FROM t",
		},
		{
			name:     "JSON operator in WHERE with AND",
			input:    "SELECT * FROM t WHERE id > 0 AND data->>'key' IS NULL",
			expected: "SELECT * FROM t WHERE id > 0 AND json_extract_string(data, 'key') IS NULL",
		},
		{
			name:     "Chained JSON operators",
			input:    "SELECT data->'a'->'b' FROM t",
			expected: "SELECT json_extract(json_extract(data, 'a'), 'b') FROM t",
		},
		{
			name:     "JSON operator with complex WHERE",
			input:    "SELECT * FROM t WHERE x = 1 AND data->>'key' = 'val' AND y = 2",
			expected: "SELECT * FROM t WHERE x = 1 AND json_extract_string(data, 'key') = 'val' AND y = 2",
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if result.SQL != tt.expected {
				t.Errorf("Transpile(%q)\n  got:  %q\n  want: %q", tt.input, result.SQL, tt.expected)
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
				"_pg_exp_1.n",                      // field access replaced
				"unnest(i.indkey)",                 // LATERAL join added
				"generate_subscripts(i.indkey, 1)", // index generation added
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
	// during Tier 1 (query has PG patterns but is invalid PG syntax).
	// DuckDB-specific queries with no PG patterns go through Tier 0 (Direct)
	// and return original SQL without FallbackToNative (no parse needed).
	tr := New(DefaultConfig())

	tests := []struct {
		name         string
		input        string
		wantFallback bool
		wantSQL      string // expected SQL in result (original for direct/fallback)
		wantErrNil   bool   // whether err should be nil
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
		// DuckDB-specific syntax: no PG patterns → Tier 0 Direct → original SQL returned.
		// These used to trigger FallbackToNative (pg_query.Parse failed), but now the
		// classifier returns Direct and the query goes to DuckDB without parsing at all.
		{
			name:       "DuckDB DESCRIBE statement - direct",
			input:      "DESCRIBE SELECT * FROM users",
			wantSQL:    "DESCRIBE SELECT * FROM users",
			wantErrNil: true,
		},
		{
			name:       "DuckDB SUMMARIZE statement - direct",
			input:      "SUMMARIZE SELECT * FROM users",
			wantSQL:    "SUMMARIZE SELECT * FROM users",
			wantErrNil: true,
		},
		{
			name:       "DuckDB PIVOT syntax - direct",
			input:      "PIVOT cities ON year USING sum(population)",
			wantSQL:    "PIVOT cities ON year USING sum(population)",
			wantErrNil: true,
		},
		{
			name:       "DuckDB UNPIVOT syntax - direct",
			input:      "UNPIVOT monthly_sales ON jan, feb, mar INTO NAME month VALUE sales",
			wantSQL:    "UNPIVOT monthly_sales ON jan, feb, mar INTO NAME month VALUE sales",
			wantErrNil: true,
		},
		{
			name:       "DuckDB FROM-first syntax - direct",
			input:      "FROM users SELECT name, email",
			wantSQL:    "FROM users SELECT name, email",
			wantErrNil: true,
		},
		{
			name:       "DuckDB INSTALL extension - direct",
			input:      "INSTALL httpfs",
			wantSQL:    "INSTALL httpfs",
			wantErrNil: true,
		},
		{
			name:       "DuckDB LOAD extension - direct",
			input:      "LOAD httpfs",
			wantSQL:    "LOAD httpfs",
			wantErrNil: true,
		},
		{
			name:       "DuckDB ATTACH database - direct",
			input:      "ATTACH 'my.db' AS mydb",
			wantSQL:    "ATTACH 'my.db' AS mydb",
			wantErrNil: true,
		},
		{
			name:       "DuckDB USE database - direct",
			input:      "USE mydb",
			wantSQL:    "USE mydb",
			wantErrNil: true,
		},
		{
			name:       "DuckDB PRAGMA statement - direct",
			input:      "PRAGMA database_list",
			wantSQL:    "PRAGMA database_list",
			wantErrNil: true,
		},
		{
			name:       "DuckDB CREATE MACRO - direct",
			input:      "CREATE MACRO add(a, b) AS a + b",
			wantSQL:    "CREATE MACRO add(a, b) AS a + b",
			wantErrNil: true,
		},
		{
			name:       "DuckDB SELECT with EXCLUDE - direct",
			input:      "SELECT * EXCLUDE (password) FROM users",
			wantSQL:    "SELECT * EXCLUDE (password) FROM users",
			wantErrNil: true,
		},
		{
			name:       "DuckDB SELECT with REPLACE - direct",
			input:      "SELECT * REPLACE (upper(name) AS name) FROM users",
			wantSQL:    "SELECT * REPLACE (upper(name) AS name) FROM users",
			wantErrNil: true,
		},
		{
			name:       "DuckDB QUALIFY clause - direct",
			input:      "SELECT * FROM sales QUALIFY row_number() OVER (PARTITION BY region) = 1",
			wantSQL:    "SELECT * FROM sales QUALIFY row_number() OVER (PARTITION BY region) = 1",
			wantErrNil: true,
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

			// For direct/fallback cases, SQL should be the original query
			if tt.wantSQL != "" && result.SQL != tt.wantSQL {
				t.Errorf("Transpile(%q) SQL = %q, want %q",
					tt.input, result.SQL, tt.wantSQL)
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

func TestConvertDropTableToDropView(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantOK     bool
		wantChange bool
	}{
		{
			name:       "DROP TABLE IF EXISTS to DROP VIEW IF EXISTS",
			input:      `DROP TABLE IF EXISTS "my_schema"."my_view"`,
			wantOK:     true,
			wantChange: true,
		},
		{
			name:       "simple DROP TABLE to DROP VIEW",
			input:      `DROP TABLE myview`,
			wantOK:     true,
			wantChange: true,
		},
		{
			name:       "DROP VIEW unchanged",
			input:      `DROP VIEW IF EXISTS myview`,
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
			name:       "ALTER TABLE unchanged",
			input:      `ALTER TABLE users ADD COLUMN email TEXT`,
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
			result, ok := ConvertDropTableToDropView(tt.input)
			if ok != tt.wantOK {
				t.Errorf("ConvertDropTableToDropView(%q) ok = %v, want %v", tt.input, ok, tt.wantOK)
			}
			if tt.wantChange {
				if result == tt.input {
					t.Errorf("ConvertDropTableToDropView(%q) should change the SQL", tt.input)
				}
				if !strings.Contains(strings.ToUpper(result), "DROP VIEW") {
					t.Errorf("ConvertDropTableToDropView(%q) = %q, should contain DROP VIEW", tt.input, result)
				}
				if strings.Contains(strings.ToUpper(result), "DROP TABLE") {
					t.Errorf("ConvertDropTableToDropView(%q) = %q, should not contain DROP TABLE", tt.input, result)
				}
			} else {
				if result != tt.input {
					t.Errorf("ConvertDropTableToDropView(%q) = %q, should return original", tt.input, result)
				}
			}
		})
	}
}

func TestTranspile_PgCatalog_ColumnRefRewrite(t *testing.T) {
	// Bug: When pg_class is rewritten to pg_class_full, column references like
	// pg_class.oid should also be rewritten to pg_class_full.oid
	// This is needed for queries generated by psql and other PostgreSQL clients
	// that describe tables using pg_attribute JOIN pg_class ON pg_class.oid = attrelid
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "column ref pg_class.oid should be rewritten",
			input:    "SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON pg_class.oid = a.attrelid",
			contains: "pg_class_full.oid",
			excludes: "pg_class.oid",
		},
		{
			name:     "column ref with alias pg_class.relname should be rewritten",
			input:    "SELECT pg_class.relname FROM pg_catalog.pg_class",
			contains: "pg_class_full.relname",
			excludes: "pg_class.relname",
		},
		{
			name:     "multiple column refs should all be rewritten",
			input:    "SELECT pg_class.oid, pg_class.relname, pg_class.relkind FROM pg_catalog.pg_class WHERE pg_class.relkind = 'r'",
			contains: "pg_class_full.oid",
			excludes: "pg_class.oid",
		},
		{
			name:     "column ref in WHERE clause",
			input:    "SELECT * FROM pg_catalog.pg_class WHERE pg_class.relnamespace = 2200",
			contains: "pg_class_full.relnamespace",
			excludes: "pg_class.relnamespace",
		},
		{
			name:     "column ref in JOIN condition",
			input:    "SELECT * FROM pg_catalog.pg_attribute JOIN pg_catalog.pg_class ON pg_class.oid = pg_attribute.attrelid",
			contains: "pg_class_full.oid",
			excludes: " pg_class.oid", // space prefix to avoid matching pg_class_full
		},
		{
			name:     "unqualified table with qualified column ref",
			input:    "SELECT pg_class.relname FROM pg_class",
			contains: "pg_class_full.relname",
			excludes: " pg_class.relname", // space prefix to avoid matching pg_class_full
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

func TestTranspile_TypeCast_JsonType(t *testing.T) {
	// Bug: ::pg_catalog.json should have pg_catalog. stripped, becoming just ::json
	// DuckDB doesn't understand pg_catalog.json type qualifier
	// Note: pg_query's deparser adds pg_catalog. prefix to certain types during deparsing,
	// so we need to strip it in all cases to produce DuckDB-compatible output.
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "pg_catalog.json cast should strip prefix",
			input:    "SELECT data::pg_catalog.json FROM t",
			excludes: "pg_catalog.json",
		},
		{
			name:     "pg_catalog.jsonb cast should strip prefix and convert to json",
			input:    "SELECT data::pg_catalog.jsonb FROM t",
			excludes: "pg_catalog.jsonb",
		},
		{
			name:     "unqualified json cast should not get pg_catalog prefix in output",
			input:    "SELECT data::json FROM t",
			excludes: "pg_catalog.json", // pg_query adds pg_catalog prefix, but we should strip it
		},
		{
			name:     "json cast in complex expression",
			input:    "SELECT custom_subscriber_attributes::pg_catalog.json AS attrs FROM subscribers",
			excludes: "pg_catalog.json",
		},
		{
			name:     "multiple json casts",
			input:    "SELECT a::pg_catalog.json, b::pg_catalog.jsonb FROM t",
			excludes: "pg_catalog.json",
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

func TestTranspile_SQLSyntaxFunctions(t *testing.T) {
	// Test SQL standard syntax functions (COERCE_SQL_SYNTAX functions)
	// These use special syntax like EXTRACT(year FROM date) instead of regular function calls.
	// The pg_query deparser only outputs SQL syntax when BOTH:
	// 1. funcformat = COERCE_SQL_SYNTAX
	// 2. function has pg_catalog prefix
	//
	// For functions that map to the same name (extract→extract), we preserve the prefix
	// so SQL syntax is preserved. For renamed functions (btrim→trim), we strip the prefix
	// so it outputs as a regular function call.
	tests := []struct {
		name     string
		input    string
		contains []string // all must be present
		excludes []string // none must be present
	}{
		// Same-name mappings - should preserve SQL syntax
		{
			name:     "EXTRACT preserves SQL syntax with FROM keyword",
			input:    "SELECT EXTRACT(year FROM DATE '2020-01-01')",
			contains: []string{"extract", " from "}, // SQL syntax uses FROM keyword
			excludes: []string{`"extract"(`},        // should NOT be quoted function call format
		},
		{
			name:     "SUBSTRING preserves SQL syntax with FROM FOR keywords",
			input:    "SELECT SUBSTRING('hello' FROM 2 FOR 3)",
			contains: []string{"substring", " from ", " for "}, // SQL syntax uses FROM...FOR
			excludes: []string{`"substring"(`},                 // should NOT be quoted function call
		},
		{
			name:     "EXTRACT with different field",
			input:    "SELECT EXTRACT(month FROM timestamp '2020-06-15 10:30:00')",
			contains: []string{"extract", " from "},
			excludes: []string{`"extract"(`},
		},

		// Unmapped SQL syntax functions - should pass through unchanged
		{
			name:     "POSITION preserves SQL syntax with IN keyword",
			input:    "SELECT POSITION('a' IN 'abc')",
			contains: []string{"position", " in "},
			excludes: []string{`"position"(`},
		},
		{
			name:     "OVERLAY preserves SQL syntax with PLACING FROM FOR keywords",
			input:    "SELECT OVERLAY('hello' PLACING 'XX' FROM 2 FOR 3)",
			contains: []string{"overlay", " placing ", " from "},
			excludes: []string{`"overlay"(`},
		},

		// Renamed SQL syntax functions - btrim->trim strips prefix, uses function call
		{
			name:     "TRIM BOTH (btrim->trim) uses function call",
			input:    "SELECT TRIM(BOTH ' ' FROM '  hello  ')",
			contains: []string{"trim"},
			excludes: []string{"pg_catalog"}, // prefix should be stripped since name changes
		},

		// Same-name SQL syntax functions - ltrim/rtrim map to themselves, SQL syntax preserved
		{
			name:     "TRIM LEADING preserves SQL syntax",
			input:    "SELECT TRIM(LEADING ' ' FROM '  hello  ')",
			contains: []string{"trim", "leading"}, // SQL syntax preserved
			excludes: []string{`"ltrim"(`},
		},
		{
			name:     "TRIM TRAILING preserves SQL syntax",
			input:    "SELECT TRIM(TRAILING ' ' FROM '  hello  ')",
			contains: []string{"trim", "trailing"}, // SQL syntax preserved
			excludes: []string{`"rtrim"(`},
		},

		// Complex expressions with SQL syntax functions
		{
			name:     "EXTRACT in expression preserves SQL syntax",
			input:    "SELECT EXTRACT(year FROM created_at) + 1 FROM events",
			contains: []string{"extract", " from created_at"}, // SQL syntax uses FROM keyword
			excludes: []string{`"extract"(`},
		},
		{
			name:     "Multiple SQL syntax functions",
			input:    "SELECT EXTRACT(year FROM d), SUBSTRING(s FROM 1 FOR 5) FROM t",
			contains: []string{"extract", "substring", " from d"},
			excludes: []string{`"extract"(`, `"substring"(`},
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
			for _, c := range tt.contains {
				if !strings.Contains(lowerSQL, strings.ToLower(c)) {
					t.Errorf("Transpile(%q) = %q, should contain %q", tt.input, result.SQL, c)
				}
			}
			for _, e := range tt.excludes {
				if strings.Contains(lowerSQL, strings.ToLower(e)) {
					t.Errorf("Transpile(%q) = %q, should NOT contain %q", tt.input, result.SQL, e)
				}
			}
		})
	}
}

func TestTranspile_FallbackParamCount(t *testing.T) {
	// Test that DuckDB-specific syntax with ConvertPlaceholders=true still correctly
	// counts $N parameter placeholders.
	// Queries with $ go through Tier 1 (FlagPlaceholder) → pg_query fails → FallbackToNative
	// Queries without $ go through Tier 0 (Direct) → param count via regex (returns 0)
	tests := []struct {
		name           string
		input          string
		paramCount     int
		wantFallback   bool // true for Tier 1 (has $), false for Tier 0 (no $)
	}{
		{
			name:         "DuckDB FROM-first with parameter",
			input:        "FROM users SELECT name WHERE id = $1",
			paramCount:   1,
			wantFallback: true,
		},
		{
			name:         "DuckDB SELECT EXCLUDE with parameter",
			input:        "SELECT * EXCLUDE (email) FROM users WHERE id = $1",
			paramCount:   1,
			wantFallback: true,
		},
		{
			name:         "DuckDB DESCRIBE with no params - Tier 0 Direct",
			input:        "DESCRIBE SELECT 1 AS num",
			paramCount:   0,
			wantFallback: false, // No $ → no FlagPlaceholder → Tier 0 Direct
		},
		{
			name:         "DuckDB QUALIFY with multiple params",
			input:        "SELECT id, name FROM users WHERE status = $1 QUALIFY row_number() OVER (ORDER BY id) <= $2",
			paramCount:   2,
			wantFallback: true,
		},
		{
			name:         "DuckDB list_filter with param",
			input:        "SELECT list_filter([1, 2, 3, 4, 5], x -> x > $1) AS filtered",
			paramCount:   1,
			wantFallback: true,
		},
		{
			name:         "Out of order params",
			input:        "FROM users SELECT $3, $1, $2",
			paramCount:   3,
			wantFallback: true,
		},
	}

	tr := New(Config{ConvertPlaceholders: true})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			if result.FallbackToNative != tt.wantFallback {
				t.Errorf("Transpile(%q) FallbackToNative = %v, want %v", tt.input, result.FallbackToNative, tt.wantFallback)
			}
			if result.ParamCount != tt.paramCount {
				t.Errorf("Transpile(%q) ParamCount = %d, want %d", tt.input, result.ParamCount, tt.paramCount)
			}
		})
	}
}

func TestTranspile_RegexOperators(t *testing.T) {
	// Test regex operator transformation from PostgreSQL to DuckDB
	// PostgreSQL: text ~ pattern -> DuckDB: regexp_matches(text, pattern)
	// PostgreSQL: text ~* pattern -> DuckDB: regexp_matches(text, pattern, 'i')
	// PostgreSQL: text !~ pattern -> DuckDB: NOT regexp_matches(text, pattern)
	// PostgreSQL: text !~* pattern -> DuckDB: NOT regexp_matches(text, pattern, 'i')
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "case-sensitive regex match",
			input:    "SELECT * FROM users WHERE name ~ '^A'",
			contains: "regexp_matches",
			excludes: " ~ ",
		},
		{
			name:     "case-insensitive regex match",
			input:    "SELECT * FROM users WHERE name ~* '^a'",
			contains: "regexp_matches",
			excludes: " ~* ",
		},
		{
			name:     "case-sensitive regex NOT match",
			input:    "SELECT * FROM users WHERE name !~ '^A'",
			contains: "NOT regexp_matches",
			excludes: " !~ ",
		},
		{
			name:     "case-insensitive regex NOT match",
			input:    "SELECT * FROM users WHERE name !~* '^a'",
			contains: "NOT regexp_matches",
			excludes: " !~* ",
		},
		{
			name:     "case-insensitive includes 'i' flag",
			input:    "SELECT * FROM users WHERE name ~* 'pattern'",
			contains: "'i'",
		},
		{
			name:     "regex in SELECT expression",
			input:    "SELECT name ~ 'test' AS matches FROM users",
			contains: "regexp_matches",
		},
		{
			name:     "regex in JOIN condition",
			input:    "SELECT * FROM users u JOIN emails e ON e.email ~ u.pattern",
			contains: "regexp_matches",
		},
		{
			name:     "regex with column references",
			input:    "SELECT * FROM patterns WHERE subject ~ regex_pattern",
			contains: "regexp_matches(subject, regex_pattern)",
		},
		{
			name:     "regex in CASE expression",
			input:    "SELECT CASE WHEN name ~ '^A' THEN 'A' ELSE 'Other' END FROM users",
			contains: "regexp_matches(name, '^A')",
		},
		{
			name:     "regex combined with AND",
			input:    "SELECT * FROM users WHERE name ~ '^A' AND active = true",
			contains: "regexp_matches",
		},
		{
			name:     "regex combined with OR",
			input:    "SELECT * FROM users WHERE name ~ '^A' OR name ~ '^B'",
			contains: "regexp_matches",
		},
		{
			name:     "regex in CTE",
			input:    "WITH filtered AS (SELECT * FROM users WHERE name ~ '^A') SELECT * FROM filtered",
			contains: "regexp_matches",
		},
		{
			name:     "unary bitwise NOT is not converted to regex",
			input:    "SELECT id, ~id AS bnot FROM users",
			excludes: "regexp_matches",
		},
		{
			name:     "unary bitwise NOT in WHERE clause",
			input:    "SELECT * FROM users WHERE ~id = -2",
			excludes: "regexp_matches",
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
				t.Errorf("Transpile(%q) = %q, want to contain %q", tt.input, result.SQL, tt.contains)
			}
			if tt.excludes != "" && strings.Contains(result.SQL, tt.excludes) {
				t.Errorf("Transpile(%q) = %q, should not contain %q", tt.input, result.SQL, tt.excludes)
			}
		})
	}
}

func TestTranspile_SimilarTo(t *testing.T) {
	// Test SIMILAR TO pattern matching
	// PostgreSQL converts SIMILAR TO to use similar_to_escape() and regex matching
	// We provide a similar_to_escape macro that converts SQL patterns to regex
	tests := []struct {
		name     string
		input    string
		contains []string
		excludes []string
	}{
		{
			name:     "SIMILAR TO converts to similar_to_escape and regexp_matches",
			input:    "SELECT 'hello' SIMILAR TO 'h%'",
			contains: []string{"similar_to_escape", "regexp_matches"},
			excludes: []string{"SIMILAR TO"},
		},
		{
			name:     "NOT SIMILAR TO includes NOT",
			input:    "SELECT 'hello' NOT SIMILAR TO 'x%'",
			contains: []string{"NOT", "similar_to_escape", "regexp_matches"},
			excludes: []string{"SIMILAR TO"},
		},
		{
			name:     "SIMILAR TO in WHERE clause",
			input:    "SELECT * FROM users WHERE name SIMILAR TO '%smith%'",
			contains: []string{"similar_to_escape", "regexp_matches"},
		},
		{
			name:     "SIMILAR TO with underscore pattern",
			input:    "SELECT 'ab' SIMILAR TO 'a_'",
			contains: []string{"similar_to_escape", "regexp_matches"},
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

func TestTranspile_CollatePgCatalogDefault(t *testing.T) {
	// Test that COLLATE pg_catalog.default is completely removed from queries
	// This is used by psql's \d command and was causing "COLLATE )" syntax errors
	// because only the collation name was being stripped, not the entire COLLATE clause
	tests := []struct {
		name     string
		input    string
		excludes []string
	}{
		{
			name:     "COLLATE pg_catalog.default in WHERE clause",
			input:    "SELECT * FROM t WHERE name COLLATE pg_catalog.default = 'test'",
			excludes: []string{"COLLATE", "pg_catalog.default"},
		},
		{
			name:     "COLLATE pg_catalog.default with regex operator",
			input:    "SELECT * FROM pg_class WHERE relname ~ '^users$' COLLATE pg_catalog.default",
			excludes: []string{"COLLATE", "pg_catalog.default"},
		},
		{
			name:     "COLLATE pg_catalog.\"default\" with quoted default",
			input:    `SELECT * FROM t WHERE name COLLATE pg_catalog."default" = 'test'`,
			excludes: []string{"COLLATE"},
		},
		{
			name:     "multiple COLLATE clauses",
			input:    "SELECT a COLLATE pg_catalog.default, b COLLATE pg_catalog.default FROM t",
			excludes: []string{"COLLATE"},
		},
		{
			name:     "COLLATE in ORDER BY",
			input:    "SELECT * FROM t ORDER BY name COLLATE pg_catalog.default",
			excludes: []string{"COLLATE"},
		},
	}

	tr := New(DefaultConfig())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Transpile(tt.input)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", tt.input, err)
			}
			for _, e := range tt.excludes {
				if strings.Contains(result.SQL, e) {
					t.Errorf("Transpile(%q) = %q, should NOT contain %q", tt.input, result.SQL, e)
				}
			}
		})
	}
}

func TestTranspile_SimilarTo_DuckLakeMode(t *testing.T) {
	// Test that similar_to_escape gets memory.main prefix in DuckLake mode
	tr := New(Config{DuckLakeMode: true})

	result, err := tr.Transpile("SELECT 'hello' SIMILAR TO 'h%'")
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}
	if !strings.Contains(result.SQL, "memory.main.similar_to_escape") {
		t.Errorf("In DuckLake mode, similar_to_escape should have memory.main prefix, got: %q", result.SQL)
	}
}

func TestTranspile_OrderByTransforms(t *testing.T) {
	// Test that ORDER BY clause expressions are properly transformed
	// This was broken because Node_SortBy wasn't being walked
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "pg_catalog function in ORDER BY",
			input:    "SELECT * FROM pg_class ORDER BY pg_catalog.pg_get_expr(relpartbound, oid)",
			contains: "pg_get_expr",
			excludes: "pg_catalog.pg_get_expr",
		},
		{
			name:     "regclass cast in ORDER BY",
			input:    "SELECT * FROM pg_class ORDER BY oid::pg_catalog.regclass",
			contains: "varchar",
			excludes: "regclass",
		},
		{
			name:     "type cast in ORDER BY",
			input:    "SELECT * FROM t ORDER BY data::pg_catalog.text",
			contains: "varchar",
			excludes: "pg_catalog.text",
		},
		{
			name:     "multiple expressions in ORDER BY",
			input:    "SELECT * FROM pg_class ORDER BY oid::pg_catalog.regclass, pg_catalog.pg_table_is_visible(oid)",
			contains: "pg_table_is_visible",
			excludes: "pg_catalog.pg_table_is_visible",
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

func TestTranspile_RangeFunction(t *testing.T) {
	// Test that functions in FROM clause (RangeFunction) are properly transformed
	// This is used by queries like "SELECT * FROM unnest(array)"
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "pg_catalog.unnest in FROM clause",
			input:    "SELECT x FROM pg_catalog.unnest(ARRAY[1,2,3]) AS x",
			contains: "unnest",
			excludes: "pg_catalog.unnest",
		},
		{
			name:     "unnest in subquery FROM clause",
			input:    "SELECT * FROM (SELECT x FROM pg_catalog.unnest(arr) AS x) sub",
			contains: "unnest",
			excludes: "pg_catalog.unnest",
		},
		{
			name:     "multiple functions in FROM clause",
			input:    "SELECT x FROM pg_catalog.unnest(ARRAY[1,2,3]) AS x, pg_catalog.unnest(ARRAY[4,5,6]) AS y",
			contains: "unnest",
			excludes: "pg_catalog.unnest",
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

func TestTranspile_ArrayUpperInRangeFunction(t *testing.T) {
	// Test that array_upper inside a FROM-clause function (RangeFunction) is properly
	// transformed to len() with the dimension argument stripped. This is the exact
	// pattern used by the pgx driver for type resolution.
	tr := New(DefaultConfig())

	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "array_upper in generate_series FROM clause",
			input:    "SELECT s.r FROM generate_series(1, array_upper(current_schemas(false), 1)) AS s(r)",
			contains: "len(current_schemas(false))",
			excludes: "array_upper",
		},
		{
			name:     "array_upper in SELECT expression",
			input:    "SELECT array_upper(ARRAY[1,2,3], 1)",
			contains: "len(ARRAY[1, 2, 3])",
			excludes: "array_upper",
		},
		{
			name:     "array_length dimension arg stripped",
			input:    "SELECT array_length(ARRAY[1,2,3], 1)",
			contains: "len(ARRAY[1, 2, 3])",
			excludes: "array_length",
		},
	}

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

func TestTranspile_CtidToRowid(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "SELECT ctid -> rowid",
			input:    "SELECT ctid FROM users",
			contains: "rowid",
			excludes: "ctid",
		},
		{
			name:     "qualified table.ctid -> table.rowid",
			input:    "SELECT u.ctid FROM users u",
			contains: "rowid",
			excludes: "ctid",
		},
		{
			name:     "ctid in WHERE clause",
			input:    "SELECT * FROM users WHERE ctid = '(0,1)'",
			contains: "rowid",
			excludes: "ctid",
		},
		{
			name:     "ctid in DELETE",
			input:    "DELETE FROM users WHERE ctid = '(0,1)'",
			contains: "rowid",
			excludes: "ctid",
		},
		{
			name:     "ctid case insensitive",
			input:    "SELECT CTID FROM users",
			contains: "rowid",
			excludes: "ctid",
		},
		{
			name:     "no false positive on ctid-like column name",
			input:    "SELECT acid FROM users",
			contains: "acid",
		},
		{
			name:     "ctid in subquery",
			input:    "SELECT * FROM (SELECT ctid, name FROM users) sub",
			contains: "rowid",
			excludes: "ctid",
		},
		{
			name:     "ctid BETWEEN ::tid replaced with TRUE",
			input:    "SELECT * FROM users WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid",
			contains: "true",
			excludes: "tid",
		},
		{
			name:     "ctid BETWEEN with other conditions preserved",
			input:    "SELECT * FROM users WHERE id > 0 AND ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid",
			contains: "true",
			excludes: "tid",
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

func TestTranspile_PgCatalogStubViews(t *testing.T) {
	// Test that new stub views (pg_constraint, pg_enum, pg_indexes) are mapped correctly
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "pg_catalog.pg_constraint -> memory.main.pg_constraint",
			input:    "SELECT * FROM pg_catalog.pg_constraint",
			contains: "memory.main.pg_constraint",
			excludes: "pg_catalog",
		},
		{
			name:     "pg_catalog.pg_enum -> memory.main.pg_enum",
			input:    "SELECT * FROM pg_catalog.pg_enum",
			contains: "memory.main.pg_enum",
			excludes: "pg_catalog",
		},
		{
			name:     "pg_catalog.pg_indexes -> memory.main.pg_indexes",
			input:    "SELECT * FROM pg_catalog.pg_indexes",
			contains: "memory.main.pg_indexes",
			excludes: "pg_catalog",
		},
		{
			name:     "unqualified pg_constraint -> memory.main.pg_constraint",
			input:    "SELECT conname FROM pg_constraint WHERE contype = 'p'",
			contains: "memory.main.pg_constraint",
		},
		{
			name:     "unqualified pg_enum -> memory.main.pg_enum",
			input:    "SELECT enumlabel FROM pg_enum",
			contains: "memory.main.pg_enum",
		},
		{
			name:     "unqualified pg_indexes -> memory.main.pg_indexes",
			input:    "SELECT indexname FROM pg_indexes WHERE tablename = 'users'",
			contains: "memory.main.pg_indexes",
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

func TestTranspile_ViewStmtWalking(t *testing.T) {
	// Test that CREATE VIEW ... AS SELECT has its inner query transformed
	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "CREATE VIEW with pg_catalog reference",
			input:    "CREATE VIEW myview AS SELECT * FROM pg_catalog.pg_class",
			contains: "memory.main.pg_class_full",
			excludes: "pg_catalog",
		},
		{
			name:     "CREATE VIEW with public schema",
			input:    "CREATE VIEW myview AS SELECT * FROM public.users",
			contains: "main.users",
			excludes: "public.users",
		},
		{
			name:     "CREATE OR REPLACE VIEW with ctid",
			input:    "CREATE OR REPLACE VIEW myview AS SELECT ctid, name FROM users",
			contains: "rowid",
			excludes: "ctid",
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

func TestTranspile_CustomMacros_DuckLakeMode(t *testing.T) {
	// Test that custom macros get memory.main. prefix in DuckLake mode
	// These macros are created in memory.main and need explicit qualification
	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{
			name:     "pg_get_expr gets memory.main prefix",
			input:    "SELECT pg_catalog.pg_get_expr(adbin, adrelid) FROM pg_attrdef",
			contains: "memory.main.pg_get_expr",
		},
		{
			name:     "pg_get_indexdef gets memory.main prefix",
			input:    "SELECT pg_catalog.pg_get_indexdef(indexrelid) FROM pg_index",
			contains: "memory.main.pg_get_indexdef",
		},
		{
			name:     "pg_get_constraintdef gets memory.main prefix",
			input:    "SELECT pg_catalog.pg_get_constraintdef(oid) FROM pg_constraint",
			contains: "memory.main.pg_get_constraintdef",
		},
		{
			name:     "obj_description gets memory.main prefix",
			input:    "SELECT pg_catalog.obj_description(oid, 'pg_class') FROM pg_class",
			contains: "memory.main.obj_description",
		},
		{
			name:     "col_description gets memory.main prefix",
			input:    "SELECT pg_catalog.col_description(attrelid, attnum) FROM pg_attribute",
			contains: "memory.main.col_description",
		},
		{
			name:     "format_type gets memory.main prefix",
			input:    "SELECT pg_catalog.format_type(atttypid, atttypmod) FROM pg_attribute",
			contains: "memory.main.format_type",
		},
	}

	tr := New(Config{DuckLakeMode: true})

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

// --- Three-Tier Intercept Tests ---

func TestClassify_Direct(t *testing.T) {
	// Plain queries with no PostgreSQL-specific patterns should be classified as Direct
	cfg := DefaultConfig()

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
		{"create table", "CREATE TABLE users (id INTEGER, name VARCHAR)"},
		{"drop table", "DROP TABLE users"},
		{"create schema", "CREATE SCHEMA test"},
		{"CTE read-only", "WITH active AS (SELECT * FROM users WHERE active) SELECT * FROM active"},
		{"subquery", "SELECT * FROM (SELECT id, name FROM users) AS u"},
		{"JOIN", "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"},
		{"UNION", "SELECT name FROM users UNION SELECT name FROM admins"},
		{"comment prefix", "/* sync_id:abc123 */ SELECT * FROM users"},
		{"DuckDB DESCRIBE", "DESCRIBE SELECT * FROM users"},
		{"DuckDB PRAGMA", "PRAGMA database_list"},
		{"DuckDB FROM-first", "FROM users SELECT name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cls := Classify(tt.input, cfg)
			if !cls.Direct {
				t.Errorf("Classify(%q) = {Direct: false, Flags: %d}, want Direct", tt.input, cls.Flags)
			}
		})
	}
}

func TestClassify_NeedsTransform(t *testing.T) {
	// Queries with PostgreSQL-specific patterns should return correct flags
	cfg := DefaultConfig()

	tests := []struct {
		name      string
		input     string
		wantFlags TransformFlags // flags that MUST be set (may have more)
	}{
		{"SET command", "SET application_name = 'test'", FlagSetShow},
		{"SHOW command", "SHOW server_version", FlagSetShow},
		{"RESET command", "RESET ALL", FlagSetShow},
		{"BEGIN", "BEGIN", FlagSetShow},
		{"pg_catalog table", "SELECT * FROM pg_catalog.pg_class", FlagPgCatalog},
		{"pg_class unqualified", "SELECT * FROM pg_class", FlagPgCatalog},
		{"information_schema", "SELECT * FROM information_schema.columns", FlagInfoSchema},
		{"public schema", "SELECT * FROM public.users", FlagPublicSchema},
		{"version()", "SELECT version()", FlagVersion | FlagPgCatalog},
		{"JSONB type", "CREATE TABLE t (data JSONB)", FlagTypeMapping | FlagPgCatalog},
		{"BYTEA type", "CREATE TABLE t (data BYTEA)", FlagTypeMapping | FlagPgCatalog},
		{"::regtype cast", "SELECT typname::regtype FROM pg_type", FlagTypeCast | FlagPgCatalog},
		{"::regclass cast", "SELECT 'users'::regclass", FlagTypeCast | FlagPgCatalog},
		{"array_agg function", "SELECT array_agg(x) FROM t", FlagFunctions | FlagPgCatalog},
		{"current_database", "SELECT current_database()", FlagFuncAlias | FlagPgCatalog},
		{"JSON arrow", "SELECT data->>'name' FROM t", FlagOperators | FlagPgCatalog},
		{"regex operator", "SELECT * FROM t WHERE name ~ '^A'", FlagOperators | FlagPgCatalog},
		{"FOR UPDATE", "SELECT * FROM t FOR UPDATE", FlagLocking},
		{"ctid", "SELECT ctid FROM t", FlagCtid},
		{"_pg_expandarray", "SELECT (_pg_expandarray(arr)).n FROM t", FlagExpandArray},
		{"ON CONFLICT", "INSERT INTO t (id) VALUES (1) ON CONFLICT DO NOTHING", FlagOnConflict},
		{"placeholder $1", "SELECT * FROM users WHERE id = $1", FlagPlaceholder},
		{"SIMILAR TO", "SELECT 'hello' SIMILAR TO 'h%'", FlagOperators | FlagPgCatalog},
		{"COLLATE", "SELECT * FROM t ORDER BY name COLLATE pg_catalog.default", FlagOperators | FlagPgCatalog},
		{"SET with comment prefix", "/* ETL */ SET statement_timeout = 5000", FlagSetShow},
		{"SET with line comment prefix", "-- setup\nSET statement_timeout = 5000", FlagSetShow},
		{"SHOW after multiple comments", "-- first\n-- second\nSHOW server_version", FlagSetShow},
		{"SET after mixed comments", "/* block */\n-- line\nSET search_path = 'main'", FlagSetShow},
	}

	for _, tt := range tests {
		cfgToUse := cfg
		// Enable ConvertPlaceholders for placeholder test
		if tt.wantFlags&FlagPlaceholder != 0 {
			cfgToUse = Config{ConvertPlaceholders: true}
		}

		t.Run(tt.name, func(t *testing.T) {
			cls := Classify(tt.input, cfgToUse)
			if cls.Direct {
				t.Errorf("Classify(%q) = Direct, want flags containing %d", tt.input, tt.wantFlags)
				return
			}
			if cls.Flags&tt.wantFlags != tt.wantFlags {
				t.Errorf("Classify(%q) flags = %d, want flags to contain %d (missing: %d)",
					tt.input, cls.Flags, tt.wantFlags, tt.wantFlags&^cls.Flags)
			}
		})
	}
}

func TestClassify_DuckLakeMode(t *testing.T) {
	cfg := Config{DuckLakeMode: true}

	tests := []struct {
		name      string
		input     string
		wantFlags TransformFlags
	}{
		{"CREATE INDEX is DDL", "CREATE INDEX idx ON users (name)", FlagDDL},
		{"PRIMARY KEY is DDL", "CREATE TABLE t (id INT PRIMARY KEY)", FlagDDL},
		{"SERIAL is DDL", "CREATE TABLE t (id SERIAL)", FlagDDL},
		{"ALTER TABLE is DDL", "ALTER TABLE users ADD CONSTRAINT pk PRIMARY KEY (id)", FlagDDL},
		{"VACUUM is DDL", "VACUUM users", FlagDDL},
		{"CASCADE is DDL", "DROP TABLE users CASCADE", FlagDDL},
		{"REINDEX is DDL", "REINDEX TABLE users", FlagDDL},
		{"CLUSTER is DDL", "CLUSTER users USING idx_name", FlagDDL},
		{"COMMENT ON is DDL", "COMMENT ON TABLE users IS 'desc'", FlagDDL},
		{"REFRESH is DDL", "REFRESH MATERIALIZED VIEW my_view", FlagDDL},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cls := Classify(tt.input, cfg)
			if cls.Direct {
				t.Errorf("Classify(%q, DuckLake) = Direct, want flags containing %d", tt.input, tt.wantFlags)
				return
			}
			if cls.Flags&tt.wantFlags != tt.wantFlags {
				t.Errorf("Classify(%q, DuckLake) flags = %d, want flags to contain %d",
					tt.input, cls.Flags, tt.wantFlags)
			}
		})
	}

	// Same queries should be Direct in non-DuckLake mode (except ones with other PG patterns)
	nonDLCfg := DefaultConfig()
	t.Run("CREATE INDEX direct in non-DuckLake", func(t *testing.T) {
		cls := Classify("CREATE INDEX idx ON users (name)", nonDLCfg)
		if !cls.Direct {
			t.Errorf("CREATE INDEX should be Direct in non-DuckLake mode, got flags %d", cls.Flags)
		}
	})
}

func TestTier0MatchesTier1(t *testing.T) {
	// Verify that queries classified as Direct produce the same result
	// as running through the full pipeline via TranspileAll.
	// This ensures the classifier doesn't miss any needed transforms.
	tr := New(DefaultConfig())

	directQueries := []string{
		"SELECT 1",
		"SELECT * FROM users",
		"SELECT * FROM users WHERE id = 1",
		"INSERT INTO users (name) VALUES ('test')",
		"UPDATE users SET name = 'test' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
		"CREATE TABLE users (id INTEGER, name VARCHAR)",
		"DROP TABLE users",
		"SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id",
		"SELECT name FROM users UNION SELECT name FROM admins",
		"WITH cte AS (SELECT 1 AS id) SELECT * FROM cte",
	}

	for _, sql := range directQueries {
		t.Run(sql, func(t *testing.T) {
			// Verify it's classified as Direct
			cls := Classify(sql, DefaultConfig())
			if !cls.Direct {
				t.Fatalf("Expected Direct classification for %q, got flags %d", sql, cls.Flags)
			}

			// Get Tier 0 result (Direct)
			tier0Result, err := tr.Transpile(sql)
			if err != nil {
				t.Fatalf("Transpile(%q) error: %v", sql, err)
			}

			// Get full pipeline result
			tier1Result, err := tr.TranspileAll(sql)
			if err != nil {
				t.Fatalf("TranspileAll(%q) error: %v", sql, err)
			}

			// Both should produce equivalent results
			// Tier 0 returns original SQL; Tier 1 does parse+deparse which may normalize
			// So we just check that both are non-empty and have the same metadata
			if tier0Result.SQL == "" {
				t.Error("Tier 0 returned empty SQL")
			}
			if tier1Result.SQL == "" {
				t.Error("Tier 1 returned empty SQL")
			}
			if tier0Result.IsNoOp != tier1Result.IsNoOp {
				t.Errorf("IsNoOp mismatch: Tier 0 = %v, Tier 1 = %v", tier0Result.IsNoOp, tier1Result.IsNoOp)
			}
			if tier0Result.IsIgnoredSet != tier1Result.IsIgnoredSet {
				t.Errorf("IsIgnoredSet mismatch: Tier 0 = %v, Tier 1 = %v", tier0Result.IsIgnoredSet, tier1Result.IsIgnoredSet)
			}
			if tier0Result.FallbackToNative != tier1Result.FallbackToNative {
				t.Errorf("FallbackToNative mismatch: Tier 0 = %v, Tier 1 = %v", tier0Result.FallbackToNative, tier1Result.FallbackToNative)
			}
		})
	}
}

func BenchmarkTranspile_Direct(b *testing.B) {
	// Tier 0: No PG patterns → skip parse entirely
	tr := New(DefaultConfig())
	query := "SELECT * FROM users WHERE id = 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tr.Transpile(query)
	}
}

func BenchmarkTranspile_OldAllTransforms(b *testing.B) {
	// Full pipeline (TranspileAll) for comparison with Tier 0
	tr := New(DefaultConfig())
	query := "SELECT * FROM users WHERE id = 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tr.TranspileAll(query)
	}
}

func BenchmarkClassify(b *testing.B) {
	cfg := DefaultConfig()
	query := "SELECT * FROM users WHERE id = 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Classify(query, cfg)
	}
}

func BenchmarkClassify_PgCatalog(b *testing.B) {
	cfg := DefaultConfig()
	query := "SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Classify(query, cfg)
	}
}
