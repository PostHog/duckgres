package server

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/server/sessionmeta"
)

type recordingQueryExecutor struct {
	noopProfiling
	query      string
	args       []any
	rowSet     RowSet
	queryError error
}

func (e *recordingQueryExecutor) QueryContext(_ context.Context, query string, args ...any) (RowSet, error) {
	e.query = query
	e.args = append([]any(nil), args...)
	if e.queryError != nil {
		return nil, e.queryError
	}
	return e.rowSet, nil
}

func (e *recordingQueryExecutor) ExecContext(context.Context, string, ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *recordingQueryExecutor) Query(string, ...any) (RowSet, error) {
	return nil, errors.New("not implemented")
}

func (e *recordingQueryExecutor) Exec(string, ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *recordingQueryExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}

func (e *recordingQueryExecutor) PingContext(context.Context) error {
	return errors.New("not implemented")
}

func (e *recordingQueryExecutor) Close() error {
	return nil
}

type staticCountRowSet struct {
	count    int
	returned bool
}

func (r *staticCountRowSet) Columns() ([]string, error) { return []string{"count"}, nil }
func (r *staticCountRowSet) ColumnTypes() ([]ColumnTyper, error) {
	return []ColumnTyper{describeColumnType("BIGINT")}, nil
}
func (r *staticCountRowSet) Next() bool {
	if r.returned {
		return false
	}
	r.returned = true
	return true
}
func (r *staticCountRowSet) Scan(dest ...any) error {
	*(dest[0].(*interface{})) = int64(r.count)
	return nil
}
func (r *staticCountRowSet) Close() error { return nil }
func (r *staticCountRowSet) Err() error   { return nil }

func TestHasAttachedCatalogEmbedsCatalogNameWithoutBoundArgs(t *testing.T) {
	exec := &recordingQueryExecutor{
		rowSet: &staticCountRowSet{count: 1},
	}

	attached, err := sessionmeta.HasAttachedCatalog(context.Background(), exec, "ducklake")
	if err != nil {
		t.Fatalf("sessionmeta.HasAttachedCatalog returned error: %v", err)
	}
	if !attached {
		t.Fatal("expected attached catalog to be detected")
	}
	wantQuery := "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'ducklake'"
	if exec.query != wantQuery {
		t.Fatalf("query = %q, want %q", exec.query, wantQuery)
	}
	if len(exec.args) != 0 {
		t.Fatalf("expected no bound args, got %d", len(exec.args))
	}
}

func TestInitSessionDatabaseMetadataOverridesCurrentDatabaseAndPgDatabase(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	executor := NewLocalExecutor(db)
	if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, "analytics"); err != nil {
		t.Fatalf("init session database metadata: %v", err)
	}

	var currentDB string
	if err := db.QueryRow("SELECT current_database()").Scan(&currentDB); err != nil {
		t.Fatalf("query current_database(): %v", err)
	}
	if currentDB != "analytics" {
		t.Fatalf("current_database() = %q, want %q", currentDB, "analytics")
	}

	var datname string
	if err := db.QueryRow("SELECT datname FROM pg_database WHERE datname = current_database()").Scan(&datname); err != nil {
		t.Fatalf("query pg_database/current_database: %v", err)
	}
	if datname != "analytics" {
		t.Fatalf("pg_database datname = %q, want %q", datname, "analytics")
	}

	if err := db.QueryRow("SELECT datname FROM memory.main.pg_database WHERE datname = current_database()").Scan(&datname); err != nil {
		t.Fatalf("query memory.main.pg_database/current_database: %v", err)
	}
	if datname != "analytics" {
		t.Fatalf("memory.main.pg_database datname = %q, want %q", datname, "analytics")
	}
}

func TestInitSessionDatabaseMetadataOverridesInformationSchemaCatalogColumns(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`ATTACH ':memory:' AS ducklake`); err != nil {
		t.Fatalf("attach ducklake: %v", err)
	}
	if err := initInformationSchema(db, true); err != nil {
		t.Fatalf("init information_schema: %v", err)
	}
	if _, err := db.Exec("USE ducklake"); err != nil {
		t.Fatalf("use ducklake: %v", err)
	}
	if _, err := db.Exec("CREATE SCHEMA bill"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE bill.users(id INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec("CREATE VIEW bill.user_view AS SELECT id FROM bill.users"); err != nil {
		t.Fatalf("create view: %v", err)
	}

	executor := NewLocalExecutor(db)
	if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, "analytics"); err != nil {
		t.Fatalf("init session database metadata: %v", err)
	}

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "tables",
			query: "SELECT table_catalog FROM memory.main.information_schema_tables_compat WHERE table_schema = 'bill' AND table_name = 'users'",
			want:  "analytics",
		},
		{
			name:  "columns",
			query: "SELECT table_catalog FROM memory.main.information_schema_columns_compat WHERE table_schema = 'bill' AND table_name = 'users' AND column_name = 'id'",
			want:  "analytics",
		},
		{
			name:  "views",
			query: "SELECT table_catalog FROM memory.main.information_schema_views_compat WHERE table_schema = 'bill' AND table_name = 'user_view'",
			want:  "analytics",
		},
		{
			name:  "schemata",
			query: "SELECT catalog_name FROM memory.main.information_schema_schemata_compat WHERE schema_name = 'public'",
			want:  "analytics",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			if err := db.QueryRow(tc.query).Scan(&got); err != nil {
				t.Fatalf("%s query: %v", tc.name, err)
			}
			if got != tc.want {
				t.Fatalf("%s = %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

func TestInitSessionDatabaseMetadataExcludesInternalDuckLakeMetadataCatalogs(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`ATTACH ':memory:' AS ducklake`); err != nil {
		t.Fatalf("attach ducklake: %v", err)
	}
	if err := initInformationSchema(db, true); err != nil {
		t.Fatalf("init information_schema: %v", err)
	}
	if _, err := db.Exec("USE ducklake"); err != nil {
		t.Fatalf("use ducklake: %v", err)
	}

	executor := NewLocalExecutor(db)
	if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, "analytics"); err != nil {
		t.Fatalf("init session database metadata: %v", err)
	}

	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM memory.main.information_schema_schemata_compat
		WHERE catalog_name LIKE '__ducklake_metadata_%'
	`).Scan(&count); err != nil {
		t.Fatalf("query internal metadata catalogs: %v", err)
	}
	if count != 0 {
		t.Fatalf("internal DuckLake metadata catalogs leaked into schemata view: got %d rows", count)
	}
}

func TestInformationSchemaCompatViewsOnlyExposeSelectedCatalog(t *testing.T) {
	tests := []struct {
		name           string
		catalog        string
		useAfterInit   string
		wantTables     string
		wantView       string
		wantColumnTbls string
	}{
		{
			name:           "ducklake",
			catalog:        "ducklake",
			wantTables:     "duck_only,duck_view",
			wantView:       "duck_view",
			wantColumnTbls: "duck_only,duck_view",
		},
		{
			name:           "iceberg",
			catalog:        "iceberg",
			useAfterInit:   "USE iceberg.public",
			wantTables:     "ice_only,ice_view",
			wantView:       "ice_view",
			wantColumnTbls: "ice_only,ice_view",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("duckdb", ":memory:")
			if err != nil {
				t.Fatalf("open duckdb: %v", err)
			}
			db.SetMaxOpenConns(1)
			defer func() { _ = db.Close() }()

			if _, err := db.Exec(`ATTACH ':memory:' AS ducklake`); err != nil {
				t.Fatalf("attach ducklake: %v", err)
			}
			if _, err := db.Exec(`ATTACH ':memory:' AS iceberg`); err != nil {
				t.Fatalf("attach iceberg: %v", err)
			}
			if err := initInformationSchema(db, true); err != nil {
				t.Fatalf("init information_schema: %v", err)
			}
			if _, err := db.Exec("USE ducklake"); err != nil {
				t.Fatalf("use ducklake: %v", err)
			}
			if _, err := db.Exec("CREATE SCHEMA shared"); err != nil {
				t.Fatalf("create ducklake schema: %v", err)
			}
			if _, err := db.Exec("CREATE TABLE shared.duck_only(id INTEGER)"); err != nil {
				t.Fatalf("create ducklake table: %v", err)
			}
			if _, err := db.Exec("CREATE VIEW shared.duck_view AS SELECT id FROM shared.duck_only"); err != nil {
				t.Fatalf("create ducklake view: %v", err)
			}
			if _, err := db.Exec("CREATE SCHEMA iceberg.public"); err != nil {
				t.Fatalf("create iceberg public schema: %v", err)
			}
			if _, err := db.Exec("CREATE SCHEMA iceberg.shared"); err != nil {
				t.Fatalf("create iceberg schema: %v", err)
			}
			if _, err := db.Exec("CREATE TABLE iceberg.shared.ice_only(id INTEGER)"); err != nil {
				t.Fatalf("create iceberg table: %v", err)
			}
			if _, err := db.Exec("CREATE VIEW iceberg.shared.ice_view AS SELECT id FROM iceberg.shared.ice_only"); err != nil {
				t.Fatalf("create iceberg view: %v", err)
			}

			executor := NewLocalExecutor(db)
			if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, tc.catalog); err != nil {
				t.Fatalf("init session database metadata: %v", err)
			}
			if tc.useAfterInit != "" {
				if _, err := db.Exec(tc.useAfterInit); err != nil {
					t.Fatalf("apply selected catalog: %v", err)
				}
			}

			assertSingleValue := func(label, query, want string) {
				t.Helper()
				var got string
				if err := db.QueryRow(query).Scan(&got); err != nil {
					t.Fatalf("%s query: %v", label, err)
				}
				if got != want {
					t.Fatalf("%s = %q, want %q", label, got, want)
				}
			}

			assertSingleValue("table", `
				SELECT string_agg(table_name, ',' ORDER BY table_name)
				FROM memory.main.information_schema_tables_compat
				WHERE table_schema = 'shared'
			`, tc.wantTables)
			assertSingleValue("view", `
				SELECT string_agg(table_name, ',' ORDER BY table_name)
				FROM memory.main.information_schema_views_compat
				WHERE table_schema = 'shared'
			`, tc.wantView)
			assertSingleValue("column table", `
				SELECT string_agg(DISTINCT table_name, ',' ORDER BY table_name)
				FROM memory.main.information_schema_columns_compat
				WHERE table_schema = 'shared'
					AND column_name = 'id'
			`, tc.wantColumnTbls)

			var schemaCount int
			if err := db.QueryRow(`
				SELECT COUNT(*)
				FROM memory.main.information_schema_schemata_compat
				WHERE schema_name = 'shared'
			`).Scan(&schemaCount); err != nil {
				t.Fatalf("schema query: %v", err)
			}
			if schemaCount != 1 {
				t.Fatalf("schema rows = %d, want 1", schemaCount)
			}
		})
	}
}

func TestPgCatalogCompatViewsOnlyExposeSelectedCatalog(t *testing.T) {
	tests := []struct {
		name          string
		selected      string
		useAfterInit  string
		wantTable     string
		rejectTable   string
		wantColumnRel string
		rejectColumn  string
	}{
		{
			name:          "ducklake session sees ducklake only",
			selected:      "ducklake",
			wantTable:     "duck_only",
			rejectTable:   "ice_only",
			wantColumnRel: "duck_only",
			rejectColumn:  "ice_only",
		},
		{
			name:          "iceberg session sees iceberg only",
			selected:      "iceberg",
			useAfterInit:  "USE iceberg.public",
			wantTable:     "ice_only",
			rejectTable:   "duck_only",
			wantColumnRel: "ice_only",
			rejectColumn:  "duck_only",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("duckdb", ":memory:")
			if err != nil {
				t.Fatalf("open duckdb: %v", err)
			}
			db.SetMaxOpenConns(1)
			defer func() { _ = db.Close() }()

			if err := initPgCatalog(db, time.Now(), time.Now(), "test", "test"); err != nil {
				t.Fatalf("init pg catalog: %v", err)
			}
			if _, err := db.Exec(`ATTACH ':memory:' AS ducklake`); err != nil {
				t.Fatalf("attach ducklake: %v", err)
			}
			if _, err := db.Exec(`ATTACH ':memory:' AS iceberg`); err != nil {
				t.Fatalf("attach iceberg: %v", err)
			}

			if _, err := db.Exec("USE ducklake"); err != nil {
				t.Fatalf("use ducklake: %v", err)
			}
			if _, err := db.Exec("CREATE SCHEMA shared"); err != nil {
				t.Fatalf("create ducklake schema: %v", err)
			}
			if _, err := db.Exec("CREATE TABLE shared.duck_only(id INTEGER, payload VARCHAR)"); err != nil {
				t.Fatalf("create ducklake table: %v", err)
			}

			if _, err := db.Exec("CREATE SCHEMA iceberg.public"); err != nil {
				t.Fatalf("create iceberg public schema: %v", err)
			}
			if _, err := db.Exec("CREATE SCHEMA iceberg.shared"); err != nil {
				t.Fatalf("create iceberg schema: %v", err)
			}
			if _, err := db.Exec("CREATE TABLE iceberg.shared.ice_only(id INTEGER, payload VARCHAR)"); err != nil {
				t.Fatalf("create iceberg table: %v", err)
			}
			if _, err := db.Exec("USE memory"); err != nil {
				t.Fatalf("use memory before pg catalog rewrite: %v", err)
			}
			if err := recreatePgClassForDuckLake(db); err != nil {
				t.Fatalf("recreate pg_class_full for ducklake: %v", err)
			}
			if err := recreatePgNamespaceForDuckLake(db); err != nil {
				t.Fatalf("recreate pg_namespace for ducklake: %v", err)
			}

			executor := NewLocalExecutor(db)
			if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, tc.selected); err != nil {
				t.Fatalf("init session database metadata: %v", err)
			}
			if tc.useAfterInit != "" {
				if _, err := db.Exec(tc.useAfterInit); err != nil {
					t.Fatalf("apply selected catalog: %v", err)
				}
			}

			var schemaCount int
			if err := db.QueryRow(`
				SELECT COUNT(*)
				FROM memory.main.pg_namespace
				WHERE nspname = 'shared'
			`).Scan(&schemaCount); err != nil {
				t.Fatalf("query pg_namespace: %v", err)
			}
			if schemaCount != 1 {
				t.Fatalf("shared schema count = %d, want 1", schemaCount)
			}

			var tables string
			if err := db.QueryRow(`
				SELECT string_agg(c.relname, ',' ORDER BY c.relname)
				FROM memory.main.pg_namespace n
				JOIN memory.main.pg_class_full c ON c.relnamespace = n.oid
				WHERE n.nspname = 'shared'
					AND c.relkind IN ('r', 'v', 'f', 'm')
			`).Scan(&tables); err != nil {
				t.Fatalf("query pg_class_full: %v", err)
			}
			if tables != tc.wantTable {
				t.Fatalf("tables = %q, want %q and not %q", tables, tc.wantTable, tc.rejectTable)
			}
			if strings.Contains(tables, tc.rejectTable) {
				t.Fatalf("tables leaked %q: %q", tc.rejectTable, tables)
			}

			var columnRelations string
			if err := db.QueryRow(`
				SELECT string_agg(DISTINCT c.relname, ',' ORDER BY c.relname)
				FROM memory.main.pg_namespace n
				JOIN memory.main.pg_class_full c ON c.relnamespace = n.oid
				JOIN memory.main.pg_attribute a ON a.attrelid = c.oid
				WHERE n.nspname = 'shared'
					AND a.attname = 'id'
					AND a.attnum > 0
					AND NOT a.attisdropped
			`).Scan(&columnRelations); err != nil {
				t.Fatalf("query pg_attribute join: %v", err)
			}
			if columnRelations != tc.wantColumnRel {
				t.Fatalf("column relations = %q, want %q and not %q", columnRelations, tc.wantColumnRel, tc.rejectColumn)
			}
			if strings.Contains(columnRelations, tc.rejectColumn) {
				t.Fatalf("column relations leaked %q: %q", tc.rejectColumn, columnRelations)
			}
		})
	}
}

func TestHexMetadataQueryDoesNotLeakDuckLakeIntoIceberg(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()

	if err := initPgCatalog(db, time.Now(), time.Now(), "test", "test"); err != nil {
		t.Fatalf("init pg catalog: %v", err)
	}
	if _, err := db.Exec(`ATTACH ':memory:' AS ducklake`); err != nil {
		t.Fatalf("attach ducklake: %v", err)
	}
	if _, err := db.Exec(`ATTACH ':memory:' AS iceberg`); err != nil {
		t.Fatalf("attach iceberg: %v", err)
	}
	if _, err := db.Exec("USE ducklake"); err != nil {
		t.Fatalf("use ducklake: %v", err)
	}
	if _, err := db.Exec("CREATE SCHEMA stripe"); err != nil {
		t.Fatalf("create ducklake stripe: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE stripe.cash_balance(id INTEGER)"); err != nil {
		t.Fatalf("create ducklake table: %v", err)
	}
	if _, err := db.Exec("CREATE SCHEMA iceberg.public"); err != nil {
		t.Fatalf("create iceberg public: %v", err)
	}
	if _, err := db.Exec("CREATE SCHEMA iceberg.stripe"); err != nil {
		t.Fatalf("create iceberg stripe: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE iceberg.stripe.product(id INTEGER)"); err != nil {
		t.Fatalf("create iceberg table: %v", err)
	}
	if _, err := db.Exec("USE memory"); err != nil {
		t.Fatalf("use memory before pg catalog rewrite: %v", err)
	}
	if err := recreatePgClassForDuckLake(db); err != nil {
		t.Fatalf("recreate pg_class_full for ducklake: %v", err)
	}
	if err := recreatePgNamespaceForDuckLake(db); err != nil {
		t.Fatalf("recreate pg_namespace for ducklake: %v", err)
	}

	executor := NewLocalExecutor(db)
	if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, "iceberg"); err != nil {
		t.Fatalf("init session metadata: %v", err)
	}
	if _, err := db.Exec("USE iceberg.public"); err != nil {
		t.Fatalf("use iceberg public: %v", err)
	}

	var tables string
	if err := db.QueryRow(`
		SELECT string_agg("TABLE_NAME", ',' ORDER BY "TABLE_NAME")
		FROM (
			SELECT current_database() AS "TABLE_CAT",
				n.nspname AS "TABLE_SCHEM",
				c.relname AS "TABLE_NAME"
			FROM memory.main.pg_namespace n,
				memory.main.pg_class_full c
			WHERE c.relnamespace = n.oid
				AND current_database() = 'iceberg'
				AND c.relkind = 'r'
				AND n.nspname = 'stripe'
		) q
	`).Scan(&tables); err != nil {
		t.Fatalf("hex table query: %v", err)
	}
	if tables != "product" {
		t.Fatalf("Hex table query returned %q, want product only", tables)
	}
}

func TestInformationSchemaColumnsCompatScopesLoadedMetadataToSelectedCatalog(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`ATTACH ':memory:' AS ducklake`); err != nil {
		t.Fatalf("attach ducklake: %v", err)
	}
	if _, err := db.Exec(`ATTACH ':memory:' AS "iceberg"`); err != nil {
		t.Fatalf("attach iceberg: %v", err)
	}
	if err := initInformationSchema(db, true); err != nil {
		t.Fatalf("init information_schema: %v", err)
	}
	if _, err := db.Exec("USE ducklake"); err != nil {
		t.Fatalf("use ducklake: %v", err)
	}
	if _, err := db.Exec("CREATE SCHEMA billing_public"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE billing_public.public_api_keys(id VARCHAR NOT NULL, permissions JSON)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec("CREATE SCHEMA iceberg.billing_public"); err != nil {
		t.Fatalf("create iceberg schema: %v", err)
	}
	if _, err := db.Exec("CREATE SCHEMA iceberg.public"); err != nil {
		t.Fatalf("create iceberg public schema: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE iceberg.billing_public.public_api_keys(__ VARCHAR)"); err != nil {
		t.Fatalf("create iceberg dummy table: %v", err)
	}

	executor := NewLocalExecutor(db)
	if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, "ducklake"); err != nil {
		t.Fatalf("init session database metadata: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO memory.main.__duckgres_iceberg_column_metadata (
			table_schema,
			table_name,
			column_name,
			ordinal_position,
			is_nullable,
			data_type,
			udt_name,
			character_maximum_length,
			character_octet_length,
			numeric_precision,
			numeric_scale,
			datetime_precision
		)
		VALUES
			('billing_public', 'public_api_keys', 'id', 1, 'YES', 'text', 'text', NULL, NULL, NULL, NULL, NULL),
			('billing_public', 'public_api_keys', 'permissions', 2, 'YES', 'text', 'text', NULL, NULL, NULL, NULL, NULL)
	`); err != nil {
		t.Fatalf("insert iceberg metadata: %v", err)
	}

	assertColumns := func(searchPath, wantCatalog, wantIDNullable, wantPermissionsType string) {
		t.Helper()

		if _, err := db.Exec("SET search_path = '" + searchPath + "'"); err != nil {
			t.Fatalf("set search_path %q: %v", searchPath, err)
		}

		var duplicateCount int
		if err := db.QueryRow(`
			SELECT COUNT(*)
			FROM memory.main.information_schema_columns_compat
			WHERE table_schema = 'billing_public'
				AND table_name = 'public_api_keys'
				AND column_name = 'id'
		`).Scan(&duplicateCount); err != nil {
			t.Fatalf("query id duplicate count: %v", err)
		}
		if duplicateCount != 1 {
			t.Fatalf("id row count with search_path %q = %d, want 1", searchPath, duplicateCount)
		}

		var gotCatalog, gotNullable string
		if err := db.QueryRow(`
			SELECT table_catalog, is_nullable
			FROM memory.main.information_schema_columns_compat
			WHERE table_schema = 'billing_public'
				AND table_name = 'public_api_keys'
				AND column_name = 'id'
		`).Scan(&gotCatalog, &gotNullable); err != nil {
			t.Fatalf("query id metadata: %v", err)
		}
		if gotCatalog != wantCatalog || gotNullable != wantIDNullable {
			t.Fatalf("id metadata with search_path %q = (%q, %q), want (%q, %q)", searchPath, gotCatalog, gotNullable, wantCatalog, wantIDNullable)
		}

		var gotPermissionsType string
		if err := db.QueryRow(`
			SELECT data_type
			FROM memory.main.information_schema_columns_compat
			WHERE table_schema = 'billing_public'
				AND table_name = 'public_api_keys'
				AND column_name = 'permissions'
		`).Scan(&gotPermissionsType); err != nil {
			t.Fatalf("query permissions metadata: %v", err)
		}
		if gotPermissionsType != wantPermissionsType {
			t.Fatalf("permissions type with search_path %q = %q, want %q", searchPath, gotPermissionsType, wantPermissionsType)
		}
	}

	assertColumns("ducklake.billing_public,memory.main", "ducklake", "NO", "json")
	assertColumns("iceberg.billing_public,memory.main", "ducklake", "NO", "json")

	if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, "iceberg"); err != nil {
		t.Fatalf("init iceberg session database metadata: %v", err)
	}
	if _, err := db.Exec("USE iceberg.public"); err != nil {
		t.Fatalf("use iceberg.public: %v", err)
	}
	assertColumns("iceberg.billing_public,memory.main", "iceberg", "YES", "text")

	var pgAttributeColumns string
	if err := db.QueryRow(`
		SELECT string_agg(a.attname, ',' ORDER BY a.attnum)
		FROM memory.main.pg_namespace n
		JOIN memory.main.pg_class_full c ON c.relnamespace = n.oid
		JOIN memory.main.pg_attribute a ON a.attrelid = c.oid
		WHERE n.nspname = 'billing_public'
			AND c.relname = 'public_api_keys'
			AND a.attnum > 0
			AND NOT a.attisdropped
	`).Scan(&pgAttributeColumns); err != nil {
		t.Fatalf("query loaded iceberg pg_attribute columns: %v", err)
	}
	if pgAttributeColumns != "id,permissions" {
		t.Fatalf("loaded iceberg pg_attribute columns = %q, want id,permissions", pgAttributeColumns)
	}
}
