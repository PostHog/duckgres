package server

import (
	"context"
	"database/sql"
	"errors"
	"testing"

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

func TestInformationSchemaColumnsCompatDeduplicatesBySearchPathCatalogPrecedence(t *testing.T) {
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

	executor := NewLocalExecutor(db)
	if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, "posthog"); err != nil {
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

	assertColumns("ducklake.billing_public,memory.main", "posthog", "NO", "json")
	assertColumns("iceberg.billing_public,memory.main", "iceberg", "YES", "text")

	if _, err := db.Exec("USE iceberg.public"); err != nil {
		t.Fatalf("use iceberg.public: %v", err)
	}
	var gotCatalog, gotNullable string
	if err := db.QueryRow(`
		SELECT table_catalog, is_nullable
		FROM memory.main.information_schema_columns_compat
		WHERE table_schema = 'billing_public'
			AND table_name = 'public_api_keys'
			AND column_name = 'id'
	`).Scan(&gotCatalog, &gotNullable); err != nil {
		t.Fatalf("query id metadata after USE iceberg.public: %v", err)
	}
	if gotCatalog != "iceberg" || gotNullable != "YES" {
		t.Fatalf("id metadata after USE iceberg.public = (%q, %q), want (iceberg, YES)", gotCatalog, gotNullable)
	}
}
