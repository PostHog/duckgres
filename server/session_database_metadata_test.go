package server

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

type recordingQueryExecutor struct {
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

func (r *staticCountRowSet) Columns() ([]string, error)                     { return []string{"count"}, nil }
func (r *staticCountRowSet) ColumnTypes() ([]ColumnTyper, error)            { return []ColumnTyper{describeColumnType("BIGINT")}, nil }
func (r *staticCountRowSet) Next() bool                                     { if r.returned { return false }; r.returned = true; return true }
func (r *staticCountRowSet) Scan(dest ...any) error                         { *(dest[0].(*interface{})) = int64(r.count); return nil }
func (r *staticCountRowSet) Close() error                                   { return nil }
func (r *staticCountRowSet) Err() error                                     { return nil }

func TestHasAttachedCatalogEmbedsCatalogNameWithoutBoundArgs(t *testing.T) {
	exec := &recordingQueryExecutor{
		rowSet: &staticCountRowSet{count: 1},
	}

	attached, err := hasAttachedCatalog(context.Background(), exec, "ducklake")
	if err != nil {
		t.Fatalf("hasAttachedCatalog returned error: %v", err)
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
	if err := initSessionDatabaseMetadata(context.Background(), executor, "analytics"); err != nil {
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
	if err := initSessionDatabaseMetadata(context.Background(), executor, "analytics"); err != nil {
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
	if err := initSessionDatabaseMetadata(context.Background(), executor, "analytics"); err != nil {
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
