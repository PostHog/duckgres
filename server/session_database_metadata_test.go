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
	// A second catalog (`other`) is attached to verify the compat views do not
	// leak objects from a foreign catalog into a ducklake session.
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`ATTACH ':memory:' AS ducklake`); err != nil {
		t.Fatalf("attach ducklake: %v", err)
	}
	if _, err := db.Exec(`ATTACH ':memory:' AS other`); err != nil {
		t.Fatalf("attach other: %v", err)
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
	if _, err := db.Exec("CREATE SCHEMA other.shared"); err != nil {
		t.Fatalf("create other schema: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE other.shared.other_only(id INTEGER)"); err != nil {
		t.Fatalf("create other table: %v", err)
	}
	if _, err := db.Exec("CREATE VIEW other.shared.other_view AS SELECT id FROM other.shared.other_only"); err != nil {
		t.Fatalf("create other view: %v", err)
	}

	executor := NewLocalExecutor(db)
	if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, "ducklake"); err != nil {
		t.Fatalf("init session database metadata: %v", err)
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
	`, "duck_only,duck_view")
	assertSingleValue("view", `
		SELECT string_agg(table_name, ',' ORDER BY table_name)
		FROM memory.main.information_schema_views_compat
		WHERE table_schema = 'shared'
	`, "duck_view")
	assertSingleValue("column table", `
		SELECT string_agg(DISTINCT table_name, ',' ORDER BY table_name)
		FROM memory.main.information_schema_columns_compat
		WHERE table_schema = 'shared'
			AND column_name = 'id'
	`, "duck_only,duck_view")

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
}

func TestPgCatalogCompatViewsOnlyExposeSelectedCatalog(t *testing.T) {
	// A second catalog (`other`) is attached to verify the pg_catalog compat
	// views scope to the session's selected catalog and do not leak a foreign
	// catalog's relations.
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
	if _, err := db.Exec(`ATTACH ':memory:' AS other`); err != nil {
		t.Fatalf("attach other: %v", err)
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

	if _, err := db.Exec("CREATE SCHEMA other.shared"); err != nil {
		t.Fatalf("create other schema: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE other.shared.other_only(id INTEGER, payload VARCHAR)"); err != nil {
		t.Fatalf("create other table: %v", err)
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
	if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, "ducklake"); err != nil {
		t.Fatalf("init session database metadata: %v", err)
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
	if tables != "duck_only" {
		t.Fatalf("tables = %q, want %q and not %q", tables, "duck_only", "other_only")
	}
	if strings.Contains(tables, "other_only") {
		t.Fatalf("tables leaked %q: %q", "other_only", tables)
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
	if columnRelations != "duck_only" {
		t.Fatalf("column relations = %q, want %q and not %q", columnRelations, "duck_only", "other_only")
	}
	if strings.Contains(columnRelations, "other_only") {
		t.Fatalf("column relations leaked %q: %q", "other_only", columnRelations)
	}
}

func TestHexMetadataQueryDoesNotLeakDuckLakeIntoOtherCatalog(t *testing.T) {
	// The pg_class/pg_namespace compat views scope by current_database(), so a
	// session selected onto a non-ducklake catalog must not see ducklake tables
	// through the Hex-style metadata query.
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
	if _, err := db.Exec(`ATTACH ':memory:' AS other`); err != nil {
		t.Fatalf("attach other: %v", err)
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
	if _, err := db.Exec("CREATE SCHEMA other.stripe"); err != nil {
		t.Fatalf("create other stripe: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE other.stripe.product(id INTEGER)"); err != nil {
		t.Fatalf("create other table: %v", err)
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
	if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, "other"); err != nil {
		t.Fatalf("init session metadata: %v", err)
	}
	if _, err := db.Exec("USE other.stripe"); err != nil {
		t.Fatalf("use other.stripe: %v", err)
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
				AND current_database() = 'other'
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

func TestPgTablesViewsSequencesOnlyExposeSelectedCatalog(t *testing.T) {
	// DuckDB's native pg_tables/pg_views/pg_sequences span every attached
	// catalog and ignore current_database(); the session-scoped compat views
	// must return only the selected catalog's objects so a multi-catalog worker
	// does not leak the other catalog's object names through these discovery
	// surfaces.
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`ATTACH ':memory:' AS ducklake`); err != nil {
		t.Fatalf("attach ducklake: %v", err)
	}
	if _, err := db.Exec(`ATTACH ':memory:' AS other`); err != nil {
		t.Fatalf("attach other: %v", err)
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
	if _, err := db.Exec("CREATE SEQUENCE shared.duck_seq"); err != nil {
		t.Fatalf("create ducklake sequence: %v", err)
	}

	if _, err := db.Exec("CREATE SCHEMA other.shared"); err != nil {
		t.Fatalf("create other schema: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE other.shared.other_only(id INTEGER)"); err != nil {
		t.Fatalf("create other table: %v", err)
	}
	if _, err := db.Exec("CREATE VIEW other.shared.other_view AS SELECT id FROM other.shared.other_only"); err != nil {
		t.Fatalf("create other view: %v", err)
	}
	if _, err := db.Exec("CREATE SEQUENCE other.shared.other_seq"); err != nil {
		t.Fatalf("create other sequence: %v", err)
	}
	if _, err := db.Exec("USE memory"); err != nil {
		t.Fatalf("use memory: %v", err)
	}

	executor := NewLocalExecutor(db)
	if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, "ducklake"); err != nil {
		t.Fatalf("init session database metadata: %v", err)
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

	assertSingleValue("pg_tables", `
		SELECT string_agg(tablename, ',' ORDER BY tablename)
		FROM memory.main.pg_tables
		WHERE schemaname = 'shared'
	`, "duck_only")
	assertSingleValue("pg_views", `
		SELECT string_agg(viewname, ',' ORDER BY viewname)
		FROM memory.main.pg_views
		WHERE schemaname = 'shared'
	`, "duck_view")
	assertSingleValue("pg_sequences", `
		SELECT string_agg(sequencename, ',' ORDER BY sequencename)
		FROM memory.main.pg_sequences
		WHERE schemaname = 'shared'
	`, "duck_seq")

	var leakCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM memory.main.pg_tables
		WHERE schemaname = 'shared' AND tablename = ?
	`, "other_only").Scan(&leakCount); err != nil {
		t.Fatalf("pg_tables leak query: %v", err)
	}
	if leakCount != 0 {
		t.Fatalf("pg_tables leaked %q into ducklake session", "other_only")
	}
}

func TestPgCatalogConvenienceViewsMatchNativeShape(t *testing.T) {
	// The scoped pg_tables/pg_views/pg_sequences compat views replace DuckDB's
	// native pg_catalog views in client queries. To make the only behavior change
	// the catalog row-filter (not the result shape), each compat view's column
	// names AND types must match the native view exactly.
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`ATTACH ':memory:' AS ducklake`); err != nil {
		t.Fatalf("attach ducklake: %v", err)
	}
	executor := NewLocalExecutor(db)
	if err := sessionmeta.InitSessionDatabaseMetadata(context.Background(), executor, "ducklake"); err != nil {
		t.Fatalf("init session database metadata: %v", err)
	}

	shapeOf := func(relation string) string {
		rows, err := db.Query("SELECT column_name || ' ' || column_type FROM (DESCRIBE SELECT * FROM " + relation + ") ORDER BY column_name")
		if err != nil {
			t.Fatalf("describe %s: %v", relation, err)
		}
		defer func() { _ = rows.Close() }()
		var cols []string
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				t.Fatalf("scan %s: %v", relation, err)
			}
			cols = append(cols, c)
		}
		return strings.Join(cols, ", ")
	}

	for _, view := range []string{"pg_tables", "pg_views", "pg_sequences"} {
		t.Run(view, func(t *testing.T) {
			native := shapeOf("pg_catalog." + view)
			compat := shapeOf("memory.main." + view)
			if native != compat {
				t.Fatalf("%s shape mismatch:\n native = %s\n compat = %s", view, native, compat)
			}
		})
	}
}

func TestProjectMetadataViewsHideRelationsOutsideAccessPolicy(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()

	for _, statement := range []string{
		`ATTACH ':memory:' AS ducklake`,
		`CREATE SCHEMA ducklake.posthog`,
		`CREATE SCHEMA ducklake.team_42`,
		`CREATE SCHEMA ducklake.team_7`,
		`CREATE TABLE ducklake.posthog.events_prod(id INTEGER)`,
		`CREATE TABLE ducklake.posthog.events_other(id INTEGER)`,
		`CREATE TABLE ducklake.team_42.owned(id INTEGER)`,
		`CREATE VIEW ducklake.team_42.owned_view AS SELECT id FROM ducklake.team_42.owned`,
		`CREATE SEQUENCE ducklake.team_42.owned_seq`,
		`CREATE TABLE ducklake.team_7.hidden(id INTEGER)`,
		`CREATE SEQUENCE ducklake.team_7.hidden_seq`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}

	executor := NewLocalExecutor(db)
	err = sessionmeta.InitSessionDatabaseMetadataWithAccess(
		context.Background(),
		executor,
		"ducklake",
		&sessionmeta.MetadataAccessPolicy{
			AllowedSchemas:   []string{"team_42"},
			AllowedRelations: []string{"posthog.events_prod"},
		},
	)
	if err != nil {
		t.Fatalf("init filtered session metadata: %v", err)
	}

	assertNames := func(query, want string) {
		t.Helper()
		var got string
		if err := db.QueryRow(query).Scan(&got); err != nil {
			t.Fatalf("metadata query: %v", err)
		}
		if got != want {
			t.Fatalf("metadata names = %q, want %q", got, want)
		}
	}
	assertNames(`
		SELECT string_agg(table_schema || '.' || table_name, ',' ORDER BY table_schema, table_name)
		FROM memory.main.information_schema_tables_compat
	`, "posthog.events_prod,team_42.owned,team_42.owned_view")
	assertNames(`
		SELECT string_agg(nspname || '.' || relname, ',' ORDER BY nspname, relname)
		FROM memory.main.pg_class_full c
		JOIN memory.main.pg_namespace n ON n.oid = c.relnamespace
	`, "posthog.events_prod,team_42.owned,team_42.owned_seq,team_42.owned_view")
	assertNames(`
		SELECT string_agg(sequence_schema || '.' || sequence_name, ',' ORDER BY sequence_schema, sequence_name)
		FROM memory.main.information_schema_sequences_compat
	`, "team_42.owned_seq")
}
