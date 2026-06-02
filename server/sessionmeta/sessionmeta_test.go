package sessionmeta

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/server/sessioncatalog"
	"github.com/posthog/duckgres/server/sqlcore"
)

type countingExecutor struct {
	execCalls   int
	execQueries []string
	queryRows   sqlcore.RowSet
}

func (e *countingExecutor) QueryContext(_ context.Context, _ string, _ ...any) (sqlcore.RowSet, error) {
	if e.queryRows == nil {
		return nil, errors.New("no query rows configured")
	}
	return e.queryRows, nil
}

func (e *countingExecutor) ExecContext(_ context.Context, query string, _ ...any) (sqlcore.ExecResult, error) {
	e.execCalls++
	e.execQueries = append(e.execQueries, query)
	return nil, nil
}

func (e *countingExecutor) Query(string, ...any) (sqlcore.RowSet, error) {
	return nil, errors.New("not implemented")
}

func (e *countingExecutor) Exec(string, ...any) (sqlcore.ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *countingExecutor) ConnContext(context.Context) (sqlcore.RawConn, error) {
	return nil, errors.New("not implemented")
}

func (e *countingExecutor) PingContext(context.Context) error { return nil }
func (e *countingExecutor) Close() error                      { return nil }
func (e *countingExecutor) LastProfilingOutput() string       { return "" }

type singleIntRow struct {
	v        int
	consumed bool
}

func (r *singleIntRow) Columns() ([]string, error) { return []string{"count"}, nil }
func (r *singleIntRow) ColumnTypes() ([]sqlcore.ColumnTyper, error) {
	return nil, errors.New("not used")
}
func (r *singleIntRow) Next() bool {
	if r.consumed {
		return false
	}
	r.consumed = true
	return true
}
func (r *singleIntRow) Scan(dest ...any) error {
	if len(dest) != 1 {
		return errors.New("expected 1 dest")
	}
	ptr, ok := dest[0].(*any)
	if !ok {
		return errors.New("expected *any dest")
	}
	*ptr = int64(r.v)
	return nil
}
func (r *singleIntRow) Close() error                 { return nil }
func (r *singleIntRow) Err() error                   { return nil }
func (r *singleIntRow) RowsAffected() (int64, error) { return 0, nil }
func (r *singleIntRow) LastInsertId() (int64, error) { return 0, nil }
func (r *singleIntRow) LastProfilingOutput() string  { return "" }

func TestInitSessionDatabaseMetadataBatchesPerSessionStatements(t *testing.T) {
	exec := &countingExecutor{
		queryRows: &singleIntRow{v: 1}, // pretend ducklake is attached for HasAttachedCatalog
	}

	if err := InitSessionDatabaseMetadata(context.Background(), exec, sessioncatalog.Selection{
		ClientDatabase:  "analytics",
		PhysicalCatalog: "ducklake",
	}); err != nil {
		t.Fatalf("InitSessionDatabaseMetadata: %v", err)
	}

	// Per-conn statements that intrinsically run as separate calls:
	//   1. CREATE OR REPLACE TEMP MACRO current_database()
	//   2. USE memory
	//   3. USE ducklake               (deferred)
	//   4. SET search_path            (deferred)
	// Plus one batched ExecContext for the metadata views (was 6 calls).
	const expectedExecCalls = 5
	if exec.execCalls != expectedExecCalls {
		t.Errorf("ExecContext call count = %d, want %d.\nQueries:\n%s",
			exec.execCalls, expectedExecCalls,
			strings.Join(exec.execQueries, "\n---\n"))
	}
}

func TestBuildSessionMetadataSQLContainsAllExpectedStatements(t *testing.T) {
	got := buildSessionMetadataSQL(sessioncatalog.Selection{
		ClientDatabase:  "analytics",
		PhysicalCatalog: "ducklake",
	})

	wants := []string{
		"CREATE TABLE IF NOT EXISTS main.__duckgres_column_metadata",
		"CREATE TABLE IF NOT EXISTS main.__duckgres_iceberg_column_metadata",
		"CREATE OR REPLACE VIEW main.pg_database",
		"CREATE OR REPLACE VIEW main.information_schema_columns_compat",
		"CREATE OR REPLACE VIEW main.information_schema_tables_compat",
		"CREATE OR REPLACE VIEW main.information_schema_schemata_compat",
		"CREATE OR REPLACE VIEW main.information_schema_views_compat",
	}
	for _, w := range wants {
		if !strings.Contains(got, w) {
			t.Errorf("buildSessionMetadataSQL missing %q", w)
		}
	}

	// Database literal should appear (used in pg_database view).
	if !strings.Contains(got, "'analytics'") {
		t.Errorf("buildSessionMetadataSQL did not interpolate database literal")
	}
}

func TestInformationSchemaColumnsCompatUsesLoadedIcebergColumnsAndFiltersDummy(t *testing.T) {
	got := buildSessionInformationSchemaColumnsViewSQL()

	for _, want := range []string{
		"WITH all_columns AS",
		"FROM main.__duckgres_iceberg_column_metadata",
		"c.table_catalog = 'iceberg'",
		"c.column_name = '__'",
		"UPPER(c.data_type) = 'UNKNOWN'",
		"UNION ALL",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("columns compat SQL missing %q in:\n%s", want, got)
		}
	}
	if count := strings.Count(got, " AS table_catalog"); count != 2 {
		t.Fatalf("columns compat SQL should project table_catalog only for native and loaded CTE sources, got %d occurrences in:\n%s", count, got)
	}
}

func TestInformationSchemaColumnsCompatLoadedIcebergColumnsUseCurrentDatabaseCatalog(t *testing.T) {
	got := buildSessionInformationSchemaColumnsViewSQL()
	if !strings.Contains(got, "'iceberg' AS source_catalog") {
		t.Fatalf("loaded Iceberg columns should retain internal source catalog in:\n%s", got)
	}
	if !strings.Contains(got, "current_database() AS table_catalog,\n\t\t\t\ttable_schema") {
		t.Fatalf("loaded Iceberg columns should expose current_database() as table_catalog in:\n%s", got)
	}
}

func TestInformationSchemaColumnsCompatPrefersLoadedIcebergMetadata(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	stmts := []string{
		`ATTACH ':memory:' AS iceberg`,
		`CREATE SCHEMA iceberg.stripe`,
		`CREATE TABLE iceberg.stripe.account (requirements_currently_due INTEGER)`,
		sessionColumnMetadataTableSQL(),
		sessionIcebergColumnMetadataTableSQL(),
		`INSERT INTO main.__duckgres_iceberg_column_metadata (
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
		) VALUES (
			'stripe',
			'account',
			'requirements_currently_due',
			1,
			'YES',
			'jsonb',
			'jsonb',
			NULL,
			NULL,
			NULL,
			NULL,
			NULL
		)`,
		buildSessionInformationSchemaColumnsViewSQL(),
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	var dataType string
	err = db.QueryRow(`
		SELECT data_type
		FROM main.information_schema_columns_compat
		WHERE table_catalog = current_database()
		AND table_schema = 'stripe'
		AND table_name = 'account'
		AND column_name = 'requirements_currently_due'
	`).Scan(&dataType)
	if err != nil {
		t.Fatalf("query compat data_type: %v", err)
	}
	if dataType != "jsonb" {
		t.Fatalf("data_type = %q, want jsonb from loaded Iceberg metadata", dataType)
	}
}

func TestInformationSchemaColumnsCompatSuppressesNativeIcebergDuplicatesExplicitly(t *testing.T) {
	got := buildSessionInformationSchemaColumnsViewSQL()
	for _, want := range []string{
		"AND NOT (",
		"FROM main.__duckgres_iceberg_column_metadata im",
		"WHERE im.table_schema = c.table_schema",
		"AND im.table_name = c.table_name",
		"AND im.column_name = c.column_name",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("columns compat SQL should explicitly suppress native Iceberg duplicates; missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "source_priority") {
		t.Fatalf("columns compat SQL should not rely on hidden source_priority ranking in:\n%s", got)
	}
}
