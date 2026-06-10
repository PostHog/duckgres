package sessionmeta

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
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

	if err := InitSessionDatabaseMetadata(context.Background(), exec, "analytics"); err != nil {
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
	got := buildSessionMetadataSQL("analytics")

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

func TestInformationSchemaColumnsCompatLoadedIcebergColumnsKeepIcebergCatalog(t *testing.T) {
	got := buildSessionInformationSchemaColumnsViewSQL()
	if !strings.Contains(got, "'iceberg' AS table_catalog") {
		t.Fatalf("loaded Iceberg columns should use the iceberg catalog in:\n%s", got)
	}
	if strings.Contains(got, "current_database() AS table_catalog,\n\t\t\t\ttable_schema") {
		t.Fatalf("loaded Iceberg columns should not use current_database() as table_catalog in:\n%s", got)
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
		`CREATE OR REPLACE TEMP MACRO current_database() AS 'iceberg'`,
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
			'STRUCT(currently_due VARCHAR[])',
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
		WHERE table_catalog = 'iceberg'
		AND table_schema = 'stripe'
		AND table_name = 'account'
		AND column_name = 'requirements_currently_due'
	`).Scan(&dataType)
	if err != nil {
		t.Fatalf("query compat data_type: %v", err)
	}
	if dataType != "json" {
		t.Fatalf("data_type = %q, want json from loaded Iceberg metadata", dataType)
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

func TestIsCatalogSettlingError(t *testing.T) {
	if !isCatalogSettlingError(errors.New(`flight execute update: rpc error: code = InvalidArgument desc = failed to execute update: Catalog Error: Schema with name "" not found`)) {
		t.Fatal("cold-catalog empty-name schema error not recognized as settling")
	}
	// A genuine missing schema must NOT be retried.
	if isCatalogSettlingError(errors.New(`Catalog Error: Schema with name "analytics" not found`)) {
		t.Fatal("named-schema error misclassified as settling")
	}
	if isCatalogSettlingError(nil) {
		t.Fatal("nil misclassified")
	}
}

// settleExecutor fails the metadata batch with the cold-catalog signature N
// times, then succeeds — modeling an Iceberg REST catalog that materializes
// after the first failed enumeration.
type settleExecutor struct {
	countingExecutor
	batchFailures int
}

func (e *settleExecutor) ExecContext(ctx context.Context, query string, args ...any) (sqlcore.ExecResult, error) {
	if strings.Contains(query, "pg_database") && e.batchFailures > 0 {
		e.batchFailures--
		e.execCalls++
		return nil, errors.New(`failed to execute update: Catalog Error: Schema with name "" not found`)
	}
	return e.countingExecutor.ExecContext(ctx, query, args...)
}

func TestInitSessionDatabaseMetadataRetriesOnceOnSettlingCatalog(t *testing.T) {
	e := &settleExecutor{batchFailures: 1}
	e.queryRows = &singleIntRow{v: 0} // ducklake-attachment probe -> not attached
	if err := InitSessionDatabaseMetadata(context.Background(), e, "iceberg"); err != nil {
		t.Fatalf("init with one settling failure should succeed via retry, got %v", err)
	}
}

func TestInitSessionDatabaseMetadataDoesNotRetryTwice(t *testing.T) {
	e := &settleExecutor{batchFailures: 2}
	e.queryRows = &singleIntRow{v: 0}
	err := InitSessionDatabaseMetadata(context.Background(), e, "iceberg")
	if err == nil {
		t.Fatal("init with two settling failures must fail (single retry only)")
	}
	if !strings.Contains(err.Error(), "after catalog-settle retry") {
		t.Fatalf("error should mark the exhausted retry, got %v", err)
	}
}

// primeExecutor fails the schema-enumeration probe N times then succeeds.
type primeExecutor struct {
	countingExecutor
	probeFailures int
	probeCalls    int
}

func (e *primeExecutor) QueryContext(ctx context.Context, query string, args ...any) (sqlcore.RowSet, error) {
	e.probeCalls++
	if e.probeFailures > 0 {
		e.probeFailures--
		return nil, errors.New(`Catalog Error: Schema with name "" not found`)
	}
	return &singleIntRow{v: 1}, nil
}

func TestPrimeIcebergCatalogRetriesUntilEnumerationSucceeds(t *testing.T) {
	e := &primeExecutor{probeFailures: 2}
	if err := PrimeIcebergCatalog(context.Background(), e, "iceberg"); err != nil {
		t.Fatalf("prime should succeed after settling, got %v", err)
	}
	if e.probeCalls != 3 {
		t.Fatalf("probe calls = %d, want 3 (2 failures + 1 success)", e.probeCalls)
	}
}

func TestPrimeIcebergCatalogGivesUpAfterBoundedAttempts(t *testing.T) {
	e := &primeExecutor{probeFailures: 99}
	if err := PrimeIcebergCatalog(context.Background(), e, "iceberg"); err == nil {
		t.Fatal("prime must give up after bounded attempts")
	}
	if e.probeCalls != 5 {
		t.Fatalf("probe calls = %d, want 5", e.probeCalls)
	}
}
