package publisher

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/perf/core"
)

type fakeDB struct {
	tx     *fakeTx
	closed bool
}

func (db *fakeDB) BeginTx(context.Context, *sql.TxOptions) (txHandle, error) {
	return db.tx, nil
}

func (db *fakeDB) Close() error {
	db.closed = true
	return nil
}

type fakeTx struct {
	execs      []execCall
	committed  bool
	rolledBack bool
	failOnExec int
}

type execCall struct {
	query string
	args  []any
}

func (tx *fakeTx) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	tx.execs = append(tx.execs, execCall{query: query, args: args})
	if tx.failOnExec > 0 && len(tx.execs) == tx.failOnExec {
		return fakeResult(0), assertErr("boom")
	}
	return fakeResult(1), nil
}

func (tx *fakeTx) Commit() error {
	tx.committed = true
	return nil
}

func (tx *fakeTx) Rollback() error {
	tx.rolledBack = true
	return nil
}

type fakeResult int64

func (r fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (r fakeResult) RowsAffected() (int64, error) { return int64(r), nil }

type assertErr string

func (e assertErr) Error() string { return string(e) }

func TestLoadArtifactsParsesSummaryAndResults(t *testing.T) {
	runDir := writeFixtureRunDir(t)

	artifacts, err := loadArtifacts(runDir)
	if err != nil {
		t.Fatalf("loadArtifacts returned error: %v", err)
	}
	if artifacts.Summary.RunID != "nightly-v1-20260311T234300Z" {
		t.Fatalf("summary run_id = %q", artifacts.Summary.RunID)
	}
	if len(artifacts.Results) != 2 {
		t.Fatalf("expected 2 result rows, got %d", len(artifacts.Results))
	}
	if artifacts.Results[0].MeasureIteration != 1 {
		t.Fatalf("measure_iteration = %d", artifacts.Results[0].MeasureIteration)
	}
	if artifacts.Results[0].Rows == nil || *artifacts.Results[0].Rows != 1 {
		t.Fatalf("rows = %#v", artifacts.Results[0].Rows)
	}
	if artifacts.Results[0].Error != nil || artifacts.Results[0].ErrorClass != nil {
		t.Fatalf("expected nil error fields for successful row")
	}
	if artifacts.Results[1].Error == nil || *artifacts.Results[1].Error != "boom" {
		t.Fatalf("unexpected error field: %#v", artifacts.Results[1].Error)
	}
}

func TestLoadArtifactsRejectsUnexpectedHeader(t *testing.T) {
	runDir := t.TempDir()
	writeSummaryFile(t, runDir)
	csvPath := filepath.Join(runDir, "query_results.csv")
	if err := os.WriteFile(csvPath, []byte("query_id,protocol\nq1,pgwire\n"), 0o644); err != nil {
		t.Fatalf("WriteFile csv: %v", err)
	}

	_, err := loadArtifacts(runDir)
	if err == nil || !strings.Contains(err.Error(), "unexpected query_results.csv header") {
		t.Fatalf("expected header validation error, got %v", err)
	}
}

func TestLoadArtifactsRejectsSummaryMissingStartedAt(t *testing.T) {
	runDir := t.TempDir()
	writeCustomSummaryFile(t, runDir, map[string]any{
		"run_id":          "nightly-v1-20260311T234300Z",
		"dataset_version": "v1",
		"finished_at":     "2026-03-11T23:43:14Z",
		"total_queries":   2,
		"total_errors":    1,
		"warmup_queries":  2,
	})
	writeMinimalCSVFile(t, runDir)

	_, err := loadArtifacts(runDir)
	if err == nil || !strings.Contains(err.Error(), "summary.json is missing started_at") {
		t.Fatalf("expected missing started_at error, got %v", err)
	}
}

func TestLoadArtifactsRejectsSummaryFinishedBeforeStarted(t *testing.T) {
	runDir := t.TempDir()
	writeCustomSummaryFile(t, runDir, map[string]any{
		"run_id":          "nightly-v1-20260311T234300Z",
		"dataset_version": "v1",
		"started_at":      "2026-03-11T23:43:14Z",
		"finished_at":     "2026-03-11T23:43:03Z",
		"total_queries":   2,
		"total_errors":    1,
		"warmup_queries":  2,
	})
	writeMinimalCSVFile(t, runDir)

	_, err := loadArtifacts(runDir)
	if err == nil || !strings.Contains(err.Error(), "summary.json finished_at is before started_at") {
		t.Fatalf("expected invalid timestamp order error, got %v", err)
	}
}

func TestPublishArtifactsBootstrapsAndReplacesRunData(t *testing.T) {
	runDir := writeFixtureRunDir(t)
	artifacts, err := loadArtifacts(runDir)
	if err != nil {
		t.Fatalf("loadArtifacts returned error: %v", err)
	}

	db := &fakeDB{tx: &fakeTx{}}
	cfg := Config{Schema: "duckgres_perf", BootstrapSchema: true}
	if err := publishArtifacts(context.Background(), cfg, db, artifacts); err != nil {
		t.Fatalf("publishArtifacts returned error: %v", err)
	}
	if !db.tx.committed {
		t.Fatalf("expected transaction commit")
	}
	if db.tx.rolledBack {
		t.Fatalf("did not expect rollback")
	}
	// bootstrap, two deletes, one run insert, two result inserts
	bootstrap := bootstrapStatementCount(t)
	if len(db.tx.execs) != bootstrap+5 {
		t.Fatalf("expected %d execs, got %d", bootstrap+5, len(db.tx.execs))
	}
	runInsert := db.tx.execs[bootstrap+2]
	if !strings.Contains(runInsert.query, "INSERT INTO duckgres_perf.runs") {
		t.Fatalf("unexpected run insert query: %s", runInsert.query)
	}
	if got, ok := runInsert.args[0].(string); !ok || got != "nightly-v1-20260311T234300Z" {
		t.Fatalf("unexpected run insert args: %#v", runInsert.args)
	}
	// A summary without a suite is a table run and carries no fixture version.
	if got := runInsert.args[8]; got != core.SuiteTables {
		t.Fatalf("run suite = %#v, want %q", got, core.SuiteTables)
	}
	if got, ok := runInsert.args[9].(*string); !ok || got != nil {
		t.Fatalf("run fixture_version = %#v, want nil", runInsert.args[9])
	}
	// A standalone run is its own nightly.
	if got := runInsert.args[10]; got != "nightly-v1-20260311T234300Z" {
		t.Fatalf("run nightly_run_id = %#v, want the run's own ID", got)
	}
	firstResultInsert := db.tx.execs[bootstrap+3]
	if !strings.Contains(firstResultInsert.query, "INSERT INTO duckgres_perf.query_results") {
		t.Fatalf("unexpected result insert query: %s", firstResultInsert.query)
	}
	if got, ok := firstResultInsert.args[8].(*int64); !ok || got == nil || *got != 1 {
		t.Fatalf("expected rows pointer with value 1, got %#v", firstResultInsert.args[8])
	}
	if got := firstResultInsert.args[15]; got != core.SuiteTables {
		t.Fatalf("result suite = %#v, want %q", got, core.SuiteTables)
	}
	secondResultInsert := db.tx.execs[bootstrap+4]
	if got, ok := secondResultInsert.args[6].(*string); !ok || got == nil || *got != "boom" {
		t.Fatalf("expected error arg, got %#v", secondResultInsert.args[6])
	}
}

func TestPublishArtifactsWritesSuiteAndFixtureVersion(t *testing.T) {
	runDir := t.TempDir()
	writeCustomSummaryFile(t, runDir, map[string]any{
		"run_id":          "scenario-dev-posthog-frozen-perf-1-properties",
		"dataset_version": "posthog-file-views-v1",
		"suite":           "properties",
		"fixture_version": "properties-sha256-abc",
		"nightly_run_id":  "scenario-dev-posthog-frozen-perf-1",
		"started_at":      "2026-03-11T23:43:03Z",
		"finished_at":     "2026-03-11T23:43:14Z",
	})
	writeFixtureCSVFile(t, runDir)
	artifacts, err := loadArtifacts(runDir)
	if err != nil {
		t.Fatalf("loadArtifacts returned error: %v", err)
	}
	db := &fakeDB{tx: &fakeTx{}}
	if err := publishArtifacts(context.Background(), Config{Schema: "duckgres_perf"}, db, artifacts); err != nil {
		t.Fatalf("publishArtifacts returned error: %v", err)
	}
	runInsert := db.tx.execs[2]
	if runInsert.args[1] != "posthog-file-views-v1" || runInsert.args[8] != core.SuiteProperties {
		t.Fatalf("run dataset/suite = %#v/%#v", runInsert.args[1], runInsert.args[8])
	}
	if got, ok := runInsert.args[9].(*string); !ok || got == nil || *got != "properties-sha256-abc" {
		t.Fatalf("run fixture_version = %#v", runInsert.args[9])
	}
	if got := runInsert.args[10]; got != "scenario-dev-posthog-frozen-perf-1" {
		t.Fatalf("run nightly_run_id = %#v", got)
	}
	for _, result := range db.tx.execs[3:] {
		if result.args[11] != "posthog-file-views-v1" || result.args[15] != core.SuiteProperties {
			t.Fatalf("result dataset/suite = %#v/%#v", result.args[11], result.args[15])
		}
		if got, ok := result.args[16].(*string); !ok || got == nil || *got != "properties-sha256-abc" {
			t.Fatalf("result fixture_version = %#v", result.args[16])
		}
		if got := result.args[17]; got != "scenario-dev-posthog-frozen-perf-1" {
			t.Fatalf("result nightly_run_id = %#v", got)
		}
	}
}

func TestLoadArtifactsRejectsUnknownSuite(t *testing.T) {
	runDir := t.TempDir()
	writeCustomSummaryFile(t, runDir, map[string]any{
		"run_id":          "r",
		"dataset_version": "v1",
		"suite":           "Properties",
		"started_at":      "2026-03-11T23:43:03Z",
		"finished_at":     "2026-03-11T23:43:14Z",
	})
	writeFixtureCSVFile(t, runDir)
	if _, err := loadArtifacts(runDir); err == nil || !strings.Contains(err.Error(), "unknown suite") {
		t.Fatalf("expected unknown suite error, got %v", err)
	}
}

func bootstrapStatementCount(t *testing.T) int {
	t.Helper()
	tx := &fakeTx{}
	if err := bootstrapSchema(context.Background(), tx, "duckgres_perf"); err != nil {
		t.Fatalf("bootstrapSchema returned error: %v", err)
	}
	return len(tx.execs)
}

func TestBootstrapSchemaDoesNotUseUnsupportedConstraints(t *testing.T) {
	tx := &fakeTx{}

	if err := bootstrapSchema(context.Background(), tx, "duckgres_perf"); err != nil {
		t.Fatalf("bootstrapSchema returned error: %v", err)
	}
	runsDDL := tx.execs[5].query
	if !strings.Contains(runsDDL, "CREATE TABLE IF NOT EXISTS duckgres_perf.runs") {
		t.Fatalf("unexpected runs ddl: %s", runsDDL)
	}
	if strings.Contains(runsDDL, "PRIMARY KEY") || strings.Contains(runsDDL, "UNIQUE") {
		t.Fatalf("runs ddl should not use unsupported constraints: %s", runsDDL)
	}

	// The suite migration follows both CREATE TABLEs, once per table, and is
	// guarded so it adds the columns and classifies old rows only once.
	migrations := tx.execs[6:]
	if len(migrations) != 2 {
		t.Fatalf("expected 2 migration statements, got %d", len(migrations))
	}
	for i, table := range []string{"runs", "query_results"} {
		stmt := migrations[i].query
		for _, want := range []string{
			"table_schema = 'duckgres_perf' AND table_name = '" + table + "' AND column_name = 'suite'",
			"ALTER TABLE duckgres_perf." + table + " ADD COLUMN suite TEXT, ADD COLUMN fixture_version TEXT, ADD COLUMN nightly_run_id TEXT",
			"suite = CASE WHEN run_id LIKE '%-properties' THEN 'properties' ELSE 'tables' END",
			"nightly_run_id = CASE WHEN run_id LIKE '%-properties' THEN LEFT(run_id, LENGTH(run_id) - LENGTH('-properties')) ELSE run_id END",
			"fixture_version = CASE WHEN dataset_version LIKE 'properties-sha256-%' THEN dataset_version END",
		} {
			if !strings.Contains(stmt, want) {
				t.Errorf("%s migration missing %q:\n%s", table, want, stmt)
			}
		}
	}
}

func TestPublishArtifactsRollsBackOnInsertFailure(t *testing.T) {
	runDir := writeFixtureRunDir(t)
	artifacts, err := loadArtifacts(runDir)
	if err != nil {
		t.Fatalf("loadArtifacts returned error: %v", err)
	}

	db := &fakeDB{tx: &fakeTx{failOnExec: bootstrapStatementCount(t) + 3}}
	cfg := Config{Schema: "duckgres_perf", BootstrapSchema: true}
	err = publishArtifacts(context.Background(), cfg, db, artifacts)
	if err == nil || !strings.Contains(err.Error(), "insert run summary") {
		t.Fatalf("expected insert failure, got %v", err)
	}
	if !db.tx.rolledBack {
		t.Fatalf("expected rollback")
	}
	if db.tx.committed {
		t.Fatalf("did not expect commit")
	}
}

func TestResolveDSNPasswordOverridesKeywordPassword(t *testing.T) {
	got, err := resolveDSNPassword(
		"host=127.0.0.1 port=5432 user=duckgres password='old-secret' dbname=perf sslmode=require",
		"new-secret",
	)
	if err != nil {
		t.Fatalf("resolveDSNPassword returned error: %v", err)
	}
	want := "host=127.0.0.1 port=5432 user=duckgres password='new-secret' dbname=perf sslmode=require"
	if got != want {
		t.Fatalf("dsn = %q, want %q", got, want)
	}
}

func TestResolveDSNPasswordOverridesBareKeywordPassword(t *testing.T) {
	got, err := resolveDSNPassword(
		"host=127.0.0.1 port=5432 user=duckgres password=oldsecret dbname=perf sslmode=require",
		"new-secret",
	)
	if err != nil {
		t.Fatalf("resolveDSNPassword returned error: %v", err)
	}
	want := "host=127.0.0.1 port=5432 user=duckgres password='new-secret' dbname=perf sslmode=require"
	if got != want {
		t.Fatalf("dsn = %q, want %q", got, want)
	}
}

func TestResolveDSNPasswordEscapesKeywordPasswordOverride(t *testing.T) {
	got, err := resolveDSNPassword(
		"host=127.0.0.1 port=5432 user=duckgres password='old-secret' dbname=perf sslmode=require",
		`new'\secret`,
	)
	if err != nil {
		t.Fatalf("resolveDSNPassword returned error: %v", err)
	}
	want := "host=127.0.0.1 port=5432 user=duckgres password='new\\'\\\\secret' dbname=perf sslmode=require"
	if got != want {
		t.Fatalf("dsn = %q, want %q", got, want)
	}
}

func TestResolveDSNPasswordTreatsDollarLiterallyInKeywordOverride(t *testing.T) {
	got, err := resolveDSNPassword(
		"host=127.0.0.1 port=5432 user=duckgres password='old-secret' dbname=perf sslmode=require",
		`pa$$word$1`,
	)
	if err != nil {
		t.Fatalf("resolveDSNPassword returned error: %v", err)
	}
	want := "host=127.0.0.1 port=5432 user=duckgres password='pa$$word$1' dbname=perf sslmode=require"
	if got != want {
		t.Fatalf("dsn = %q, want %q", got, want)
	}
}

func writeFixtureRunDir(t *testing.T) string {
	t.Helper()
	runDir := t.TempDir()
	writeSummaryFile(t, runDir)
	writeFixtureCSVFile(t, runDir)
	return runDir
}

func writeSummaryFile(t *testing.T, runDir string) {
	t.Helper()
	summary := core.RunSummary{
		RunID:          "nightly-v1-20260311T234300Z",
		DatasetVersion: "v1",
		StartedAt:      time.Date(2026, 3, 11, 23, 43, 3, 0, time.UTC),
		FinishedAt:     time.Date(2026, 3, 11, 23, 43, 14, 0, time.UTC),
		TotalQueries:   2,
		TotalErrors:    1,
		WarmupQueries:  2,
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent summary: %v", err)
	}
	summaryPath := filepath.Join(runDir, "summary.json")
	if err := os.WriteFile(summaryPath, data, 0o644); err != nil {
		t.Fatalf("WriteFile summary: %v", err)
	}
}

func writeCustomSummaryFile(t *testing.T, runDir string, payload map[string]any) {
	t.Helper()
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent summary payload: %v", err)
	}
	summaryPath := filepath.Join(runDir, "summary.json")
	if err := os.WriteFile(summaryPath, data, 0o644); err != nil {
		t.Fatalf("WriteFile summary: %v", err)
	}
}

func writeFixtureCSVFile(t *testing.T, runDir string) {
	t.Helper()
	csvText := strings.Join([]string{
		"query_id,intent_id,measure_iteration,protocol,status,error,error_class,rows,duration_ms,started_at",
		"q_orders_total,intent_orders_total,1,pgwire,ok,,,1,11.629588,2026-03-11T23:43:12.669631583Z",
		"q_orders_total,intent_orders_total,1,pgwire,error,boom,execution_error,0,12.500000,2026-03-11T23:43:12.770000000Z",
		"",
	}, "\n")
	csvPath := filepath.Join(runDir, "query_results.csv")
	if err := os.WriteFile(csvPath, []byte(csvText), 0o644); err != nil {
		t.Fatalf("WriteFile csv: %v", err)
	}
}

func writeMinimalCSVFile(t *testing.T, runDir string) {
	t.Helper()
	csvText := strings.Join([]string{
		"query_id,intent_id,measure_iteration,protocol,status,error,error_class,rows,duration_ms,started_at",
		"",
	}, "\n")
	csvPath := filepath.Join(runDir, "query_results.csv")
	if err := os.WriteFile(csvPath, []byte(csvText), 0o644); err != nil {
		t.Fatalf("WriteFile csv: %v", err)
	}
}
