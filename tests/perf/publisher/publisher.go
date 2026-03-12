package publisher

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/posthog/duckgres/tests/perf/core"
)

const defaultSchema = "duckgres_perf"

var schemaNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

var expectedQueryResultsHeader = []string{
	"query_id",
	"intent_id",
	"measure_iteration",
	"protocol",
	"status",
	"error",
	"error_class",
	"rows",
	"duration_ms",
	"started_at",
}

type Config struct {
	DSN             string
	Password        string
	Schema          string
	BootstrapSchema bool
	Strict          bool
}

type artifacts struct {
	Summary core.RunSummary
	Results []resultRow
}

type resultRow struct {
	QueryID          string
	IntentID         string
	MeasureIteration int
	Protocol         string
	Status           string
	Error            *string
	ErrorClass       *string
	Rows             *int64
	DurationMS       float64
	StartedAt        time.Time
}

type dbHandle interface {
	BeginTx(context.Context, *sql.TxOptions) (txHandle, error)
	Close() error
}

type txHandle interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

type sqlDBWrapper struct {
	db *sql.DB
}

func (w *sqlDBWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (txHandle, error) {
	return w.db.BeginTx(ctx, opts)
}

func (w *sqlDBWrapper) Close() error {
	return w.db.Close()
}

func (c Config) Enabled() bool {
	return strings.TrimSpace(c.DSN) != ""
}

func PublishRunDir(ctx context.Context, cfg Config, runDir string) error {
	loaded, err := loadArtifacts(runDir)
	if err != nil {
		return err
	}
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	return publishArtifacts(ctx, cfg, db, loaded)
}

func openDB(cfg Config) (dbHandle, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, fmt.Errorf("publisher dsn is required")
	}
	resolvedDSN, err := resolveDSNPassword(dsn, cfg.Password)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("postgres", resolvedDSN)
	if err != nil {
		return nil, fmt.Errorf("open publisher database: %w", err)
	}
	return &sqlDBWrapper{db: db}, nil
}

func loadArtifacts(runDir string) (artifacts, error) {
	summary, err := loadSummary(filepath.Join(runDir, "summary.json"))
	if err != nil {
		return artifacts{}, err
	}
	results, err := loadQueryResults(filepath.Join(runDir, "query_results.csv"))
	if err != nil {
		return artifacts{}, err
	}
	return artifacts{
		Summary: summary,
		Results: results,
	}, nil
}

func loadSummary(path string) (core.RunSummary, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return core.RunSummary{}, fmt.Errorf("read summary.json: %w", err)
	}
	var summary core.RunSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		return core.RunSummary{}, fmt.Errorf("decode summary.json: %w", err)
	}
	if summary.RunID == "" {
		return core.RunSummary{}, fmt.Errorf("summary.json is missing run_id")
	}
	if summary.StartedAt.IsZero() {
		return core.RunSummary{}, fmt.Errorf("summary.json is missing started_at")
	}
	if summary.FinishedAt.IsZero() {
		return core.RunSummary{}, fmt.Errorf("summary.json is missing finished_at")
	}
	if summary.FinishedAt.Before(summary.StartedAt) {
		return core.RunSummary{}, fmt.Errorf("summary.json finished_at is before started_at")
	}
	if summary.TotalQueries < 0 {
		return core.RunSummary{}, fmt.Errorf("summary.json total_queries must be >= 0")
	}
	if summary.TotalErrors < 0 {
		return core.RunSummary{}, fmt.Errorf("summary.json total_errors must be >= 0")
	}
	if summary.WarmupQueries < 0 {
		return core.RunSummary{}, fmt.Errorf("summary.json warmup_queries must be >= 0")
	}
	if summary.TotalErrors > summary.TotalQueries {
		return core.RunSummary{}, fmt.Errorf("summary.json total_errors exceeds total_queries")
	}
	return summary, nil
}

func loadQueryResults(path string) ([]resultRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open query_results.csv: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()
	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read query_results.csv header: %w", err)
	}
	if !equalStringSlices(header, expectedQueryResultsHeader) {
		return nil, fmt.Errorf("unexpected query_results.csv header: %q", strings.Join(header, ","))
	}
	var results []resultRow
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read query_results.csv row: %w", err)
		}
		row, err := parseResultRow(record)
		if err != nil {
			return nil, err
		}
		results = append(results, row)
	}
	return results, nil
}

func parseResultRow(record []string) (resultRow, error) {
	if len(record) != len(expectedQueryResultsHeader) {
		return resultRow{}, fmt.Errorf("unexpected query_results.csv column count: got %d want %d", len(record), len(expectedQueryResultsHeader))
	}
	measureIteration, err := strconv.Atoi(record[2])
	if err != nil {
		return resultRow{}, fmt.Errorf("parse measure_iteration %q: %w", record[2], err)
	}
	var rows *int64
	if record[7] != "" {
		value, err := strconv.ParseInt(record[7], 10, 64)
		if err != nil {
			return resultRow{}, fmt.Errorf("parse rows %q: %w", record[7], err)
		}
		rows = &value
	}
	durationMS, err := strconv.ParseFloat(record[8], 64)
	if err != nil {
		return resultRow{}, fmt.Errorf("parse duration_ms %q: %w", record[8], err)
	}
	startedAt, err := time.Parse(time.RFC3339Nano, record[9])
	if err != nil {
		return resultRow{}, fmt.Errorf("parse started_at %q: %w", record[9], err)
	}
	return resultRow{
		QueryID:          record[0],
		IntentID:         record[1],
		MeasureIteration: measureIteration,
		Protocol:         record[3],
		Status:           record[4],
		Error:            stringPtrOrNil(record[5]),
		ErrorClass:       stringPtrOrNil(record[6]),
		Rows:             rows,
		DurationMS:       durationMS,
		StartedAt:        startedAt,
	}, nil
}

func publishArtifacts(ctx context.Context, cfg Config, db dbHandle, loaded artifacts) (err error) {
	schema, err := validatedSchema(cfg.Schema)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin publish transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if cfg.BootstrapSchema {
		if err = bootstrapSchema(ctx, tx, schema); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s.query_results WHERE run_id = $1", schema), loaded.Summary.RunID); err != nil {
		return fmt.Errorf("delete existing query results: %w", err)
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s.runs WHERE run_id = $1", schema), loaded.Summary.RunID); err != nil {
		return fmt.Errorf("delete existing run summary: %w", err)
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s.runs (run_id, dataset_version, started_at, finished_at, total_queries, total_errors, warmup_queries, run_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		schema,
	), loaded.Summary.RunID, loaded.Summary.DatasetVersion, loaded.Summary.StartedAt, loaded.Summary.FinishedAt, loaded.Summary.TotalQueries, loaded.Summary.TotalErrors, loaded.Summary.WarmupQueries, loaded.Summary.StartedAt.UTC().Format("2006-01-02")); err != nil {
		return fmt.Errorf("insert run summary: %w", err)
	}
	insertQuery := fmt.Sprintf(
		"INSERT INTO %s.query_results (run_id, query_id, intent_id, measure_iteration, protocol, status, error, error_class, rows, duration_ms, started_at, dataset_version, run_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
		schema,
	)
	runDate := loaded.Summary.StartedAt.UTC().Format("2006-01-02")
	for _, result := range loaded.Results {
		if _, err = tx.ExecContext(ctx, insertQuery, loaded.Summary.RunID, result.QueryID, result.IntentID, result.MeasureIteration, result.Protocol, result.Status, result.Error, result.ErrorClass, result.Rows, result.DurationMS, result.StartedAt, loaded.Summary.DatasetVersion, runDate); err != nil {
			return fmt.Errorf("insert query result (%s/%s/%d/%s): %w", loaded.Summary.RunID, result.QueryID, result.MeasureIteration, result.Protocol, err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit publish transaction: %w", err)
	}
	return nil
}

func bootstrapSchema(ctx context.Context, tx txHandle, schema string) error {
	statements := []string{
		fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.query_results (
  run_id TEXT NOT NULL,
  query_id TEXT NOT NULL,
  intent_id TEXT,
  measure_iteration INTEGER,
  protocol TEXT NOT NULL,
  status TEXT NOT NULL,
  error TEXT,
  error_class TEXT,
  rows BIGINT,
  duration_ms DOUBLE PRECISION,
  started_at TIMESTAMPTZ NOT NULL,
  dataset_version TEXT NOT NULL,
  run_date DATE NOT NULL
)`, schema),
		fmt.Sprintf("ALTER TABLE %s.query_results ADD COLUMN IF NOT EXISTS measure_iteration INTEGER", schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.runs (
  run_id TEXT PRIMARY KEY,
  dataset_version TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ NOT NULL,
  total_queries BIGINT NOT NULL,
  total_errors BIGINT NOT NULL,
  warmup_queries BIGINT NOT NULL,
  run_date DATE NOT NULL
)`, schema),
	}
	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("bootstrap publish schema: %w", err)
		}
	}
	return nil
}

func validatedSchema(schema string) (string, error) {
	if strings.TrimSpace(schema) == "" {
		return defaultSchema, nil
	}
	if !schemaNamePattern.MatchString(schema) {
		return "", fmt.Errorf("invalid publish schema %q", schema)
	}
	return schema, nil
}

func stringPtrOrNil(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func resolveDSNPassword(dsn, password string) (string, error) {
	if password == "" {
		return dsn, nil
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			return "", fmt.Errorf("parse publisher dsn: %w", err)
		}
		username := ""
		if parsed.User != nil {
			username = parsed.User.Username()
		}
		parsed.User = url.UserPassword(username, password)
		return parsed.String(), nil
	}
	if strings.Contains(dsn, "password=") {
		return dsn, nil
	}
	escaped := strings.ReplaceAll(password, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return fmt.Sprintf("%s password='%s'", dsn, escaped), nil
}
