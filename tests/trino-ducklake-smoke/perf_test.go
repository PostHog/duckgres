package trino_ducklake_smoke

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	integration "github.com/posthog/duckgres/tests/integration"
)

const (
	defaultPerfRows       = 1_000_000
	maxPerfRows           = 5_000_000
	perfWarmupIterations  = 1
	perfMeasureIterations = 3
)

type localPerfArtifact struct {
	CreatedAt         time.Time         `json:"created_at"`
	Profile           string            `json:"profile,omitempty"`
	Rows              int               `json:"rows"`
	WarmupIterations  int               `json:"warmup_iterations"`
	MeasureIterations int               `json:"measure_iterations"`
	TrinoImage        string            `json:"trino_image"`
	Results           []localPerfResult `json:"results"`
}

type localPerfResult struct {
	QueryID          string    `json:"query_id"`
	Query            string    `json:"query"`
	TrinoQuery       string    `json:"trino_query,omitempty"`
	ResultChecksum   string    `json:"result_checksum"`
	PGWireDurationMS []float64 `json:"pgwire_duration_ms"`
	TrinoDurationMS  []float64 `json:"trino_duration_ms"`
}

func TestTrinoDuckLakePerf(t *testing.T) {
	setDuckLakeTestTimeZone(t)
	if os.Getenv("TRINO_DUCKLAKE_PERF") != "1" {
		t.Skip("set TRINO_DUCKLAKE_PERF=1 or run just perf-trino-ducklake")
	}
	rowCount := localPerfRowCount(t)
	root := repositoryRoot(t)
	composeFile := filepath.Join(root, "tests", "integration", "docker-compose.yml")
	composeUp(t, composeFile, "ducklake-metadata", "minio")
	compose(t, composeFile, "run", "--rm", "--no-deps", "minio-init")
	compose(t, composeFile, "run", "--rm", "--no-deps", "trino-metadata-reader-init")

	cfg := integration.DefaultConfig()
	cfg.SkipPostgres = true
	harness, err := integration.NewTestHarness(cfg)
	if err != nil {
		t.Fatalf("start Duckgres DuckLake writer: %v", err)
	}
	t.Cleanup(func() { _ = harness.Close() })

	seedSyntheticEvents(t, harness.DuckgresDB, rowCount)
	// The reader grant must follow table creation so Trino can discover it.
	compose(t, composeFile, "run", "--rm", "--no-deps", "trino-metadata-reader-init")
	composeUp(t, composeFile, "trino")

	trino := newTrinoClient(2 * time.Minute)
	assertTrinoUTC(t, trino)
	artifact := localPerfArtifact{
		CreatedAt:         time.Now().UTC(),
		Rows:              rowCount,
		WarmupIterations:  perfWarmupIterations,
		MeasureIterations: perfMeasureIterations,
		TrinoImage:        trinoImage,
	}
	for _, benchmark := range localPerfQueries {
		want := duckgresRows(t, harness.DuckgresDB, benchmark.sql)
		got := canonicalTrinoRows(trino.query(t, benchmark.sql))
		checksum := checksumRows(want)
		if trinoChecksum := checksumRows(got); trinoChecksum != checksum {
			t.Fatalf("Trino result checksum differs for %s: trino=%s duckgres=%s", benchmark.id, trinoChecksum, checksum)
		}

		for range perfWarmupIterations {
			_ = duckgresRows(t, harness.DuckgresDB, benchmark.sql)
			_ = trino.query(t, benchmark.sql)
		}
		result := localPerfResult{QueryID: benchmark.id, Query: benchmark.sql, ResultChecksum: checksum}
		for range perfMeasureIterations {
			_, duration := timedDuckgresQuery(t, harness.DuckgresDB, benchmark.sql)
			result.PGWireDurationMS = append(result.PGWireDurationMS, durationMilliseconds(duration))
			_, duration = timedTrinoQuery(t, trino, benchmark.sql)
			result.TrinoDurationMS = append(result.TrinoDurationMS, durationMilliseconds(duration))
		}
		artifact.Results = append(artifact.Results, result)
	}
	writeLocalPerfArtifact(t, artifact)
}

type localPerfQuery struct {
	id  string
	sql string
}

var localPerfQueries = []localPerfQuery{
	{
		id:  "events_total",
		sql: "SELECT COUNT(*) FROM ducklake.main.perf_events",
	},
	{
		id:  "events_one_day",
		sql: "SELECT COUNT(*) FROM ducklake.main.perf_events WHERE event_time >= TIMESTAMP '2024-03-01 00:00:00' AND event_time < TIMESTAMP '2024-03-02 00:00:00'",
	},
	{
		id:  "events_by_name",
		sql: "SELECT event, COUNT(*) FROM ducklake.main.perf_events WHERE event_time >= TIMESTAMP '2024-03-01 00:00:00' AND event_time < TIMESTAMP '2024-04-01 00:00:00' GROUP BY event ORDER BY count(*) DESC, event",
	},
	{
		id:  "distinct_persons",
		sql: "SELECT COUNT(DISTINCT person_id) FROM ducklake.main.perf_events WHERE event_time >= TIMESTAMP '2024-03-01 00:00:00' AND event_time < TIMESTAMP '2024-04-01 00:00:00'",
	},
}

func localPerfRowCount(t *testing.T) int {
	t.Helper()
	value := os.Getenv("TRINO_DUCKLAKE_PERF_ROWS")
	if value == "" {
		return defaultPerfRows
	}
	rows, err := strconv.Atoi(value)
	if err != nil || rows < 1 || rows > maxPerfRows {
		t.Fatalf("TRINO_DUCKLAKE_PERF_ROWS must be an integer from 1 through %d, got %q", maxPerfRows, value)
	}
	return rows
}

func seedSyntheticEvents(t *testing.T, db *sql.DB, rowCount int) {
	t.Helper()
	if _, err := db.Exec("DROP TABLE IF EXISTS ducklake.main.perf_events"); err != nil {
		t.Fatalf("drop synthetic events table: %v", err)
	}
	statement := fmt.Sprintf(`
CREATE TABLE ducklake.main.perf_events AS
SELECT
  id,
  'event_' || (id %% 20)::VARCHAR AS event,
  id %% 100000 AS person_id,
  TIMESTAMP '2024-01-01 00:00:00' + (id %% 180) * INTERVAL 1 DAY AS event_time,
  id %% 100 AS team_id,
  id %% 100000 AS amount_cents
FROM range(1, %d) AS source(id)`, rowCount+1)
	if _, err := db.Exec(statement); err != nil {
		t.Fatalf("create %d synthetic DuckLake events: %v", rowCount, err)
	}
}

func timedDuckgresQuery(t *testing.T, db *sql.DB, query string) ([][]string, time.Duration) {
	t.Helper()
	started := time.Now()
	rows := duckgresRows(t, db, query)
	return rows, time.Since(started)
}

func timedTrinoQuery(t *testing.T, trino *trinoClient, query string) ([][]string, time.Duration) {
	t.Helper()
	started := time.Now()
	rows := canonicalTrinoRows(trino.query(t, query))
	return rows, time.Since(started)
}

func durationMilliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

func writeLocalPerfArtifact(t *testing.T, artifact localPerfArtifact) {
	t.Helper()
	dir := os.Getenv("TRINO_DUCKLAKE_PERF_ARTIFACT_DIR")
	if dir == "" {
		dir = t.TempDir()
	} else if !filepath.IsAbs(dir) {
		dir = filepath.Join(repositoryRoot(t), dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create local perf artifact directory: %v", err)
	}
	data, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatalf("encode local perf artifact: %v", err)
	}
	path := filepath.Join(dir, "trino-ducklake-perf-"+artifact.CreatedAt.Format("20060102T150405Z")+".json")
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatalf("write local perf artifact: %v", err)
	}
	t.Logf("local performance artifact: %s", path)
}

func TestLocalPerfRowCount(t *testing.T) {
	t.Setenv("TRINO_DUCKLAKE_PERF_ROWS", "1000")
	if got := localPerfRowCount(t); got != 1000 {
		t.Fatalf("localPerfRowCount() = %d, want 1000", got)
	}
}
