package trino_ducklake_smoke

import (
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	integration "github.com/posthog/duckgres/tests/integration"
)

const (
	realisticSmokeRows         = 100_000
	realisticLocalRows         = 1_000_000
	realisticMaxRows           = 1_000_000
	realisticPersonDivisor     = 10
	realisticInsertBatchRows   = 1_000
	realisticWarmupIterations  = 1
	realisticMeasureIterations = 3
)

type realisticPerfProfile struct {
	Name        string
	DefaultRows int
}

var realisticPerfProfiles = map[string]realisticPerfProfile{
	"realistic-smoke": {Name: "realistic-smoke", DefaultRows: realisticSmokeRows},
	"realistic-local": {Name: "realistic-local", DefaultRows: realisticLocalRows},
}

func TestTrinoDuckLakeRealisticPerf(t *testing.T) {
	setDuckLakeTestTimeZone(t)
	if os.Getenv("TRINO_DUCKLAKE_REALISTIC_PERF") != "1" {
		t.Skip("set TRINO_DUCKLAKE_REALISTIC_PERF=1 or run just perf-trino-ducklake-realistic")
	}
	profile, rows := realisticPerfConfig(t)
	root := repositoryRoot(t)
	composeFile := filepath.Join(root, "tests", "integration", "docker-compose.yml")
	composeUp(t, composeFile, "ducklake-metadata", "minio")
	compose(t, composeFile, "run", "--rm", "--no-deps", "minio-init")

	cfg := integration.DefaultConfig()
	cfg.SkipPostgres = true
	harness, err := integration.NewTestHarness(cfg)
	if err != nil {
		t.Fatalf("start Duckgres DuckLake writer: %v", err)
	}
	t.Cleanup(func() { _ = harness.Close() })

	seedRealisticEvents(t, harness.DuckgresDB, rows)
	compose(t, composeFile, "run", "--rm", "--no-deps", "trino-metadata-reader-init")
	composeUp(t, composeFile, "trino")
	trino := newTrinoClient(2 * time.Minute)
	assertTrinoUTC(t, trino)

	artifact := localPerfArtifact{
		CreatedAt:         time.Now().UTC(),
		Profile:           profile.Name,
		Rows:              rows,
		WarmupIterations:  realisticWarmupIterations,
		MeasureIterations: realisticMeasureIterations,
		TrinoImage:        trinoImage,
	}
	for _, benchmark := range realisticPerfQueries {
		result := compareAndMeasureQuery(t, harness.DuckgresDB, trino, benchmark, realisticWarmupIterations, realisticMeasureIterations)
		artifact.Results = append(artifact.Results, result)
	}
	writeLocalPerfArtifact(t, artifact)
}

func realisticPerfConfig(t *testing.T) (realisticPerfProfile, int) {
	t.Helper()
	name := os.Getenv("TRINO_DUCKLAKE_REALISTIC_PERF_PROFILE")
	if name == "" {
		name = "realistic-smoke"
	}
	profile, ok := realisticPerfProfiles[name]
	if !ok {
		t.Fatalf("TRINO_DUCKLAKE_REALISTIC_PERF_PROFILE must be realistic-smoke or realistic-local, got %q", name)
	}
	rows := profile.DefaultRows
	if value := os.Getenv("TRINO_DUCKLAKE_REALISTIC_PERF_ROWS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > realisticMaxRows {
			t.Fatalf("TRINO_DUCKLAKE_REALISTIC_PERF_ROWS must be an integer from 1 through %d, got %q", realisticMaxRows, value)
		}
		rows = parsed
	}
	return profile, rows
}

func seedRealisticEvents(t *testing.T, db *sql.DB, rowCount int) {
	t.Helper()
	personCount := max(1, rowCount/realisticPersonDivisor)
	for _, statement := range []string{
		"DROP TABLE IF EXISTS ducklake.posthog.events",
		"DROP TABLE IF EXISTS ducklake.posthog.persons",
		"CREATE SCHEMA IF NOT EXISTS ducklake.posthog",
		`CREATE TABLE ducklake.posthog.events (
uuid VARCHAR, event VARCHAR, properties VARCHAR, timestamp TIMESTAMPTZ,
team_id BIGINT, project_id BIGINT, distinct_id VARCHAR, elements_chain VARCHAR,
created_at TIMESTAMPTZ, person_id VARCHAR, person_created_at TIMESTAMPTZ,
person_properties VARCHAR, group0_properties VARCHAR, group1_properties VARCHAR,
group2_properties VARCHAR, group3_properties VARCHAR, group4_properties VARCHAR,
group0_created_at TIMESTAMPTZ, group1_created_at TIMESTAMPTZ,
group2_created_at TIMESTAMPTZ, group3_created_at TIMESTAMPTZ,
group4_created_at TIMESTAMPTZ, person_mode VARCHAR, historical_migration BOOLEAN,
_inserted_at TIMESTAMPTZ)`,
		"ALTER TABLE ducklake.posthog.events SET PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp))",
		`CREATE TABLE ducklake.posthog.persons (
team_id BIGINT, distinct_id VARCHAR, id VARCHAR, properties VARCHAR,
created_at TIMESTAMPTZ, is_identified BOOLEAN, person_distinct_id_version BIGINT,
person_version UBIGINT, _timestamp TIMESTAMPTZ, _inserted_at TIMESTAMPTZ)`,
		"ALTER TABLE ducklake.posthog.persons SET PARTITIONED BY (year(_timestamp), month(_timestamp))",
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("prepare realistic DuckLake fixture: %v", err)
		}
	}

	eventsSQL := `WITH payload_templates AS (
  SELECT 'small' AS kind, json_group_object('property_' || key, CASE WHEN key = 0 THEN repeat('s', 7000) ELSE 'value' END)::VARCHAR AS properties FROM range(0, 50) AS keys(key)
  UNION ALL
  SELECT 'medium', json_group_object('property_' || key, CASE WHEN key = 0 THEN repeat('m', 23000) ELSE 'value' END)::VARCHAR FROM range(0, 150) AS keys(key)
  UNION ALL
  SELECT 'large', json_group_object('property_' || key, CASE WHEN key = 0 THEN repeat('l', 50000) ELSE 'value' END)::VARCHAR FROM range(0, 500) AS keys(key)
  UNION ALL
  SELECT 'wide', json_group_object('property_' || key, CASE WHEN key = 0 THEN repeat('w', 150000) ELSE 'value' END)::VARCHAR FROM range(0, 1200) AS keys(key)
), person_template AS (
  SELECT json_group_object('person_property_' || key, CASE WHEN key = 0 THEN repeat('p', 4000) ELSE 'value' END)::VARCHAR AS properties FROM range(0, 150) AS keys(key)
)
INSERT INTO ducklake.posthog.events
SELECT
  md5('event-' || id::VARCHAR),
  'event_' || (id % 100)::VARCHAR,
  CASE WHEN id % 100 < 50 THEN small.properties WHEN id % 100 < 75 THEN medium.properties WHEN id % 100 < 89 THEN large.properties ELSE wide.properties END,
  TIMESTAMPTZ '2024-01-01 00:00:00+00' + ((id - 1) * 180 / {{TOTAL_ROWS}}) * INTERVAL 1 DAY + (id % 86400) * INTERVAL 1 SECOND,
  id % 10, id % 25, 'distinct_' || (id % {{PERSON_COUNT}})::VARCHAR, repeat('el>', 66),
  TIMESTAMPTZ '2023-01-01 00:00:00+00' + (id % 365) * INTERVAL 1 DAY,
  'person_' || (id % {{PERSON_COUNT}})::VARCHAR,
  TIMESTAMPTZ '2023-01-01 00:00:00+00' + (id % 365) * INTERVAL 1 DAY,
  CASE WHEN id % 20 = 0 THEN '' ELSE person_template.properties END,
  CASE WHEN id % 10 = 0 THEN medium.properties END,
  CASE WHEN id % 50 = 0 THEN small.properties END,
  CASE WHEN id % 100 = 0 THEN small.properties END,
  NULL, NULL,
  TIMESTAMPTZ '2023-01-01 00:00:00+00', TIMESTAMPTZ '2023-01-01 00:00:00+00',
  TIMESTAMPTZ '2023-01-01 00:00:00+00', TIMESTAMPTZ '2023-01-01 00:00:00+00',
  TIMESTAMPTZ '2023-01-01 00:00:00+00', 'identified', false,
  TIMESTAMPTZ '2024-01-01 00:00:00+00'
FROM range({{ROW_START}}, {{ROW_END}}) AS source(id)
CROSS JOIN (SELECT properties FROM payload_templates WHERE kind = 'small') AS small
CROSS JOIN (SELECT properties FROM payload_templates WHERE kind = 'medium') AS medium
CROSS JOIN (SELECT properties FROM payload_templates WHERE kind = 'large') AS large
CROSS JOIN (SELECT properties FROM payload_templates WHERE kind = 'wide') AS wide
CROSS JOIN person_template`
	for start := 1; start <= rowCount; start += realisticInsertBatchRows {
		end := min(start+realisticInsertBatchRows, rowCount+1)
		batchSQL := strings.NewReplacer(
			"{{PERSON_COUNT}}", strconv.Itoa(personCount),
			"{{ROW_START}}", strconv.Itoa(start),
			"{{ROW_END}}", strconv.Itoa(end),
			"{{TOTAL_ROWS}}", strconv.Itoa(rowCount),
		).Replace(eventsSQL)
		if _, err := db.Exec(batchSQL); err != nil {
			t.Fatalf("seed realistic DuckLake events %d through %d: %v", start, end-1, err)
		}
	}
	personsSQL := `WITH person_template AS (
  SELECT json_group_object('person_property_' || key, CASE WHEN key = 0 THEN repeat('p', 4000) ELSE 'value' END)::VARCHAR AS properties FROM range(0, 150) AS keys(key)
)
INSERT INTO ducklake.posthog.persons
SELECT id % 10, 'distinct_' || id::VARCHAR, 'person_' || id::VARCHAR, person_template.properties,
  TIMESTAMPTZ '2023-01-01 00:00:00+00', true, 1, id,
  TIMESTAMPTZ '2023-01-01 00:00:00+00' + (id % 365) * INTERVAL 1 DAY,
  TIMESTAMPTZ '2024-01-01 00:00:00+00'
FROM range(1, {{PERSON_ROW_END}}) AS source(id)
CROSS JOIN person_template`
	personsSQL = strings.ReplaceAll(personsSQL, "{{PERSON_ROW_END}}", strconv.Itoa(personCount+1))
	if _, err := db.Exec(personsSQL); err != nil {
		t.Fatalf("seed %d realistic DuckLake persons: %v", personCount, err)
	}
}

type crossEnginePerfQuery struct {
	id          string
	duckgresSQL string
	trinoSQL    string
}

var realisticPerfQueries = []crossEnginePerfQuery{
	{"events_total", "SELECT COUNT(*) FROM ducklake.posthog.events", "SELECT COUNT(*) FROM ducklake.posthog.events"},
	{"events_one_day", "SELECT COUNT(*) FROM ducklake.posthog.events WHERE timestamp >= TIMESTAMPTZ '2024-03-01 00:00:00+00' AND timestamp < TIMESTAMPTZ '2024-03-02 00:00:00+00'", "SELECT COUNT(*) FROM ducklake.posthog.events WHERE timestamp >= from_iso8601_timestamp('2024-03-01T00:00:00Z') AND timestamp < from_iso8601_timestamp('2024-03-02T00:00:00Z')"},
	{"events_by_name", "SELECT event, COUNT(*) FROM ducklake.posthog.events WHERE timestamp >= TIMESTAMPTZ '2024-03-01 00:00:00+00' AND timestamp < TIMESTAMPTZ '2024-04-01 00:00:00+00' GROUP BY event ORDER BY count(*) DESC, event", "SELECT event, COUNT(*) FROM ducklake.posthog.events WHERE timestamp >= from_iso8601_timestamp('2024-03-01T00:00:00Z') AND timestamp < from_iso8601_timestamp('2024-04-01T00:00:00Z') GROUP BY event ORDER BY count(*) DESC, event"},
	{"distinct_persons", "SELECT COUNT(DISTINCT person_id) FROM ducklake.posthog.events WHERE timestamp >= TIMESTAMPTZ '2024-03-01 00:00:00+00' AND timestamp < TIMESTAMPTZ '2024-04-01 00:00:00+00'", "SELECT COUNT(DISTINCT person_id) FROM ducklake.posthog.events WHERE timestamp >= from_iso8601_timestamp('2024-03-01T00:00:00Z') AND timestamp < from_iso8601_timestamp('2024-04-01T00:00:00Z')"},
	{"event_person_join", "SELECT e.event, COUNT(*) FROM ducklake.posthog.events e JOIN ducklake.posthog.persons p ON p.id = e.person_id WHERE e.timestamp >= TIMESTAMPTZ '2024-03-01 00:00:00+00' AND e.timestamp < TIMESTAMPTZ '2024-04-01 00:00:00+00' GROUP BY e.event ORDER BY e.event", "SELECT e.event, COUNT(*) FROM ducklake.posthog.events e JOIN ducklake.posthog.persons p ON p.id = e.person_id WHERE e.timestamp >= from_iso8601_timestamp('2024-03-01T00:00:00Z') AND e.timestamp < from_iso8601_timestamp('2024-04-01T00:00:00Z') GROUP BY e.event ORDER BY e.event"},
	{"wide_payload_scan", "SELECT SUM(length(properties)) FROM ducklake.posthog.events WHERE timestamp >= TIMESTAMPTZ '2024-03-01 00:00:00+00' AND timestamp < TIMESTAMPTZ '2024-04-01 00:00:00+00'", "SELECT SUM(length(properties)) FROM ducklake.posthog.events WHERE timestamp >= from_iso8601_timestamp('2024-03-01T00:00:00Z') AND timestamp < from_iso8601_timestamp('2024-04-01T00:00:00Z')"},
	{"json_property_filter", "SELECT COUNT(*) FROM ducklake.posthog.events WHERE json_extract_string(properties, '$.property_1') = 'value'", "SELECT COUNT(*) FROM ducklake.posthog.events WHERE json_extract_scalar(properties, '$.property_1') = 'value'"},
	{"sparse_group_payload", "SELECT COUNT(*) FROM ducklake.posthog.events WHERE group0_properties IS NOT NULL AND length(group0_properties) > 10000", "SELECT COUNT(*) FROM ducklake.posthog.events WHERE group0_properties IS NOT NULL AND length(group0_properties) > 10000"},
}

func compareAndMeasureQuery(t *testing.T, duckgres *sql.DB, trino *trinoClient, query crossEnginePerfQuery, warmups, measurements int) localPerfResult {
	t.Helper()
	want := duckgresRows(t, duckgres, query.duckgresSQL)
	got := canonicalTrinoRows(trino.query(t, query.trinoSQL))
	checksum := checksumRows(want)
	if trinoChecksum := checksumRows(got); trinoChecksum != checksum {
		t.Fatalf("Trino result checksum differs for %s: trino=%s duckgres=%s trino_rows=%v duckgres_rows=%v", query.id, trinoChecksum, checksum, got, want)
	}
	for range warmups {
		_ = duckgresRows(t, duckgres, query.duckgresSQL)
		_ = trino.query(t, query.trinoSQL)
	}
	result := localPerfResult{QueryID: query.id, Query: query.duckgresSQL, TrinoQuery: query.trinoSQL, ResultChecksum: checksum}
	for range measurements {
		_, duration := timedDuckgresQuery(t, duckgres, query.duckgresSQL)
		result.PGWireDurationMS = append(result.PGWireDurationMS, durationMilliseconds(duration))
		_, duration = timedTrinoQuery(t, trino, query.trinoSQL)
		result.TrinoDurationMS = append(result.TrinoDurationMS, durationMilliseconds(duration))
	}
	return result
}

func TestRealisticPerfConfig(t *testing.T) {
	t.Setenv("TRINO_DUCKLAKE_REALISTIC_PERF_PROFILE", "realistic-smoke")
	t.Setenv("TRINO_DUCKLAKE_REALISTIC_PERF_ROWS", "1000")
	profile, rows := realisticPerfConfig(t)
	if profile.Name != "realistic-smoke" || rows != 1000 {
		t.Fatalf("realisticPerfConfig() = (%+v, %d), want realistic-smoke/1000", profile, rows)
	}
}
