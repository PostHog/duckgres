package publisher

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/posthog/duckgres/tests/perf/core"
)

// serviceMetricsRow is one query_service_metrics.csv row. Blank cells are
// NULL: Athena rows have no Trino statistics and Trino rows have no DPUs.
type serviceMetricsRow struct {
	QueryID           string
	IntentID          string
	MeasureIteration  int
	Protocol          string
	QueueMS           *float64
	PlanningMS        *float64
	EngineMS          *float64
	ServiceMS         *float64
	BytesScanned      *int64
	DPUCount          *float64
	ResultReused      *bool
	EngineVersion     *string
	Representation    string
	RunLabel          *string
	TotalSplits       *int64
	CompletedSplits   *int64
	PhysicalInputRows *int64
	CPUMS             *float64
	PeakMemoryBytes   *int64
	EngineQueryID     *string
	StatsSource       *string
}

// loadServiceMetrics reads the optional sidecar. Artifacts written before it
// existed have no file; older ones have the original header, a prefix of the
// current one.
func loadServiceMetrics(path string) ([]serviceMetricsRow, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open query_service_metrics.csv: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()
	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read query_service_metrics.csv header: %w", err)
	}
	if len(header) < core.LegacyServiceMetricsColumnCount || len(header) > len(core.ServiceMetricsColumns) ||
		!equalStringSlices(header, core.ServiceMetricsColumns[:len(header)]) {
		return nil, fmt.Errorf("unexpected query_service_metrics.csv header: %q", strings.Join(header, ","))
	}
	var rows []serviceMetricsRow
	for {
		record, err := r.Read()
		if err == io.EOF {
			return rows, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read query_service_metrics.csv row: %w", err)
		}
		row, err := parseServiceMetricsRow(record)
		if err != nil {
			return nil, fmt.Errorf("query_service_metrics.csv: %w", err)
		}
		rows = append(rows, row)
	}
}

func parseServiceMetricsRow(record []string) (serviceMetricsRow, error) {
	cell := func(index int) string {
		if index < len(record) {
			return record[index]
		}
		return ""
	}
	var err error
	float := func(index int) *float64 {
		if err != nil || cell(index) == "" {
			return nil
		}
		value, parseErr := strconv.ParseFloat(cell(index), 64)
		if parseErr != nil {
			err = fmt.Errorf("parse %s %q: %w", core.ServiceMetricsColumns[index], cell(index), parseErr)
			return nil
		}
		return &value
	}
	integer := func(index int) *int64 {
		if err != nil || cell(index) == "" {
			return nil
		}
		value, parseErr := strconv.ParseInt(cell(index), 10, 64)
		if parseErr != nil {
			err = fmt.Errorf("parse %s %q: %w", core.ServiceMetricsColumns[index], cell(index), parseErr)
			return nil
		}
		return &value
	}
	boolean := func(index int) *bool {
		if err != nil || cell(index) == "" {
			return nil
		}
		value, parseErr := strconv.ParseBool(cell(index))
		if parseErr != nil {
			err = fmt.Errorf("parse %s %q: %w", core.ServiceMetricsColumns[index], cell(index), parseErr)
			return nil
		}
		return &value
	}
	measureIteration, parseErr := strconv.Atoi(cell(2))
	if parseErr != nil {
		return serviceMetricsRow{}, fmt.Errorf("parse measure_iteration %q: %w", cell(2), parseErr)
	}
	row := serviceMetricsRow{
		QueryID:           cell(0),
		IntentID:          cell(1),
		MeasureIteration:  measureIteration,
		Protocol:          cell(3),
		QueueMS:           float(4),
		PlanningMS:        float(5),
		EngineMS:          float(6),
		ServiceMS:         float(7),
		BytesScanned:      integer(8),
		DPUCount:          float(9),
		ResultReused:      boolean(10),
		EngineVersion:     stringPtrOrNil(cell(11)),
		Representation:    cell(12),
		RunLabel:          stringPtrOrNil(cell(13)),
		TotalSplits:       integer(14),
		CompletedSplits:   integer(15),
		PhysicalInputRows: integer(16),
		CPUMS:             float(17),
		PeakMemoryBytes:   integer(18),
		EngineQueryID:     stringPtrOrNil(cell(19)),
		StatsSource:       stringPtrOrNil(cell(20)),
	}
	if err != nil {
		return serviceMetricsRow{}, err
	}
	if row.QueryID == "" || row.Protocol == "" {
		return serviceMetricsRow{}, errors.New("row is missing query_id or protocol")
	}
	return row, nil
}

// publishServiceMetrics replaces the run's service metrics. It only touches
// the table when the run has rows, so PGWire-only runs keep publishing to
// schemas that were never bootstrapped with it.
func publishServiceMetrics(ctx context.Context, tx txHandle, schema string, loaded artifacts) error {
	if len(loaded.ServiceMetrics) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s.query_service_metrics WHERE run_id = $1", schema), loaded.Summary.RunID); err != nil {
		return fmt.Errorf("delete existing query service metrics: %w", err)
	}
	insert := fmt.Sprintf(`INSERT INTO %s.query_service_metrics (run_id, query_id, intent_id, measure_iteration, protocol, queue_ms, planning_ms, engine_ms, service_ms, bytes_scanned, dpu_count, result_reused, engine_version, representation, run_label, total_splits, completed_splits, physical_input_rows, cpu_ms, peak_memory_bytes, engine_query_id, stats_source, dataset_version, run_date, suite, fixture_version, nightly_run_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)`, schema)
	summary := loaded.Summary
	runDate := summary.StartedAt.UTC().Format("2006-01-02")
	fixtureVersion := stringPtrOrNil(summary.FixtureVersion)
	for _, row := range loaded.ServiceMetrics {
		runLabel := row.RunLabel
		if runLabel == nil {
			label := core.Protocol(row.Protocol).RunLabel(row.Representation)
			runLabel = &label
		}
		if _, err := tx.ExecContext(ctx, insert,
			summary.RunID, row.QueryID, row.IntentID, row.MeasureIteration, row.Protocol,
			row.QueueMS, row.PlanningMS, row.EngineMS, row.ServiceMS, row.BytesScanned,
			row.DPUCount, row.ResultReused, row.EngineVersion, row.Representation, runLabel,
			row.TotalSplits, row.CompletedSplits, row.PhysicalInputRows, row.CPUMS, row.PeakMemoryBytes,
			row.EngineQueryID, row.StatsSource,
			summary.DatasetVersion, runDate, summary.Suite, fixtureVersion, summary.NightlyRunID,
		); err != nil {
			return fmt.Errorf("insert query service metrics (%s/%s/%d/%s): %w", summary.RunID, row.QueryID, row.MeasureIteration, row.Protocol, err)
		}
	}
	return nil
}

func serviceMetricsBootstrapStatements(schema string) []string {
	return []string{fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.query_service_metrics (
  run_id TEXT NOT NULL,
  query_id TEXT NOT NULL,
  intent_id TEXT,
  measure_iteration INTEGER,
  protocol TEXT NOT NULL,
  queue_ms DOUBLE PRECISION,
  planning_ms DOUBLE PRECISION,
  engine_ms DOUBLE PRECISION,
  service_ms DOUBLE PRECISION,
  bytes_scanned BIGINT,
  dpu_count DOUBLE PRECISION,
  result_reused BOOLEAN,
  engine_version TEXT,
  representation TEXT,
  run_label TEXT,
  total_splits BIGINT,
  completed_splits BIGINT,
  physical_input_rows BIGINT,
  cpu_ms DOUBLE PRECISION,
  peak_memory_bytes BIGINT,
  engine_query_id TEXT,
  stats_source TEXT,
  dataset_version TEXT NOT NULL,
  run_date DATE NOT NULL,
  suite TEXT,
  fixture_version TEXT,
  nightly_run_id TEXT
)`, schema)}
}
