package core

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestArtifactSinkWritesSummaryCSVAndMetrics(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewArtifactSink(dir)
	if err != nil {
		t.Fatalf("NewArtifactSink returned error: %v", err)
	}

	if err := sink.Record(QueryResult{
		QueryID:          "q1",
		IntentID:         "i1",
		MeasureIteration: 1,
		Protocol:         ProtocolPGWire,
		Status:           "ok",
		Rows:             2,
		Duration:         10 * time.Millisecond,
		StartedAt:        time.Unix(1700000000, 0),
	}); err != nil {
		t.Fatalf("Record returned error: %v", err)
	}
	if err := sink.Record(QueryResult{
		QueryID:          "q2",
		IntentID:         "i2",
		MeasureIteration: 2,
		Protocol:         ProtocolPGWire,
		Status:           "error",
		Error:            "boom",
		Duration:         5 * time.Millisecond,
		StartedAt:        time.Unix(1700000010, 0),
	}); err != nil {
		t.Fatalf("Record returned error: %v", err)
	}

	summary := RunSummary{
		RunID:          "run-1",
		DatasetVersion: "v1",
		StartedAt:      time.Unix(1700000000, 0),
		FinishedAt:     time.Unix(1700000030, 0),
		TotalQueries:   2,
		TotalErrors:    1,
	}
	if err := sink.Close(summary, "# HELP sample sample\nsample 1\n"); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	summaryPath := filepath.Join(dir, "summary.json")
	csvPath := filepath.Join(dir, "query_results.csv")
	promPath := filepath.Join(dir, "server_metrics.prom")
	for _, p := range []string{summaryPath, csvPath, promPath} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected artifact file %s: %v", p, err)
		}
	}

	b, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("ReadFile summary: %v", err)
	}
	var got RunSummary
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("summary json parse: %v", err)
	}
	if got.TotalQueries != 2 || got.TotalErrors != 1 || got.DatasetVersion != "v1" {
		t.Fatalf("unexpected summary in file: %+v", got)
	}

	csvBytes, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("ReadFile csv: %v", err)
	}
	csvText := string(csvBytes)
	if !strings.Contains(csvText, "query_id,") || !strings.Contains(csvText, ",measure_iteration,") || !strings.Contains(csvText, ",protocol,") {
		t.Fatalf("csv header missing query_id/measure_iteration/protocol: %q", csvText)
	}
	if !strings.Contains(csvText, "\nq1,i1,1,pgwire,ok,") || !strings.Contains(csvText, "\nq2,i2,2,pgwire,error,boom,") {
		t.Fatalf("csv rows missing measure_iteration values: %q", csvText)
	}
}

func TestPairedQueriesPreserveArtifactCSVContract(t *testing.T) {
	catalog, err := ParseCatalog([]byte(pairedCatalogYAML(`
paired_queries:
  - query_id_base: q_events
    intent_id: ph.events.v1
    sql_template: SELECT COUNT(*) FROM {{ relation "events" }}
`)))
	if err != nil {
		t.Fatalf("ParseCatalog returned error: %v", err)
	}
	dir := t.TempDir()
	sink, err := NewArtifactSink(dir)
	if err != nil {
		t.Fatalf("NewArtifactSink returned error: %v", err)
	}
	for _, query := range catalog.Queries {
		if err := sink.Record(QueryResult{
			QueryID:          query.QueryID,
			IntentID:         query.IntentID,
			MeasureIteration: 1,
			Protocol:         ProtocolPGWire,
			Status:           "ok",
			Rows:             1,
			Duration:         time.Millisecond,
			StartedAt:        time.Unix(1700000000, 0),
		}); err != nil {
			t.Fatalf("Record returned error: %v", err)
		}
	}
	if err := sink.Close(RunSummary{}, ""); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	file, err := os.Open(filepath.Join(dir, "query_results.csv"))
	if err != nil {
		t.Fatalf("open query_results.csv: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("close query_results.csv: %v", err)
		}
	}()
	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatalf("read query_results.csv: %v", err)
	}
	wantHeader := []string{"query_id", "intent_id", "measure_iteration", "protocol", "status", "error", "error_class", "rows", "duration_ms", "started_at", "representation", "run_label"}
	if !reflect.DeepEqual(records[0], wantHeader) {
		t.Fatalf("CSV header: got %v want %v", records[0], wantHeader)
	}
	if got, want := len(records), 2; got != want {
		t.Fatalf("CSV rows: got %d want %d", got, want)
	}
	if got, want := records[1][0], "q_events__ducklake_table"; got != want {
		t.Fatalf("CSV query ID: got %v want %v", got, want)
	}
}

func TestArtifactSinkWritesAthenaServiceMetrics(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewArtifactSink(dir)
	if err != nil {
		t.Fatalf("NewArtifactSink returned error: %v", err)
	}
	if err := sink.Record(QueryResult{
		QueryID:          "q1__athena_external",
		IntentID:         "i1",
		MeasureIteration: 1,
		Protocol:         ProtocolAthena,
		Status:           "ok",
		Rows:             1,
		Duration:         3 * time.Second,
		StartedAt:        time.Unix(1700000000, 0),
		ServiceMetrics: &ServiceMetrics{
			QueueDuration:    100 * time.Millisecond,
			PlanningDuration: 200 * time.Millisecond,
			EngineDuration:   2 * time.Second,
			ServiceDuration:  2500 * time.Millisecond,
			BytesScanned:     4096,
			DPUCount:         4,
			ResultReused:     false,
			EngineVersion:    "Athena engine version 3",
		},
	}); err != nil {
		t.Fatalf("Record returned error: %v", err)
	}
	if err := sink.Close(RunSummary{}, ""); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	queryFile, err := os.Open(filepath.Join(dir, "query_results.csv"))
	if err != nil {
		t.Fatalf("open query_results.csv: %v", err)
	}
	queryRecords, err := csv.NewReader(queryFile).ReadAll()
	_ = queryFile.Close()
	if err != nil {
		t.Fatalf("read query_results.csv: %v", err)
	}
	wantQueryHeader := []string{"query_id", "intent_id", "measure_iteration", "protocol", "status", "error", "error_class", "rows", "duration_ms", "started_at", "representation", "run_label"}
	if !reflect.DeepEqual(queryRecords[0], wantQueryHeader) {
		t.Fatalf("query_results.csv header: got %v want %v", queryRecords[0], wantQueryHeader)
	}

	metricsFile, err := os.Open(filepath.Join(dir, "query_service_metrics.csv"))
	if err != nil {
		t.Fatalf("open query_service_metrics.csv: %v", err)
	}
	metricsRecords, err := csv.NewReader(metricsFile).ReadAll()
	_ = metricsFile.Close()
	if err != nil {
		t.Fatalf("read query_service_metrics.csv: %v", err)
	}
	if !reflect.DeepEqual(metricsRecords[0], wantServiceMetricsHeader) {
		t.Fatalf("service metrics header: got %v want %v", metricsRecords[0], wantServiceMetricsHeader)
	}
	// Athena values are unchanged; the appended Trino-only columns stay blank.
	if got, want := metricsRecords[1], []string{"q1__athena_external", "i1", "1", "athena", "100.000000", "200.000000", "2000.000000", "2500.000000", "4096", "4", "false", "Athena engine version 3", "", "athena", "", "", "", "", "", "", ""}; !reflect.DeepEqual(got, want) {
		t.Fatalf("service metrics row: got %v want %v", got, want)
	}
}

// The original columns keep their positions so existing readers still work;
// the Trino statistics columns are appended.
var wantServiceMetricsHeader = []string{
	"query_id", "intent_id", "measure_iteration", "protocol", "queue_ms", "planning_ms", "engine_ms", "service_ms",
	"bytes_scanned", "dpu_count", "result_reused", "engine_version", "representation", "run_label",
	"total_splits", "completed_splits", "physical_input_rows", "cpu_ms", "peak_memory_bytes", "engine_query_id", "stats_source",
}

func TestArtifactSinkWritesTrinoQueryStatistics(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewArtifactSink(dir)
	if err != nil {
		t.Fatalf("NewArtifactSink returned error: %v", err)
	}
	physicalRows := int64(0)
	base := QueryResult{
		QueryID:          "q_events_total_v5__hoglake_table",
		IntentID:         "intent_events_total_v5",
		MeasureIteration: 2,
		Protocol:         ProtocolTrinoCached,
		Status:           "ok",
		Rows:             1,
		Duration:         450 * time.Millisecond,
		StartedAt:        time.Unix(1700000000, 0),
	}
	fromQueryInfo := base
	fromQueryInfo.ServiceMetrics = &ServiceMetrics{
		QueueDuration:    1210 * time.Microsecond,
		PlanningDuration: 55500 * time.Microsecond,
		EngineDuration:   398 * time.Millisecond,
		ServiceDuration:  412350 * time.Microsecond,
		BytesScanned:     2 << 20,
		Trino: &TrinoQueryStats{
			QueryID: "20260924_101500_00042_abcde", Source: TrinoStatsSourceQueryInfo,
			TotalSplits: 97, CompletedSplits: 97, PhysicalInputRows: &physicalRows,
			CPUDuration: 1520 * time.Millisecond, PeakMemoryBytes: 1310720,
		},
	}
	fromStatement := base
	fromStatement.MeasureIteration = 3
	fromStatement.ServiceMetrics = &ServiceMetrics{
		QueueDuration:    time.Millisecond,
		PlanningDuration: 55 * time.Millisecond,
		ServiceDuration:  412 * time.Millisecond,
		BytesScanned:     2 << 20,
		Trino: &TrinoQueryStats{
			QueryID: "20260924_101500_00043_abcde", Source: TrinoStatsSourceStatement,
			TotalSplits: 92, CompletedSplits: 92, CPUDuration: 1520 * time.Millisecond, PeakMemoryBytes: 1310720,
		},
	}
	withoutStats := base
	withoutStats.MeasureIteration = 4
	for _, result := range []QueryResult{fromQueryInfo, fromStatement, withoutStats} {
		if err := sink.Record(result); err != nil {
			t.Fatalf("Record returned error: %v", err)
		}
	}
	if err := sink.Close(RunSummary{}, ""); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	metricsFile, err := os.Open(filepath.Join(dir, "query_service_metrics.csv"))
	if err != nil {
		t.Fatalf("open query_service_metrics.csv: %v", err)
	}
	records, err := csv.NewReader(metricsFile).ReadAll()
	_ = metricsFile.Close()
	if err != nil {
		t.Fatalf("read query_service_metrics.csv: %v", err)
	}
	if !reflect.DeepEqual(records[0], wantServiceMetricsHeader) {
		t.Fatalf("service metrics header: got %v want %v", records[0], wantServiceMetricsHeader)
	}
	// One row per Trino iteration with statistics. Athena-only fields are blank
	// rather than a misleading zero DPU count or result-reuse flag.
	want := [][]string{
		{"q_events_total_v5__hoglake_table", "intent_events_total_v5", "2", "trino_cached", "1.210000", "55.500000", "398.000000", "412.350000", "2097152", "", "", "", "", "trino_cached",
			"97", "97", "0", "1520.000000", "1310720", "20260924_101500_00042_abcde", "query_info"},
		// The statement fallback has no execution time or physical input rows.
		{"q_events_total_v5__hoglake_table", "intent_events_total_v5", "3", "trino_cached", "1.000000", "55.000000", "", "412.000000", "2097152", "", "", "", "", "trino_cached",
			"92", "92", "", "1520.000000", "1310720", "20260924_101500_00043_abcde", "statement_stats"},
	}
	if got := records[1:]; !reflect.DeepEqual(got, want) {
		t.Fatalf("service metrics rows:\ngot  %v\nwant %v", got, want)
	}
}
