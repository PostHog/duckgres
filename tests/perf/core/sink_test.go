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
	wantHeader := []string{"query_id", "intent_id", "measure_iteration", "protocol", "status", "error", "error_class", "rows", "duration_ms", "started_at"}
	if !reflect.DeepEqual(records[0], wantHeader) {
		t.Fatalf("CSV header: got %v want %v", records[0], wantHeader)
	}
	if got, want := []string{records[1][0], records[2][0]}, []string{"q_events__raw_view", "q_events__ducklake_table"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CSV query IDs: got %v want %v", got, want)
	}
}
