package core

import (
	"encoding/json"
	"os"
	"path/filepath"
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
		QueryID:   "q1",
		IntentID:  "i1",
		Protocol:  ProtocolPGWire,
		Status:    "ok",
		Rows:      2,
		Duration:  10 * time.Millisecond,
		StartedAt: time.Unix(1700000000, 0),
	}); err != nil {
		t.Fatalf("Record returned error: %v", err)
	}
	if err := sink.Record(QueryResult{
		QueryID:   "q2",
		IntentID:  "i2",
		Protocol:  ProtocolFlight,
		Status:    "error",
		Error:     "boom",
		Duration:  5 * time.Millisecond,
		StartedAt: time.Unix(1700000010, 0),
	}); err != nil {
		t.Fatalf("Record returned error: %v", err)
	}

	summary := RunSummary{
		RunID:        "run-1",
		StartedAt:    time.Unix(1700000000, 0),
		FinishedAt:   time.Unix(1700000030, 0),
		TotalQueries: 2,
		TotalErrors:  1,
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
	if got.TotalQueries != 2 || got.TotalErrors != 1 {
		t.Fatalf("unexpected summary in file: %+v", got)
	}

	csvBytes, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("ReadFile csv: %v", err)
	}
	csvText := string(csvBytes)
	if !strings.Contains(csvText, "query_id,") || !strings.Contains(csvText, ",protocol,") {
		t.Fatalf("csv header missing query_id/protocol: %q", csvText)
	}
}
