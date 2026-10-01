package core

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func int64Ptr(value int64) *int64 { return &value }

func TestParseCatalogAttachesExpectationsToServingVariantsOnly(t *testing.T) {
	catalog, err := ParseCatalog([]byte(athenaCatalogYAML(`
paired_queries:
  - query_id_base: q_events
    intent_id: ph.events.v1
    sql_template: SELECT COUNT(*) FROM {{ relation "events" }}
    expectations:
      trino:
        max_total_splits: 200
        max_bytes_scanned: 10MiB
      athena:
        max_bytes_scanned: 1073741824
queries:
  - query_id: q_legacy
    intent_id: legacy.v1
    pgwire_sql: SELECT 1
    targets: [trino]
    expectations:
      trino:
        max_bytes_scanned: 64KiB
`)))
	if err != nil {
		t.Fatalf("ParseCatalog returned error: %v", err)
	}
	byID := map[string]Query{}
	for _, query := range catalog.Queries {
		byID[query.QueryID] = query
	}
	if got := byID["q_events__ducklake_table"].Expectations; len(got) != 0 {
		t.Fatalf("DuckLake variant expectations = %+v, want none: PGWire never runs the Trino or Athena bounds", got)
	}
	if got, want := byID["q_events__hoglake_table"].Expectations, map[Protocol]QueryExpectations{
		ProtocolTrino: {MaxTotalSplits: int64Ptr(200), MaxBytesScanned: int64Ptr(10 << 20)},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Hoglake variant expectations = %+v, want %+v", got, want)
	}
	if got, want := byID["q_events__athena_external"].Expectations, map[Protocol]QueryExpectations{
		ProtocolAthena: {MaxBytesScanned: int64Ptr(1 << 30)},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Athena variant expectations = %+v, want %+v", got, want)
	}
	if got, want := byID["q_legacy"].Expectations, map[Protocol]QueryExpectations{
		ProtocolTrino: {MaxBytesScanned: int64Ptr(64 << 10)},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("legacy expectations = %+v, want %+v", got, want)
	}
}

func TestParseCatalogRejectsInvalidExpectations(t *testing.T) {
	for name, tc := range map[string]struct {
		expectations string
		wantErr      string
	}{
		"unknown bound":           {"trino:\n        max_splits: 1", `unknown expectation "max_splits"`},
		"latency is not a bound":  {"trino:\n        max_duration_ms: 1", `unknown expectation "max_duration_ms"`},
		"empty protocol entry":    {"trino: {}", "sets no bound"},
		"protocol not targeted":   {"trino_cached:\n        max_total_splits: 1", `protocol "trino_cached" is not a target`},
		"splits outside Trino":    {"athena:\n        max_total_splits: 1", "max_total_splits applies only to Trino"},
		"negative bound":          {"trino:\n        max_total_splits: -1", "must be a non-negative"},
		"ambiguous decimal units": {"trino:\n        max_bytes_scanned: 10MB", "KiB, MiB, GiB"},
		"not a number":            {"trino:\n        max_total_splits: many", "must be a non-negative"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseCatalog([]byte(athenaCatalogYAML(`
paired_queries:
  - query_id_base: q_events
    intent_id: ph.events.v1
    sql_template: SELECT COUNT(*) FROM {{ relation "events" }}
    expectations:
      ` + tc.expectations + `
`)))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), "q_events") {
				t.Fatalf("ParseCatalog error = %v, want it to name q_events and contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestParseCatalogWithoutExpectationsHasNone(t *testing.T) {
	catalog, err := ParseCatalog([]byte(athenaCatalogYAML(`
paired_queries:
  - query_id_base: q_events
    intent_id: ph.events.v1
    sql_template: SELECT COUNT(*) FROM {{ relation "events" }}
`)))
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range catalog.Queries {
		if query.Expectations != nil {
			t.Fatalf("query %s expectations = %+v, want nil", query.QueryID, query.Expectations)
		}
	}
	if got := EvaluateExpectations(catalog, []QueryResult{trinoResult("q_events__hoglake_table", 1, 7300, 1<<40)}); len(got) != 0 {
		t.Fatalf("violations without expectations = %v", got)
	}
}

func gateCatalog() Catalog {
	return Catalog{
		Name:    "gate",
		Targets: []Protocol{ProtocolTrino, ProtocolTrinoCached, ProtocolAthena},
		Queries: []Query{
			{
				QueryID: "q_events_total__hoglake_table", IntentID: "i", StorageTarget: StorageTargetHoglakeTable,
				Expectations: map[Protocol]QueryExpectations{
					ProtocolTrino:       {MaxTotalSplits: int64Ptr(200), MaxBytesScanned: int64Ptr(10 << 20)},
					ProtocolTrinoCached: {MaxTotalSplits: int64Ptr(200)},
				},
			},
			{
				QueryID: "q_events_total__athena_external", IntentID: "i", StorageTarget: StorageTargetAthenaExternal,
				Expectations: map[Protocol]QueryExpectations{ProtocolAthena: {MaxBytesScanned: int64Ptr(1 << 20)}},
			},
		},
	}
}

func trinoResult(queryID string, iteration int, splits, bytes int64) QueryResult {
	return QueryResult{
		QueryID: queryID, IntentID: "i", MeasureIteration: iteration, Protocol: ProtocolTrino, Status: "ok",
		Duration: time.Second,
		ServiceMetrics: &ServiceMetrics{
			BytesScanned: bytes,
			Trino:        &TrinoQueryStats{QueryID: "20260924_101500_0000" + string(rune('0'+iteration)) + "_abcde", Source: TrinoStatsSourceQueryInfo, TotalSplits: splits},
		},
	}
}

func TestEvaluateExpectationsPassesWhenBoundsAreMet(t *testing.T) {
	results := []QueryResult{
		trinoResult("q_events_total__hoglake_table", 1, 92, 0),
		trinoResult("q_events_total__hoglake_table", 2, 200, 10<<20), // bounds are inclusive
		{QueryID: "q_events_total__athena_external", MeasureIteration: 1, Protocol: ProtocolAthena, Status: "ok", ServiceMetrics: &ServiceMetrics{BytesScanned: 4096}},
	}
	if got := EvaluateExpectations(gateCatalog(), results); len(got) != 0 {
		t.Fatalf("violations = %v, want none", got)
	}
	if err := ExpectationsError(nil); err != nil {
		t.Fatalf("ExpectationsError(nil) = %v", err)
	}
}

func TestEvaluateExpectationsReportsViolatedBoundWithObservedAndLimit(t *testing.T) {
	results := []QueryResult{
		trinoResult("q_events_total__hoglake_table", 1, 92, 0),
		trinoResult("q_events_total__hoglake_table", 2, 7300, 0),
		trinoResult("q_events_total__hoglake_table", 3, 7298, 12<<20),
		{QueryID: "q_events_total__athena_external", MeasureIteration: 1, Protocol: ProtocolAthena, Status: "ok", ServiceMetrics: &ServiceMetrics{BytesScanned: 4096}},
	}
	violations := EvaluateExpectations(gateCatalog(), results)
	if len(violations) != 2 {
		t.Fatalf("violations = %v, want splits and bytes", violations)
	}
	err := ExpectationsError(violations)
	if err == nil {
		t.Fatal("ExpectationsError returned nil for violations")
	}
	message := err.Error()
	for _, want := range []string{
		"perf gate failed: 2 expectation(s) violated",
		"q_events_total__hoglake_table on trino: total_splits 7300 exceeds max_total_splits 200 in 2 of 3 measured iterations (worst: iteration 2, Trino query 20260924_101500_00002_abcde)",
		"q_events_total__hoglake_table on trino: bytes_scanned 12582912 (12.0 MiB) exceeds max_bytes_scanned 10485760 (10.0 MiB) in 1 of 3 measured iterations (worst: iteration 3, Trino query 20260924_101500_00003_abcde)",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("gate message missing %q:\n%s", want, message)
		}
	}
}

func TestEvaluateExpectationsFailsWhenBoundedMetricWasNotCaptured(t *testing.T) {
	missingTrino := trinoResult("q_events_total__hoglake_table", 2, 0, 0)
	missingTrino.ServiceMetrics = nil
	results := []QueryResult{
		trinoResult("q_events_total__hoglake_table", 1, 92, 0),
		missingTrino,
	}
	violations := EvaluateExpectations(gateCatalog(), results)
	if len(violations) != 2 {
		t.Fatalf("violations = %v, want both bounded metrics reported as not captured", violations)
	}
	if got, want := violations[0].String(), "q_events_total__hoglake_table on trino: total_splits was not captured in 1 of 2 measured iterations (first: iteration 2); max_total_splits 200 cannot be checked"; got != want {
		t.Fatalf("violation = %q\nwant        %q", got, want)
	}
}

func TestEvaluateExpectationsSkipsWarmupErroredAndUnboundedResults(t *testing.T) {
	warmup := trinoResult("q_events_total__hoglake_table", 0, 7300, 0)
	errored := trinoResult("q_events_total__hoglake_table", 1, 7300, 0)
	errored.Status = "error"
	errored.ServiceMetrics = nil
	otherQuery := trinoResult("q_unbounded__hoglake_table", 1, 7300, 1<<40)
	// trino_cached bounds only splits, so its bytes are not checked.
	cached := trinoResult("q_events_total__hoglake_table", 1, 150, 1<<40)
	cached.Protocol = ProtocolTrinoCached
	if got := EvaluateExpectations(gateCatalog(), []QueryResult{warmup, errored, otherQuery, cached}); len(got) != 0 {
		t.Fatalf("violations = %v, want none", got)
	}
}

func TestEvaluateExpectationsIgnoresProtocolsOutsideRestrictedTargets(t *testing.T) {
	catalog := gateCatalog()
	catalog.Targets = []Protocol{ProtocolAthena}
	if got := EvaluateExpectations(catalog, []QueryResult{trinoResult("q_events_total__hoglake_table", 1, 7300, 0)}); len(got) != 0 {
		t.Fatalf("violations = %v, want none for an untargeted protocol", got)
	}
}

func TestCheckedInPostHogCatalogCarriesTrinoPerfGate(t *testing.T) {
	catalog, err := LoadCatalog(filepath.Join("..", "queries", "ducklake_posthog_tables.yaml"))
	if err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	got := map[string]map[Protocol]QueryExpectations{}
	for _, query := range catalog.Queries {
		if len(query.Expectations) > 0 {
			got[query.QueryID] = query.Expectations
		}
	}
	eventsTotal := QueryExpectations{MaxTotalSplits: int64Ptr(200), MaxBytesScanned: int64Ptr(10 << 20)}
	// Split pruning is not yet confirmed on the lane, so only bytes are bounded.
	eventsOneDay := QueryExpectations{MaxBytesScanned: int64Ptr(64 << 20)}
	personsTotal := QueryExpectations{MaxTotalSplits: int64Ptr(150)}
	want := map[string]map[Protocol]QueryExpectations{
		"q_events_total_v5__hoglake_table":         {ProtocolTrino: eventsTotal, ProtocolTrinoCached: eventsTotal},
		"q_events_count_one_day_v5__hoglake_table": {ProtocolTrino: eventsOneDay, ProtocolTrinoCached: eventsOneDay},
		"q_persons_total_v5__hoglake_table":        {ProtocolTrino: personsTotal, ProtocolTrinoCached: personsTotal},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("checked-in perf gate = %+v\nwant %+v", got, want)
	}
}
