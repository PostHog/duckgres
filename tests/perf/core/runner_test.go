package core

import (
	"context"
	"reflect"
	"testing"
	"time"
)

type testDriver struct {
	protocol Protocol
	calls    int
	queryIDs []string
}

func (d *testDriver) Protocol() Protocol { return d.protocol }

func (d *testDriver) Execute(_ context.Context, query Query, _ []any) (ExecutionResult, error) {
	d.calls++
	d.queryIDs = append(d.queryIDs, query.QueryID)
	return ExecutionResult{Rows: 1}, nil
}

func TestRunnerExecutesPairedQueriesThroughExistingRuntimeContract(t *testing.T) {
	catalog, err := ParseCatalog([]byte(pairedCatalogYAML(`
paired_queries:
  - query_id_base: q_events
    intent_id: ph.events.v1
    tags: [posthog]
    params: {}
    sql_template: SELECT COUNT(*) FROM {{ relation "events" }}
`)))
	if err != nil {
		t.Fatalf("ParseCatalog returned error: %v", err)
	}
	driver := &testDriver{protocol: ProtocolPGWire}
	sink := &inMemorySink{}
	runner := NewQueryRunner(RunnerConfig{
		Catalog: catalog,
		Drivers: map[Protocol]ProtocolDriver{
			ProtocolPGWire: driver,
		},
		Sink: sink,
		Now:  func() time.Time { return time.Unix(1700000000, 0) },
	})
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	wantIDs := []string{
		"q_events__raw_view", "q_events__ducklake_table",
		"q_events__ducklake_table", "q_events__raw_view",
	}
	if !reflect.DeepEqual(driver.queryIDs, wantIDs) {
		t.Fatalf("driver query order: got %v want %v", driver.queryIDs, wantIDs)
	}
	gotIDs := make([]string, 0, len(sink.results))
	for _, result := range sink.results {
		gotIDs = append(gotIDs, result.QueryID)
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("result query IDs: got %v want %v", gotIDs, wantIDs)
	}
}

func TestQueriesForIterationSwapsOnlyCompletePairsWithoutMutatingCatalogOrder(t *testing.T) {
	queries := []Query{
		{QueryID: "legacy_before"},
		{QueryID: "pair_a_raw", IntentID: "intent_a", StorageTarget: StorageTargetRawView},
		{QueryID: "pair_a_table", IntentID: "intent_a", StorageTarget: StorageTargetDuckLakeTable},
		{QueryID: "legacy_middle"},
		{QueryID: "pair_b_raw", IntentID: "intent_b", StorageTarget: StorageTargetRawView},
		{QueryID: "pair_b_table", IntentID: "intent_b", StorageTarget: StorageTargetDuckLakeTable},
		{QueryID: "legacy_after"},
	}
	declaredOrder := queryIDOrder(queries)
	if got := queryIDOrder(queriesForIteration(queries, 1)); !reflect.DeepEqual(got, declaredOrder) {
		t.Fatalf("odd iteration order: got %v want %v", got, declaredOrder)
	}
	wantEven := []string{
		"legacy_before",
		"pair_a_table", "pair_a_raw",
		"legacy_middle",
		"pair_b_table", "pair_b_raw",
		"legacy_after",
	}
	if got := queryIDOrder(queriesForIteration(queries, 2)); !reflect.DeepEqual(got, wantEven) {
		t.Fatalf("even iteration order: got %v want %v", got, wantEven)
	}
	if got := queryIDOrder(queries); !reflect.DeepEqual(got, declaredOrder) {
		t.Fatalf("catalog order mutated: got %v want %v", got, declaredOrder)
	}
}

func queryIDOrder(queries []Query) []string {
	ids := make([]string, 0, len(queries))
	for _, query := range queries {
		ids = append(ids, query.QueryID)
	}
	return ids
}

func TestRunnerBalancesPairedQueryOrderAcrossMeasuredIterations(t *testing.T) {
	driver := &testDriver{protocol: ProtocolPGWire}
	sink := &inMemorySink{}
	runner := NewQueryRunner(RunnerConfig{
		Catalog: Catalog{
			Name:              "paired",
			MeasureIterations: 4,
			Targets:           []Protocol{ProtocolPGWire},
			Queries: []Query{
				{
					QueryID:       "q_events__raw_view",
					IntentID:      "intent_events",
					PGWireSQL:     "SELECT COUNT(*) FROM frozen_v1.events_file_view",
					StorageTarget: StorageTargetRawView,
				},
				{
					QueryID:       "q_events__ducklake_table",
					IntentID:      "intent_events",
					PGWireSQL:     "SELECT COUNT(*) FROM posthog.events",
					StorageTarget: StorageTargetDuckLakeTable,
				},
			},
		},
		Drivers: map[Protocol]ProtocolDriver{
			ProtocolPGWire: driver,
		},
		Sink: sink,
		Now:  func() time.Time { return time.Unix(1700000000, 0) },
	})

	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	wantIDs := []string{
		"q_events__raw_view", "q_events__ducklake_table",
		"q_events__ducklake_table", "q_events__raw_view",
		"q_events__raw_view", "q_events__ducklake_table",
		"q_events__ducklake_table", "q_events__raw_view",
	}
	if !reflect.DeepEqual(driver.queryIDs, wantIDs) {
		t.Fatalf("driver query order: got %v want %v", driver.queryIDs, wantIDs)
	}
	gotIDs := make([]string, 0, len(sink.results))
	for _, result := range sink.results {
		gotIDs = append(gotIDs, result.QueryID)
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("result query order: got %v want %v", gotIDs, wantIDs)
	}
}

func (d *testDriver) Close() error { return nil }

type inMemorySink struct {
	results []QueryResult
}

func (s *inMemorySink) Record(r QueryResult) error {
	s.results = append(s.results, r)
	return nil
}

func (s *inMemorySink) Close(RunSummary, string) error { return nil }

func TestRunnerLifecycleAndPerQueryRecording(t *testing.T) {
	pg := &testDriver{protocol: ProtocolPGWire}
	sink := &inMemorySink{}

	setupCalled := 0
	teardownCalled := 0
	runner := NewQueryRunner(RunnerConfig{
		Catalog: Catalog{
			Name:              "smoke",
			WarmupIterations:  1,
			MeasureIterations: 2,
			Targets:           []Protocol{ProtocolPGWire},
			Queries: []Query{
				{
					QueryID:   "q1",
					IntentID:  "i1",
					PGWireSQL: "SELECT 1",
				},
			},
		},
		Drivers: map[Protocol]ProtocolDriver{
			ProtocolPGWire: pg,
		},
		Sink:           sink,
		DatasetVersion: "v1",
		OnSetup: func(context.Context) error {
			setupCalled++
			return nil
		},
		OnTeardown: func(context.Context) error {
			teardownCalled++
			return nil
		},
		Now: func() time.Time { return time.Unix(1700000000, 0) },
	})

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if setupCalled != 1 || teardownCalled != 1 {
		t.Fatalf("expected setup/teardown once, got %d/%d", setupCalled, teardownCalled)
	}
	if pg.calls != 3 {
		t.Fatalf("expected pgwire driver to run 3 times (warmup+measure), got %d", pg.calls)
	}
	if len(sink.results) != 2 {
		t.Fatalf("expected 2 measured records, got %d", len(sink.results))
	}
	for i, got := range []int{
		sink.results[0].MeasureIteration,
		sink.results[1].MeasureIteration,
	} {
		want := i + 1
		if got != want {
			t.Fatalf("result %d measure iteration = %d, want %d", i, got, want)
		}
	}
	if summary.TotalQueries != 2 || summary.TotalErrors != 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if summary.DatasetVersion != "v1" {
		t.Fatalf("expected dataset version v1, got %q", summary.DatasetVersion)
	}
}

func TestRunnerUsesConfiguredRunID(t *testing.T) {
	pg := &testDriver{protocol: ProtocolPGWire}
	sink := &inMemorySink{}

	runner := NewQueryRunner(RunnerConfig{
		RunID: "nightly-v1-20260311T234300Z",
		Catalog: Catalog{
			Name:              "smoke",
			WarmupIterations:  0,
			MeasureIterations: 1,
			Targets:           []Protocol{ProtocolPGWire},
			Queries: []Query{
				{
					QueryID:   "q1",
					IntentID:  "i1",
					PGWireSQL: "SELECT 1",
				},
			},
		},
		Drivers: map[Protocol]ProtocolDriver{
			ProtocolPGWire: pg,
		},
		Sink: sink,
		Now:  func() time.Time { return time.Unix(1700000000, 0) },
	})

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if summary.RunID != "nightly-v1-20260311T234300Z" {
		t.Fatalf("summary run_id = %q", summary.RunID)
	}
}
