package core

import (
	"context"
	"testing"
	"time"
)

type testDriver struct {
	protocol Protocol
	calls    int
}

func (d *testDriver) Protocol() Protocol { return d.protocol }

func (d *testDriver) Execute(_ context.Context, _ Query, _ []any) (ExecutionResult, error) {
	d.calls++
	return ExecutionResult{Rows: 1}, nil
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
	fl := &testDriver{protocol: ProtocolFlight}
	sink := &inMemorySink{}

	setupCalled := 0
	teardownCalled := 0
	runner := NewQueryRunner(RunnerConfig{
		Catalog: Catalog{
			Name:              "smoke",
			WarmupIterations:  1,
			MeasureIterations: 2,
			Targets:           []Protocol{ProtocolPGWire, ProtocolFlight},
			Queries: []Query{
				{
					QueryID:    "q1",
					IntentID:   "i1",
					PGWireSQL:  "SELECT 1",
					DuckhogSQL: "SELECT 1",
				},
			},
		},
		Drivers: map[Protocol]ProtocolDriver{
			ProtocolPGWire: pg,
			ProtocolFlight: fl,
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
	if pg.calls != 3 || fl.calls != 3 {
		t.Fatalf("expected each driver to run 3 times (warmup+measure), got pg=%d flight=%d", pg.calls, fl.calls)
	}
	if len(sink.results) != 4 {
		t.Fatalf("expected 4 measured records, got %d", len(sink.results))
	}
	for i, got := range []int{
		sink.results[0].MeasureIteration,
		sink.results[1].MeasureIteration,
		sink.results[2].MeasureIteration,
		sink.results[3].MeasureIteration,
	} {
		want := (i / 2) + 1
		if got != want {
			t.Fatalf("result %d measure iteration = %d, want %d", i, got, want)
		}
	}
	if summary.TotalQueries != 4 || summary.TotalErrors != 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if summary.DatasetVersion != "v1" {
		t.Fatalf("expected dataset version v1, got %q", summary.DatasetVersion)
	}
}
