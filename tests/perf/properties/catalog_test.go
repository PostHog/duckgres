package properties

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/posthog/duckgres/tests/perf/core"
)

func TestCatalogTargets(t *testing.T) {
	catalog := Catalog()
	if len(catalog.Queries) != 6 {
		t.Fatalf("got %d queries, want 6", len(catalog.Queries))
	}
	want := map[string][]core.Protocol{
		"json":    {core.ProtocolPGWireUncached, core.ProtocolPGWireCached, core.ProtocolTrino},
		"struct":  {core.ProtocolAthena},
		"variant": {core.ProtocolTrinoCached},
	}
	for _, q := range catalog.Queries {
		if !reflect.DeepEqual(q.Targets, want[q.Representation]) {
			t.Errorf("%s targets = %v, want %v", q.QueryID, q.Targets, want[q.Representation])
		}
	}
}

type recordingDriver struct {
	protocol          core.Protocol
	mismatchIntent    string
	reads, executions []core.Query
}

func (d *recordingDriver) Protocol() core.Protocol { return d.protocol }
func (d *recordingDriver) Close() error            { return nil }
func (d *recordingDriver) Execute(_ context.Context, q core.Query, _ []any) (core.ExecutionResult, error) {
	d.executions = append(d.executions, q)
	return core.ExecutionResult{Rows: 1}, nil
}
func (d *recordingDriver) ReadResults(_ context.Context, q core.Query, _ []any) ([][]*string, error) {
	d.reads = append(d.reads, q)
	value := q.IntentID
	if q.IntentID == d.mismatchIntent {
		value = "different result"
	}
	return [][]*string{{&value}}, nil
}

func TestCatalogRunnerSharedJSONBaseline(t *testing.T) {
	protocols := []core.Protocol{core.ProtocolPGWireUncached, core.ProtocolPGWireCached, core.ProtocolTrino, core.ProtocolAthena}
	run := func(t *testing.T, mismatchProtocol core.Protocol, mismatchIntent string) {
		t.Helper()
		drivers := map[core.Protocol]core.ProtocolDriver{}
		for _, p := range protocols {
			d := &recordingDriver{protocol: p}
			if p == mismatchProtocol {
				d.mismatchIntent = mismatchIntent
			}
			drivers[p] = d
		}
		summary, err := core.NewQueryRunner(core.RunnerConfig{Catalog: Catalog(), Drivers: drivers}).Run(context.Background())
		if mismatchIntent != "" {
			if err == nil || !strings.Contains(err.Error(), "complete ordered results differ from JSON baseline") || !strings.Contains(err.Error(), mismatchIntent) {
				t.Fatalf("expected shared baseline mismatch for %s, got %v", mismatchIntent, err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if summary.WarmupQueries != 8 || summary.TotalQueries != 32 || summary.TotalErrors != 0 {
				t.Fatalf("unexpected measurement counts: %+v", summary)
			}
		}
		for _, p := range protocols {
			d := drivers[p].(*recordingDriver)
			for _, q := range append(append([]core.Query{}, d.reads...), d.executions...) {
				if p == core.ProtocolAthena && q.Representation != "struct" {
					t.Errorf("Athena executed %s during validation or measurement", q.QueryID)
				}
			}
			if mismatchIntent != "" {
				if len(d.executions) != 0 {
					t.Errorf("%s executed measurements after failed correctness gate", p)
				}
			} else if len(d.reads) != 2 || len(d.executions) != 10 {
				t.Errorf("%s: got %d validation reads and %d warmup/timed executions, want 2 and 10", p, len(d.reads), len(d.executions))
			}
		}
	}
	t.Run("matching", func(t *testing.T) { run(t, "", "") })
	for _, p := range protocols[1:] {
		for _, intent := range []string{"properties.browser_breakdown.v1", "properties.chrome_events.v1"} {
			t.Run(string(p)+"/"+intent, func(t *testing.T) { run(t, p, intent) })
		}
	}
}
