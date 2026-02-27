package flight

import (
	"context"
	"testing"

	"github.com/posthog/duckgres/tests/perf/core"
)

type fakeExec struct {
	lastQuery string
}

func (f *fakeExec) Execute(_ context.Context, query string, _ []any) (int64, error) {
	f.lastQuery = query
	return 4, nil
}

func (f *fakeExec) Close() error { return nil }

func TestDriverUsesDuckhogVariant(t *testing.T) {
	exec := &fakeExec{}
	driver := NewWithExecutor(exec)
	_, err := driver.Execute(context.Background(), core.Query{
		QueryID:    "q1",
		IntentID:   "i1",
		PGWireSQL:  "SELECT 1",
		DuckhogSQL: "SELECT 2",
	}, nil)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if exec.lastQuery != "SELECT 2" {
		t.Fatalf("expected duckhog SQL, got %q", exec.lastQuery)
	}
}
