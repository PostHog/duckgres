package pgwire

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
	return 3, nil
}

func (f *fakeExec) Close() error { return nil }

func TestDriverUsesPGWireVariant(t *testing.T) {
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
	if exec.lastQuery != "SELECT 1" {
		t.Fatalf("expected pgwire SQL, got %q", exec.lastQuery)
	}
}
