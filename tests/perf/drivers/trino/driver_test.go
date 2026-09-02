package trino

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/perf/core"
)

type fakeExecutor struct {
	queries []string
	args    [][]any
	errors  []error
	closed  bool
}

func (f *fakeExecutor) Execute(_ context.Context, query string, args []any) (int64, error) {
	f.queries = append(f.queries, query)
	f.args = append(f.args, append([]any(nil), args...))
	index := len(f.queries) - 1
	if index < len(f.errors) && f.errors[index] != nil {
		return 0, f.errors[index]
	}
	return 3, nil
}

func (f *fakeExecutor) Close() error {
	f.closed = true
	return nil
}

func TestDriverUsesCanonicalRenderedSQL(t *testing.T) {
	exec := &fakeExecutor{}
	driver := NewWithExecutor(exec)

	result, err := driver.Execute(context.Background(), core.Query{
		QueryID:   "q1",
		IntentID:  "i1",
		PGWireSQL: `SELECT COUNT(*) FROM "posthog"."events"`,
	}, []any{42})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if driver.Protocol() != core.ProtocolTrino {
		t.Fatalf("Protocol = %q, want %q", driver.Protocol(), core.ProtocolTrino)
	}
	if got, want := exec.queries, []string{`SELECT COUNT(*) FROM "posthog"."events"`}; len(got) != 1 || got[0] != want[0] {
		t.Fatalf("queries = %v, want %v", got, want)
	}
	if result.Rows != 3 || result.Duration <= 0 {
		t.Fatalf("result = %+v, want 3 rows and measured duration", result)
	}
}

func TestDriverRejectsMissingCanonicalSQL(t *testing.T) {
	driver := NewWithExecutor(&fakeExecutor{})
	_, err := driver.Execute(context.Background(), core.Query{QueryID: "q1"}, nil)
	if err == nil || !strings.Contains(err.Error(), "missing canonical SQL") {
		t.Fatalf("error = %v, want missing canonical SQL", err)
	}
}

func TestWaitReadyRetriesOutsideMeasuredExecution(t *testing.T) {
	exec := &fakeExecutor{errors: []error{errors.New("credentials not loaded"), errors.New("credentials not loaded")}}
	driver := NewWithExecutor(exec)
	var sleeps int

	err := driver.WaitReady(context.Background(), StartupOptions{
		Timeout:      time.Second,
		PollInterval: time.Millisecond,
		Sleep: func(context.Context, time.Duration) error {
			sleeps++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("WaitReady returned error: %v", err)
	}
	if got, want := exec.queries, []string{"SELECT 1", "SELECT 1", "SELECT 1"}; len(got) != len(want) {
		t.Fatalf("queries = %v, want %v", got, want)
	}
	if sleeps != 2 {
		t.Fatalf("sleeps = %d, want 2", sleeps)
	}

	_, err = driver.Execute(context.Background(), core.Query{QueryID: "q1", PGWireSQL: "SELECT 42"}, nil)
	if err != nil {
		t.Fatalf("measured Execute returned error: %v", err)
	}
	if got := exec.queries[len(exec.queries)-1]; got != "SELECT 42" {
		t.Fatalf("last query = %q, want measured query", got)
	}
}

func TestWaitReadyIsBoundedAndReturnsLastFailure(t *testing.T) {
	exec := &fakeExecutor{errors: []error{errors.New("access denied")}}
	driver := NewWithExecutor(exec)
	ctx, cancel := context.WithCancel(context.Background())

	err := driver.WaitReady(ctx, StartupOptions{
		Timeout:      time.Second,
		PollInterval: time.Millisecond,
		Sleep: func(context.Context, time.Duration) error {
			cancel()
			return context.Canceled
		},
	})
	if err == nil || !strings.Contains(err.Error(), "access denied") || !strings.Contains(err.Error(), "trino startup smoke") {
		t.Fatalf("error = %v, want bounded startup error with last failure", err)
	}
}

func TestConnectionConfigRequiresVerifiedHTTPSAndPreservesCAPath(t *testing.T) {
	config := ConnectionConfig{
		ServerURL:  "https://trino.example.test:8443",
		Username:   "org_database",
		Password:   "p@ss:/word",
		Catalog:    "org_catalog",
		Schema:     "posthog",
		Source:     "duckgres-perf",
		CACertFile: "/trino-ca/ca.crt",
	}
	dsn, err := config.DSN()
	if err != nil {
		t.Fatalf("DSN returned error: %v", err)
	}
	for _, want := range []string{
		"https://org_database:p%40ss%3A%2Fword@trino.example.test:8443",
		"catalog=org_catalog",
		"schema=posthog",
		"source=duckgres-perf",
		"SSLCertPath=%2Ftrino-ca%2Fca.crt",
	} {
		if !strings.Contains(dsn, want) {
			t.Fatalf("DSN = %q, want %q", dsn, want)
		}
	}

	config.ServerURL = "http://trino.example.test:8080"
	if _, err := config.DSN(); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("insecure DSN error = %v, want HTTPS requirement", err)
	}
}
