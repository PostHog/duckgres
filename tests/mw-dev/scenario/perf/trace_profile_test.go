package perf

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDistinctTraceLateExport(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/select/jaeger/api/traces" || r.URL.Query().Get("service") != "trino" || r.URL.Query().Get("tags") != `{"trino.query_id":"query-test"}` {
			t.Errorf("unexpected trace request")
		}
		if r.URL.Query().Get("start") != "1780271995000000" {
			t.Errorf("unexpected start: %s", r.URL.Query().Get("start"))
		}
		if calls == 1 {
			_, _ = fmt.Fprint(w, `{"data":[],"errors":null}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"data":[{"traceID":"abc","spans":[{"tags":[{"key":"trino.query_id","value":"query-test"}]}]}],"errors":null}`)
	}))
	defer server.Close()
	raw, err := captureDistinctTraceFrom(context.Background(), server.URL+"/insert/opentelemetry/v1/traces", []byte(`{"queryId":"query-test","queryStats":{"createTime":"2026-06-01T00:00:00Z"}}`), time.Millisecond)
	if err != nil || len(raw) == 0 || calls != 2 {
		t.Fatalf("capture failed: %v, calls %d", err, calls)
	}
}

func TestDistinctTraceRejectsInvalidResponse(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"mismatch", `{"data":[{"spans":[{"tags":[{"key":"trino.query_id","value":"other"}]}]}]}`, 200},
		{"oversize", strings.Repeat("x", maxDistinctTraceBytes+1), 200},
		{"server", "private response", 403},
		{"invalid", "private response", 200},
		{"api error", `{"data":[],"errors":[{"msg":"private response"}]}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); _, _ = fmt.Fprint(w, tc.body) }))
			defer server.Close()
			_, err := captureDistinctTraceFrom(context.Background(), server.URL, []byte(`{"queryId":"query-test"}`), time.Millisecond)
			if err == nil || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), server.URL) {
				t.Fatalf("expected sanitized error, got %v", err)
			}
		})
	}
}

func TestDistinctTraceCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprint(w, `{"data":[]}`) }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := captureDistinctTraceFrom(ctx, server.URL, []byte(`{"queryId":"query-test"}`), time.Millisecond)
	if err == nil {
		t.Fatal("expected bounded wait to end")
	}
}

func TestDistinctTraceDisabled(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_TRINO_OTLP_ENDPOINT", "")
	raw, err := captureDistinctTrace(context.Background(), nil)
	if err == nil || raw != nil {
		t.Fatal("missing endpoint must fail trace capture")
	}
}

func TestDistinctTraceWaitsForAllStageLifetimes(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		spans := `{"operationName":"stage","duration":1000,"tags":[{"key":"trino.query_id","value":"query-test"},{"key":"trino.stage_id","value":"query-test.0"}]}`
		if calls > 1 {
			spans += `,{"operationName":"stage","duration":2000,"tags":[{"key":"trino.query_id","value":"query-test"},{"key":"trino.stage_id","value":"query-test.1"}]}`
		}
		_, _ = fmt.Fprintf(w, `{"data":[{"traceID":"abc","spans":[%s]}],"errors":null}`, spans)
	}))
	defer server.Close()
	raw, err := captureDistinctTraceFrom(context.Background(), server.URL, []byte(`{"queryId":"query-test","stages":{"stages":[{"stageId":"query-test.0"},{"stageId":"query-test.1"}]}}`), time.Millisecond)
	if err != nil || len(raw) == 0 || calls != 2 {
		t.Fatalf("expected to wait for second stage, got %d calls, %v", calls, err)
	}
}

func TestDistinctTracePreservesIncompleteResponse(t *testing.T) {
	const response = `{"data":[{"traceID":"0123456789abcdef0123456789abcdef","spans":[{"operationName":"stage","duration":1000,"tags":[{"key":"trino.query_id","value":"query-test"},{"key":"trino.stage_id","value":"query-test.0"}]}]}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/jaeger/api/traces" {
			t.Errorf("expected trace lookup")
		}
		_, _ = fmt.Fprint(w, response)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	raw, err := captureDistinctTraceFrom(ctx, server.URL, []byte(`{"queryId":"query-test","stages":{"stages":[{"stageId":"query-test.0"},{"stageId":"query-test.1"}]}}`), time.Second)
	if string(raw) != response || err == nil || !strings.Contains(err.Error(), "1/2 stage spans") {
		t.Fatalf("expected retained incomplete trace and numeric completeness: %s, %v", raw, err)
	}
}

func TestDistinctTraceAcceptsLargeValidResponse(t *testing.T) {
	// Split-heavy queries can export traces larger than 32 MiB. Keep the fixture
	// synthetic while exercising the HTTP reader and JSON validation together.
	response := `{"data":[{"spans":[{"operationName":"stage","duration":1000,"tags":[{"key":"trino.query_id","value":"query-test"},{"key":"trino.stage_id","value":"query-test.0"},{"key":"synthetic.padding","value":"` + strings.Repeat("x", 40<<20) + `"}]}]}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, response)
	}))
	defer server.Close()
	raw, err := captureDistinctTraceFrom(context.Background(), server.URL, []byte(`{"queryId":"query-test","stages":{"stages":[{"stageId":"query-test.0"}]}}`), time.Millisecond)
	if err != nil || len(raw) != len(response) {
		t.Fatalf("expected complete large trace: bytes=%d, err=%v", len(raw), err)
	}
}
