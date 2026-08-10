package trino

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/posthog/duckgres/tests/perf/core"
)

func TestDriverExecutesStatementAndFollowsPages(t *testing.T) {
	var serverURL string
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			if r.Method != http.MethodPost || r.URL.Path != "/v1/statement" {
				t.Fatalf("first request = %s %s, want POST /v1/statement", r.Method, r.URL.Path)
			}
			if got := r.Header.Get("X-Trino-Catalog"); got != "ducklake" {
				t.Fatalf("catalog header = %q, want ducklake", got)
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read statement body: %v", err)
			}
			if string(body) != "SELECT 1" {
				t.Fatalf("statement body = %q", body)
			}
			_, _ = w.Write([]byte(`{"id":"query-1","nextUri":"` + serverURL + `/v1/next/1","data":[[1],[2]]}`))
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/v1/next/1" {
			t.Fatalf("next request = %s %s, want GET /v1/next/1", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"id":"query-1","data":[[3]]}`))
	}))
	defer server.Close()
	serverURL = server.URL

	driver, err := New(Config{Endpoint: server.URL, Catalog: "ducklake", Schema: "posthog", User: "perf"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	result, err := driver.Execute(context.Background(), core.Query{QueryID: "q1", TrinoSQL: "SELECT 1"}, nil)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Rows != 3 {
		t.Fatalf("rows = %d, want 3", result.Rows)
	}
}

func TestDriverReportsEngineEnvironmentFromCoordinator(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/info" {
			t.Fatalf("request path = %q, want /v1/info", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"nodeVersion":{"version":"483"},"starting":false}`))
	}))
	defer server.Close()

	driver, err := New(Config{Endpoint: server.URL, Catalog: "ducklake", Schema: "posthog"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	env, err := driver.Environment(context.Background())
	if err != nil {
		t.Fatalf("Environment returned error: %v", err)
	}
	if env.Protocol != core.ProtocolTrino || env.Engine != "trino" || env.Version != "483" {
		t.Fatalf("environment = %+v", env)
	}
	if env.Catalog != "ducklake" || env.Schema != "posthog" || env.TimeZone != "UTC" {
		t.Fatalf("catalog identity = %+v", env)
	}
}

// Comparison metadata is best-effort: an unreachable info endpoint still yields
// the catalog identity the artifact needs.
func TestDriverEnvironmentDegradesWhenInfoIsUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	driver, err := New(Config{Endpoint: server.URL, Catalog: "ducklake", Schema: "posthog"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	env, _ := driver.Environment(context.Background())
	if env.Engine != "trino" || env.Catalog != "ducklake" {
		t.Fatalf("environment = %+v", env)
	}
	if env.Version != "" {
		t.Fatalf("version = %q, want empty when the probe fails", env.Version)
	}
}
