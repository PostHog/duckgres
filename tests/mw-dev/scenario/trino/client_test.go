package trino

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := NewClient(ClientConfig{
		BaseURL:        server.URL,
		InternalSecret: "internal-secret",
		HTTPClient:     server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client, server
}

func TestClientProvisionPostsRequestWithInternalSecret(t *testing.T) {
	var (
		mu     sync.Mutex
		path   string
		secret string
		body   map[string]any
	)
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		path = r.URL.Path
		secret = r.Header.Get("X-Duckgres-Internal-Secret")
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"id":"trino-bench-bench-org","state":"pending"}`))
	})

	cluster, err := client.ProvisionTrino(context.Background(), ProvisionRequest{
		OrgID:  "bench-org",
		Config: map[string]any{"workers": 4},
	})
	if err != nil {
		t.Fatalf("ProvisionTrino: %v", err)
	}
	if cluster.ID != "trino-bench-bench-org" || cluster.State != StatePending {
		t.Fatalf("cluster = %+v", cluster)
	}
	if path != "/api/v1/trino-benchmarks/orgs/bench-org/provision" {
		t.Fatalf("path = %q", path)
	}
	if secret != "internal-secret" {
		t.Fatalf("internal secret header = %q", secret)
	}
	if workers, ok := body["workers"].(float64); !ok || workers != 4 {
		t.Fatalf("request body = %#v", body)
	}
}

// The lifecycle client must actually POLL: a single status request would report
// "not ready" for every cluster that has not finished converging.
func TestClientWaitPollsUntilEveryWorkerIsReady(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		attempt := requests
		mu.Unlock()
		if r.Method != http.MethodGet {
			t.Errorf("status request method = %s, want GET", r.Method)
		}
		switch attempt {
		case 1:
			_, _ = w.Write([]byte(`{"id":"trino-bench-bench-org","state":"pending","ready_workers":0,"requested_workers":4}`))
		case 2:
			_, _ = w.Write([]byte(`{"id":"trino-bench-bench-org","state":"pending","ready_workers":3,"requested_workers":4}`))
		default:
			_, _ = w.Write([]byte(`{"id":"trino-bench-bench-org","state":"ready","endpoint":"http://trino:8080","ready_workers":4,"requested_workers":4}`))
		}
	})

	ready, err := client.WaitTrinoReady(context.Background(), Cluster{ID: "trino-bench-bench-org"}, WaitOptions{
		PollInterval: time.Millisecond,
		Timeout:      5 * time.Second,
	})
	if err != nil {
		t.Fatalf("WaitTrinoReady: %v", err)
	}
	if ready.Endpoint != "http://trino:8080" || ready.State != StateReady {
		t.Fatalf("ready cluster = %+v", ready)
	}
	if ready.ReadyWorkers != 4 || ready.RequestedWorkers != 4 {
		t.Fatalf("worker counts = %d/%d", ready.ReadyWorkers, ready.RequestedWorkers)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests < 3 {
		t.Fatalf("status requests = %d, want the client to keep polling", requests)
	}
}

func TestClientWaitStopsImmediatelyOnTerminalFailure(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	client, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		_, _ = w.Write([]byte(`{"id":"trino-bench-bench-org","state":"failed"}`))
	})

	_, err := client.WaitTrinoReady(context.Background(), Cluster{ID: "trino-bench-bench-org"}, WaitOptions{
		PollInterval: time.Millisecond,
		Timeout:      5 * time.Second,
	})
	if !errors.Is(err, ErrClusterFailed) {
		t.Fatalf("error = %v, want ErrClusterFailed", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 1 {
		t.Fatalf("status requests = %d, want the poller to stop at the terminal state", requests)
	}
}

func TestClientWaitHonoursMaxAttempts(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	client, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		_, _ = w.Write([]byte(`{"id":"trino-bench-bench-org","state":"pending"}`))
	})

	_, err := client.WaitTrinoReady(context.Background(), Cluster{ID: "trino-bench-bench-org"}, WaitOptions{
		PollInterval: time.Millisecond,
		Timeout:      time.Minute,
		MaxAttempts:  3,
	})
	if err == nil {
		t.Fatal("expected the attempt budget to be exhausted")
	}
	if errors.Is(err, ErrClusterFailed) {
		t.Fatalf("error = %v, want an exhaustion error rather than a terminal failure", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 3 {
		t.Fatalf("status requests = %d, want exactly the 3 allowed attempts", requests)
	}
}

func TestClientWaitHonoursTimeout(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"trino-bench-bench-org","state":"pending"}`))
	})

	started := time.Now()
	_, err := client.WaitTrinoReady(context.Background(), Cluster{ID: "trino-bench-bench-org"}, WaitOptions{
		PollInterval: 5 * time.Millisecond,
		Timeout:      60 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected the wait to time out")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("wait took %s, want it bounded by the timeout", elapsed)
	}
	if !strings.Contains(err.Error(), "trino-bench-bench-org") {
		t.Fatalf("timeout error %q should name the cluster", err)
	}
}

// A ready state with no endpoint is not usable, so it stays a polling state.
func TestClientWaitKeepsPollingWhenReadyClusterHasNoEndpoint(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	client, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requests++
		attempt := requests
		mu.Unlock()
		if attempt == 1 {
			_, _ = w.Write([]byte(`{"id":"trino-bench-bench-org","state":"ready"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"trino-bench-bench-org","state":"ready","endpoint":"http://trino:8080"}`))
	})

	ready, err := client.WaitTrinoReady(context.Background(), Cluster{ID: "trino-bench-bench-org"}, WaitOptions{
		PollInterval: time.Millisecond,
		Timeout:      5 * time.Second,
	})
	if err != nil {
		t.Fatalf("WaitTrinoReady: %v", err)
	}
	if ready.Endpoint != "http://trino:8080" {
		t.Fatalf("endpoint = %q", ready.Endpoint)
	}
}

// A transient 5xx must not end the wait — the control plane may still be
// converging — but it must be reported if the wait ultimately fails.
func TestClientWaitRetriesTransientErrorsAndReportsTheLastOne(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})

	_, err := client.WaitTrinoReady(context.Background(), Cluster{ID: "trino-bench-bench-org"}, WaitOptions{
		PollInterval: time.Millisecond,
		Timeout:      50 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected the wait to fail")
	}
	if !strings.Contains(err.Error(), "502") {
		t.Fatalf("error %q should carry the last transport failure", err)
	}
}

func TestClientDeprovisionAcceptsNoContent(t *testing.T) {
	var mu sync.Mutex
	var path, method string
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		path, method = r.URL.Path, r.Method
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})

	if err := client.DeprovisionTrino(context.Background(), Cluster{ID: "trino-bench-bench-org"}); err != nil {
		t.Fatalf("DeprovisionTrino: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if method != http.MethodPost || path != "/api/v1/trino-benchmarks/deprovision/trino-bench-bench-org" {
		t.Fatalf("request = %s %s", method, path)
	}
}

func TestClientErrorsNeverEchoTheInternalSecret(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"provision failed"}`))
	})

	_, err := client.ProvisionTrino(context.Background(), ProvisionRequest{OrgID: "bench-org", Config: map[string]any{}})
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "internal-secret") {
		t.Fatalf("error %q leaked the internal secret", err)
	}
}
