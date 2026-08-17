package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// fakeTrinoBenchmarkLifecycle is an in-memory stand-in for the Kubernetes
// lifecycle manager. It never holds credential material, mirroring the real
// manager's contract.
type fakeTrinoBenchmarkLifecycle struct {
	provision   func(context.Context, string, TrinoBenchmarkRequest) (TrinoBenchmarkProvisionResult, error)
	status      func(context.Context, string) (TrinoBenchmarkCluster, error)
	deprovision func(context.Context, string) error

	provisionCalls   int
	deprovisionCalls int
	lastRequest      TrinoBenchmarkRequest
	lastOrgID        string
	lastClusterID    string
}

func (f *fakeTrinoBenchmarkLifecycle) ProvisionTrinoBenchmark(ctx context.Context, orgID string, request TrinoBenchmarkRequest) (TrinoBenchmarkProvisionResult, error) {
	f.provisionCalls++
	f.lastOrgID = orgID
	f.lastRequest = request
	if f.provision != nil {
		return f.provision(ctx, orgID, request)
	}
	return TrinoBenchmarkProvisionResult{
		Cluster: TrinoBenchmarkCluster{ID: "trino-bench-" + orgID, State: TrinoBenchmarkStatePending},
		Created: true,
	}, nil
}

func (f *fakeTrinoBenchmarkLifecycle) TrinoBenchmarkStatus(ctx context.Context, clusterID string) (TrinoBenchmarkCluster, error) {
	f.lastClusterID = clusterID
	if f.status != nil {
		return f.status(ctx, clusterID)
	}
	return TrinoBenchmarkCluster{ID: clusterID, State: TrinoBenchmarkStateReady, Endpoint: "http://trino:8080"}, nil
}

func (f *fakeTrinoBenchmarkLifecycle) DeprovisionTrinoBenchmark(ctx context.Context, clusterID string) error {
	f.deprovisionCalls++
	f.lastClusterID = clusterID
	if f.deprovision != nil {
		return f.deprovision(ctx, clusterID)
	}
	return nil
}

const trinoTestInternalSecret = "test-internal-secret"

// newTrinoBenchmarkTestEngine mounts the API with a stand-in for the admin
// gate. The real admin.AuthMiddleware/RoleGate/RequireAdmin wiring lives under
// the kubernetes build tag, so it is exercised in
// trino_benchmark_api_authz_test.go; here the stub keeps the handler behavior
// itself testable in the default build.
func newTrinoBenchmarkTestEngine(lifecycle TrinoBenchmarkLifecycle) *gin.Engine {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	api := engine.Group("/api/v1")
	registerTrinoBenchmarkAPI(api, lifecycle, stubRequireAdmin())
	return engine
}

// stubRequireAdmin mirrors admin.RequireAdmin's contract: reject an
// unauthenticated caller with 401 before the handler runs.
func stubRequireAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.GetHeader("X-Duckgres-Internal-Secret") != trinoTestInternalSecret {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
			return
		}
		c.Next()
	}
}

func trinoBenchmarkRequest(t *testing.T, engine *gin.Engine, method, path, body string, auth bool) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body == "" {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth {
		req.Header.Set("X-Duckgres-Internal-Secret", trinoTestInternalSecret)
	}
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	return rec
}

func TestTrinoBenchmarkAPIRequiresAuthentication(t *testing.T) {
	engine := newTrinoBenchmarkTestEngine(&fakeTrinoBenchmarkLifecycle{})

	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/api/v1/trino-benchmarks/orgs/bench-org/provision"},
		{http.MethodGet, "/api/v1/trino-benchmarks/status/trino-bench-bench-org"},
		{http.MethodPost, "/api/v1/trino-benchmarks/deprovision/trino-bench-bench-org"},
	} {
		rec := trinoBenchmarkRequest(t, engine, tc.method, tc.path, "", false)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s unauthenticated = %d, want 401", tc.method, tc.path, rec.Code)
		}
	}
}

func TestTrinoBenchmarkAPIRouteTopology(t *testing.T) {
	engine := newTrinoBenchmarkTestEngine(&fakeTrinoBenchmarkLifecycle{})

	var got []string
	for _, route := range engine.Routes() {
		if strings.Contains(route.Path, "trino-benchmarks") {
			got = append(got, route.Method+" "+route.Path)
		}
	}
	sort.Strings(got)
	want := []string{
		"GET /api/v1/trino-benchmarks/status/:cluster_id",
		"POST /api/v1/trino-benchmarks/deprovision/:cluster_id",
		"POST /api/v1/trino-benchmarks/orgs/:org_id/provision",
	}
	if len(got) != len(want) {
		t.Fatalf("routes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("routes = %v, want %v", got, want)
		}
	}
}

func TestTrinoBenchmarkAPIFailsClosedWithoutLifecycle(t *testing.T) {
	engine := newTrinoBenchmarkTestEngine(nil)

	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/api/v1/trino-benchmarks/orgs/bench-org/provision", `{"workers":4}`},
		{http.MethodGet, "/api/v1/trino-benchmarks/status/trino-bench-bench-org", ""},
		{http.MethodPost, "/api/v1/trino-benchmarks/deprovision/trino-bench-bench-org", ""},
	} {
		rec := trinoBenchmarkRequest(t, engine, tc.method, tc.path, tc.body, true)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s %s without lifecycle = %d, want 503", tc.method, tc.path, rec.Code)
		}
	}
}

func TestTrinoBenchmarkAPIProvisionValidatesRequest(t *testing.T) {
	lifecycle := &fakeTrinoBenchmarkLifecycle{}
	engine := newTrinoBenchmarkTestEngine(lifecycle)

	for name, tc := range map[string]struct{ path, body string }{
		"invalid org id":     {"/api/v1/trino-benchmarks/orgs/Bench_Org!/provision", `{"workers":4}`},
		"malformed json":     {"/api/v1/trino-benchmarks/orgs/bench-org/provision", `{"workers":`},
		"unknown field":      {"/api/v1/trino-benchmarks/orgs/bench-org/provision", `{"metadata_password":"hunter2"}`},
		"negative workers":   {"/api/v1/trino-benchmarks/orgs/bench-org/provision", `{"workers":-1}`},
		"worker count large": {"/api/v1/trino-benchmarks/orgs/bench-org/provision", fmt.Sprintf(`{"workers":%d}`, maxTrinoBenchmarkWorkers+1)},
	} {
		t.Run(name, func(t *testing.T) {
			rec := trinoBenchmarkRequest(t, engine, http.MethodPost, tc.path, tc.body, true)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body = %s, want 400", rec.Code, rec.Body.String())
			}
		})
	}
	if lifecycle.provisionCalls != 0 {
		t.Fatalf("provision calls = %d, want 0 for rejected requests", lifecycle.provisionCalls)
	}
}

func TestTrinoBenchmarkAPIProvisionAcceptsEmptyBody(t *testing.T) {
	lifecycle := &fakeTrinoBenchmarkLifecycle{}
	engine := newTrinoBenchmarkTestEngine(lifecycle)

	rec := trinoBenchmarkRequest(t, engine, http.MethodPost, "/api/v1/trino-benchmarks/orgs/bench-org/provision", "", true)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s, want 202", rec.Code, rec.Body.String())
	}
	if lifecycle.lastRequest.Workers != 0 {
		t.Fatalf("workers = %d, want 0 so the control plane applies its configured default", lifecycle.lastRequest.Workers)
	}
}

func TestTrinoBenchmarkAPIProvisionIsIdempotent(t *testing.T) {
	created := true
	lifecycle := &fakeTrinoBenchmarkLifecycle{
		provision: func(_ context.Context, orgID string, request TrinoBenchmarkRequest) (TrinoBenchmarkProvisionResult, error) {
			result := TrinoBenchmarkProvisionResult{
				Cluster: TrinoBenchmarkCluster{
					ID:               "trino-bench-" + orgID,
					State:            TrinoBenchmarkStatePending,
					RequestedWorkers: request.Workers,
				},
				Created: created,
			}
			created = false
			return result, nil
		},
	}
	engine := newTrinoBenchmarkTestEngine(lifecycle)

	first := trinoBenchmarkRequest(t, engine, http.MethodPost, "/api/v1/trino-benchmarks/orgs/bench-org/provision", `{"workers":4}`, true)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first provision = %d, want 202", first.Code)
	}
	second := trinoBenchmarkRequest(t, engine, http.MethodPost, "/api/v1/trino-benchmarks/orgs/bench-org/provision", `{"workers":4}`, true)
	if second.Code != http.StatusOK {
		t.Fatalf("repeat provision = %d, want 200", second.Code)
	}

	var cluster TrinoBenchmarkCluster
	if err := json.Unmarshal(second.Body.Bytes(), &cluster); err != nil {
		t.Fatalf("decode repeat provision: %v", err)
	}
	if cluster.ID != "trino-bench-bench-org" || cluster.RequestedWorkers != 4 {
		t.Fatalf("cluster = %+v", cluster)
	}
}

func TestTrinoBenchmarkAPIMapsLifecycleErrorsToStatusCodes(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want int
	}{
		"conflict":      {ErrTrinoBenchmarkConflict, http.StatusConflict},
		"not found":     {ErrTrinoBenchmarkNotFound, http.StatusNotFound},
		"disabled":      {ErrTrinoBenchmarkDisabled, http.StatusServiceUnavailable},
		"misconfigured": {ErrTrinoBenchmarkConfig, http.StatusServiceUnavailable},
		"invalid":       {ErrTrinoBenchmarkInvalidRequest, http.StatusBadRequest},
		"unknown":       {errors.New("boom"), http.StatusInternalServerError},
	} {
		t.Run(name, func(t *testing.T) {
			lifecycle := &fakeTrinoBenchmarkLifecycle{
				provision: func(context.Context, string, TrinoBenchmarkRequest) (TrinoBenchmarkProvisionResult, error) {
					return TrinoBenchmarkProvisionResult{}, fmt.Errorf("wrapped: %w", tc.err)
				},
			}
			engine := newTrinoBenchmarkTestEngine(lifecycle)
			rec := trinoBenchmarkRequest(t, engine, http.MethodPost, "/api/v1/trino-benchmarks/orgs/bench-org/provision", `{"workers":4}`, true)
			if rec.Code != tc.want {
				t.Fatalf("status = %d body = %s, want %d", rec.Code, rec.Body.String(), tc.want)
			}
		})
	}
}

func TestTrinoBenchmarkAPIStatusReportsLifecycleStates(t *testing.T) {
	for _, state := range []TrinoBenchmarkState{TrinoBenchmarkStatePending, TrinoBenchmarkStateReady, TrinoBenchmarkStateFailed} {
		t.Run(string(state), func(t *testing.T) {
			lifecycle := &fakeTrinoBenchmarkLifecycle{
				status: func(_ context.Context, clusterID string) (TrinoBenchmarkCluster, error) {
					return TrinoBenchmarkCluster{
						ID:               clusterID,
						State:            state,
						Endpoint:         "http://trino-bench-bench-org.duckgres.svc.cluster.local:8080",
						RequestedWorkers: 4,
						ReadyWorkers:     4,
					}, nil
				},
			}
			engine := newTrinoBenchmarkTestEngine(lifecycle)
			rec := trinoBenchmarkRequest(t, engine, http.MethodGet, "/api/v1/trino-benchmarks/status/trino-bench-bench-org", "", true)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			var cluster TrinoBenchmarkCluster
			if err := json.Unmarshal(rec.Body.Bytes(), &cluster); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if cluster.State != state {
				t.Fatalf("state = %q, want %q", cluster.State, state)
			}
		})
	}
}

func TestTrinoBenchmarkAPIStatusValidatesClusterID(t *testing.T) {
	lifecycle := &fakeTrinoBenchmarkLifecycle{}
	engine := newTrinoBenchmarkTestEngine(lifecycle)

	rec := trinoBenchmarkRequest(t, engine, http.MethodGet, "/api/v1/trino-benchmarks/status/NOT%20A%20CLUSTER", "", true)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestTrinoBenchmarkAPIDeprovisionIsIdempotent(t *testing.T) {
	lifecycle := &fakeTrinoBenchmarkLifecycle{
		deprovision: func(context.Context, string) error { return nil },
	}
	engine := newTrinoBenchmarkTestEngine(lifecycle)

	for i := 0; i < 2; i++ {
		rec := trinoBenchmarkRequest(t, engine, http.MethodPost, "/api/v1/trino-benchmarks/deprovision/trino-bench-bench-org", "", true)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("deprovision %d = %d, want 204", i, rec.Code)
		}
	}
	if lifecycle.deprovisionCalls != 2 {
		t.Fatalf("deprovision calls = %d, want 2", lifecycle.deprovisionCalls)
	}
}

func TestTrinoBenchmarkAPIDeprovisionTreatsMissingClusterAsDeleted(t *testing.T) {
	lifecycle := &fakeTrinoBenchmarkLifecycle{
		deprovision: func(context.Context, string) error { return ErrTrinoBenchmarkNotFound },
	}
	engine := newTrinoBenchmarkTestEngine(lifecycle)

	rec := trinoBenchmarkRequest(t, engine, http.MethodPost, "/api/v1/trino-benchmarks/deprovision/trino-bench-bench-org", "", true)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("deprovision of an absent cluster = %d, want 204", rec.Code)
	}
}

func TestTrinoBenchmarkAPISanitizesErrorsAndNeverEchoesSecrets(t *testing.T) {
	const leak = "super-secret-metadata-password"
	lifecycle := &fakeTrinoBenchmarkLifecycle{
		provision: func(context.Context, string, TrinoBenchmarkRequest) (TrinoBenchmarkProvisionResult, error) {
			return TrinoBenchmarkProvisionResult{}, fmt.Errorf("connect to postgres://reader:%s@metadata:5432/ducklake: refused", leak)
		},
		status: func(context.Context, string) (TrinoBenchmarkCluster, error) {
			return TrinoBenchmarkCluster{}, fmt.Errorf("read secret value %s", leak)
		},
		deprovision: func(context.Context, string) error {
			return fmt.Errorf("delete secret holding %s", leak)
		},
	}
	engine := newTrinoBenchmarkTestEngine(lifecycle)

	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/api/v1/trino-benchmarks/orgs/bench-org/provision", `{"workers":4}`},
		{http.MethodGet, "/api/v1/trino-benchmarks/status/trino-bench-bench-org", ""},
		{http.MethodPost, "/api/v1/trino-benchmarks/deprovision/trino-bench-bench-org", ""},
	} {
		rec := trinoBenchmarkRequest(t, engine, tc.method, tc.path, tc.body, true)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("%s %s = %d, want 500", tc.method, tc.path, rec.Code)
		}
		if strings.Contains(rec.Body.String(), leak) {
			t.Fatalf("%s %s response leaked internal error detail: %s", tc.method, tc.path, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "postgres://") {
			t.Fatalf("%s %s response leaked a connection string: %s", tc.method, tc.path, rec.Body.String())
		}
	}
}

func TestTrinoBenchmarkClusterJSONExposesOnlyNonSecretFields(t *testing.T) {
	raw, err := json.Marshal(TrinoBenchmarkCluster{
		ID:               "trino-bench-bench-org",
		State:            TrinoBenchmarkStateReady,
		Endpoint:         "http://trino-bench-bench-org.duckgres.svc.cluster.local:8080",
		RequestedWorkers: 4,
		ReadyWorkers:     4,
		Image:            "registry.example/trino-brikk@sha256:abc",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	allowed := map[string]bool{
		"id": true, "state": true, "endpoint": true,
		"requested_workers": true, "ready_workers": true, "image": true,
	}
	for name := range fields {
		if !allowed[name] {
			t.Fatalf("cluster JSON exposes unexpected field %q", name)
		}
	}
}
