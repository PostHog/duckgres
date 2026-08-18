//go:build kubernetes

package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

type fakeMonitoringStore struct {
	snapshot    *configstore.Snapshot
	workers     map[string][]configstore.WorkerRecord
	connections map[string]configstore.OrgConnectionMonitoringStatus
	orgIDs      []string
}

func (f *fakeMonitoringStore) Snapshot() *configstore.Snapshot { return f.snapshot }

func (f *fakeMonitoringStore) ListWorkerRecordsForOrg(orgID string) ([]configstore.WorkerRecord, error) {
	f.orgIDs = append(f.orgIDs, orgID)
	return f.workers[orgID], nil
}

func (f *fakeMonitoringStore) OrgConnectionMonitoringState(orgID string) (configstore.OrgConnectionMonitoringStatus, error) {
	f.orgIDs = append(f.orgIDs, orgID)
	return f.connections[orgID], nil
}

func internalMonitoringRouter(store monitoringStore, live LiveInfo, fetcher PeerFetcher, metrics *MetricsProxy) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxIdentityKey, &Identity{Role: RoleAdmin, Source: "internal-secret"})
		c.Next()
	})
	registerMonitoringAPI(r.Group("/api/v1"), store, live, fetcher, metrics, MonitoringWorkerDefaults{
		CPU: "750m", Memory: "3Gi", TTL: 45 * time.Minute,
	})
	return r
}

func TestMonitoringSnapshotIsOrgScopedSanitizedAndReportsPartialCoverage(t *testing.T) {
	created := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	heartbeat := created.Add(5 * time.Minute)
	store := &fakeMonitoringStore{
		snapshot: &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{
			"org-a": {
				Name:                    "org-a",
				MaxWorkers:              4,
				MaxVCPUs:                8,
				MaxMemoryBytes:          16 * 1024 * 1024 * 1024,
				DefaultWorkerCPU:        "1",
				DefaultWorkerMemory:     "2Gi",
				DefaultWorkerTTL:        "30m",
				DefaultWorkerMinHotIdle: 1,
				Warehouse: &configstore.ManagedWarehouseConfig{
					OrgID: "org-a",
					State: configstore.ManagedWarehouseStateReady,
				},
			},
		}},
		workers: map[string][]configstore.WorkerRecord{
			"org-a": {
				{
					WorkerID:          7,
					PodName:           "sensitive-pod-name",
					Image:             "sensitive-image",
					ProfileCPU:        "2",
					ProfileMemory:     "4Gi",
					TTLMinutes:        15,
					State:             configstore.WorkerStateHot,
					OrgID:             "org-a",
					OwnerCPInstanceID: "sensitive-control-plane",
					CreatedAt:         created,
					LastHeartbeatAt:   heartbeat,
				},
				{
					WorkerID:        8,
					ProfileCPU:      "",
					ProfileMemory:   "",
					State:           configstore.WorkerStateHotIdle,
					OrgID:           "org-a",
					CreatedAt:       created,
					LastHeartbeatAt: heartbeat,
				},
			},
		},
		connections: map[string]configstore.OrgConnectionMonitoringStatus{
			"org-a": {ActiveLeases: 2, QueuedConns: 3},
		},
	}
	live := &fakeLiveInfo{queries: []QueryStatus{
		{Org: "org-a", User: "sensitive-user", PID: 101, WorkerID: 7, Protocol: "pg", State: "active", ElapsedMS: 1200, Percentage: -1},
		{Org: "org-b", User: "other-org-user", PID: 102, WorkerID: 99, Protocol: "pg", State: "active"},
	}}
	peerBody, err := json.Marshal(map[string]any{"queries": []QueryStatus{
		{Org: "org-a", User: "peer-user", PID: 201, WorkerID: 8, Protocol: "flight", State: "idle"},
		{Org: "org-b", User: "other-peer-user", PID: 202, WorkerID: 98, Protocol: "pg", State: "active"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	fetcher := &fakePeerFetcher{byPath: map[string][][]byte{
		"/api/v1/queries": {peerBody, []byte("not-json")},
	}}

	rec := httptest.NewRecorder()
	internalMonitoringRouter(store, live, fetcher, nil).ServeHTTP(
		rec,
		httptest.NewRequest(http.MethodGet, "/api/v1/orgs/org-a/monitoring/snapshot", nil),
	)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	var got monitoringSnapshotResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.OrgID != "org-a" || got.Warehouse.State != "ready" {
		t.Fatalf("identity/state = %q/%q, want org-a/ready", got.OrgID, got.Warehouse.State)
	}
	if got.Limits.DefaultWorkerCPU != "1" || got.Limits.DefaultWorkerMemory != "2Gi" || got.Limits.DefaultWorkerTTLSeconds != 1800 {
		t.Fatalf("configured defaults = %+v, want current org defaults", got.Limits)
	}
	if got.Limits.MaxMemoryBytes != 16*1024*1024*1024 {
		t.Fatalf("max memory bytes = %d, want 16Gi", got.Limits.MaxMemoryBytes)
	}
	if got.Totals.Workers != 2 || got.Totals.AllocatedCPUCores != 2.75 || got.Totals.AllocatedMemoryBytes != 7*1024*1024*1024 {
		t.Fatalf("worker totals = %+v, want 2 workers / 2.75 cores / 7Gi", got.Totals)
	}
	if got.Totals.ActiveSessions != 2 || got.Totals.RunningQueries != 1 || got.Totals.QueuedConnections != 3 {
		t.Fatalf("session totals = %+v, want 2 active / 1 running / 3 queued", got.Totals)
	}
	if len(got.Workers) != 2 || got.Workers[0].Session == nil || got.Workers[1].Session == nil {
		t.Fatalf("workers/sessions = %+v, want two joined worker sessions", got.Workers)
	}
	if got.Workers[0].Session.Percentage != nil {
		t.Fatalf("unknown query percentage = %v, want null", *got.Workers[0].Session.Percentage)
	}
	if got.Workers[1].Session.Percentage == nil || *got.Workers[1].Session.Percentage != 0 {
		t.Fatalf("known query percentage = %v, want 0", got.Workers[1].Session.Percentage)
	}
	if got.Workers[0].CPU != "2" || got.Workers[0].Memory != "4Gi" || got.Workers[0].TTLSeconds != 900 {
		t.Fatalf("explicit worker profile = %+v", got.Workers[0])
	}
	if got.Workers[1].CPU != "750m" || got.Workers[1].Memory != "3Gi" || got.Workers[1].TTLSeconds != 2700 {
		t.Fatalf("deployment-default worker profile = %+v", got.Workers[1])
	}
	if got.Coverage.CPResponders != 2 || got.Coverage.CPTotal != 3 || !got.Coverage.Partial {
		t.Fatalf("coverage = %+v, want partial 2/3", got.Coverage)
	}
	if len(store.orgIDs) != 2 || store.orgIDs[0] != "org-a" || store.orgIDs[1] != "org-a" {
		t.Fatalf("store org scopes = %v, want only org-a", store.orgIDs)
	}
	for _, forbidden := range []string{
		"sensitive-pod-name", "sensitive-image", "sensitive-control-plane",
		"sensitive-user", "peer-user", "other-org-user", "other-peer-user", "org-b",
		"pod_name", "image", "owner_cp_instance_id", "user", "pid",
	} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Errorf("snapshot leaked forbidden value/field %q: %s", forbidden, rec.Body.String())
		}
	}
}

func TestMonitoringSnapshotUsesDeploymentDefaultsForDefaultProfileSentinel(t *testing.T) {
	created := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	store := &fakeMonitoringStore{
		snapshot: monitoringTestSnapshot("org-a"),
		workers: map[string][]configstore.WorkerRecord{
			"org-a": {{WorkerID: 1, State: configstore.WorkerStateHotIdle, OrgID: "org-a", CreatedAt: created}},
		},
		connections: map[string]configstore.OrgConnectionMonitoringStatus{"org-a": {}},
	}

	rec := httptest.NewRecorder()
	internalMonitoringRouter(store, nil, nil, nil).ServeHTTP(
		rec,
		httptest.NewRequest(http.MethodGet, "/api/v1/orgs/org-a/monitoring/snapshot", nil),
	)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	var got monitoringSnapshotResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Limits.DefaultWorkerCPU != "750m" || got.Limits.DefaultWorkerMemory != "3Gi" || got.Limits.DefaultWorkerTTLSeconds != 2700 {
		t.Fatalf("effective limits = %+v, want deployment defaults", got.Limits)
	}
	if len(got.Workers) != 1 || got.Workers[0].CPU != "750m" || got.Workers[0].Memory != "3Gi" || got.Workers[0].TTLSeconds != 2700 {
		t.Fatalf("default-profile worker = %+v, want deployment defaults", got.Workers)
	}
	if got.Totals.AllocatedCPUCores != 0.75 || got.Totals.AllocatedMemoryBytes != 3*1024*1024*1024 {
		t.Fatalf("allocated totals = %+v, want 0.75 cores / 3Gi", got.Totals)
	}
}

func TestMonitoringRequiresInternalSecret(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxIdentityKey, &Identity{Role: RoleAdmin, Source: "sso"})
		c.Next()
	})
	registerMonitoringAPI(r.Group("/api/v1"), &fakeMonitoringStore{}, nil, nil, nil, MonitoringWorkerDefaults{})

	for _, path := range []string{
		"/api/v1/orgs/org-a/monitoring/snapshot",
		"/api/v1/orgs/org-a/monitoring/series?metric=query_rate&window=1h",
	} {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusForbidden {
			t.Errorf("GET %s status = %d, want 403", path, rec.Code)
		}
	}
}

func TestMonitoringSeriesIsAllowListedOrgScopedAndNormalized(t *testing.T) {
	var upstreamQuery url.Values
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamQuery = r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"org":"org-a","status":"success","reason":"none","pod":"must-not-leak"},"values":[[1723456800,"2.5"],[1723456815,"3"]]}]}}`))
	}))
	defer prom.Close()

	metrics := NewMetricsProxy(prom.URL)
	r := internalMonitoringRouter(&fakeMonitoringStore{snapshot: monitoringTestSnapshot("org-a")}, nil, nil, metrics)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/orgs/org-a/monitoring/series?metric=query_rate&window=6h", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if q := upstreamQuery.Get("query"); !strings.Contains(q, `org="org-a"`) || !strings.Contains(q, "duckgres_query_total") {
		t.Fatalf("upstream PromQL is not fixed to org-a query metric: %s", q)
	}

	var got monitoringSeriesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.OrgID != "org-a" || got.Metric != "query_rate" || got.Unit != "queries_per_second" {
		t.Fatalf("series identity = %+v", got)
	}
	if got.StepSeconds <= 0 || len(got.Series) != 1 || len(got.Series[0].Points) != 2 {
		t.Fatalf("series shape = %+v", got)
	}
	endUnix, err := strconv.ParseInt(upstreamQuery.Get("end"), 10, 64)
	if err != nil {
		t.Fatalf("end = %q: %v", upstreamQuery.Get("end"), err)
	}
	stepSeconds := got.StepSeconds
	if endUnix%stepSeconds != 0 {
		t.Fatalf("query_range end = %d, want alignment to %ds step", endUnix, stepSeconds)
	}
	startUnix, err := strconv.ParseInt(upstreamQuery.Get("start"), 10, 64)
	if err != nil {
		t.Fatalf("start = %q: %v", upstreamQuery.Get("start"), err)
	}
	if endUnix-startUnix != int64((6*time.Hour)/time.Second) {
		t.Fatalf("query_range span = %ds, want 21600s", endUnix-startUnix)
	}
	if len(got.Series[0].Labels) != 2 || got.Series[0].Labels["status"] != "success" || got.Series[0].Labels["reason"] != "none" {
		t.Fatalf("labels = %v, want only status=success and reason=none", got.Series[0].Labels)
	}
	if got.Series[0].Points[0].Value != 2.5 {
		t.Fatalf("first value = %v, want 2.5", got.Series[0].Points[0].Value)
	}
}

func TestMonitoringSeriesRejectsUnknownMetricAndWindowBeforePrometheus(t *testing.T) {
	var calls int
	prom := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer prom.Close()
	r := internalMonitoringRouter(&fakeMonitoringStore{snapshot: monitoringTestSnapshot("org-a")}, nil, nil, NewMetricsProxy(prom.URL))

	for _, path := range []string{
		"/api/v1/orgs/org-a/monitoring/series?metric=worker_states&window=1h",
		"/api/v1/orgs/org-a/monitoring/series?metric=s3_bytes_rate&window=1h",
		"/api/v1/orgs/org-a/monitoring/series?metric=query_rate&window=2h",
	} {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("GET %s status = %d, want 400: %s", path, rec.Code, rec.Body.String())
		}
	}
	if calls != 0 {
		t.Fatalf("Prometheus called %d times for rejected requests, want 0", calls)
	}
}

func TestMonitoringMetricAllowListAlwaysRequiresOrgSelector(t *testing.T) {
	for metric, spec := range monitoringMetrics {
		if !strings.Contains(spec.PromQL, "$ORG") {
			t.Errorf("monitoring metric %q does not require an org selector: %s", metric, spec.PromQL)
		}
	}
}

func TestMonitoringErrorRatioPreservesLowTrafficRatios(t *testing.T) {
	spec := monitoringMetrics["error_ratio"]
	rendered := renderPanel(spec.PromQL, `{org="org-a"}`, `{org="org-a",status="error"}`, "5m")
	if !strings.Contains(rendered, "1e-9") {
		t.Fatalf("error_ratio denominator does not use a near-zero safety floor: %s", rendered)
	}
	if strings.Contains(rendered, "clamp_min(sum(rate(duckgres_query_total"+`{org="org-a"}`+"[5m])), 1)") {
		t.Fatalf("error_ratio still clamps low-traffic orgs to one query per second: %s", rendered)
	}
	if !strings.Contains(rendered, "or vector(0)") {
		t.Fatalf("error_ratio does not return zero when the error counter is absent: %s", rendered)
	}
}

func TestMonitoringSparseGaugesAndCountersAvoidDoubleCountingOrMissingSeries(t *testing.T) {
	if promQL := monitoringMetrics["worker_crash_rate"].PromQL; !strings.Contains(promQL, "or vector(0)") {
		t.Fatalf("worker_crash_rate does not return zero when the counter is absent: %s", promQL)
	}
	if promQL := monitoringMetrics["storage_bytes"].PromQL; !strings.HasPrefix(promQL, "max(") {
		t.Fatalf("storage_bytes can sum stale leader series: %s", promQL)
	}
}

func TestMonitoringSeriesRejectsUnknownOrgBeforePrometheus(t *testing.T) {
	var calls int
	prom := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer prom.Close()
	r := internalMonitoringRouter(&fakeMonitoringStore{snapshot: monitoringTestSnapshot("org-a")}, nil, nil, NewMetricsProxy(prom.URL))

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/orgs/org-b/monitoring/series?metric=query_rate&window=1h", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != monitoringWarehouseNotFoundCode {
		t.Fatalf("404 code = %q, want %q", body.Code, monitoringWarehouseNotFoundCode)
	}
	if calls != 0 {
		t.Fatalf("Prometheus called %d times for unknown org, want 0", calls)
	}
}

func TestMonitoringSnapshotUnknownWarehouseHasStableCode(t *testing.T) {
	r := internalMonitoringRouter(&fakeMonitoringStore{snapshot: monitoringTestSnapshot("org-a")}, nil, nil, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/orgs/org-b/monitoring/snapshot", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != monitoringWarehouseNotFoundCode {
		t.Fatalf("404 code = %q, want %q", body.Code, monitoringWarehouseNotFoundCode)
	}
}

func TestMonitoringSnapshotMarksPeerDiscoveryFailurePartial(t *testing.T) {
	store := &fakeMonitoringStore{
		snapshot:    monitoringTestSnapshot("org-a"),
		workers:     map[string][]configstore.WorkerRecord{"org-a": {}},
		connections: map[string]configstore.OrgConnectionMonitoringStatus{"org-a": {}},
	}
	fetcher := &fakePeerFetcher{discoveryFailed: true}
	rec := httptest.NewRecorder()
	internalMonitoringRouter(store, &fakeLiveInfo{}, fetcher, nil).ServeHTTP(
		rec,
		httptest.NewRequest(http.MethodGet, "/api/v1/orgs/org-a/monitoring/snapshot", nil),
	)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got monitoringSnapshotResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Coverage.Partial || got.Coverage.CPResponders != 1 || got.Coverage.CPTotal != 2 {
		t.Fatalf("coverage = %+v, want discovery-failed partial 1/2", got.Coverage)
	}
}

func TestMonitoringSeriesRejectsUnknownOrgWhenMetricsAreUnconfigured(t *testing.T) {
	r := internalMonitoringRouter(&fakeMonitoringStore{snapshot: monitoringTestSnapshot("org-a")}, nil, nil, NewMetricsProxy(""))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/orgs/org-b/monitoring/series?metric=query_rate&window=1h", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
	}
}

func monitoringTestSnapshot(orgID string) *configstore.Snapshot {
	return &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{
		orgID: {Name: orgID, Warehouse: &configstore.ManagedWarehouseConfig{OrgID: orgID, State: configstore.ManagedWarehouseStateReady}},
	}}
}

var _ PeerFetcher = (*fakePeerFetcher)(nil)
