package trino

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/perf/core"
)

func TestParseQueryInfoReadsCoordinatorStatistics(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "query_info_finished.json"))
	if err != nil {
		t.Fatal(err)
	}
	info, err := ParseQueryInfo(raw)
	if err != nil {
		t.Fatalf("ParseQueryInfo returned error: %v", err)
	}
	if info.QueryID != "20260924_101500_00042_abcde" || info.State != "FINISHED" || !info.Final {
		t.Fatalf("query info identity = %+v", info)
	}
	zeroRows := int64(0)
	want := core.ServiceMetrics{
		QueueDuration: 1210 * time.Microsecond,
		// Trino reports analysis and planning separately; both precede execution.
		PlanningDuration: 55500 * time.Microsecond,
		EngineDuration:   398 * time.Millisecond,
		ServiceDuration:  412350 * time.Microsecond,
		BytesScanned:     2 << 20,
		Trino: &core.TrinoQueryStats{
			QueryID:           "20260924_101500_00042_abcde",
			Source:            core.TrinoStatsSourceQueryInfo,
			TotalSplits:       97,
			CompletedSplits:   97,
			PhysicalInputRows: &zeroRows,
			CPUDuration:       1520 * time.Millisecond,
			PeakMemoryBytes:   1310720,
		},
	}
	if !reflect.DeepEqual(info.Metrics, want) {
		t.Fatalf("metrics = %+v / %+v\nwant %+v / %+v", info.Metrics, *info.Metrics.Trino, want, *want.Trino)
	}
}

func TestParseQueryInfoRejectsIncompleteDocuments(t *testing.T) {
	for name, raw := range map[string]string{
		"not json":        `<html>`,
		"no query id":     `{"state":"FINISHED","queryStats":{"elapsedTime":"1.00ms"}}`,
		"no stats":        `{"queryId":"q1","state":"FINISHED"}`,
		"bad duration":    `{"queryId":"q1","state":"FINISHED","queryStats":{"elapsedTime":"soon"}}`,
		"bad data size":   `{"queryId":"q1","state":"FINISHED","queryStats":{"physicalInputDataSize":"12 parsecs"}}`,
		"negative splits": `{"queryId":"q1","state":"FINISHED","queryStats":{"totalDrivers":-1}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseQueryInfo([]byte(raw)); err == nil {
				t.Fatalf("ParseQueryInfo(%s) succeeded, want error", raw)
			}
		})
	}
}

func TestParseAirliftUnits(t *testing.T) {
	durations := map[string]time.Duration{
		"0.00ns":   0,
		"250.00us": 250 * time.Microsecond,
		"1.21ms":   1210 * time.Microsecond,
		"12.50s":   12500 * time.Millisecond,
		"1.50m":    90 * time.Second,
		"2.00h":    2 * time.Hour,
		"1.00d":    24 * time.Hour,
	}
	for text, want := range durations {
		got, err := parseAirliftDuration(text)
		if err != nil || got != want {
			t.Fatalf("parseAirliftDuration(%q) = %v, %v; want %v", text, got, err, want)
		}
	}
	sizes := map[string]int64{
		"0B":     0,
		"92B":    92,
		"4.36kB": 4465,
		"512kB":  512 << 10,
		"2.00MB": 2 << 20,
		"3GB":    3 << 30,
		"1.50TB": 3 << 39,
	}
	for text, want := range sizes {
		got, err := parseAirliftDataSize(text)
		if err != nil || got != want {
			t.Fatalf("parseAirliftDataSize(%q) = %d, %v; want %d", text, got, err, want)
		}
	}
	for _, bad := range []string{"", "ms", "1.0", "-1ms", "1.0 fortnights"} {
		if _, err := parseAirliftDuration(bad); err == nil {
			t.Fatalf("parseAirliftDuration(%q) succeeded, want error", bad)
		}
	}
	for _, bad := range []string{"", "B", "1.5", "-1B", "1XB"} {
		if _, err := parseAirliftDataSize(bad); err == nil {
			t.Fatalf("parseAirliftDataSize(%q) succeeded, want error", bad)
		}
	}
}

func TestDriverRecordsCoordinatorQueryInfoOutsideMeasuredDuration(t *testing.T) {
	info, err := os.ReadFile(filepath.Join("testdata", "query_info_finished.json"))
	if err != nil {
		t.Fatal(err)
	}
	coordinator := newFakeCoordinator(t)
	coordinator.queryInfo = func(queryID string, call int) (int, string) {
		// The coordinator can answer before the final query info is frozen.
		body := strings.ReplaceAll(string(info), "20260924_101500_00042_abcde", queryID)
		if call == 1 {
			body = strings.Replace(body, `"finalQueryInfo": true`, `"finalQueryInfo": false`, 1)
			body = strings.Replace(body, `"totalDrivers": 97`, `"totalDrivers": 3`, 1)
		}
		return http.StatusOK, body
	}
	driver := coordinator.driver(t)

	result, err := driver.Execute(context.Background(), core.Query{
		QueryID:   "q_events_total__hoglake_table",
		PGWireSQL: `SELECT COUNT(*) AS events FROM "posthog"."events"`,
	}, nil)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Rows != 1 {
		t.Fatalf("rows = %d, want 1", result.Rows)
	}
	metrics := result.ServiceMetrics
	if metrics == nil || metrics.Trino == nil {
		t.Fatalf("service metrics = %+v, want Trino statistics", metrics)
	}
	measuredQueryID := coordinator.lastStatementID()
	if metrics.Trino.QueryID != measuredQueryID || metrics.Trino.Source != core.TrinoStatsSourceQueryInfo {
		t.Fatalf("Trino stats identity = %+v, want query %s from query info", metrics.Trino, measuredQueryID)
	}
	if metrics.Trino.TotalSplits != 97 || metrics.BytesScanned != 2<<20 || metrics.EngineDuration != 398*time.Millisecond {
		t.Fatalf("metrics = %+v / %+v, want final query info values", metrics, metrics.Trino)
	}
	calls := coordinator.infoRequests()
	if len(calls) != 2 {
		t.Fatalf("query info requests = %v, want a retry until final query info", calls)
	}
	for _, call := range calls {
		if call.queryID != measuredQueryID || call.user != "org_example" || call.password != "tenant-password" || call.trinoUser != "org_example" || !call.pruned {
			t.Fatalf("query info request = %+v, want authenticated pruned request for %s", call, measuredQueryID)
		}
	}
	// The readiness smoke is not a benchmark query and never pays for query info.
	if coordinator.statementCount() != 2 {
		t.Fatalf("statements = %d, want readiness smoke plus measured query", coordinator.statementCount())
	}
}

func TestDriverFallsBackToStatementStatsWhenQueryInfoIsUnavailable(t *testing.T) {
	coordinator := newFakeCoordinator(t)
	coordinator.queryInfo = func(string, int) (int, string) {
		return http.StatusForbidden, `{"message":"Access Denied"}`
	}
	driver := coordinator.driver(t)

	result, err := driver.Execute(context.Background(), core.Query{
		QueryID:   "q_persons_total__hoglake_table",
		PGWireSQL: `SELECT COUNT(*) AS persons FROM "posthog"."persons"`,
	}, nil)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	want := &core.ServiceMetrics{
		QueueDuration:    time.Millisecond,
		PlanningDuration: 55 * time.Millisecond,
		ServiceDuration:  412 * time.Millisecond,
		BytesScanned:     2 << 20,
		Trino: &core.TrinoQueryStats{
			QueryID:         coordinator.lastStatementID(),
			Source:          core.TrinoStatsSourceStatement,
			TotalSplits:     92,
			CompletedSplits: 92,
			CPUDuration:     1520 * time.Millisecond,
			PeakMemoryBytes: 1310720,
		},
	}
	if !reflect.DeepEqual(result.ServiceMetrics, want) {
		t.Fatalf("metrics = %+v / %+v\nwant %+v / %+v", result.ServiceMetrics, result.ServiceMetrics.Trino, want, want.Trino)
	}
	if calls := coordinator.infoRequests(); len(calls) != 1 {
		t.Fatalf("query info requests = %d, want one non-retried forbidden request", len(calls))
	}
}

func TestDriverWithoutStatsCollectorLeavesServiceMetricsEmpty(t *testing.T) {
	driver := NewWithExecutor(&fakeExecutor{})
	result, err := driver.Execute(context.Background(), core.Query{QueryID: "q1", PGWireSQL: "SELECT 1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.ServiceMetrics != nil {
		t.Fatalf("service metrics = %+v, want nil without a coordinator", result.ServiceMetrics)
	}
}

type infoRequest struct {
	queryID   string
	user      string
	password  string
	trinoUser string
	pruned    bool
}

type fakeCoordinator struct {
	server    *httptest.Server
	caPath    string
	queryInfo func(queryID string, call int) (int, string)

	mu         sync.Mutex
	statements []string
	infoCalls  []infoRequest
}

func newFakeCoordinator(t *testing.T) *fakeCoordinator {
	t.Helper()
	coordinator := &fakeCoordinator{}
	coordinator.server = httptest.NewTLSServer(http.HandlerFunc(coordinator.serve))
	t.Cleanup(coordinator.server.Close)
	coordinator.caPath = filepath.Join(t.TempDir(), "ca.pem")
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: coordinator.server.Certificate().Raw})
	if err := os.WriteFile(coordinator.caPath, certificate, 0o600); err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func (c *fakeCoordinator) driver(t *testing.T) *Driver {
	t.Helper()
	driver, err := New(context.Background(), ConnectionConfig{
		ServerURL:  c.server.URL,
		Username:   "org_example",
		Password:   "tenant-password",
		Catalog:    "org_example",
		CACertFile: c.caPath,
		Startup:    StartupOptions{Timeout: 5 * time.Second, PollInterval: 10 * time.Millisecond},
		QueryStats: QueryStatsOptions{RetryInterval: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	t.Cleanup(func() { _ = driver.Close() })
	return driver
}

func (c *fakeCoordinator) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/statement":
		sql, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		c.statements = append(c.statements, string(sql))
		id := fmt.Sprintf("20260924_101500_%05d_abcde", len(c.statements))
		c.mu.Unlock()
		writeJSON(w, map[string]any{
			"id":      id,
			"infoUri": c.server.URL + "/ui/query.html?" + id,
			"nextUri": c.server.URL + "/v1/statement/queued/" + id + "/y0/1",
			"stats":   map[string]any{"state": "QUEUED", "queued": true, "totalSplits": 0},
		})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/statement/queued/"):
		id := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/statement/queued/"), "/")[0]
		writeJSON(w, map[string]any{
			"id":      id,
			"columns": []any{map[string]any{"name": "_col0", "type": "bigint", "typeSignature": map[string]any{"rawType": "bigint", "arguments": []any{}}}},
			"data":    []any{[]any{1}},
			"stats": map[string]any{
				"state": "FINISHED", "totalSplits": 92, "completedSplits": 92, "cpuTimeMillis": 1520,
				"queuedTimeMillis": 1, "elapsedTimeMillis": 412, "analysisTimeMillis": 35, "planningTimeMillis": 20,
				"physicalInputBytes": 2 << 20, "peakMemoryBytes": 1310720, "processedRows": 92,
			},
		})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/query/"):
		user, password, _ := r.BasicAuth()
		id := strings.TrimPrefix(r.URL.Path, "/v1/query/")
		c.mu.Lock()
		c.infoCalls = append(c.infoCalls, infoRequest{
			queryID: id, user: user, password: password,
			trinoUser: r.Header.Get("X-Trino-User"), pruned: r.URL.Query().Get("pruned") == "true",
		})
		call := len(c.infoCalls)
		c.mu.Unlock()
		status, body := http.StatusNotFound, `{}`
		if c.queryInfo != nil {
			status, body = c.queryInfo(id, call)
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	case r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (c *fakeCoordinator) lastStatementID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return fmt.Sprintf("20260924_101500_%05d_abcde", len(c.statements))
}

func (c *fakeCoordinator) statementCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.statements)
}

func (c *fakeCoordinator) infoRequests() []infoRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]infoRequest(nil), c.infoCalls...)
}

func writeJSON(w http.ResponseWriter, value any) {
	_ = json.NewEncoder(w).Encode(value)
}
