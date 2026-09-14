//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type managedLifecycleFake struct {
	status  configstore.TrinoCellLifecycleStatus
	writes  int
	pending bool
	err     error
}

func (s *managedLifecycleFake) GetTrinoCellLifecycle(context.Context, string) (*configstore.TrinoCellLifecycleStatus, error) {
	copy := s.status
	return &copy, s.err
}

func (s *managedLifecycleFake) FreezeTrinoCellAdmissions(_ context.Context, cell, operation, hash, target string, expected int64) (*configstore.TrinoCellFreeze, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.status.Freeze != nil {
		return s.status.Freeze, nil
	}
	if expected != s.status.AdmissionEpoch {
		return nil, configstore.ErrTrinoCellConflict
	}
	s.writes++
	s.status.AdmissionEpoch++
	s.status.Freeze = &configstore.TrinoCellFreeze{OperationID: operation, PlanHash: hash, TargetBackend: target, AdmissionEpoch: s.status.AdmissionEpoch, Stable: !s.pending}
	return s.status.Freeze, nil
}

func (s *managedLifecycleFake) ReleaseTrinoCellAdmissions(_ context.Context, cell, operation string, epoch int64) error {
	if s.err != nil {
		return s.err
	}
	if s.status.Freeze == nil || s.status.AdmissionEpoch != epoch {
		return configstore.ErrTrinoCellConflict
	}
	s.writes++
	s.status.Freeze = nil
	s.status.ReleasedOperationID, s.status.ReleasedAdmissionEpoch = operation, epoch
	s.status.AdmissionEpoch++
	return nil
}

type managedReaderFake struct {
	observation  trinoManagedGatewayObservation
	backend      trinoManagedGatewayBackend
	err          error
	reads        int
	changeOnRead int
}

func (g *managedReaderFake) Observe(context.Context, string) (*trinoManagedGatewayObservation, error) {
	g.reads++
	copy := g.observation
	if g.changeOnRead == g.reads {
		copy.Route.Generation++
	}
	return &copy, g.err
}

func (g *managedReaderFake) Backend(context.Context, string) (*trinoManagedGatewayBackend, error) {
	copy := g.backend
	return &copy, g.err
}

func provisioningFixture(t *testing.T) (*trinoRolloutProvisioningHandler, *managedLifecycleFake, *managedReaderFake) {
	t.Helper()
	store := &managedLifecycleFake{status: configstore.TrinoCellLifecycleStatus{CellID: "registered:logical-a", AdmissionEpoch: 7}}
	op := managedTestRollout()
	reader := &managedReaderFake{observation: trinoManagedGatewayObservation{Route: managedTestRoute(), Rollout: &op}}
	handler, err := newTrinoRolloutProvisioningHandler(strings.Repeat("t", 48), map[string]trinoRolloutProvisioningCell{
		"cell-a": {StoredCellID: "registered:logical-a", BlueBackend: "cell-a-blue", GreenBackend: "cell-a-green"},
	}, store, reader, func(context.Context, string, string) (string, string, error) {
		return reader.backend.NodeID, reader.backend.CoordinatorID, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler, store, reader
}

func provisioningRequest(h http.Handler, method, suffix, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/internal/trino/rollout-provisioning/cell-a"+suffix, strings.NewReader(body))
	r.Header.Set("X-Gateway-Transaction-Admin-Token", strings.Repeat("t", 48))
	w := httptest.NewRecorder()
	h.ServeHTTP(provisioningDeadlineRecorder{w}, r)
	return w
}

type provisioningDeadlineRecorder struct{ *httptest.ResponseRecorder }

func (provisioningDeadlineRecorder) SetReadDeadline(time.Time) error { return nil }

func freezeRequestBody() string {
	return `{"operationId":"operation-a","planHash":"` + strings.Repeat("a", 64) + `","expectedAdmissionEpoch":7}`
}
func releaseRequestBody() string {
	return `{"operationId":"operation-a","planHash":"` + strings.Repeat("a", 64) + `","admissionEpoch":8}`
}

func decodeProvisioning(t *testing.T, w *httptest.ResponseRecorder) trinoRolloutProvisioningResponse {
	t.Helper()
	var response trinoRolloutProvisioningResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func TestRolloutProvisioningFreezePollAndReadOnlyStatus(t *testing.T) {
	h, store, gateway := provisioningFixture(t)
	w := provisioningRequest(h, http.MethodGet, "", "")
	response := decodeProvisioning(t, w)
	if w.Code != 200 || response.AdmissionEpoch != 7 || response.Frozen || store.writes != 0 || gateway.reads != 0 {
		t.Fatal("GET must be a read-only epoch observation")
	}
	store.pending = true
	w = provisioningRequest(h, http.MethodPost, "/freeze", freezeRequestBody())
	response = decodeProvisioning(t, w)
	if w.Code != 202 || !response.Frozen || response.Stable || response.AdmissionEpoch != 8 || response.TargetBackend != "cell-a-green" || store.writes != 1 {
		t.Fatalf("pending freeze: %d %s", w.Code, w.Body.String())
	}
	store.status.Freeze.Stable = true
	w = provisioningRequest(h, http.MethodPost, "/freeze", freezeRequestBody())
	if w.Code != 200 || store.writes != 1 || !decodeProvisioning(t, w).Stable {
		t.Fatal("same operation must poll without another epoch change")
	}
}

func TestRolloutProvisioningRejectsInvalidAuthorityAndBodies(t *testing.T) {
	h, store, gateway := provisioningFixture(t)
	for _, headers := range [][]string{nil, {"wrong"}, {strings.Repeat("t", 48), strings.Repeat("t", 48)}} {
		r := httptest.NewRequest(http.MethodPost, "/internal/trino/rollout-provisioning/cell-a/freeze", strings.NewReader(freezeRequestBody()))
		for _, value := range headers {
			r.Header.Add("X-Gateway-Transaction-Admin-Token", value)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Errorf("invalid capability accepted: %d", w.Code)
		}
	}
	for _, body := range []string{`{}`, `null`, freezeRequestBody() + `{}`, strings.Replace(freezeRequestBody(), `,"expectedAdmissionEpoch":7`, "", 1), strings.Replace(freezeRequestBody(), `:7`, `:-1`, 1), strings.Replace(freezeRequestBody(), `:7`, `:7.1`, 1), strings.Replace(freezeRequestBody(), `:7`, `:7,"expectedAdmissionEpoch":7`, 1), strings.Replace(freezeRequestBody(), `}`, `,"targetBackend":"cell-b-blue"}`, 1), strings.Repeat(" ", 8193)} {
		w := provisioningRequest(h, http.MethodPost, "/freeze", body)
		if w.Code != 400 {
			t.Errorf("invalid body accepted: %d", w.Code)
		}
	}
	if store.writes != 0 || gateway.reads != 0 {
		t.Fatal("invalid authority or syntax reached the data plane")
	}
}

func TestRolloutProvisioningFreezeRejectsStaleOrForeignPlan(t *testing.T) {
	for _, mode := range []string{"stale_epoch", "wrong_operation", "wrong_hash", "wrong_phase", "foreign_source", "foreign_target", "route_changed", "no_operation", "gateway_error", "store_error"} {
		t.Run(mode, func(t *testing.T) {
			h, store, gateway := provisioningFixture(t)
			switch mode {
			case "stale_epoch":
				store.status.AdmissionEpoch++
			case "wrong_operation":
				gateway.observation.Rollout.OperationID = "another"
			case "wrong_hash":
				gateway.observation.Rollout.Plan.PlanHash = strings.Repeat("b", 64)
			case "wrong_phase":
				gateway.observation.Rollout.Phase = "WARMED"
			case "foreign_source":
				gateway.observation.Rollout.Plan.SourceBackend = "cell-b-blue"
			case "foreign_target":
				gateway.observation.Rollout.Plan.TargetBackend = "cell-b-green"
			case "route_changed":
				gateway.observation.Route.Generation++
			case "no_operation":
				gateway.observation.Rollout = nil
			case "gateway_error":
				gateway.err = errors.New("sensitive error")
			case "store_error":
				store.err = errors.New("sensitive database")
			}
			w := provisioningRequest(h, http.MethodPost, "/freeze", freezeRequestBody())
			if w.Code < 400 || store.writes != 0 || strings.Contains(w.Body.String(), "sensitive") {
				t.Fatalf("unsafe freeze: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func preparedFixture(t *testing.T) (*trinoRolloutProvisioningHandler, *managedLifecycleFake, *managedReaderFake) {
	t.Helper()
	h, store, gateway := provisioningFixture(t)
	store.status.AdmissionEpoch = 8
	store.status.Freeze = &configstore.TrinoCellFreeze{OperationID: "operation-a", PlanHash: strings.Repeat("a", 64), TargetBackend: "cell-a-green", AdmissionEpoch: 8, Stable: true,
		Certificate: &configstore.TrinoCellCertificate{TargetBackend: "cell-a-green", NodeID: "node-new", CoordinatorID: "process-new", RosterHash: strings.Repeat("c", 64), AdmittedCount: 3}}
	gateway.observation.Rollout.Phase = "CUTOVER"
	gateway.observation.Route = trinoManagedGatewayRoute{RoutingGroup: "cell-a", Generation: 4, BackendName: "cell-a-green", BackendIncarnation: managedTestTargetIncarnation}
	gateway.backend = trinoManagedGatewayBackend{BackendName: "cell-a-green", Incarnation: managedTestTargetIncarnation, State: "ACTIVE", NodeID: "node-new", CoordinatorID: "process-new"}
	return h, store, gateway
}

func TestRolloutProvisioningReleaseReceiptAndReplay(t *testing.T) {
	h, store, _ := preparedFixture(t)
	before := provisioningRequest(h, http.MethodGet, "", "")
	if !decodeProvisioning(t, before).Prepared {
		t.Fatal("GET must expose the immutable certificate without changing it")
	}
	for range 2 {
		w := provisioningRequest(h, http.MethodPost, "/release", releaseRequestBody())
		result := decodeProvisioning(t, w)
		if w.Code != 200 || result.OperationID != "operation-a" || result.AdmissionEpoch != 9 || result.Frozen || result.Prepared || store.writes != 1 {
			t.Fatalf("release receipt: %d %s", w.Code, w.Body.String())
		}
	}
}

func TestRolloutProvisioningReleaseRequiresExactCutoverProcess(t *testing.T) {
	for _, mode := range []string{"unprepared", "unstable", "wrong_phase", "old_route", "wrong_incarnation", "restarted", "live_restarted", "live_unavailable", "not_active", "changed_after_backend", "wrong_plan", "wrong_epoch"} {
		t.Run(mode, func(t *testing.T) {
			h, store, gateway := preparedFixture(t)
			switch mode {
			case "unprepared":
				store.status.Freeze.Certificate = nil
			case "unstable":
				store.status.Freeze.Stable = false
			case "wrong_phase":
				gateway.observation.Rollout.Phase = "VERIFIED"
			case "old_route":
				gateway.observation.Route = managedTestRoute()
			case "wrong_incarnation":
				gateway.backend.Incarnation = managedTestIncarnation
			case "restarted":
				gateway.backend.CoordinatorID = "replacement"
			case "live_restarted":
				h.processProbe = func(context.Context, string, string) (string, string, error) {
					return gateway.backend.NodeID, "different-live-process", nil
				}
			case "live_unavailable":
				h.processProbe = func(context.Context, string, string) (string, string, error) {
					return "", "", errors.New("sensitive live error")
				}
			case "not_active":
				gateway.backend.State = "SEALED"
			case "changed_after_backend":
				gateway.changeOnRead = 2
			case "wrong_plan":
				gateway.observation.Rollout.Plan.PlanHash = strings.Repeat("d", 64)
			case "wrong_epoch":
				store.status.AdmissionEpoch++
			}
			w := provisioningRequest(h, http.MethodPost, "/release", releaseRequestBody())
			if w.Code < 400 || store.writes != 0 {
				t.Fatalf("unsafe release: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestRolloutProvisioningUsesGatewayWireContractAcrossReincarnation(t *testing.T) {
	h, store, fixture := provisioningFixture(t)
	op := *fixture.observation.Rollout
	op.Plan.TargetIncarnation = managedTestTargetIncarnation
	route := fixture.observation.Route
	backend := trinoManagedGatewayBackend{BackendName: "cell-a-green", Incarnation: "33333333-3333-4333-8333-333333333333", State: "ACTIVE", NodeID: "fresh-node", CoordinatorID: "fresh-process"}
	h.processProbe = func(context.Context, string, string) (string, string, error) {
		return backend.NodeID, backend.CoordinatorID, nil
	}
	h.gateway = managedTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/routes/"):
			_ = json.NewEncoder(w).Encode(route)
		case strings.Contains(r.URL.Path, "/rollouts/"):
			_ = json.NewEncoder(w).Encode(op)
		case strings.Contains(r.URL.Path, "/backends/"):
			_ = json.NewEncoder(w).Encode(backend)
		default:
			t.Error("unexpected Gateway request")
			w.WriteHeader(404)
		}
	}))
	if w := provisioningRequest(h, http.MethodPost, "/freeze", freezeRequestBody()); w.Code != 200 {
		t.Fatalf("actual CLAIMED version-zero shape rejected: %d %s", w.Code, w.Body.String())
	}
	store.status.Freeze.Certificate = &configstore.TrinoCellCertificate{TargetBackend: backend.BackendName, NodeID: backend.NodeID, CoordinatorID: backend.CoordinatorID, RosterHash: strings.Repeat("c", 64), AdmittedCount: 1}
	op.Phase, op.Version = "CUTOVER", 6
	route.Generation, route.BackendName, route.BackendIncarnation = 4, backend.BackendName, backend.Incarnation
	if w := provisioningRequest(h, http.MethodPost, "/release", releaseRequestBody()); w.Code != 200 {
		t.Fatalf("current reincarnated target must match certificate, not prewarm plan incarnation: %d %s", w.Code, w.Body.String())
	}
	if store.writes != 2 {
		t.Fatal("expected exactly one freeze and one release")
	}
}

func TestRolloutProvisioningBoundsSlowRequestBody(t *testing.T) {
	for name, body := range map[string]string{"incomplete": `{"operationId":`, "oversized": strings.Repeat(" ", 8193)} {
		t.Run(name, func(t *testing.T) { testProvisioningSlowBody(t, body) })
	}
}

func testProvisioningSlowBody(t *testing.T, body string) {
	t.Helper()
	h, store, _ := provisioningFixture(t)
	h.timeout = 50 * time.Millisecond
	router := gin.New()
	router.Any(trinoRolloutProvisioningPrefix+"*path", gin.WrapH(h))
	server := httptest.NewServer(router)
	defer server.Close()
	reader, writer := io.Pipe()
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	request, err := http.NewRequest(http.MethodPost, server.URL+trinoRolloutProvisioningPrefix+"cell-a/freeze", reader)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Gateway-Transaction-Admin-Token", strings.Repeat("t", 48))
	finished := make(chan error, 1)
	go func() {
		response, err := server.Client().Do(request)
		if err == nil {
			defer func() { _ = response.Body.Close() }()
			if response.StatusCode < 400 {
				err = errors.New("slow incomplete body was accepted")
			}
		}
		finished <- err
	}()
	if _, err := writer.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		_ = writer.Close()
		<-finished
		t.Fatal("handler deadline did not interrupt the body read")
	}
	if store.writes != 0 {
		t.Fatal("incomplete body mutated lifecycle state")
	}
	if len(h.limit) != 0 {
		t.Fatal("slow body leaked its request slot")
	}
}

func TestRolloutProvisioningResetsSuccessfulBodyDeadline(t *testing.T) {
	h, _, _ := provisioningFixture(t)
	h.timeout = 50 * time.Millisecond
	router := gin.New()
	router.Any(trinoRolloutProvisioningPrefix+"*path", gin.WrapH(h))
	server := httptest.NewServer(router)
	defer server.Close()
	for attempt := range 2 {
		if attempt == 1 {
			time.Sleep(75 * time.Millisecond)
		}
		request, err := http.NewRequest(http.MethodPost, server.URL+trinoRolloutProvisioningPrefix+"cell-a/freeze", strings.NewReader(freezeRequestBody()))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Gateway-Transaction-Admin-Token", strings.Repeat("t", 48))
		reused := false
		request = request.WithContext(httptrace.WithClientTrace(request.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}))
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != 200 || (attempt == 1 && !reused) {
			t.Fatal("successful body deadline prevented connection reuse")
		}
	}
}

func TestRolloutProvisioningRejectsUnsupportedDeadlineWriter(t *testing.T) {
	h, store, _ := provisioningFixture(t)
	r := httptest.NewRequest(http.MethodPost, trinoRolloutProvisioningPrefix+"cell-a/freeze", strings.NewReader(freezeRequestBody()))
	r.Header.Set("X-Gateway-Transaction-Admin-Token", strings.Repeat("t", 48))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 503 || store.writes != 0 || !r.Close {
		t.Fatal("unsupported writer must fail closed before reading or mutating")
	}
}
