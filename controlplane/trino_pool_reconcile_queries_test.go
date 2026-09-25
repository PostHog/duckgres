//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/posthog/duckgres/controlplane/trinogateway"
)

type drainGateway struct {
	*fakePoolGateway
	candidates   []trinogateway.DrainQueryCandidate
	requests     []trinogateway.ReconcileQueriesRequest
	after        []string
	candidateErr error
	reconcileErr error
}

func (g *drainGateway) GetDrainCandidates(_ context.Context, _, _, after string) ([]trinogateway.DrainQueryCandidate, error) {
	g.after = append(g.after, after)
	var result []trinogateway.DrainQueryCandidate
	for _, q := range g.candidates {
		if q.QueryID > after {
			result = append(result, q)
		}
	}
	return result, g.candidateErr
}
func (g *drainGateway) ReconcileQueries(_ context.Context, _, _ string, r trinogateway.ReconcileQueriesRequest) (trinogateway.ReconcileQueriesResult, error) {
	g.requests = append(g.requests, r)
	return trinogateway.ReconcileQueriesResult{Reconciled: len(r.Queries)}, g.reconcileErr
}

func TestTrinoPoolReconcilesOnlyProvenAbsentQueries(t *testing.T) {
	h, i := drainingCandidateHarness(t)
	i.CoordinatorNodeID = "node-1"
	i.CoordinatorID = "process-1"
	g := &drainGateway{fakePoolGateway: h.gateway, candidates: []trinogateway.DrainQueryCandidate{{QueryID: "absent", AdmissionCount: 3}, {QueryID: "active", AdmissionCount: 4}, {QueryID: "restarted", AdmissionCount: 5}, {QueryID: "unavailable", AdmissionCount: 6}}}
	h.operator.gateway = g
	h.gateway.obligations[i.InstanceID] = trinogateway.Obligations{ActiveQueries: 4}
	h.operator.queryDrainStatus = func(_ context.Context, endpoint, q string) (trinoQueryDrainStatus, error) {
		if endpoint != i.EndpointURL {
			t.Fatalf("wrong endpoint %q", endpoint)
		}
		switch q {
		case "absent":
			return trinoQueryDrainStatus{NodeID: "node-1", CoordinatorID: "process-1", Absent: true}, nil
		case "active":
			return trinoQueryDrainStatus{NodeID: "node-1", CoordinatorID: "process-1"}, nil
		case "restarted":
			return trinoQueryDrainStatus{NodeID: "node-1", CoordinatorID: "process-2", Absent: true}, nil
		default:
			return trinoQueryDrainStatus{}, errors.New("unavailable")
		}
	}
	if _, err := h.operator.sealWhenDrained(context.Background(), *i); err != nil {
		t.Fatal(err)
	}
	if len(g.requests) != 1 || len(g.requests[0].Queries) != 1 || g.requests[0].Queries[0].QueryID != "absent" {
		t.Fatalf("unsafe reconciliation: %+v", g.requests)
	}
	r := g.requests[0]
	if r.ExpectedGeneration != i.GatewayGeneration || r.NodeID != "node-1" || r.CoordinatorID != "process-1" || r.ControllerEpoch != h.operator.lease.Epoch {
		t.Fatalf("missing fences: %+v", r)
	}
	if countCalls(g.calls, "seal:") != 0 || h.kube.deleted[i.InstanceID] {
		t.Fatal("proof bypassed Gateway retention/seal")
	}
}

func TestTrinoPoolDrainPaginationDoesNotStarveLaterQueries(t *testing.T) {
	h, i := drainingCandidateHarness(t)
	i.CoordinatorNodeID = "node"
	i.CoordinatorID = "process"
	g := &drainGateway{fakePoolGateway: h.gateway}
	h.operator.gateway = g
	for n := 0; n < 25; n++ {
		g.candidates = append(g.candidates, trinogateway.DrainQueryCandidate{QueryID: fmt.Sprintf("q%02d", n), AdmissionCount: 1})
	}
	h.gateway.obligations[i.InstanceID] = trinogateway.Obligations{ActiveQueries: 25}
	probes := 0
	h.operator.queryDrainStatus = func(_ context.Context, _, q string) (trinoQueryDrainStatus, error) {
		probes++
		return trinoQueryDrainStatus{NodeID: "node", CoordinatorID: "process", Absent: q == "q24"}, nil
	}
	for n := 0; n < 4; n++ {
		before := probes
		if _, err := h.operator.sealWhenDrained(context.Background(), *i); err != nil {
			t.Fatal(err)
		}
		if probes-before > 10 {
			t.Fatal("unbounded tick")
		}
	}
	if len(g.requests) != 1 || g.requests[0].Queries[0].QueryID != "q24" {
		t.Fatalf("later query starved: %+v", g.requests)
	}
	if g.after[3] != "" {
		t.Fatalf("cursor did not wrap: %v", g.after)
	}
}

func TestTrinoPoolDrainPendingRequestsAndOldGatewayFailClosed(t *testing.T) {
	h, i := drainingCandidateHarness(t)
	i.CoordinatorNodeID = "node"
	i.CoordinatorID = "process"
	g := &drainGateway{fakePoolGateway: h.gateway, candidateErr: trinogateway.ErrNotFound}
	h.operator.gateway = g
	h.operator.queryDrainStatus = func(context.Context, string, string) (trinoQueryDrainStatus, error) {
		t.Fatal("unexpected probe")
		return trinoQueryDrainStatus{}, nil
	}
	h.gateway.obligations[i.InstanceID] = trinogateway.Obligations{ActiveQueries: 1, PendingRequests: 1}
	if _, err := h.operator.sealWhenDrained(context.Background(), *i); err != nil {
		t.Fatal(err)
	}
	if len(g.after) != 0 {
		t.Fatal("queried candidates while requests are admitted")
	}
	h.gateway.obligations[i.InstanceID] = trinogateway.Obligations{ActiveQueries: 1}
	if _, err := h.operator.sealWhenDrained(context.Background(), *i); err != nil {
		t.Fatal(err)
	}
	if len(g.requests) != 0 || countCalls(g.calls, "seal:") != 0 {
		t.Fatal("old Gateway caused state change")
	}
}

func TestTrinoDrainProofRequiresExplicitSuccessfulResponse(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		want       bool
	}{{"absent", `{"nodeId":"node","coordinatorId":"process","absent":true}`, 200, true}, {"missing field", `{"nodeId":"node","coordinatorId":"process"}`, 200, false}, {"gone", `{}`, 410, false}, {"old server", `{}`, 404, false}, {"unauthorized", `{}`, 403, false}, {"invalid", `not json`, 200, false}} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u, p, ok := r.BasicAuth()
				if !ok || u != "observer" || p != "test-password" || r.Header.Get("X-Forwarded-Proto") != "https" || r.URL.Path != "/v1/query/q1/drain-status" {
					t.Error("wrong authenticated request")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer s.Close()
			proof, err := probeTrinoQueryDrainStatus(context.Background(), s.Client(), s.URL, func() (string, string) { return "observer", "test-password" }, "q1")
			if (err == nil && proof.Absent) != tc.want {
				t.Fatalf("proof=%+v err=%v", proof, err)
			}
		})
	}
}

func TestTrinoPoolReconcileRetryKeepsTheProofStep(t *testing.T) {
	h, i := drainingCandidateHarness(t)
	i.CoordinatorNodeID = "node"
	i.CoordinatorID = "process"
	g := &drainGateway{fakePoolGateway: h.gateway, candidates: []trinogateway.DrainQueryCandidate{{QueryID: "query", AdmissionCount: 4}}, reconcileErr: errors.New("lost response")}
	h.operator.gateway = g
	h.gateway.obligations[i.InstanceID] = trinogateway.Obligations{ActiveQueries: 1}
	h.operator.queryDrainStatus = func(context.Context, string, string) (trinoQueryDrainStatus, error) {
		return trinoQueryDrainStatus{NodeID: "node", CoordinatorID: "process", Absent: true}, nil
	}
	if _, err := h.operator.sealWhenDrained(context.Background(), *i); err == nil {
		t.Fatal("lost response hidden")
	}
	g.reconcileErr = nil
	if _, err := h.operator.sealWhenDrained(context.Background(), *i); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,64}$`).MatchString(g.requests[0].StepID) {
		t.Fatalf("Gateway rejects step ID %q", g.requests[0].StepID)
	}
	if g.requests[0].StepID != g.requests[1].StepID {
		t.Fatal("identical proof changed replay identity")
	}
	g.candidates[0].AdmissionCount++
	if _, err := h.operator.sealWhenDrained(context.Background(), *i); err != nil {
		t.Fatal(err)
	}
	if g.requests[1].StepID == g.requests[2].StepID {
		t.Fatal("changed admission snapshot reused recorded result")
	}
}

func TestTrinoPoolDrainPlain404IsUnavailable(t *testing.T) {
	if !drainEndpointUnavailable(&trinogateway.Error{Status: http.StatusNotFound}) {
		t.Fatal("old route was treated as a protocol failure")
	}
	if drainEndpointUnavailable(&trinogateway.Error{Status: http.StatusForbidden}) {
		t.Fatal("authorization error was hidden as version mismatch")
	}
}
