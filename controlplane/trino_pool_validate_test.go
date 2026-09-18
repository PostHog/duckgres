//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeCoordinator struct {
	server   *httptest.Server
	info     map[string]any
	sync     map[string]any
	nodes    [][]any
	requests []string
}

func newFakeCoordinator(t *testing.T) *fakeCoordinator {
	t.Helper()
	coordinator := &fakeCoordinator{
		info: map[string]any{"coordinator": true, "starting": false, "nodeId": "node-1", "coordinatorId": "abcde"},
		sync: map[string]any{
			"nodeId": "node-1", "nodeVersion": "484", "processId": "process-1", "coordinatorId": "abcde",
			"enabled": true, "ready": true, "observedRevision": 42, "appliedRevision": 42,
			"activeCatalogs": 7, "failedCatalogs": 0,
			"securityRevisions": []any{
				map[string]any{"kind": "password-authenticator", "name": "file", "revision": "9"},
				map[string]any{"kind": "group-provider", "name": "file", "revision": "4"},
			},
		},
		nodes: [][]any{
			{"node-1", true, "active"},
			{"worker-1", false, "active"},
			{"worker-2", false, "active"},
		},
	}
	// TLS, because the probe refuses a plaintext coordinator endpoint: it
	// carries an operational credential.
	coordinator.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		coordinator.requests = append(coordinator.requests, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/info":
			_ = json.NewEncoder(w).Encode(coordinator.info)
		case "/v1/catalog/sync":
			_ = json.NewEncoder(w).Encode(coordinator.sync)
		case "/v1/statement":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": coordinator.nodes})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(coordinator.server.Close)
	return coordinator
}

func (c *fakeCoordinator) validate(t *testing.T, observedWorkers int, requiredRevision int64) (trinoPoolValidation, error) {
	t.Helper()
	return validateTrinoPoolCandidate(
		context.Background(),
		c.server.Client(),
		c.server.URL,
		func() (string, string) { return "observer", "secret" },
		observedWorkers,
		requiredRevision,
	)
}

func TestValidateCandidateAcceptsAReadyCoordinator(t *testing.T) {
	coordinator := newFakeCoordinator(t)
	validation, err := coordinator.validate(t, 2, 42)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if validation.NodeID != "node-1" || validation.ProcessID != "process-1" || validation.CoordinatorID != "abcde" {
		t.Fatalf("validation identity = %+v", validation)
	}
	if validation.ReadyWorkers != 2 || validation.AppliedRevision != 42 {
		t.Fatalf("validation = %+v", validation)
	}
	if validation.AuthRevision == "" || validation.CertificateHash == "" {
		t.Fatal("the receipt carries no auth revision or certificate hash")
	}
	if len(validation.Checks) != 5 {
		t.Fatalf("checks = %v", validation.Checks)
	}
}

// A coordinator with a healthy HTTP endpoint is not a ready member. These are
// the cases where /v1/info alone would have admitted a cluster that cannot
// serve.
func TestValidateCandidateRejects(t *testing.T) {
	cases := map[string]func(*fakeCoordinator){
		"catalog sync disabled": func(c *fakeCoordinator) { c.sync["enabled"] = false },
		"not ready":             func(c *fakeCoordinator) { c.sync["ready"] = false; c.sync["notReadyReason"] = "still applying" },
		"failed catalogs": func(c *fakeCoordinator) {
			c.sync["failedCatalogs"] = 2
		},
		"no revision applied yet": func(c *fakeCoordinator) { c.sync["appliedRevision"] = nil },
		"revision behind the published one": func(c *fakeCoordinator) {
			c.sync["appliedRevision"] = 41
			c.sync["observedRevision"] = 41
		},
		"no process identity": func(c *fakeCoordinator) { c.sync["processId"] = "" },
		"identity changed mid-validation": func(c *fakeCoordinator) {
			c.sync["nodeId"] = "node-2"
		},
		// Zero registered workers with a healthy coordinator is the classic
		// "looks up, cannot plan a query" state.
		"no workers registered": func(c *fakeCoordinator) {
			c.nodes = [][]any{{"node-1", true, "active"}}
		},
		"a worker is not active": func(c *fakeCoordinator) {
			c.nodes = [][]any{{"node-1", true, "active"}, {"worker-1", false, "shutting_down"}}
		},
		"the inventory names another coordinator": func(c *fakeCoordinator) {
			c.nodes = [][]any{{"node-9", true, "active"}, {"worker-1", false, "active"}}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			coordinator := newFakeCoordinator(t)
			mutate(coordinator)
			if _, err := coordinator.validate(t, 2, 42); !errors.Is(err, errTrinoPoolCandidateNotReady) {
				t.Fatalf("error = %v, want errTrinoPoolCandidateNotReady", err)
			}
		})
	}
}

// The count the coordinator reports must agree with the pods Kubernetes is
// running. A mismatch means the cluster is still converging.
func TestValidateCandidateRequiresWorkerCountsToAgree(t *testing.T) {
	coordinator := newFakeCoordinator(t)
	if _, err := coordinator.validate(t, 4, 42); !errors.Is(err, errTrinoPoolCandidateNotReady) {
		t.Fatalf("error = %v, want a mismatch to be rejected", err)
	}
}

// The auth revision is reported as an opaque fingerprint: component revisions
// can carry credential-derived data, and this value is sent to the Gateway and
// shown to operators.
func TestAuthRevisionFingerprintIsOpaqueAndSensitive(t *testing.T) {
	base := authRevisionFingerprint([]componentRevision{
		{Kind: "password-authenticator", Name: "file", Revision: "9"},
		{Kind: "group-provider", Name: "file", Revision: "4"},
	})
	reordered := authRevisionFingerprint([]componentRevision{
		{Kind: "group-provider", Name: "file", Revision: "4"},
		{Kind: "password-authenticator", Name: "file", Revision: "9"},
	})
	if base != reordered {
		t.Fatal("the fingerprint depends on component ordering")
	}
	changed := authRevisionFingerprint([]componentRevision{
		{Kind: "password-authenticator", Name: "file", Revision: "10"},
		{Kind: "group-provider", Name: "file", Revision: "4"},
	})
	if changed == base {
		t.Fatal("a changed component revision produced the same fingerprint")
	}
	// A component that FAILED to load must never fingerprint the same as one
	// that loaded cleanly at the same revision.
	failed := authRevisionFingerprint([]componentRevision{
		{Kind: "password-authenticator", Name: "file", Revision: "9", Error: "cannot read password.db"},
		{Kind: "group-provider", Name: "file", Revision: "4"},
	})
	if failed == base {
		t.Fatal("a failed component fingerprinted as a healthy one")
	}
	if len(base) != 64 {
		t.Fatalf("fingerprint %q is not opaque", base)
	}
	if authRevisionFingerprint(nil) != "none" {
		t.Fatal("an absent revision set must be reported explicitly")
	}
}

// The certificate binds the receipt to the facts it asserts, so it cannot be
// replayed for a different process or a different revision.
func TestCertificateHashBindsTheObservedFacts(t *testing.T) {
	base := trinoPoolValidation{
		NodeID: "node-1", ProcessID: "process-1", CoordinatorID: "abcde",
		AppliedRevision: 42, AuthRevision: "auth", ReadyWorkers: 2,
		Checks: []string{trinoPoolCheckImage},
	}
	hash := certificateHash(base)
	for name, mutate := range map[string]func(*trinoPoolValidation){
		"process":  func(v *trinoPoolValidation) { v.ProcessID = "process-2" },
		"revision": func(v *trinoPoolValidation) { v.AppliedRevision = 43 },
		"auth":     func(v *trinoPoolValidation) { v.AuthRevision = "other" },
		"workers":  func(v *trinoPoolValidation) { v.ReadyWorkers = 3 },
	} {
		changed := base
		mutate(&changed)
		if certificateHash(changed) == hash {
			t.Errorf("changing the %s did not change the certificate hash", name)
		}
	}
}

// A component that cannot report what it loaded has acknowledged nothing.
// Trino's file password authenticator and group provider DO report; the OPA
// access control does not, so on a cluster with OPA the auth-revision check
// must be absent from the receipt and the silent component named. The Gateway
// records the check list verbatim, so claiming the check here would write a
// false acknowledgement into the operator's evidence.
func TestValidationDoesNotClaimAnUnacknowledgedAuthRevision(t *testing.T) {
	coordinator := newFakeCoordinator(t)
	coordinator.sync["securityRevisions"] = []any{
		map[string]any{"kind": "password-authenticator", "name": "file", "revision": "9"},
		map[string]any{"kind": "group-provider", "name": "file", "revision": "4"},
		// What the OPA access control actually reports today.
		map[string]any{
			"kind": "system-access-control", "name": "opa", "revision": nil,
			"error": "system-access-control 'opa' does not report the configuration it has loaded",
		},
	}

	validation, err := coordinator.validate(t, 2, 42)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	for _, check := range validation.Checks {
		if check == trinoPoolCheckAuthRevision {
			t.Fatal("the receipt claimed an auth-revision check no component acknowledged")
		}
	}
	if len(validation.Unacknowledged) != 1 || validation.Unacknowledged[0] != "system-access-control/opa" {
		t.Fatalf("unacknowledged = %v, want the OPA access control named", validation.Unacknowledged)
	}
}

// When every component does report, the check is claimed and nothing is left
// unacknowledged.
func TestValidationClaimsTheAuthRevisionWhenEveryComponentReports(t *testing.T) {
	coordinator := newFakeCoordinator(t)
	validation, err := coordinator.validate(t, 2, 42)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	claimed := false
	for _, check := range validation.Checks {
		if check == trinoPoolCheckAuthRevision {
			claimed = true
		}
	}
	if !claimed {
		t.Fatalf("checks = %v, want the auth revision claimed", validation.Checks)
	}
	if len(validation.Unacknowledged) != 0 {
		t.Fatalf("unacknowledged = %v", validation.Unacknowledged)
	}
}
