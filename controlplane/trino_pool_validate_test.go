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
