//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/posthog/duckgres/controlplane/provisioner"
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
				map[string]any{"kind": "password-authenticator", "name": "file", "revision": fakePasswordRevision},
				map[string]any{"kind": "group-provider", "name": "file", "revision": fakeGroupRevision},
				// The authorization projection the coordinator's OPA reports
				// deciding with. This is the value the controller compares
				// against what it currently serves.
				map[string]any{"kind": "system-access-control", "name": "opa", "revision": fakePolicyRevision},
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

// fakePolicyRevision stands in for the authorization projection this control
// plane serves. Its exact shape does not matter; that the two sides compare
// EQUAL does.
// The fingerprints of the authentication files this control plane projects,
// in the form Trino's file components publish.
const (
	fakePasswordRevision = "sha256:00000000000000000000000000000000000000000000000000000000000000aa"
	fakeGroupRevision    = "sha256:00000000000000000000000000000000000000000000000000000000000000bb"
)

const fakePolicyRevision = "v2.0000000000000000000000000000000000000000000000000000000000000001"

const fakeCoordinatorImage = "registry.example.invalid/trino@sha256:1111111111111111111111111111111111111111111111111111111111111111"

func (c *fakeCoordinator) validate(t *testing.T, observedWorkers int, requiredRevision int64) (trinoPoolValidation, error) {
	t.Helper()
	return c.validateWith(t, trinoPoolObservation{
		ReadyWorkers: observedWorkers, DesiredWorkers: observedWorkers,
		CoordinatorReady: true, CoordinatorPodUID: "pod-uid",
		CoordinatorImage: fakeCoordinatorImage, WorkerImage: fakeCoordinatorImage,
	}, trinoPoolExpectation{
		Image: fakeCoordinatorImage, CatalogRevision: requiredRevision,
		// The ACCEPTED projection, as the durable record names it.
		ProjectionDigest: provisioner.TrinoProjectionDigest(
			fakePolicyRevision, fakePasswordRevision, fakeGroupRevision),
		InternalHTTP: false,
	})
}

func (c *fakeCoordinator) validateWith(t *testing.T, observed trinoPoolObservation, expected trinoPoolExpectation) (trinoPoolValidation, error) {
	t.Helper()
	return validateTrinoPoolCandidate(
		context.Background(),
		c.server.Client(),
		c.server.URL,
		func() (string, string) { return "observer", "secret" },
		observed,
		expected,
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
		map[string]any{"kind": "password-authenticator", "name": "file", "revision": fakePasswordRevision},
		map[string]any{"kind": "group-provider", "name": "file", "revision": fakeGroupRevision},
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

// The image check must COMPARE something. Claiming it unconditionally wrote a
// false acknowledgement into the Gateway's durable evidence, which is exactly
// what the receipt exists to prevent.
func TestValidationRejectsAnImageThatIsNotTheRelease(t *testing.T) {
	coordinator := newFakeCoordinator(t)
	other := "registry.example.invalid/trino@sha256:2222222222222222222222222222222222222222222222222222222222222222"

	cases := map[string]trinoPoolObservation{
		"coordinator runs another image": {
			ReadyWorkers: 2, DesiredWorkers: 2, CoordinatorReady: true,
			CoordinatorImage: other, WorkerImage: fakeCoordinatorImage,
		},
		"workers run another image": {
			ReadyWorkers: 2, DesiredWorkers: 2, CoordinatorReady: true,
			CoordinatorImage: fakeCoordinatorImage, WorkerImage: other,
		},
		"no image observed at all": {
			ReadyWorkers: 2, DesiredWorkers: 2, CoordinatorReady: true,
		},
	}
	for name, observed := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := coordinator.validateWith(t, observed,
				trinoPoolExpectation{Image: fakeCoordinatorImage, CatalogRevision: 42})
			if !errors.Is(err, errTrinoPoolCandidateNotReady) {
				t.Fatalf("error = %v, want the image mismatch to be rejected", err)
			}
		})
	}
}

// A candidate at an older catalog revision is structurally healthy and still
// not certified: it would serve a catalog set that does not include the newest
// tenant. The required revision comes from the pool's durable publication
// revision, so this is the check that binds them.
func TestValidationRequiresThePublishedCatalogRevision(t *testing.T) {
	coordinator := newFakeCoordinator(t)
	coordinator.sync["appliedRevision"] = 41

	observed := trinoPoolObservation{
		ReadyWorkers: 2, DesiredWorkers: 2, CoordinatorReady: true,
		CoordinatorImage: fakeCoordinatorImage, WorkerImage: fakeCoordinatorImage,
	}
	if _, err := coordinator.validateWith(t, observed,
		trinoPoolExpectation{Image: fakeCoordinatorImage, CatalogRevision: 42}); !errors.Is(err, errTrinoPoolCandidateNotReady) {
		t.Fatalf("error = %v, want an older applied revision to be refused", err)
	}
	// At the published revision it passes.
	if _, err := coordinator.validateWith(t, observed,
		trinoPoolExpectation{Image: fakeCoordinatorImage, CatalogRevision: 41}); err != nil {
		t.Fatalf("validate at the published revision: %v", err)
	}
}

// Internal HTTP carries the credential, so the probe must declare the Gateway's
// terminated TLS rather than silently dropping the requirement. A coordinator
// with process-forwarded=true refuses an authenticated request without it.
func TestInternalHTTPProbeDeclaresForwardedHTTPS(t *testing.T) {
	var forwarded []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded = append(forwarded, r.Header.Get("X-Forwarded-Proto"))
		if _, _, ok := r.BasicAuth(); !ok {
			t.Error("the probe dropped its credential")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"nodeId": "node-1", "processId": "process-1", "coordinatorId": "abcde",
			"enabled": true, "ready": true, "observedRevision": 1, "appliedRevision": 1,
		})
	}))
	defer server.Close()

	processID, err := probeProcessIdentity(context.Background(), server.Client(), server.URL,
		func() (string, string) { return "observer", "secret" }, true)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if processID != "process-1" {
		t.Fatalf("processId = %q", processID)
	}
	if len(forwarded) == 0 || forwarded[0] != "https" {
		t.Fatalf("forwarded proto = %v, want https declared", forwarded)
	}
}

// A coordinator whose policy engine reports a DIFFERENT projection than the one
// this control plane serves is not certified, however healthy it looks. This is
// the case the check exists for: OPA answers every question perfectly while
// deciding with a bundle that predates the newest tenant, so "the component
// reported something" is not evidence of anything.
func TestValidationDoesNotClaimAStalePolicyRevision(t *testing.T) {
	coordinator := newFakeCoordinator(t)
	coordinator.sync["securityRevisions"] = []any{
		map[string]any{"kind": "password-authenticator", "name": "file", "revision": fakePasswordRevision},
		map[string]any{"kind": "group-provider", "name": "file", "revision": fakeGroupRevision},
		map[string]any{"kind": "system-access-control", "name": "opa", "revision": "v2.an-older-projection"},
	}

	validation, err := coordinator.validate(t, 2, 42)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	for _, check := range validation.Checks {
		if check == trinoPoolCheckAuthRevision {
			t.Fatal("a coordinator deciding with an older projection claimed the auth-revision check")
		}
	}
}

// Before this control plane has served a projection it cannot say what a
// coordinator ought to be deciding with, so it claims nothing - and the Gateway
// refuses the admission, which is the fail-closed direction.
func TestValidationDoesNotClaimAnUnknownPolicyRevision(t *testing.T) {
	coordinator := newFakeCoordinator(t)
	validation, err := coordinator.validateWith(t, trinoPoolObservation{
		ReadyWorkers: 2, DesiredWorkers: 2, CoordinatorReady: true, CoordinatorPodUID: "pod-uid",
		CoordinatorImage: fakeCoordinatorImage, WorkerImage: fakeCoordinatorImage,
	}, trinoPoolExpectation{Image: fakeCoordinatorImage, CatalogRevision: 42})
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	for _, check := range validation.Checks {
		if check == trinoPoolCheckAuthRevision {
			t.Fatal("the auth-revision check was claimed with no served projection to compare against")
		}
	}
}

// The OPA bundle and the authentication Secret reach a coordinator by different
// paths and settle at different times. A candidate whose authorization data is
// current but whose password file predates the tenant about to be admitted
// looks healthy and then rejects that tenant's very first request, so each
// projected component is compared on its own.
func TestValidationRequiresEveryProjectedComponentToBeCurrent(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		revisions []any
	}{
		{
			name: "current policy, stale password file",
			revisions: []any{
				map[string]any{"kind": "password-authenticator", "name": "file", "revision": "sha256:older"},
				map[string]any{"kind": "group-provider", "name": "file", "revision": fakeGroupRevision},
				map[string]any{"kind": "system-access-control", "name": "opa", "revision": fakePolicyRevision},
			},
		},
		{
			name: "current password file, stale groups",
			revisions: []any{
				map[string]any{"kind": "password-authenticator", "name": "file", "revision": fakePasswordRevision},
				map[string]any{"kind": "group-provider", "name": "file", "revision": "sha256:older"},
				map[string]any{"kind": "system-access-control", "name": "opa", "revision": fakePolicyRevision},
			},
		},
		{
			name: "no password authenticator at all",
			revisions: []any{
				map[string]any{"kind": "group-provider", "name": "file", "revision": fakeGroupRevision},
				map[string]any{"kind": "system-access-control", "name": "opa", "revision": fakePolicyRevision},
			},
		},
		{
			// A second authenticator reads a file this control plane does not
			// write, so it can authenticate principals outside the projection.
			// Admitting on the strength of the one component that agrees would
			// pull that file inside the pool's trust boundary silently.
			name: "a second password authenticator this control plane does not write",
			revisions: []any{
				map[string]any{"kind": "password-authenticator", "name": "file", "revision": fakePasswordRevision},
				map[string]any{"kind": "password-authenticator", "name": "file-2", "revision": "sha256:someone-elses-file"},
				map[string]any{"kind": "group-provider", "name": "file", "revision": fakeGroupRevision},
				map[string]any{"kind": "system-access-control", "name": "opa", "revision": fakePolicyRevision},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			coordinator := newFakeCoordinator(t)
			coordinator.sync["securityRevisions"] = testCase.revisions

			validation, err := coordinator.validate(t, 2, 42)
			if err != nil {
				t.Fatalf("validate: %v", err)
			}
			for _, check := range validation.Checks {
				if check == trinoPoolCheckAuthRevision {
					t.Fatal("the auth-revision check was claimed while a projected component was not current")
				}
			}
		})
	}
}

// With every projected component reporting exactly what is being served, the
// check is claimed.
func TestValidationClaimsTheAuthRevisionWhenEveryProjectionMatches(t *testing.T) {
	coordinator := newFakeCoordinator(t)
	coordinator.sync["securityRevisions"] = []any{
		map[string]any{"kind": "password-authenticator", "name": "file", "revision": fakePasswordRevision},
		map[string]any{"kind": "group-provider", "name": "file", "revision": fakeGroupRevision},
		map[string]any{"kind": "system-access-control", "name": "opa", "revision": fakePolicyRevision},
	}

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
}
