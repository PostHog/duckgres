package provisioning

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// newServiceCredentialRouter mounts the provisioning API with an explicit
// managed tenant ingress suffix so tests can assert the exact connect.host the
// handler derives from it (orgID + suffix).
func newServiceCredentialRouter(store Store, ingressSuffix string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	tenantStore, _ := store.(TenantStore)
	RegisterAPIWithIngressSuffix(r.Group("/api/v1"), store, tenantStore, "", nil, ingressSuffix)
	return r
}

// assertConnectBlock checks the always-present connect block's exact shape and
// values against the org's canonical ingress hostname.
func assertConnectBlock(t *testing.T, body map[string]any, wantHost string) {
	t.Helper()
	connect, ok := body["connect"].(map[string]any)
	if !ok {
		t.Fatalf("connect block must be an object, got top-level body: %v", body)
	}
	if len(connect) != 4 {
		t.Fatalf("connect must have exactly 4 keys, got %v", connect)
	}
	if got := connect["host"]; got != wantHost {
		t.Fatalf("connect.host = %v, want %v (the org's canonical ingress hostname)", got, wantHost)
	}
	if got := connect["port"]; got != float64(5432) {
		t.Fatalf("connect.port = %v, want 5432", got)
	}
	if got := connect["database"]; got != "ducklake" {
		t.Fatalf("connect.database = %v, want \"ducklake\"", got)
	}
	if got := connect["sslmode"]; got != "require" {
		t.Fatalf("connect.sslmode = %v, want \"require\"", got)
	}
}

func TestIssueServiceCredentialFreshMintShape(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/service-credentials",
		`{"principal": "dagster:events-backfill"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	credID, _ := body["credential_id"].(string)
	if !strings.HasPrefix(credID, "svc_") {
		t.Fatalf("credential_id = %q, want svc_-prefixed", credID)
	}
	if pw, _ := body["credential_secret"].(string); pw == "" {
		t.Fatal("credential_secret must be non-empty on a fresh mint")
	}
	if body["expires_at"] == nil {
		t.Fatal("expires_at must be present")
	}
	// Exactly the contract keys — no username/password echo from the old shape.
	for k := range body {
		switch k {
		case "credential_id", "credential_secret", "expires_at", "connect":
		default:
			t.Fatalf("unexpected key %q in mint response: %v", k, body)
		}
	}
	// newTestRouter wires no ingress suffix ⇒ the handler falls back to the
	// default managed ingress suffix.
	assertConnectBlock(t, body, "acme"+DefaultManagedIngressSuffix)
}

func TestIssueServiceCredentialRejectsBadInput(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newTestRouter(store)

	cases := []struct {
		name, body string
	}{
		{"missing principal", `{}`},
		{"empty principal", `{"principal":""}`},
		{"malformed json", `{"principal":`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/service-credentials", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestIssueServiceCredentialGhostOrg404(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/ghost/service-credentials",
		`{"principal": "dagster:x"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
	}
	if len(store.mintCreds) != 0 {
		t.Fatalf("mint must not be attempted against a ghost org, got %d calls", len(store.mintCreds))
	}
}

func TestIssueServiceCredentialThreadsPrincipalAndTTL(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/service-credentials",
		`{"principal": "dagster:events-backfill", "ttl_seconds": 900}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if len(store.mintCreds) != 1 {
		t.Fatalf("mintCreds len = %d, want 1", len(store.mintCreds))
	}
	got := store.mintCreds[0]
	if got.Principal != "dagster:events-backfill" || got.TTLSeconds != 900 {
		t.Fatalf("mintCreds[0] = %+v, want principal dagster:events-backfill / ttl 900", got)
	}
	if store.reloadSnapshotN != 1 {
		t.Fatalf("ReloadSnapshot called %d times, want exactly 1 (fresh credential must auth without waiting a poll)", store.reloadSnapshotN)
	}
}

func TestIssueServiceCredentialClampsTTL(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newTestRouter(store)

	// Tiny ttl_seconds must be raised to the floor, not rejected — a caller
	// never has to guess server policy to get a usable credential.
	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/service-credentials",
		`{"principal": "d", "ttl_seconds": 1}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if got := store.mintCreds[0].TTLSeconds; got != int(minCredentialTTL/time.Second) {
		t.Fatalf("clamped ttl_seconds = %d, want %d floor", got, int(minCredentialTTL/time.Second))
	}

	// Huge ttl_seconds comes down to the ceiling.
	store.mintCreds = nil
	rec = doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/service-credentials",
		`{"principal": "d", "ttl_seconds": 999999}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if got := store.mintCreds[0].TTLSeconds; got != int(maxCredentialTTL/time.Second) {
		t.Fatalf("clamped ttl_seconds = %d, want %d ceiling", got, int(maxCredentialTTL/time.Second))
	}

	// Absent ttl_seconds → the default.
	store.mintCreds = nil
	rec = doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/service-credentials",
		`{"principal": "d"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if got := store.mintCreds[0].TTLSeconds; got != int(defaultCredentialTTL/time.Second) {
		t.Fatalf("default ttl_seconds = %d, want %d", got, int(defaultCredentialTTL/time.Second))
	}
}

func TestIssueServiceCredentialLegacyForceRotateFieldIsIgnored(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newTestRouter(store)

	// An older caller may still send force_rotate after the new server deploys.
	// Gin ignores the removed field; the request remains a normal fresh mint.
	// This is intentionally one-way compatibility: callers may drop their old
	// reuse fallback only after every Duckgres server has always-create mint.
	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/service-credentials",
		`{"principal": "d", "force_rotate": true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if len(store.mintCreds) != 1 || store.mintCreds[0].Principal != "d" {
		t.Fatalf("legacy request did not produce a normal mint: %+v", store.mintCreds)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	assertConnectBlock(t, body, "acme"+DefaultManagedIngressSuffix)
	if secret, _ := body["credential_secret"].(string); secret == "" {
		t.Fatalf("legacy force_rotate request must still return the fresh secret: %v", body)
	}
}

// TestIssueServiceCredentialConnectHostUsesWiredSuffix locks the wiring: the
// connect block's host is orgID + the ingress suffix the CP was wired with —
// the same DNS suffix the pgwire TLS server_name pins for managed tenants —
// never an IP and never a caller-network-resolved address, so both a
// public-ingress client and a PrivateLink client get the one logical name.
func TestIssueServiceCredentialConnectHostUsesWiredSuffix(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newServiceCredentialRouter(store, ".dw.eu.postwh.com")

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/service-credentials",
		`{"principal": "dagster:events-backfill"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	assertConnectBlock(t, body, "acme.dw.eu.postwh.com")
}

// The team-scoped route is GONE: service credentials are not project-scoped,
// so the old path must not resolve (gin has no such route).
func TestTeamScopedServiceCredentialRouteRemoved(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 42, SchemaName: "team_42", Enabled: true})
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams/42/service-credentials",
		`{"team_id": 42, "principal": "dagster:x"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (route removed): %s", rec.Code, rec.Body.String())
	}
	if len(store.mintCreds) != 0 {
		t.Fatalf("no mint call expected on the removed route, got %d", len(store.mintCreds))
	}
}

func TestRefreshServiceCredentialAlwaysReturnsSecret(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/service-credentials/refresh",
		`{"credential_id": "svc_0123456789abcdef01234567", "ttl_seconds": 300}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if len(store.refreshCreds) != 1 {
		t.Fatalf("refreshCreds len = %d, want 1", len(store.refreshCreds))
	}
	got := store.refreshCreds[0]
	if got.CredentialID != "svc_0123456789abcdef01234567" || got.TTLSeconds != 300 {
		t.Fatalf("refreshCreds[0] = %+v, want the named credential at ttl 300", got)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["credential_id"] != "svc_0123456789abcdef01234567" {
		t.Fatalf("credential_id = %v", body["credential_id"])
	}
	if pw, _ := body["credential_secret"].(string); pw == "" {
		t.Fatal("refresh ALWAYS returns the freshly rotated credential_secret")
	}
	if body["expires_at"] == nil {
		t.Fatal("expires_at must be present")
	}
	assertConnectBlock(t, body, "acme"+DefaultManagedIngressSuffix)
	if store.reloadSnapshotN != 1 {
		t.Fatalf("ReloadSnapshot called %d times, want exactly 1", store.reloadSnapshotN)
	}
}

func TestRefreshServiceCredentialRejectsBadInput(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/service-credentials/refresh",
		`{}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (credential_id required): %s", rec.Code, rec.Body.String())
	}
}

func TestRefreshServiceCredentialGhostOrg404(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/ghost/service-credentials/refresh",
		`{"credential_id": "svc_0123456789abcdef01234567"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
	}
	if len(store.refreshCreds) != 0 {
		t.Fatalf("refresh must not be attempted against a ghost org, got %d calls", len(store.refreshCreds))
	}
}

func TestRefreshServiceCredentialUnknownCredential404(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	store.refreshCredsErr = configstore.ErrServiceCredentialNotFound
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/service-credentials/refresh",
		`{"credential_id": "svc_eeeeeeeeeeeeeeeeeeeeeeee"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
	}
}

func TestRefreshServiceCredentialRevoked410(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	store.refreshCredsErr = configstore.ErrServiceCredentialRevoked
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/service-credentials/refresh",
		`{"credential_id": "svc_0123456789abcdef01234567"}`)
	if rec.Code != http.StatusGone {
		t.Fatalf("status = %d, want 410 (revoked is terminal): %s", rec.Code, rec.Body.String())
	}
}
