package provisioning

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// pinServiceCredentialUsername locks the CP-issued login name to the same
// derivation the admin console uses for a team's read/write project login
// (controlplane/admin/api.go projectUserUsername). If the admin side ever
// changes its name mint, this test failing is the tripwire that says "the
// service-credential path is now handing out credentials for a DIFFERENT
// login than the console manages".
func TestIssueServiceCredentialUsernameMatchesAdminProjectUser(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 42, SchemaName: "team_42", Enabled: true})
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams/42/service-credentials",
		`{"team_id": 42, "principal": "dagster:events-backfill", "force_rotate": true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := body["username"], "posthog_team_42_rw"; got != want {
		t.Fatalf("username = %v, want %v", got, want)
	}
	if pw, _ := body["password"].(string); pw == "" {
		t.Fatal("password must be non-empty on rotate")
	}
	if body["expires_at"] == nil {
		t.Fatal("expires_at must be present")
	}
}

func TestIssueServiceCredentialRejectsBadInput(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newTestRouter(store)

	cases := []struct {
		name, body string
	}{
		{"missing team_id", `{"principal":"dagster:x"}`},
		{"negative team_id", `{"team_id":-1,"principal":"dagster:x"}`},
		{"missing principal", `{"team_id":42}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams/42/service-credentials", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestIssueServiceCredentialRejectsDisabledTeam(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 42, SchemaName: "team_42", Enabled: false})
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams/42/service-credentials",
		`{"team_id": 42, "principal": "dagster:events-backfill", "force_rotate": true}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body.String())
	}
}

func TestIssueServiceCredentialThreadsTTLAndForceRotate(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 42, SchemaName: "team_42", Enabled: true})
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams/42/service-credentials",
		`{"team_id": 42, "principal": "dagster:events-backfill", "ttl_seconds": 900, "force_rotate": true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if len(store.issueCreds) != 1 {
		t.Fatalf("issueCreds len = %d, want 1", len(store.issueCreds))
	}
	got := store.issueCreds[0]
	if got.TeamID != 42 || got.Principal != "dagster:events-backfill" || got.TTLSeconds != 900 || !got.ForceRotate {
		t.Fatalf("issueCreds[0] = %+v, want team 42 / principal dagster:events-backfill / ttl 900 / force_rotate true", got)
	}
	if store.reloadSnapshotN != 1 {
		t.Fatalf("ReloadSnapshot called %d times, want exactly 1 (fresh credential must auth without waiting a poll)", store.reloadSnapshotN)
	}
}

func TestIssueServiceCredentialClampsTTL(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 42, SchemaName: "team_42", Enabled: true})
	router := newTestRouter(store)

	// Tiny ttl_seconds must be raised to the floor, not rejected — a caller
	// never has to guess server policy to get a usable credential.
	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams/42/service-credentials",
		`{"team_id": 42, "principal": "d", "ttl_seconds": 1, "force_rotate": true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if got := store.issueCreds[0].TTLSeconds; got != int(minCredentialTTL/time.Second) {
		t.Fatalf("clamped ttl_seconds = %d, want %d floor", got, int(minCredentialTTL/time.Second))
	}

	// Absent ttl_seconds → the default.
	store.issueCreds = nil
	rec = doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams/42/service-credentials",
		`{"team_id": 42, "principal": "d", "force_rotate": true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if got := store.issueCreds[0].TTLSeconds; got != int(defaultCredentialTTL/time.Second) {
		t.Fatalf("default ttl_seconds = %d, want %d", got, int(defaultCredentialTTL/time.Second))
	}
}

func TestIssueServiceCredentialReuseOmitsPasswordWhenGrantStillValid(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 42, SchemaName: "team_42", Enabled: true})
	router := newTestRouter(store)

	// The store reports a still-valid live grant (Rotated=false ⇒ the CP did
	// not touch the hash) ⇒ the handler surfaces an EMPTY password: the
	// caller already holds it (or must force_rotate). The whole point of the
	// reuse path is NOT to leak plaintext for a credential the CP didn't
	// just mint.
	store.issueCredsIssue = &configstore.ServiceCredentialIssue{
		Rotated:   false,
		Username:  "posthog_team_42_rw",
		Plaintext: "",
		ExpiresAt: time.Now().UTC().Add(10 * time.Minute),
	}
	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams/42/service-credentials",
		`{"team_id": 42, "principal": "d"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); strings.Contains(body, `"password"`) {
		t.Fatalf("reuse path must not echo a password, got: %s", body)
	}
}
