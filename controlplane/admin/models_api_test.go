//go:build kubernetes

package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// TestModelDescriptorsRedaction is the no-DB guard for the load-bearing
// security invariant of the models explorer: secret-bearing columns must never
// appear as browsable columns. Because the listing scans into the typed model
// and marshals via json tags, a field tagged json:"-" is dropped — this test
// pins that the sensitive fields stay tagged and that the column derivation
// (jsonFieldOrder) honors it. If someone retags Password/Ciphertext/S3SecretKey
// to be serialized, this fails before the secret can leak through the UI.
func TestModelDescriptorsRedaction(t *testing.T) {
	descs := modelDescriptors()

	// Registry must cover exactly the persisted models we expect.
	wantKeys := map[string]bool{
		"orgs": true, "org-users": true, "org-user-secrets": true, "managed-warehouses": true,
		"cp-instances": true, "worker-records": true, "flight-session-records": true,
		"org-connection-queue": true, "org-connection-leases": true,
		"operators": true,
	}
	seen := map[string]bool{}
	for _, d := range descs {
		if seen[d.Key] {
			t.Fatalf("duplicate descriptor key %q", d.Key)
		}
		seen[d.Key] = true
		if d.Table == "" {
			t.Errorf("descriptor %q has empty Table (missing TableName?)", d.Key)
		}
		if d.newSlice == nil || d.elem == nil {
			t.Errorf("descriptor %q missing newSlice/elem", d.Key)
		}
	}
	for k := range wantKeys {
		if !seen[k] {
			t.Errorf("registry missing expected model %q", k)
		}
	}
	for k := range seen {
		if !wantKeys[k] {
			t.Errorf("registry has unexpected model %q (update test if intentional)", k)
		}
	}

	// Sensitive columns must not be derivable for the UI.
	mustHide := map[string][]string{
		"org-users":        {"password"},
		"org-user-secrets": {"ciphertext"},
	}
	for _, d := range descs {
		cols := jsonFieldOrder(d.elem)
		lower := map[string]bool{}
		for _, c := range cols {
			lower[strings.ToLower(c)] = true
		}
		for _, bad := range mustHide[d.Key] {
			if lower[strings.ToLower(bad)] {
				t.Errorf("model %q exposes column %q — must be json:\"-\"", d.Key, bad)
			}
		}
	}
}

// TestModelsAPIPostgres exercises the real query path against the config store:
// sidebar counts, a config-schema listing, a runtime-schema listing (schema
// qualification), and the end-to-end redaction guarantee — a seeded password
// hash must never appear in the API response bytes.
func TestModelsAPIPostgres(t *testing.T) {
	store := newPostgresConfigStore(t)

	const secretHash = "$2a$10$DEADBEEFdeadbeefDEADBEEFdeadbeefDEADBEEFdeadbeefDEADBE"
	if err := store.DB().Create(&configstore.Org{Name: "acme", DatabaseName: "acme_db"}).Error; err != nil {
		t.Fatalf("seed org: %v", err)
	}
	if err := store.DB().Create(&configstore.OrgUser{OrgID: "acme", Username: "reader", Password: secretHash}).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerModelsAPI(r.Group("/api/v1"), store)

	get := func(path string) (*httptest.ResponseRecorder, map[string]json.RawMessage) {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		var body map[string]json.RawMessage
		if rec.Code == http.StatusOK {
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode %s: %v (%s)", path, err, rec.Body.String())
			}
		}
		return rec, body
	}

	// Sidebar listing.
	rec, body := get("/api/v1/models")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /models = %d", rec.Code)
	}
	var summaries []modelSummary
	if err := json.Unmarshal(body["models"], &summaries); err != nil {
		t.Fatalf("decode summaries: %v", err)
	}
	gotOrgUsers := false
	for _, s := range summaries {
		if s.Key == "org-users" {
			gotOrgUsers = true
			if s.Count != 1 {
				t.Errorf("org-users count = %d, want 1", s.Count)
			}
		}
	}
	if !gotOrgUsers {
		t.Fatalf("sidebar missing org-users")
	}

	// Config-schema listing must not leak the password hash.
	rec, _ = get("/api/v1/models/org-users")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /models/org-users = %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), secretHash) {
		t.Fatalf("org-users listing leaked password hash:\n%s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "reader") {
		t.Fatalf("org-users listing missing seeded user")
	}

	// Runtime-schema listing (schema qualification) must succeed even when empty.
	rec, _ = get("/api/v1/models/worker-records")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /models/worker-records = %d (runtime schema qualification broken)", rec.Code)
	}

	// Unknown model → 404.
	rec, _ = get("/api/v1/models/nope")
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET /models/nope = %d, want 404", rec.Code)
	}
}
