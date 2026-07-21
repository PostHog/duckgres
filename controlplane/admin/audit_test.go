//go:build kubernetes

package admin

import (
	"net/http"
	"testing"
)

// TestAuditActionFor pins the resource-specific Action derivation: operators,
// org-warehouse, and org-user mutations each get their own prefix, and anything
// else falls back to the generic config.<verb>. Both the resolved path (with the
// /api/v1 version prefix) and the bare route shape must resolve identically.
func TestAuditActionFor(t *testing.T) {
	cases := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{"operator upsert", http.MethodPost, "/api/v1/operators", "operators.create"},
		{"operator delete by email", http.MethodDelete, "/api/v1/operators/alice@example.com", "operators.delete"},
		{"operator delete no prefix", http.MethodDelete, "/operators/:email", "operators.delete"},
		{"org user delete", http.MethodDelete, "/api/v1/orgs/acme/users/bob", "user.delete"},
		{"org user update", http.MethodPut, "/api/v1/orgs/acme/users/bob", "user.update"},
		{"org user kill", http.MethodPost, "/api/v1/orgs/acme/users/bob/kill", "user.kill"},
		{"org user disable", http.MethodPost, "/api/v1/orgs/acme/users/bob/disable", "user.disable"},
		{"org user enable", http.MethodPost, "/api/v1/orgs/acme/users/bob/enable", "user.enable"},
		{"warehouse put", http.MethodPut, "/api/v1/orgs/acme/warehouse", "warehouse.update"},
		{"team create (top-level)", http.MethodPost, "/api/v1/teams", "team.create"},
		{"org team upsert", http.MethodPost, "/api/v1/orgs/acme/teams", "team.create"},
		{"org team update", http.MethodPut, "/api/v1/orgs/acme/teams/7", "team.update"},
		{"org team delete", http.MethodDelete, "/api/v1/orgs/acme/teams/7", "team.delete"},
		{"warehouse pinning patch", http.MethodPatch, "/api/v1/orgs/acme/warehouse/pinning", "warehouse.update"},
		{"org create", http.MethodPost, "/api/v1/orgs", "org.create"},
		{"org update", http.MethodPut, "/api/v1/orgs/acme", "org.update"},
		{"org delete", http.MethodDelete, "/api/v1/orgs/acme", "org.delete"},
		{"top-level users create", http.MethodPost, "/api/v1/users", "user.create"},
		{"user secret delete", http.MethodDelete, "/api/v1/orgs/acme/users/bob/secrets/mysecret", "secret.delete"},
		{"session cancel", http.MethodPost, "/api/v1/sessions/1234/cancel", "session.cancel"},
		{"session cancel by worker", http.MethodPost, "/api/v1/sessions/by-worker/w-1/cancel", "session.cancel"},
		{"activate admission offers", http.MethodPost, "/api/v1/admission/offers/activate", "admission_offers.activate"},
		{"unknown resource", http.MethodPost, "/api/v1/something", "config.create"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := auditActionFor(tc.method, tc.path); got != tc.want {
				t.Fatalf("auditActionFor(%q, %q) = %q, want %q", tc.method, tc.path, got, tc.want)
			}
		})
	}
}
