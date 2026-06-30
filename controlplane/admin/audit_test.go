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
		{"warehouse put", http.MethodPut, "/api/v1/orgs/acme/warehouse", "warehouse.update"},
		{"warehouse pinning patch", http.MethodPatch, "/api/v1/orgs/acme/warehouse/pinning", "warehouse.update"},
		{"org create", http.MethodPost, "/api/v1/orgs", "config.create"},
		{"org update", http.MethodPut, "/api/v1/orgs/acme", "config.update"},
		{"top-level users create", http.MethodPost, "/api/v1/users", "config.create"},
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
