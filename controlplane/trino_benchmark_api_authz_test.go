//go:build kubernetes

package controlplane

import (
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/posthog/duckgres/controlplane/admin"
)

// The default-build API tests use a stub admin gate (controlplane/admin is
// kubernetes-tagged). These cases pin the REAL gate the control plane mounts in
// multitenant.go: internal secret ⇒ admin, SSO viewer ⇒ 403, anonymous ⇒ 401.

func newTrinoBenchmarkAuthzEngine(t *testing.T) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	tokens := admin.NewTokenSet(trinoTestInternalSecret, nil)
	api := engine.Group("/api/v1",
		admin.AuthMiddleware(tokens, func(string) admin.Role { return admin.RoleViewer }),
		admin.RoleGate(),
	)
	registerTrinoBenchmarkAPI(api, &fakeTrinoBenchmarkLifecycle{}, admin.RequireAdmin())
	return engine
}

func TestTrinoBenchmarkAPIAcceptsInternalSecretIdentity(t *testing.T) {
	engine := newTrinoBenchmarkAuthzEngine(t)

	rec := trinoBenchmarkRequest(t, engine, http.MethodGet, "/api/v1/trino-benchmarks/status/trino-bench-bench-org", "", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("internal-secret status = %d body = %s, want 200", rec.Code, rec.Body.String())
	}
}

func TestTrinoBenchmarkAPIRejectsAnonymousAndViewerIdentities(t *testing.T) {
	engine := newTrinoBenchmarkAuthzEngine(t)

	anonymous := trinoBenchmarkRequest(t, engine, http.MethodPost, "/api/v1/trino-benchmarks/orgs/bench-org/provision", `{"workers":4}`, false)
	if anonymous.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous provision = %d, want 401", anonymous.Code)
	}

	// An SSO viewer authenticates but must not reach any Trino benchmark route,
	// including the GET (RequireAdmin, not just RoleGate's mutation gate).
	viewerEngine := gin.New()
	viewerAPI := viewerEngine.Group("/api/v1",
		func(c *gin.Context) {
			c.Set("duckgres_identity", &admin.Identity{Email: "viewer@posthog.com", Role: admin.RoleViewer, Source: "sso"})
			c.Next()
		},
		admin.RoleGate(),
	)
	registerTrinoBenchmarkAPI(viewerAPI, &fakeTrinoBenchmarkLifecycle{}, admin.RequireAdmin())

	viewer := trinoBenchmarkRequest(t, viewerEngine, http.MethodGet, "/api/v1/trino-benchmarks/status/trino-bench-bench-org", "", false)
	if viewer.Code != http.StatusForbidden {
		t.Fatalf("viewer status = %d, want 403", viewer.Code)
	}
}
