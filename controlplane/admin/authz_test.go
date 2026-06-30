//go:build kubernetes

package admin

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// mkOIDC builds a fake ALB x-amzn-oidc-data JWT (header.payload.sig) with the
// given claims. Only the payload segment is read by the decoder.
func mkOIDC(claims map[string]any) string {
	payload, _ := json.Marshal(claims)
	seg := base64.RawURLEncoding.EncodeToString(payload)
	return "eyJ0eXAiOiJKV1QifQ." + seg + ".sig"
}

// roleByEmail builds a fake RoleResolver from an email→role map. Unknown
// emails resolve to viewer (the production fail-closed default).
func roleByEmail(admins ...string) RoleResolver {
	set := map[string]bool{}
	for _, e := range admins {
		set[e] = true
	}
	return func(email string) Role {
		if set[email] {
			return RoleAdmin
		}
		return RoleViewer
	}
}

func TestAuthMiddlewareInternalSecretIsAdmin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/x", AuthMiddleware(NewTokenSet("secret", nil), roleByEmail()), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"role": IdentityFromContext(c).Role})
	})
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("X-Duckgres-Internal-Secret", "secret")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != `{"role":"admin"}` {
		t.Fatalf("body = %s, want admin", got)
	}
}

// The SSO email is resolved to a role by the injected RoleResolver (in
// production, the operators-table lookup). Unknown emails fail closed to viewer.
func TestAuthMiddlewareSSORoleMapping(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resolve := roleByEmail("a@posthog.com")
	cases := []struct {
		name  string
		email string
		want  Role
	}{
		{"known admin email", "a@posthog.com", RoleAdmin},
		{"known viewer email", "v@posthog.com", RoleViewer},
		{"unknown email defaults to viewer", "nobody@posthog.com", RoleViewer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			r.GET("/x", AuthMiddleware(NewTokenSet("secret", nil), resolve), func(c *gin.Context) {
				c.JSON(http.StatusOK, gin.H{"role": IdentityFromContext(c).Role})
			})
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			req.Header.Set(albOIDCDataHeader, mkOIDC(map[string]any{"email": tc.email}))
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			want := `{"role":"` + string(tc.want) + `"}`
			if got := rec.Body.String(); got != want {
				t.Fatalf("body = %s, want %s", got, want)
			}
		})
	}
}

// Domain hardening: a non-@posthog.com email (even if it would resolve to a
// role) is treated as unauthenticated, and an explicit email_verified=false is
// rejected too.
func TestAuthMiddlewareDomainHardening(t *testing.T) {
	gin.SetMode(gin.TestMode)
	// Resolver returns admin for everything to prove rejection is upstream of it.
	resolve := func(string) Role { return RoleAdmin }
	for _, tc := range []struct {
		name   string
		claims map[string]any
	}{
		{"foreign domain rejected", map[string]any{"email": "attacker@evil.com"}},
		{"unverified email rejected", map[string]any{"email": "u@posthog.com", "email_verified": false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			r.GET("/x", AuthMiddleware(NewTokenSet("secret", nil), resolve), func(c *gin.Context) {
				c.JSON(http.StatusOK, gin.H{"ok": true})
			})
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			req.Header.Set(albOIDCDataHeader, mkOIDC(tc.claims))
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
		})
	}
}

// RequireAdmin gates a route regardless of method (used by the audit read).
func TestRequireAdmin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resolve := roleByEmail("a@posthog.com")
	r := gin.New()
	r.GET("/audit", AuthMiddleware(NewTokenSet("secret", nil), resolve), RequireAdmin(), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	viewer := mkOIDC(map[string]any{"email": "v@posthog.com"})
	admin := mkOIDC(map[string]any{"email": "a@posthog.com"})
	for _, tc := range []struct {
		name, oidc string
		want       int
	}{
		{"viewer blocked", viewer, http.StatusForbidden},
		{"admin allowed", admin, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/audit", nil)
			req.Header.Set(albOIDCDataHeader, tc.oidc)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

func TestAuthMiddlewareRejectsUnauthenticated(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/x", AuthMiddleware(NewTokenSet("secret", nil), roleByEmail()), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

// RoleGate: viewers can GET but not mutate; the audit log GET is admin-only.
func TestRoleGate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resolve := roleByEmail("a@posthog.com")
	build := func() *gin.Engine {
		r := gin.New()
		grp := r.Group("/api/v1",
			AuthMiddleware(NewTokenSet("secret", nil), resolve),
			RoleGate("/api/v1/audit"),
		)
		grp.GET("/orgs", func(c *gin.Context) { c.Status(http.StatusOK) })
		grp.POST("/orgs", func(c *gin.Context) { c.Status(http.StatusOK) })
		grp.GET("/audit", func(c *gin.Context) { c.Status(http.StatusOK) })
		return r
	}
	viewer := mkOIDC(map[string]any{"email": "v@posthog.com"})
	admin := mkOIDC(map[string]any{"email": "a@posthog.com"})

	cases := []struct {
		name, method, path, oidc string
		want                     int
	}{
		{"viewer reads orgs", http.MethodGet, "/api/v1/orgs", viewer, http.StatusOK},
		{"viewer blocked from POST", http.MethodPost, "/api/v1/orgs", viewer, http.StatusForbidden},
		{"viewer blocked from audit", http.MethodGet, "/api/v1/audit", viewer, http.StatusForbidden},
		{"admin can POST", http.MethodPost, "/api/v1/orgs", admin, http.StatusOK},
		{"admin can read audit", http.MethodGet, "/api/v1/audit", admin, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := build()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Header.Set(albOIDCDataHeader, tc.oidc)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

func TestIsReadOnlySQL(t *testing.T) {
	cases := []struct {
		sql  string
		want bool
	}{
		{"SELECT 1", true},
		{"  select * from t ", true},
		{"EXPLAIN SELECT 1", true},
		// EXPLAIN ANALYZE executes the inner statement, so it is a write when
		// the inner statement mutates (and conservatively flagged even for SELECT).
		{"EXPLAIN ANALYZE INSERT INTO t VALUES (1)", false},
		{"EXPLAIN (ANALYZE, VERBOSE) DELETE FROM t", false},
		{"explain analyze update t set x=1", false},
		{"EXPLAIN ANALYZE SELECT 1", false},
		{"SHOW TABLES", true},
		{"PRAGMA database_list", true},
		{"VALUES (1)", true},
		{"INSERT INTO t VALUES (1)", false},
		{"UPDATE t SET x=1", false},
		{"DELETE FROM t", false},
		{"DROP TABLE t", false},
		{"CREATE SECRET s (...)", false},
		// WITH is treated as a potential writable CTE.
		{"WITH x AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM x", false},
		// Multi-statement batch with a read-only prefix must be rejected.
		{"SELECT 1; DROP TABLE t", false},
	}
	for _, tc := range cases {
		if got := isReadOnlySQL(tc.sql); got != tc.want {
			t.Errorf("isReadOnlySQL(%q) = %v, want %v", tc.sql, got, tc.want)
		}
	}
}
