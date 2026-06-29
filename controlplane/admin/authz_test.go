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

func TestAuthMiddlewareInternalSecretIsAdmin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/x", AuthMiddleware(NewTokenSet("secret", nil), SSOConfig{AdminGroup: "admins"}), func(c *gin.Context) {
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

func TestAuthMiddlewareSSORoleMapping(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		name    string
		groups  []string
		adminGr string
		want    Role
	}{
		{"member of admin group", []string{"eng", "admins"}, "admins", RoleAdmin},
		{"not a member", []string{"eng"}, "admins", RoleViewer},
		// Fail closed: no admin group configured (and no allow-all) => viewer.
		{"no admin group configured = viewer", []string{"eng"}, "", RoleViewer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			r.GET("/x", AuthMiddleware(NewTokenSet("secret", nil), SSOConfig{AdminGroup: tc.adminGr}), func(c *gin.Context) {
				c.JSON(http.StatusOK, gin.H{"role": IdentityFromContext(c).Role})
			})
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			req.Header.Set(albOIDCDataHeader, mkOIDC(map[string]any{
				"email":          "u@posthog.com",
				"cognito:groups": tc.groups,
			}))
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

// Without an admin group, SSO users must default to viewer (fail closed) —
// unless AllowAllSSO is explicitly opted in (dev convenience).
func TestAuthMiddlewareFailClosedVsAllowAll(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name     string
		allowAll bool
		want     Role
	}{
		{"fail closed by default", false, RoleViewer},
		{"allow-all opt-in", true, RoleAdmin},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			r.GET("/x", AuthMiddleware(NewTokenSet("secret", nil), SSOConfig{AllowAllSSO: tc.allowAll}), func(c *gin.Context) {
				c.JSON(http.StatusOK, gin.H{"role": IdentityFromContext(c).Role})
			})
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			req.Header.Set(albOIDCDataHeader, mkOIDC(map[string]any{"email": "u@posthog.com"}))
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if want := `{"role":"` + string(tc.want) + `"}`; rec.Body.String() != want {
				t.Fatalf("body = %s, want %s", rec.Body.String(), want)
			}
		})
	}
}

// RequireAdmin gates a route regardless of method (used by the audit read).
func TestRequireAdmin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/audit", AuthMiddleware(NewTokenSet("secret", nil), SSOConfig{AdminGroup: "admins"}), RequireAdmin(), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	viewer := mkOIDC(map[string]any{"email": "v@posthog.com", "cognito:groups": []any{"eng"}})
	admin := mkOIDC(map[string]any{"email": "a@posthog.com", "cognito:groups": []any{"admins"}})
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
	r.GET("/x", AuthMiddleware(NewTokenSet("secret", nil), SSOConfig{}), func(c *gin.Context) {
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
	build := func() *gin.Engine {
		r := gin.New()
		grp := r.Group("/api/v1",
			AuthMiddleware(NewTokenSet("secret", nil), SSOConfig{AdminGroup: "admins"}),
			RoleGate("/api/v1/audit"),
		)
		grp.GET("/orgs", func(c *gin.Context) { c.Status(http.StatusOK) })
		grp.POST("/orgs", func(c *gin.Context) { c.Status(http.StatusOK) })
		grp.GET("/audit", func(c *gin.Context) { c.Status(http.StatusOK) })
		return r
	}
	viewer := mkOIDC(map[string]any{"email": "v@posthog.com", "cognito:groups": []any{"eng"}})
	admin := mkOIDC(map[string]any{"email": "a@posthog.com", "cognito:groups": []any{"admins"}})

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
