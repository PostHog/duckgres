//go:build kubernetes

package admin

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestAPIMiddlewareRejectsMissingAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/status", APIAuthMiddleware(NewTokenSet("secret", nil)), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAPIMiddlewareAcceptsCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/status", APIAuthMiddleware(NewTokenSet("secret", nil)), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	req.AddCookie(&http.Cookie{Name: adminTokenCookieName, Value: "secret"})
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// Regression test for #721: the admin token must never be accepted via a URL
// query parameter — URL-borne secrets persist in browser history and proxy
// access logs. A GET /login carrying the correct token in ?token= must still
// just render the login page (401), with no Set-Cookie and no redirect.
func TestDashboardRejectsTokenQueryParam(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterLogin(r, NewTokenSet("secret", nil))

	for _, path := range []string{"/login?token=secret", "/login?token=secret&foo=bar"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("GET %s status = %d, want %d", path, rec.Code, http.StatusUnauthorized)
		}
		if got := rec.Header().Get("Set-Cookie"); got != "" {
			t.Errorf("GET %s set a cookie: %q, want none", path, got)
		}
		if got := rec.Header().Get("Location"); got != "" {
			t.Errorf("GET %s redirected to %q, want no redirect", path, got)
		}
		// The login page must not echo the secret back (e.g. inside the
		// hidden "next" field) — that would re-embed it in the page and
		// redirect it back into the URL bar after login.
		if strings.Contains(rec.Body.String(), "secret") {
			t.Errorf("GET %s login page echoes the token", path)
		}
	}
}

func TestLoginCookieIsHttpOnlyStrict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterLogin(r, NewTokenSet("secret", nil))

	form := url.Values{"token": {"secret"}, "next": {"/"}}
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	cookies := rec.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != adminTokenCookieName {
		t.Fatalf("cookies = %v, want exactly one %q cookie", cookies, adminTokenCookieName)
	}
	if !cookies[0].HttpOnly {
		t.Error("cookie is not HttpOnly")
	}
	if cookies[0].SameSite != http.SameSiteStrictMode {
		t.Errorf("cookie SameSite = %v, want %v", cookies[0].SameSite, http.SameSiteStrictMode)
	}
}

func TestDashboardRequiresLoginThenSetsCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterLogin(r, NewTokenSet("secret", nil))

	req := httptest.NewRequest(http.MethodGet, "/login", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /login status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if body := rec.Body.String(); body == "" {
		t.Fatal("expected login page body")
	}

	form := url.Values{
		"token": {"secret"},
		"next":  {"/"},
	}
	loginReq := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	loginRec := httptest.NewRecorder()
	r.ServeHTTP(loginRec, loginReq)

	if loginRec.Code != http.StatusSeeOther {
		t.Fatalf("POST /login status = %d, want %d", loginRec.Code, http.StatusSeeOther)
	}
	if got := loginRec.Header().Get("Set-Cookie"); got == "" {
		t.Fatal("expected Set-Cookie header")
	}
	if got := loginRec.Header().Get("Location"); got != "/" {
		t.Fatalf("redirect = %q, want %q", got, "/")
	}
}

func TestTokenSet(t *testing.T) {
	tests := []struct {
		name      string
		primary   string
		fallbacks []string
		got       string
		want      bool
	}{
		{"primary matches", "new", []string{"old"}, "new", true},
		{"fallback matches", "new", []string{"old"}, "old", true},
		{"second fallback matches", "new", []string{"old1", "old2"}, "old2", true},
		{"garbage rejected", "new", []string{"old"}, "wrong", false},
		{"empty got rejected", "new", []string{"old"}, "", false},
		{"no fallbacks, primary matches", "new", nil, "new", true},
		{"no fallbacks, garbage rejected", "new", nil, "wrong", false},
		// Empty strings must never validate: an unset slot (empty primary or
		// an empty entry in the fallback list) is filtered, not matchable.
		{"empty primary not matchable", "", []string{"old"}, "", false},
		{"empty primary, fallback still works", "", []string{"old"}, "old", true},
		{"empty fallback entry not matchable", "new", []string{""}, "", false},
		{"zero tokens reject everything", "", nil, "anything", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := NewTokenSet(tt.primary, tt.fallbacks)
			if got := ts.Valid(tt.got); got != tt.want {
				t.Errorf("NewTokenSet(%q, %v).Valid(%q) = %v, want %v",
					tt.primary, tt.fallbacks, tt.got, got, tt.want)
			}
		})
	}
}

func TestTokenSetCount(t *testing.T) {
	if got := NewTokenSet("new", []string{"old", ""}).Count(); got != 2 {
		t.Errorf("Count() = %d, want 2 (empty entries filtered)", got)
	}
}

// During a secret rotation the server accepts the previous secret(s) as
// fallbacks while clients are flipped over to the new primary — on both the
// service-to-service header path and the dashboard cookie path.
func TestAPIMiddlewareAcceptsFallbackDuringRotation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/status", APIAuthMiddleware(NewTokenSet("new-secret", []string{"old-secret"})), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	cases := []struct {
		name   string
		setup  func(req *http.Request)
		status int
	}{
		{"fallback via header", func(req *http.Request) {
			req.Header.Set("X-Duckgres-Internal-Secret", "old-secret")
		}, http.StatusOK},
		{"primary via header", func(req *http.Request) {
			req.Header.Set("X-Duckgres-Internal-Secret", "new-secret")
		}, http.StatusOK},
		{"fallback via cookie", func(req *http.Request) {
			req.AddCookie(&http.Cookie{Name: adminTokenCookieName, Value: "old-secret"})
		}, http.StatusOK},
		{"garbage via header", func(req *http.Request) {
			req.Header.Set("X-Duckgres-Internal-Secret", "wrong")
		}, http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
			tc.setup(req)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Errorf("status = %d, want %d", rec.Code, tc.status)
			}
		})
	}
}

// A dashboard login with a fallback secret must succeed and mint a working
// cookie: a rotation must not lock out an operator holding the old value
// until the fallback is dropped in the final phase.
func TestLoginWithFallbackTokenWorksEndToEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	tokens := NewTokenSet("new-secret", []string{"old-secret"})
	RegisterLogin(r, tokens)
	// A cookie-authenticated request must be accepted by AuthMiddleware (and
	// mapped to the admin role via the internal-secret path).
	r.GET("/api/v1/ping", AuthMiddleware(tokens, nil), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"role": IdentityFromContext(c).Role})
	})

	form := url.Values{"token": {"old-secret"}, "next": {"/"}}
	loginReq := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	loginRec := httptest.NewRecorder()
	r.ServeHTTP(loginRec, loginReq)

	if loginRec.Code != http.StatusSeeOther {
		t.Fatalf("POST /login status = %d, want %d", loginRec.Code, http.StatusSeeOther)
	}
	cookies := loginRec.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != adminTokenCookieName {
		t.Fatalf("cookies = %v, want exactly one %q cookie", cookies, adminTokenCookieName)
	}

	// The cookie minted from the fallback must authenticate an API request.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
	req.AddCookie(cookies[0])
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/ping with fallback cookie status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// serveWithSecret hits the router with X-Duckgres-Internal-Secret set (the
// same header carries admin and discovery tokens; the value decides).
func serveWithSecret(r *gin.Engine, path, secret string) int {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if secret != "" {
		req.Header.Set("X-Duckgres-Internal-Secret", secret)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec.Code
}

// TestAnyTokenAuthMiddlewareScoping pins the two-surface topology from
// multitenant.go: the discovery secret works ONLY on the discovery group,
// the admin secret works on both, and neither surface accepts junk. This
// is the security property the scoped token exists for — a discovery
// credential must never reach the admin/provisioning surface.
func TestAnyTokenAuthMiddlewareScoping(t *testing.T) {
	gin.SetMode(gin.TestMode)
	adminTokens := NewTokenSet("admin-secret", nil)
	discoveryTokens := NewTokenSet("discovery-secret", []string{"old-discovery"})

	r := gin.New()
	ok := func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) }
	r.GET("/api/v1/orgs", APIAuthMiddleware(adminTokens), ok)
	r.GET("/api/v1/warehouses", AnyTokenAuthMiddleware(discoveryTokens, adminTokens), ok)

	cases := []struct {
		name, path, secret string
		want               int
	}{
		{"discovery token on discovery route", "/api/v1/warehouses", "discovery-secret", http.StatusOK},
		{"discovery fallback on discovery route", "/api/v1/warehouses", "old-discovery", http.StatusOK},
		{"admin token on discovery route", "/api/v1/warehouses", "admin-secret", http.StatusOK},
		{"discovery token on admin route", "/api/v1/orgs", "discovery-secret", http.StatusUnauthorized},
		{"discovery fallback on admin route", "/api/v1/orgs", "old-discovery", http.StatusUnauthorized},
		{"admin token on admin route", "/api/v1/orgs", "admin-secret", http.StatusOK},
		{"junk on discovery route", "/api/v1/warehouses", "junk", http.StatusUnauthorized},
		{"missing token on discovery route", "/api/v1/warehouses", "", http.StatusUnauthorized},
	}
	for _, tc := range cases {
		if got := serveWithSecret(r, tc.path, tc.secret); got != tc.want {
			t.Errorf("%s: status = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestAnyTokenAuthMiddlewareUnsetDiscoverySecret pins the pre-rollout
// behavior: with no discovery secret configured (empty TokenSet), the
// discovery surface still accepts the internal secret and nothing else —
// in particular an empty header must not match the empty token set.
func TestAnyTokenAuthMiddlewareUnsetDiscoverySecret(t *testing.T) {
	gin.SetMode(gin.TestMode)
	adminTokens := NewTokenSet("admin-secret", nil)
	discoveryTokens := NewTokenSet("", nil) // unset secret ⇒ empty set

	r := gin.New()
	r.GET("/api/v1/warehouses", AnyTokenAuthMiddleware(discoveryTokens, adminTokens), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	if got := serveWithSecret(r, "/api/v1/warehouses", "admin-secret"); got != http.StatusOK {
		t.Errorf("admin token: status = %d, want %d", got, http.StatusOK)
	}
	if got := serveWithSecret(r, "/api/v1/warehouses", ""); got != http.StatusUnauthorized {
		t.Errorf("empty token: status = %d, want %d", got, http.StatusUnauthorized)
	}
	if got := serveWithSecret(r, "/api/v1/warehouses", "anything"); got != http.StatusUnauthorized {
		t.Errorf("junk token: status = %d, want %d", got, http.StatusUnauthorized)
	}
}

// TestAnyTokenAuthMiddlewareIgnoresCookies pins the header-only contract:
// unlike the admin surface, a browser cookie holding a valid token must NOT
// authenticate the service-to-service discovery surface.
func TestAnyTokenAuthMiddlewareIgnoresCookies(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tokens := NewTokenSet("discovery-secret", nil)

	r := gin.New()
	r.GET("/api/v1/warehouses", AnyTokenAuthMiddleware(tokens), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/warehouses", nil)
	req.AddCookie(&http.Cookie{Name: adminTokenCookieName, Value: "discovery-secret"})
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("cookie-carried token: status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}
