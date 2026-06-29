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
// access logs. A request carrying the correct token in ?token= must get the
// login page (401), with no Set-Cookie and no redirect.
func TestDashboardRejectsTokenQueryParam(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterDashboard(r, NewTokenSet("secret", nil))

	for _, path := range []string{"/?token=secret", "/models?token=secret&foo=bar"} {
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
	RegisterDashboard(r, NewTokenSet("secret", nil))

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
	RegisterDashboard(r, NewTokenSet("secret", nil))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET / status = %d, want %d", rec.Code, http.StatusUnauthorized)
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
	RegisterDashboard(r, NewTokenSet("new-secret", []string{"old-secret"}))

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

	// The cookie minted from the fallback must authenticate a dashboard GET.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(cookies[0])
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / with fallback cookie status = %d, want %d", rec.Code, http.StatusOK)
	}
}
