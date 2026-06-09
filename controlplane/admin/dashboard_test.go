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
	r.GET("/api/v1/status", APIAuthMiddleware("secret"), func(c *gin.Context) {
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
	r.GET("/api/v1/status", APIAuthMiddleware("secret"), func(c *gin.Context) {
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
	RegisterDashboard(r, "secret")

	for _, path := range []string{"/?token=secret", "/workers?token=secret&foo=bar"} {
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
	}
}

func TestLoginCookieIsHttpOnlyStrict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterDashboard(r, "secret")

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
	RegisterDashboard(r, "secret")

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
