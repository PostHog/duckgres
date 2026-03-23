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
