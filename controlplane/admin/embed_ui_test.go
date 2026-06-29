//go:build kubernetes

package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRegisterUIServesSPA(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	// A real API route must take precedence over the SPA fallback.
	r.GET("/api/v1/me", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })
	if err := RegisterUI(r); err != nil {
		t.Fatalf("RegisterUI: %v", err)
	}

	cases := []struct {
		name        string
		path        string
		wantStatus  int
		wantBodyHas string
		wantCType   string
	}{
		{"root serves index", "/", http.StatusOK, "<!doctype html", "text/html"},
		{"client route falls back to index", "/orgs/acme", http.StatusOK, "<!doctype html", "text/html"},
		{"unknown api path is JSON 404", "/api/v1/nope", http.StatusNotFound, `"error"`, ""},
		{"real api route still wins", "/api/v1/me", http.StatusOK, `"ok"`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if !strings.Contains(strings.ToLower(rec.Body.String()), strings.ToLower(tc.wantBodyHas)) {
				t.Fatalf("body = %q, want substring %q", rec.Body.String(), tc.wantBodyHas)
			}
			if tc.wantCType != "" && !strings.Contains(rec.Header().Get("Content-Type"), tc.wantCType) {
				t.Fatalf("content-type = %q, want %q", rec.Header().Get("Content-Type"), tc.wantCType)
			}
		})
	}
}
