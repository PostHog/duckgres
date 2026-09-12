//go:build kubernetes

package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/provisioner/opa"
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

func TestRegisterUIPreservesBundleRouting(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	store := &opa.BundleStore{}
	store.Set(opa.NewBundle([]byte("registered-bundle")))
	// Registry-only startup registers only cell-specific bundle routes. Use
	// the real authenticated handler alongside the same UI fallback as the server.
	engine.Any("/bundles/trino/cell-test", gin.WrapH(opa.NewHandler(store, opa.BearerTokenAuth("fixture-token"))))
	if err := RegisterUI(engine); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, method, path, token, contentType string
		status                                 int
	}{
		{"missing legacy bundle", http.MethodGet, "/bundles/trino", "", "application/json", http.StatusNotFound},
		{"missing authenticated legacy bundle", http.MethodGet, "/bundles/trino", "fixture-token", "application/json", http.StatusNotFound},
		{"missing registered cell", http.MethodGet, "/bundles/trino/missing-cell", "fixture-token", "application/json", http.StatusNotFound},
		{"bundle namespace root", http.MethodGet, "/bundles", "", "application/json", http.StatusNotFound},
		{"bundle namespace slash", http.MethodGet, "/bundles/", "", "application/json", http.StatusNotFound},
		{"missing bundle rejects POST fallback", http.MethodPost, "/bundles/trino", "", "application/json", http.StatusNotFound},
		{"registered bundle authentication remains enforced", http.MethodGet, "/bundles/trino/cell-test", "", "text/plain", http.StatusUnauthorized},
		{"registered bundle remains served", http.MethodGet, "/bundles/trino/cell-test", "fixture-token", "application/gzip", http.StatusOK},
		{"client route remains served", http.MethodGet, "/orgs/example", "", "text/html", http.StatusOK},
		{"similarly named client route remains served", http.MethodGet, "/bundles-overview", "", "text/html", http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			if test.token != "" {
				request.Header.Set("Authorization", "Bearer "+test.token)
			}
			response := httptest.NewRecorder()
			engine.ServeHTTP(response, request)
			if response.Code != test.status || !strings.HasPrefix(response.Header().Get("Content-Type"), test.contentType) {
				t.Fatalf("response = %d %q, want %d %q", response.Code, response.Header().Get("Content-Type"), test.status, test.contentType)
			}
			if test.contentType == "application/gzip" && response.Body.String() != "registered-bundle" {
				t.Fatalf("registered bundle response was replaced: %q", response.Body.String())
			}
		})
	}
}
