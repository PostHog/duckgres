//go:build kubernetes

package controlplane

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestHealthHandlerReturnsServiceUnavailableWhenUnhealthy(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.GET("/health", newHealthHandler(func() bool { return false }))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 while unhealthy, got %d", rec.Code)
	}
}

func TestHealthHandlerReturnsOKWhenHealthy(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.GET("/health", newHealthHandler(func() bool { return true }))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 while healthy, got %d", rec.Code)
	}
}
