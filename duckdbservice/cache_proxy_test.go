package duckdbservice

import (
	"testing"

	"github.com/posthog/duckgres/server"
)

func TestOverrideS3EndpointForCacheProxy(t *testing.T) {
	t.Run("no-op when cache disabled", func(t *testing.T) {
		t.Setenv("DUCKGRES_CACHE_ENABLED", "")
		t.Setenv("NODE_IP", "10.0.0.1")
		cfg := server.DuckLakeConfig{
			S3Endpoint: "s3.amazonaws.com",
			S3UseSSL:   true,
			S3URLStyle: "vhost",
		}
		overrideS3EndpointForCacheProxy(&cfg)
		if cfg.S3Endpoint != "s3.amazonaws.com" {
			t.Errorf("expected endpoint unchanged, got %q", cfg.S3Endpoint)
		}
		if !cfg.S3UseSSL {
			t.Error("expected SSL unchanged (true), got false")
		}
	})

	t.Run("overrides when cache enabled with NODE_IP", func(t *testing.T) {
		t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
		t.Setenv("NODE_IP", "10.0.0.1")
		cfg := server.DuckLakeConfig{
			S3Endpoint: "s3.amazonaws.com",
			S3UseSSL:   true,
			S3URLStyle: "vhost",
		}
		overrideS3EndpointForCacheProxy(&cfg)
		if cfg.S3Endpoint != "10.0.0.1:8080" {
			t.Errorf("expected endpoint 10.0.0.1:8080, got %q", cfg.S3Endpoint)
		}
		if cfg.S3UseSSL {
			t.Error("expected SSL false, got true")
		}
		if cfg.S3URLStyle != "path" {
			t.Errorf("expected URL style 'path', got %q", cfg.S3URLStyle)
		}
	})

	t.Run("falls back to localhost when NODE_IP unset", func(t *testing.T) {
		t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
		t.Setenv("NODE_IP", "")
		cfg := server.DuckLakeConfig{S3Endpoint: "s3.amazonaws.com", S3UseSSL: true}
		overrideS3EndpointForCacheProxy(&cfg)
		if cfg.S3Endpoint != "localhost:8080" {
			t.Errorf("expected localhost:8080 fallback, got %q", cfg.S3Endpoint)
		}
	})
}

func TestCacheProxyHealthURL(t *testing.T) {
	t.Run("empty when disabled", func(t *testing.T) {
		t.Setenv("DUCKGRES_CACHE_ENABLED", "")
		if url := cacheProxyHealthURL(); url != "" {
			t.Errorf("expected empty URL, got %q", url)
		}
	})
	t.Run("uses NODE_IP when set", func(t *testing.T) {
		t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
		t.Setenv("NODE_IP", "10.0.0.1")
		if url := cacheProxyHealthURL(); url != "http://10.0.0.1:8082/health" {
			t.Errorf("unexpected URL: %q", url)
		}
	})
}
