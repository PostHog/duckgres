package server

import (
	"strings"
	"testing"
)

// TestResolveS3SecretTransport pins down the discriminator that decides
// what URL_STYLE / USE_SSL we embed on the duckdb_s3 secret. The choice
// is operationally load-bearing: with HTTPProxy set, every S3 byte must
// flow as plain HTTP through the cache proxy's forwardUncached path so
// each request gets a logged Started/Finished pair. Any value other than
// path + false bumps writes back to HTTPS CONNECT, where the proxy can
// only see target+byte counts (TLS terminates between the worker and S3).
func TestResolveS3SecretTransport(t *testing.T) {
	tests := []struct {
		name       string
		cfg        DuckLakeConfig
		wantStyle  string
		wantUseSSL string
	}{
		{
			name:       "HTTPProxy set forces path + false regardless of cfg",
			cfg:        DuckLakeConfig{HTTPProxy: "http://10.0.0.1:8080", S3URLStyle: "vhost", S3UseSSL: true},
			wantStyle:  "path",
			wantUseSSL: "false",
		},
		{
			name:       "HTTPProxy set with no other overrides",
			cfg:        DuckLakeConfig{HTTPProxy: "http://10.0.0.1:8080"},
			wantStyle:  "path",
			wantUseSSL: "false",
		},
		{
			name:       "no proxy: defaults match MinIO compatibility",
			cfg:        DuckLakeConfig{},
			wantStyle:  "path",
			wantUseSSL: "false",
		},
		{
			name:       "no proxy: vhost+ssl honored",
			cfg:        DuckLakeConfig{S3URLStyle: "vhost", S3UseSSL: true},
			wantStyle:  "vhost",
			wantUseSSL: "true",
		},
		{
			name:       "no proxy: explicit path style preserved",
			cfg:        DuckLakeConfig{S3URLStyle: "path", S3UseSSL: true},
			wantStyle:  "path",
			wantUseSSL: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStyle, gotUseSSL := resolveS3SecretTransport(tt.cfg)
			if gotStyle != tt.wantStyle {
				t.Errorf("urlStyle = %q, want %q", gotStyle, tt.wantStyle)
			}
			if gotUseSSL != tt.wantUseSSL {
				t.Errorf("useSSL = %q, want %q", gotUseSSL, tt.wantUseSSL)
			}
		})
	}
}

// TestBuildConfigSecretEmitsHTTPWhenProxySet asserts the SQL produced by
// buildConfigSecret contains URL_STYLE 'path' / USE_SSL false when an
// HTTP proxy is in front, and the org's preferred values otherwise.
func TestBuildConfigSecretEmitsHTTPWhenProxySet(t *testing.T) {
	withProxy := buildConfigSecret(DuckLakeConfig{
		S3AccessKey: "AKIA",
		S3SecretKey: "secret",
		S3Region:    "us-east-1",
		S3Endpoint:  "s3.us-east-1.amazonaws.com",
		S3URLStyle:  "vhost",
		S3UseSSL:    true,
		HTTPProxy:   "http://10.0.0.1:8080",
	})
	if !strings.Contains(withProxy, "URL_STYLE 'path'") {
		t.Errorf("expected URL_STYLE 'path' with proxy set, got:\n%s", withProxy)
	}
	if !strings.Contains(withProxy, "USE_SSL false") {
		t.Errorf("expected USE_SSL false with proxy set, got:\n%s", withProxy)
	}

	withoutProxy := buildConfigSecret(DuckLakeConfig{
		S3AccessKey: "AKIA",
		S3SecretKey: "secret",
		S3Region:    "us-east-1",
		S3Endpoint:  "s3.us-east-1.amazonaws.com",
		S3URLStyle:  "vhost",
		S3UseSSL:    true,
	})
	if !strings.Contains(withoutProxy, "URL_STYLE 'vhost'") {
		t.Errorf("expected URL_STYLE 'vhost' without proxy, got:\n%s", withoutProxy)
	}
	if !strings.Contains(withoutProxy, "USE_SSL true") {
		t.Errorf("expected USE_SSL true without proxy, got:\n%s", withoutProxy)
	}
}

// TestBuildCredentialChainSecretEmitsHTTPWhenProxySet covers the
// credential-chain branch, which previously only emitted USE_SSL /
// URL_STYLE when an explicit endpoint was configured. With HTTPProxy set,
// we always need them on the secret regardless of whether an endpoint
// was given, otherwise the secret falls back to the AWS default
// (use_ssl=true) and writes go via HTTPS CONNECT.
func TestBuildCredentialChainSecretEmitsHTTPWhenProxySet(t *testing.T) {
	// No endpoint, but proxy set — must still emit USE_SSL false / path.
	noEndpointWithProxy := buildCredentialChainSecret(DuckLakeConfig{
		HTTPProxy: "http://10.0.0.1:8080",
	})
	if !strings.Contains(noEndpointWithProxy, "USE_SSL false") {
		t.Errorf("expected USE_SSL false with proxy + no endpoint, got:\n%s", noEndpointWithProxy)
	}
	if !strings.Contains(noEndpointWithProxy, "URL_STYLE 'path'") {
		t.Errorf("expected URL_STYLE 'path' with proxy + no endpoint, got:\n%s", noEndpointWithProxy)
	}

	// No endpoint, no proxy — secret stays minimal (DuckDB defaults apply).
	bare := buildCredentialChainSecret(DuckLakeConfig{})
	if strings.Contains(bare, "USE_SSL") || strings.Contains(bare, "URL_STYLE") {
		t.Errorf("expected no SSL/URL_STYLE clauses when neither endpoint nor proxy set, got:\n%s", bare)
	}
}
