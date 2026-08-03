package server

import (
	"database/sql"
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

// Regression for the env-inheritance hazard: a PROVIDER config secret that
// omits SESSION_TOKEN silently inherits a host AWS_SESSION_TOKEN env var
// (httpfs copies it into the global s3_session_token setting at load, and
// secret lookup falls back to settings for keys the secret omits) and signs
// with a mismatched (key, token) pair. buildConfigSecret must always pin the
// token explicitly — empty means "no token".
func TestBuildConfigSecretAlwaysEmitsSessionToken(t *testing.T) {
	noToken := buildConfigSecret(DuckLakeConfig{
		S3AccessKey: "AKIA",
		S3SecretKey: "sk",
	})
	if !strings.Contains(noToken, "SESSION_TOKEN ''") {
		t.Errorf("config secret without a token must pin SESSION_TOKEN '':\n%s", noToken)
	}
	withToken := buildConfigSecret(DuckLakeConfig{
		S3AccessKey:    "ASIA",
		S3SecretKey:    "sk",
		S3SessionToken: "tok",
	})
	if !strings.Contains(withToken, "SESSION_TOKEN 'tok'") {
		t.Errorf("config secret must carry the explicit token:\n%s", withToken)
	}
}

func TestBuildStagingDeltaHTTPSSecret(t *testing.T) {
	t.Run("uses the activated credentials with a bucket-root staging scope", func(t *testing.T) {
		stmt, ok := buildStagingDeltaHTTPSSecret(DuckLakeConfig{
			ObjectStore:    "s3://managed-warehouse/catalog/",
			S3Provider:     "config",
			S3AccessKey:    "ASIAEXAMPLE",
			S3SecretKey:    "secret",
			S3SessionToken: "token",
			S3Region:       "us-east-1",
			S3Endpoint:     "s3.us-east-1.amazonaws.com",
			S3UseSSL:       false,
			S3URLStyle:     "path",
			HTTPProxy:      "http://10.0.0.1:8080",
		})
		if !ok {
			t.Fatal("expected an explicit staging secret")
		}

		for _, want := range []string{
			"CREATE OR REPLACE SECRET posthog_staging_delta_https",
			"PROVIDER config",
			"KEY_ID 'ASIAEXAMPLE'",
			"SECRET 'secret'",
			"SESSION_TOKEN 'token'",
			"REGION 'us-east-1'",
			"URL_STYLE 'vhost'",
			"USE_SSL true",
			"SCOPE 's3://managed-warehouse/__posthog_staging'",
		} {
			if !strings.Contains(stmt, want) {
				t.Errorf("staging secret missing %q:\n%s", want, stmt)
			}
		}
		for _, forbidden := range []string{"PROVIDER credential_chain", "ENDPOINT", "10.0.0.1"} {
			if strings.Contains(stmt, forbidden) {
				t.Errorf("staging secret must bypass the cache transport; found %q:\n%s", forbidden, stmt)
			}
		}
	})

	t.Run("pins an empty session token", func(t *testing.T) {
		stmt, ok := buildStagingDeltaHTTPSSecret(DuckLakeConfig{
			ObjectStore: "s3://managed-warehouse/",
			S3Provider:  "config",
			S3AccessKey: "AKIAEXAMPLE",
			S3SecretKey: "secret",
		})
		if !ok {
			t.Fatal("expected an explicit staging secret")
		}
		if !strings.Contains(stmt, "SESSION_TOKEN ''") {
			t.Fatalf("staging secret must pin the empty token:\n%s", stmt)
		}
	})

	for _, tt := range []struct {
		name string
		cfg  DuckLakeConfig
	}{
		{
			name: "credential chain remains a legacy client fallback",
			cfg: DuckLakeConfig{
				ObjectStore: "s3://legacy-bucket/",
				S3Provider:  "credential_chain",
			},
		},
		{
			name: "non-S3 object store",
			cfg: DuckLakeConfig{
				ObjectStore: "file:///tmp/ducklake",
				S3Provider:  "config",
				S3AccessKey: "key",
				S3SecretKey: "secret",
			},
		},
		{
			name: "missing explicit credentials",
			cfg: DuckLakeConfig{
				ObjectStore: "s3://managed-warehouse/",
				S3Provider:  "config",
			},
		},
		{
			name: "S3-compatible non-AWS endpoint",
			cfg: DuckLakeConfig{
				ObjectStore: "s3://local-bucket/",
				S3Provider:  "config",
				S3AccessKey: "key",
				S3SecretKey: "secret",
				S3Endpoint:  "localhost:19000",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if stmt, ok := buildStagingDeltaHTTPSSecret(tt.cfg); ok || stmt != "" {
				t.Fatalf("buildStagingDeltaHTTPSSecret() = (%q, %v), want no secret", stmt, ok)
			}
		})
	}
}

func TestCreateAndRefreshS3SecretManageStagingDeltaHTTPSSecret(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		t.Skip("httpfs extension not available:", err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		t.Skip("httpfs extension not loadable:", err)
	}

	cfg := DuckLakeConfig{
		ObjectStore:    "s3://managed-warehouse/",
		S3Provider:     "config",
		S3AccessKey:    "ASIAINITIAL",
		S3SecretKey:    "initial-secret",
		S3SessionToken: "initial-token",
		S3Region:       "us-east-1",
	}
	if err := createS3Secret(db, cfg); err != nil {
		t.Fatalf("createS3Secret() error: %v", err)
	}

	var count int
	if err := db.QueryRow(`
		SELECT count(*)
		FROM duckdb_secrets()
		WHERE name IN ('ducklake_s3', 'posthog_staging_delta_https')
	`).Scan(&count); err != nil {
		t.Fatalf("query managed secrets: %v", err)
	}
	if count != 2 {
		t.Fatalf("managed secret count = %d, want 2", count)
	}
	if _, err := db.Exec("DROP SECRET posthog_staging_delta_https"); err != nil {
		t.Fatalf("drop staging secret: %v", err)
	}
	if err := createS3Secret(db, cfg); err != nil {
		t.Fatalf("createS3Secret() with existing catalog secret error: %v", err)
	}
	if err := db.QueryRow(`
		SELECT count(*)
		FROM duckdb_secrets()
		WHERE name = 'posthog_staging_delta_https'
	`).Scan(&count); err != nil {
		t.Fatalf("query restored staging secret: %v", err)
	}
	if count != 1 {
		t.Fatalf("restored staging secret count = %d, want 1", count)
	}

	cfg.S3AccessKey = "ASIAROTATED"
	cfg.S3SecretKey = "rotated-secret"
	cfg.S3SessionToken = "rotated-token"
	if err := RefreshS3Secret(db, cfg, nil); err != nil {
		t.Fatalf("RefreshS3Secret() error: %v", err)
	}
	if err := db.QueryRow(`
		SELECT count(*)
		FROM duckdb_secrets()
		WHERE name IN ('ducklake_s3', 'posthog_staging_delta_https')
	`).Scan(&count); err != nil {
		t.Fatalf("query refreshed managed secrets: %v", err)
	}
	if count != 2 {
		t.Fatalf("refreshed managed secret count = %d, want 2", count)
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
