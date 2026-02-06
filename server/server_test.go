package server

import (
	"database/sql"
	"testing"
	"time"
)

func TestParseExtensionName(t *testing.T) {
	tests := []struct {
		input       string
		wantName    string
		wantInstall string
	}{
		{"ducklake", "ducklake", "ducklake"},
		{"httpfs", "httpfs", "httpfs"},
		{"cache_httpfs FROM community", "cache_httpfs", "cache_httpfs FROM community"},
		{"cache_httpfs from community", "cache_httpfs", "cache_httpfs from community"},
		{"cache_httpfs FROM COMMUNITY", "cache_httpfs", "cache_httpfs FROM COMMUNITY"},
		{"my_ext FROM my_repo", "my_ext", "my_ext FROM my_repo"},
		{"ext  FROM  source", "ext", "ext  FROM  source"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			name, installCmd := parseExtensionName(tt.input)
			if name != tt.wantName {
				t.Errorf("parseExtensionName(%q) name = %q, want %q", tt.input, name, tt.wantName)
			}
			if installCmd != tt.wantInstall {
				t.Errorf("parseExtensionName(%q) installCmd = %q, want %q", tt.input, installCmd, tt.wantInstall)
			}
		})
	}
}

func TestNeedsCredentialRefresh(t *testing.T) {
	tests := []struct {
		name string
		cfg  DuckLakeConfig
		want bool
	}{
		{
			"credential_chain with object store",
			DuckLakeConfig{ObjectStore: "s3://bucket/path/", S3Provider: "credential_chain"},
			true,
		},
		{
			"implicit credential_chain (no access key, no provider)",
			DuckLakeConfig{ObjectStore: "s3://bucket/path/"},
			true,
		},
		{
			"config provider with explicit credentials",
			DuckLakeConfig{ObjectStore: "s3://bucket/path/", S3Provider: "config", S3AccessKey: "key", S3SecretKey: "secret"},
			false,
		},
		{
			"implicit config provider (access key set)",
			DuckLakeConfig{ObjectStore: "s3://bucket/path/", S3AccessKey: "key", S3SecretKey: "secret"},
			false,
		},
		{
			"no object store",
			DuckLakeConfig{MetadataStore: "postgres:..."},
			false,
		},
		{
			"empty config",
			DuckLakeConfig{},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := needsCredentialRefresh(tt.cfg)
			if got != tt.want {
				t.Errorf("needsCredentialRefresh() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStartCredentialRefresh_NoOpForStaticCredentials(t *testing.T) {
	// Static credentials should return a no-op stop function immediately
	stop := StartCredentialRefresh(nil, DuckLakeConfig{
		ObjectStore: "s3://bucket/path/",
		S3Provider:  "config",
		S3AccessKey: "key",
		S3SecretKey: "secret",
	})
	stop() // Should not panic
	stop() // Calling twice should be safe (sync.Once)
}

func TestStartCredentialRefresh_NoOpForNoObjectStore(t *testing.T) {
	stop := StartCredentialRefresh(nil, DuckLakeConfig{})
	stop() // Should not panic
}

func TestStartCredentialRefresh_StopsCleanly(t *testing.T) {
	// Open a real DuckDB connection to test that the refresh goroutine works
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Install httpfs extension so CREATE SECRET works
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		t.Skip("httpfs extension not available:", err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		t.Skip("httpfs extension not loadable:", err)
	}

	// Use credential_chain config to trigger refresh
	cfg := DuckLakeConfig{
		ObjectStore: "s3://bucket/path/",
		S3Provider:  "credential_chain",
	}

	stop := StartCredentialRefresh(db, cfg)

	// Give the goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Stop should not hang or panic
	stop()
}

func TestHasCacheHTTPFS(t *testing.T) {
	tests := []struct {
		name       string
		extensions []string
		want       bool
	}{
		{"present bare", []string{"ducklake", "cache_httpfs"}, true},
		{"present with source", []string{"ducklake", "cache_httpfs FROM community"}, true},
		{"absent", []string{"ducklake", "httpfs"}, false},
		{"empty list", []string{}, false},
		{"nil list", nil, false},
		{"similar name", []string{"not_cache_httpfs"}, false},
		{"substring match", []string{"cache_httpfs_v2 FROM community"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasCacheHTTPFS(tt.extensions)
			if got != tt.want {
				t.Errorf("hasCacheHTTPFS(%v) = %v, want %v", tt.extensions, got, tt.want)
			}
		})
	}
}
