package server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewACMEManagerCreatesCache(t *testing.T) {
	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "acme-cache", "nested")

	// Use a non-privileged port to avoid permission issues in tests
	mgr, err := NewACMEManager("test.example.com", "test@example.com", cacheDir, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewACMEManager failed: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	// Verify cache directory was created
	info, err := os.Stat(cacheDir)
	if err != nil {
		t.Fatalf("cache directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("cache path is not a directory")
	}
}

func TestNewACMEManagerDefaultCacheDir(t *testing.T) {
	// Override default cache dir to use a temp directory
	tmpDir := t.TempDir()
	defaultDir := filepath.Join(tmpDir, "default-acme-cache")

	mgr, err := NewACMEManager("test.example.com", "test@example.com", defaultDir, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewACMEManager failed: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	if mgr.cacheDir != defaultDir {
		t.Fatalf("expected cache dir %q, got %q", defaultDir, mgr.cacheDir)
	}
}

func TestACMEManagerTLSConfig(t *testing.T) {
	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "acme-cache")

	mgr, err := NewACMEManager("test.example.com", "test@example.com", cacheDir, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewACMEManager failed: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	tlsCfg := mgr.TLSConfig()
	if tlsCfg == nil {
		t.Fatal("TLSConfig returned nil")
	}
	if tlsCfg.GetCertificate == nil {
		t.Fatal("TLSConfig.GetCertificate is nil")
	}
	if tlsCfg.MinVersion != 0x0303 { // tls.VersionTLS12
		t.Fatalf("expected MinVersion TLS 1.2 (0x0303), got 0x%04x", tlsCfg.MinVersion)
	}
}

func TestACMEManagerClose(t *testing.T) {
	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "acme-cache")

	mgr, err := NewACMEManager("test.example.com", "test@example.com", cacheDir, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewACMEManager failed: %v", err)
	}

	// Close should not error
	if err := mgr.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Double close should be safe (sync.Once)
	if err := mgr.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}
