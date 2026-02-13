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
	defer mgr.Close()

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
	// Use port 0 to get a random available port
	mgr, err := NewACMEManager("test.example.com", "test@example.com", "", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewACMEManager failed: %v", err)
	}
	defer mgr.Close()

	// Default cache dir should be used
	if mgr.cacheDir != "./certs/acme" {
		t.Fatalf("expected default cache dir './certs/acme', got %q", mgr.cacheDir)
	}

	// Clean up default cache dir
	os.RemoveAll("./certs/acme")
}

func TestACMEManagerTLSConfig(t *testing.T) {
	tmpDir := t.TempDir()
	cacheDir := filepath.Join(tmpDir, "acme-cache")

	mgr, err := NewACMEManager("test.example.com", "test@example.com", cacheDir, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewACMEManager failed: %v", err)
	}
	defer mgr.Close()

	tlsCfg := mgr.TLSConfig()
	if tlsCfg == nil {
		t.Fatal("TLSConfig returned nil")
	}
	if tlsCfg.GetCertificate == nil {
		t.Fatal("TLSConfig.GetCertificate is nil")
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
}
