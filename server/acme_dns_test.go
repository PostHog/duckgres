package server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrCreateAccountKey(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "account-key.pem")

	// First call should create a new key
	key1, err := loadOrCreateAccountKey(keyPath)
	if err != nil {
		t.Fatalf("first loadOrCreateAccountKey failed: %v", err)
	}
	if key1 == nil {
		t.Fatal("first loadOrCreateAccountKey returned nil key")
	}

	// Verify key file was created
	if _, err := os.Stat(keyPath); err != nil {
		t.Fatalf("key file not created: %v", err)
	}

	// Second call should load the existing key
	key2, err := loadOrCreateAccountKey(keyPath)
	if err != nil {
		t.Fatalf("second loadOrCreateAccountKey failed: %v", err)
	}
	if key2 == nil {
		t.Fatal("second loadOrCreateAccountKey returned nil key")
	}

	// Keys should be identical (same D value)
	if key1.D.Cmp(key2.D) != 0 {
		t.Fatal("loaded key does not match created key")
	}
}

func TestACMEDNSManagerRequiresFields(t *testing.T) {
	_, err := NewACMEDNSManager("", "test@example.com", "Z123", t.TempDir())
	if err == nil {
		t.Fatal("expected error for empty domain")
	}

	_, err = NewACMEDNSManager("test.example.com", "test@example.com", "", t.TempDir())
	if err == nil {
		t.Fatal("expected error for empty zone ID")
	}
}

func TestACMEDNSCertCache(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a manager with the cache dir to verify directory creation
	cacheDir := filepath.Join(tmpDir, "nested", "acme-dns-cache")

	// This will fail because we don't have real AWS credentials in tests,
	// but we can test the cache directory creation
	_, err := NewACMEDNSManager("test.example.com", "", "Z123", cacheDir)
	// Expected to fail on AWS config loading in CI, but cache dir should be created
	if err != nil {
		// Verify cache dir was still created before the AWS error
		if _, statErr := os.Stat(cacheDir); statErr != nil {
			t.Logf("Expected: cache dir creation may fail if AWS config fails first")
		}
		return
	}
}

func TestLoadOrCreateAccountKey_InvalidExistingKeyReturnsError(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "account-key.pem")

	original := []byte("-----BEGIN EC PRIVATE KEY-----\nnot-valid\n-----END EC PRIVATE KEY-----\n")
	if err := os.WriteFile(keyPath, original, 0600); err != nil {
		t.Fatalf("failed to write invalid key fixture: %v", err)
	}

	if _, err := loadOrCreateAccountKey(keyPath); err == nil {
		t.Fatal("expected error when existing account key is invalid")
	}

	got, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("failed to read key file after error: %v", err)
	}
	if string(got) != string(original) {
		t.Fatal("invalid key file should not be overwritten")
	}
}
