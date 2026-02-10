package server

import (
	"encoding/json"
	"syscall"
	"testing"
	"time"
)

func TestChildConfigSerialization(t *testing.T) {
	cfg := ChildConfig{
		RemoteAddr:       "192.168.1.1:12345",
		DataDir:          "/data",
		Extensions:       []string{"ducklake", "json"},
		IdleTimeout:      int64(10 * time.Minute),
		TLSCertFile:      "/certs/server.crt",
		TLSKeyFile:       "/certs/server.key",
		Users:            map[string]string{"postgres": "secret123"},
		BackendSecretKey: 12345678,
		DuckLake: DuckLakeConfig{
			MetadataStore: "postgres:host=localhost",
			S3Region:      "us-east-1",
		},
	}

	// Serialize
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Failed to marshal ChildConfig: %v", err)
	}

	// Deserialize
	var cfg2 ChildConfig
	if err := json.Unmarshal(data, &cfg2); err != nil {
		t.Fatalf("Failed to unmarshal ChildConfig: %v", err)
	}

	// Verify fields
	if cfg2.RemoteAddr != cfg.RemoteAddr {
		t.Errorf("RemoteAddr mismatch: got %q, want %q", cfg2.RemoteAddr, cfg.RemoteAddr)
	}
	if cfg2.DataDir != cfg.DataDir {
		t.Errorf("DataDir mismatch: got %q, want %q", cfg2.DataDir, cfg.DataDir)
	}
	if len(cfg2.Extensions) != len(cfg.Extensions) {
		t.Errorf("Extensions length mismatch: got %d, want %d", len(cfg2.Extensions), len(cfg.Extensions))
	}
	if cfg2.IdleTimeout != cfg.IdleTimeout {
		t.Errorf("IdleTimeout mismatch: got %d, want %d", cfg2.IdleTimeout, cfg.IdleTimeout)
	}
	if cfg2.TLSCertFile != cfg.TLSCertFile {
		t.Errorf("TLSCertFile mismatch: got %q, want %q", cfg2.TLSCertFile, cfg.TLSCertFile)
	}
	if cfg2.BackendSecretKey != cfg.BackendSecretKey {
		t.Errorf("BackendSecretKey mismatch: got %d, want %d", cfg2.BackendSecretKey, cfg.BackendSecretKey)
	}
	if pw, ok := cfg2.Users["postgres"]; !ok || pw != "secret123" {
		t.Errorf("Users mismatch: got %v", cfg2.Users)
	}
	if cfg2.DuckLake.MetadataStore != cfg.DuckLake.MetadataStore {
		t.Errorf("DuckLake.MetadataStore mismatch: got %q, want %q", cfg2.DuckLake.MetadataStore, cfg.DuckLake.MetadataStore)
	}
}

func TestChildConfigSecurityNote(t *testing.T) {
	// This test documents the security improvement: config is passed via stdin
	// instead of environment variables, which prevents other processes from
	// reading passwords via /proc/<pid>/environ

	cfg := ChildConfig{
		Users: map[string]string{
			"admin":    "supersecret",
			"readonly": "lesssecret",
		},
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	// Verify sensitive data is in the JSON (it will be passed via stdin pipe)
	dataStr := string(data)
	if dataStr == "" {
		t.Error("Expected non-empty serialized config")
	}

	// The key security property is that this JSON is passed via stdin pipe
	// (a private file descriptor) rather than DUCKGRES_CHILD_CONFIG env var
	// which would be visible in /proc/<pid>/environ

	// Verify the Users map is properly serialized
	var cfg2 ChildConfig
	if err := json.Unmarshal(data, &cfg2); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}
	if cfg2.Users["admin"] != "supersecret" {
		t.Error("Password not properly serialized/deserialized")
	}
}

func TestChildTrackerBasicOperations(t *testing.T) {
	tracker := NewChildTracker()

	// Initially empty
	if tracker.Count() != 0 {
		t.Errorf("Expected 0 children, got %d", tracker.Count())
	}

	// Add a child
	child1 := &ChildProcess{
		PID:        1001,
		Username:   "user1",
		RemoteAddr: "192.168.1.1:1111",
		BackendKey: BackendKey{Pid: 1001, SecretKey: 111},
		StartTime:  time.Now(),
		done:       make(chan struct{}),
	}
	tracker.Add(child1)

	if tracker.Count() != 1 {
		t.Errorf("Expected 1 child, got %d", tracker.Count())
	}

	// Get by PID
	got := tracker.Get(1001)
	if got == nil {
		t.Fatal("Expected to find child by PID")
	} else if got.Username != "user1" {
		t.Errorf("Username mismatch: got %q, want %q", got.Username, "user1")
	}

	// Add another child
	child2 := &ChildProcess{
		PID:        1002,
		Username:   "user2",
		RemoteAddr: "192.168.1.2:2222",
		BackendKey: BackendKey{Pid: 1002, SecretKey: 222},
		StartTime:  time.Now(),
		done:       make(chan struct{}),
	}
	tracker.Add(child2)

	if tracker.Count() != 2 {
		t.Errorf("Expected 2 children, got %d", tracker.Count())
	}

	// Find by backend key
	found := tracker.FindByBackendKey(BackendKey{Pid: 1002, SecretKey: 222})
	if found == nil {
		t.Fatal("Expected to find child by backend key")
	} else if found.PID != 1002 {
		t.Errorf("PID mismatch: got %d, want %d", found.PID, 1002)
	}

	// Find non-existent key
	notFound := tracker.FindByBackendKey(BackendKey{Pid: 9999, SecretKey: 999})
	if notFound != nil {
		t.Error("Expected nil for non-existent backend key")
	}

	// Remove child
	removed := tracker.Remove(1001)
	if removed == nil {
		t.Error("Expected to remove child")
	}
	if tracker.Count() != 1 {
		t.Errorf("Expected 1 child after removal, got %d", tracker.Count())
	}

	// Verify removed child is gone
	if tracker.Get(1001) != nil {
		t.Error("Expected child 1001 to be gone")
	}
}

func TestChildTrackerSignalAll(t *testing.T) {
	tracker := NewChildTracker()

	// We can't easily test actual signal sending without real processes,
	// but we can verify the method doesn't panic with nil processes
	child := &ChildProcess{
		PID:        9999,
		done:       make(chan struct{}),
		BackendKey: BackendKey{Pid: 9999, SecretKey: 123},
	}
	// Cmd is nil, so SignalAll should handle gracefully
	tracker.Add(child)

	// This should not panic
	tracker.SignalAll(syscall.SIGTERM)
}

func TestChildTrackerWaitAll(t *testing.T) {
	tracker := NewChildTracker()

	child1 := &ChildProcess{
		PID:  1001,
		done: make(chan struct{}),
	}
	child2 := &ChildProcess{
		PID:  1002,
		done: make(chan struct{}),
	}

	tracker.Add(child1)
	tracker.Add(child2)

	// Start waiting in background
	waitCh := tracker.WaitAll()

	// Channel should not be closed yet
	select {
	case <-waitCh:
		t.Error("WaitAll returned before children were done")
	case <-time.After(10 * time.Millisecond):
		// Expected
	}

	// Signal children as done
	close(child1.done)
	close(child2.done)

	// Now WaitAll should return
	select {
	case <-waitCh:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("WaitAll did not return after children were done")
	}
}

func TestExitCodes(t *testing.T) {
	// Verify exit code constants
	if ExitSuccess != 0 {
		t.Errorf("ExitSuccess should be 0, got %d", ExitSuccess)
	}
	if ExitError != 1 {
		t.Errorf("ExitError should be 1, got %d", ExitError)
	}
	if ExitAuthFailure != 10 {
		t.Errorf("ExitAuthFailure should be 10, got %d", ExitAuthFailure)
	}
}

func TestBackendKeyGeneration(t *testing.T) {
	// Generate multiple keys and verify they're different (high probability)
	keys := make(map[int32]bool)
	for i := 0; i < 100; i++ {
		key := generateSecretKey()
		if key == 0 {
			t.Error("Generated secret key should not be 0")
		}
		keys[key] = true
	}

	// With good randomness, we should have close to 100 unique keys
	if len(keys) < 90 {
		t.Errorf("Expected mostly unique keys, got only %d unique out of 100", len(keys))
	}
}
