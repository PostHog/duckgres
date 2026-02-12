package server

import (
	"runtime"
	"sync"
	"testing"
)

func TestAutoMemoryLimit(t *testing.T) {
	// Reset cached value so test is independent
	autoMemoryLimitOnce = sync.Once{}
	autoMemoryLimitValue = ""

	result := autoMemoryLimit()
	if result == "" {
		t.Fatal("autoMemoryLimit returned empty string")
	}

	// On Linux (CI and production), we should detect system memory
	if runtime.GOOS == "linux" {
		totalBytes := readSystemMemoryBytes()
		if totalBytes == 0 {
			t.Fatal("totalSystemMemoryBytes returned 0 on Linux")
		}
		t.Logf("Detected system memory: %d bytes, auto limit: %s", totalBytes, result)
	}
}

func TestAutoMemoryLimitFormat(t *testing.T) {
	// Reset cached value so test is independent
	autoMemoryLimitOnce = sync.Once{}
	autoMemoryLimitValue = ""

	result := autoMemoryLimit()
	// Should end with GB or MB
	validSuffix := false
	for _, suffix := range []string{"GB", "MB"} {
		if len(result) > len(suffix) && result[len(result)-len(suffix):] == suffix {
			validSuffix = true
			break
		}
	}
	if !validSuffix {
		t.Fatalf("autoMemoryLimit returned %q, expected suffix GB or MB", result)
	}
}

func TestAutoMemoryLimitCached(t *testing.T) {
	// Reset and call twice — should return same value
	autoMemoryLimitOnce = sync.Once{}
	autoMemoryLimitValue = ""

	first := autoMemoryLimit()
	second := autoMemoryLimit()
	if first != second {
		t.Fatalf("autoMemoryLimit not stable: %q vs %q", first, second)
	}
}

func TestTotalSystemMemoryBytes(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Only runs on Linux")
	}

	total := readSystemMemoryBytes()
	if total == 0 {
		t.Fatal("Expected non-zero memory on Linux")
	}

	// Sanity check: at least 64MB (any reasonable system)
	if total < 64*1024*1024 {
		t.Fatalf("Suspiciously low memory: %d bytes", total)
	}
	t.Logf("System memory: %d bytes (%.1f GB)", total, float64(total)/(1024*1024*1024))
}

func TestValidateMemoryLimit(t *testing.T) {
	valid := []string{"4GB", "512MB", "1TB", "256kb", "100 MB", "2 gb"}
	for _, v := range valid {
		if !ValidateMemoryLimit(v) {
			t.Errorf("expected %q to be valid", v)
		}
	}

	invalid := []string{"", "4", "GB", "lots", "4GB; DROP TABLE", "4'GB"}
	for _, v := range invalid {
		if ValidateMemoryLimit(v) {
			t.Errorf("expected %q to be invalid", v)
		}
	}
}
