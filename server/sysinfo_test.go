package server

import (
	"runtime"
	"testing"
)

func TestAutoMemoryLimit(t *testing.T) {
	// autoMemoryLimit should return a non-empty string
	result := autoMemoryLimit()
	if result == "" {
		t.Fatal("autoMemoryLimit returned empty string")
	}

	// On Linux (CI and production), we should detect system memory
	if runtime.GOOS == "linux" {
		totalBytes := totalSystemMemoryBytes()
		if totalBytes == 0 {
			t.Fatal("totalSystemMemoryBytes returned 0 on Linux")
		}
		t.Logf("Detected system memory: %d bytes, auto limit: %s", totalBytes, result)
	}
}

func TestAutoMemoryLimitFormat(t *testing.T) {
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

func TestTotalSystemMemoryBytes(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Only runs on Linux")
	}

	total := totalSystemMemoryBytes()
	if total == 0 {
		t.Fatal("Expected non-zero memory on Linux")
	}

	// Sanity check: at least 64MB (any reasonable system)
	if total < 64*1024*1024 {
		t.Fatalf("Suspiciously low memory: %d bytes", total)
	}
	t.Logf("System memory: %d bytes (%.1f GB)", total, float64(total)/(1024*1024*1024))
}
