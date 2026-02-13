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

func TestParseMemoryBytes(t *testing.T) {
	tests := []struct {
		input string
		want  uint64
	}{
		// Integer values
		{"4GB", 4 * 1024 * 1024 * 1024},
		{"512MB", 512 * 1024 * 1024},
		{"1TB", 1024 * 1024 * 1024 * 1024},
		{"256KB", 256 * 1024},
		// Fractional values
		{"1.5GB", 1536 * 1024 * 1024},
		{"0.5GB", 512 * 1024 * 1024},
		{"2.5MB", 2560 * 1024},
		// Case insensitive
		{"4gb", 4 * 1024 * 1024 * 1024},
		{"512mb", 512 * 1024 * 1024},
		// Edge cases
		{"", 0},
		{"  4GB  ", 4 * 1024 * 1024 * 1024},
		{"0GB", 0},
		// Invalid
		{"4", 0},
		{"GB", 0},
		{"abc", 0},
	}

	for _, tt := range tests {
		got := ParseMemoryBytes(tt.input)
		if got != tt.want {
			t.Errorf("ParseMemoryBytes(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestValidateMemoryLimit(t *testing.T) {
	valid := []string{"4GB", "512MB", "1TB", "256kb", "100 MB", "2 gb", "1.5GB", "0.5MB", "2.75TB"}
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
