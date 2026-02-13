package server

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
)

// systemMemoryBytesOnce caches the result of reading /proc/meminfo.
var (
	systemMemoryBytesOnce  sync.Once
	systemMemoryBytesValue uint64
)

// SystemMemoryBytes returns total physical memory in bytes, cached after first call.
// Returns 0 if the system memory cannot be detected (e.g., on non-Linux systems).
func SystemMemoryBytes() uint64 {
	systemMemoryBytesOnce.Do(func() {
		systemMemoryBytesValue = readSystemMemoryBytes()
	})
	return systemMemoryBytesValue
}

// readSystemMemoryBytes reads total physical memory from /proc/meminfo on Linux.
// Returns 0 if the file cannot be read or parsed (e.g., on non-Linux systems).
func readSystemMemoryBytes() uint64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			var kb uint64
			if _, err := fmt.Sscanf(line, "MemTotal: %d kB", &kb); err == nil {
				return kb * 1024
			}
			return 0
		}
	}
	return 0
}

var (
	autoMemoryLimitOnce  sync.Once
	autoMemoryLimitValue string
)

// autoMemoryLimit computes a DuckDB memory_limit based on system memory.
// Formula: totalMem * 0.75 / 4, with a floor of 256MB.
// The /4 accounts for typical concurrency (multiple sessions sharing RAM).
// Returns "4GB" as a safe default if system memory cannot be detected.
// The result is computed once and cached since system memory doesn't change.
func autoMemoryLimit() string {
	autoMemoryLimitOnce.Do(func() {
		totalBytes := SystemMemoryBytes()
		if totalBytes == 0 {
			autoMemoryLimitValue = "4GB"
			return
		}

		const mb = 1024 * 1024
		const gb = 1024 * mb

		limitBytes := totalBytes * 3 / 4 / 4 // 75% / 4 sessions
		if limitBytes < 256*mb {
			limitBytes = 256 * mb
		}

		// Format as human-readable: use GB if >= 1GB, else MB
		if limitBytes >= gb {
			limitGB := limitBytes / gb
			autoMemoryLimitValue = fmt.Sprintf("%dGB", limitGB)
		} else {
			limitMB := limitBytes / mb
			autoMemoryLimitValue = fmt.Sprintf("%dMB", limitMB)
		}
	})
	return autoMemoryLimitValue
}

// validMemoryLimit matches DuckDB memory limit values like "4GB", "512MB", "1TB".
var validMemoryLimit = regexp.MustCompile(`(?i)^\d+\s*(KB|MB|GB|TB)$`)

// ValidateMemoryLimit checks that a memory_limit string is a valid DuckDB size value.
func ValidateMemoryLimit(v string) bool {
	return validMemoryLimit.MatchString(v)
}

// ParseMemoryBytes parses a DuckDB memory size string (e.g., "4GB", "512MB", "1.5GB") into bytes.
// Supports both integer and fractional values. Returns 0 if the string is empty or invalid.
func ParseMemoryBytes(s string) uint64 {
	if s == "" {
		return 0
	}
	s = strings.TrimSpace(strings.ToUpper(s))

	var value float64
	var unit string
	if _, err := fmt.Sscanf(s, "%f%s", &value, &unit); err != nil || value < 0 {
		return 0
	}

	var multiplier float64
	switch unit {
	case "KB":
		multiplier = 1024
	case "MB":
		multiplier = 1024 * 1024
	case "GB":
		multiplier = 1024 * 1024 * 1024
	case "TB":
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0
	}

	return uint64(value * multiplier)
}
