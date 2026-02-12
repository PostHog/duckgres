package server

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// totalSystemMemoryBytes reads total physical memory from /proc/meminfo on Linux.
// Returns 0 if the file cannot be read or parsed (e.g., on non-Linux systems).
func totalSystemMemoryBytes() uint64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()

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

// autoMemoryLimit computes a DuckDB memory_limit based on system memory.
// Formula: totalMem * 0.75 / 4, with a floor of 256MB.
// The /4 accounts for typical concurrency (multiple sessions sharing RAM).
// Returns "4GB" as a safe default if system memory cannot be detected.
func autoMemoryLimit() string {
	totalBytes := totalSystemMemoryBytes()
	if totalBytes == 0 {
		return "4GB"
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
		return fmt.Sprintf("%dGB", limitGB)
	}
	limitMB := limitBytes / mb
	return fmt.Sprintf("%dMB", limitMB)
}
