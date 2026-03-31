package k8s_test

import (
	"strings"
	"testing"
	"time"
)

func TestFormatWorkerRetirementTimeoutDiagnostic(t *testing.T) {
	got := formatWorkerRetirementTimeoutDiagnostic(
		"duckgres-worker-17",
		30*time.Second,
		"17|duckgres-worker-17|analytics|hot|2026-03-31 16:12:56+00",
		"duckgres-worker-17 Running",
		"time=2026-03-31T16:12:55Z level=INFO msg=\"Client disconnected.\"",
	)

	for _, want := range []string{
		"worker pod duckgres-worker-17 did not reach retired state within 30s",
		"runtime record:",
		"17|duckgres-worker-17|analytics|hot|2026-03-31 16:12:56+00",
		"worker pods:",
		"duckgres-worker-17 Running",
		"control-plane logs:",
		"Client disconnected.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected diagnostic to contain %q, got %q", want, got)
		}
	}
}
