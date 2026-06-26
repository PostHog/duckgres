//go:build kubernetes

package controlplane

import (
	"testing"
	"time"
)

func TestEffectiveDefaultWorkerTTL(t *testing.T) {
	if got := effectiveDefaultWorkerTTL(0); got != defaultWorkerTTL {
		t.Fatalf("expected unset hot-idle TTL to fall back to %s, got %s", defaultWorkerTTL, got)
	}
	if got := effectiveDefaultWorkerTTL(70 * time.Minute); got != 70*time.Minute {
		t.Fatalf("expected configured hot-idle TTL 70m to win, got %s", got)
	}
	if got := effectiveDefaultWorkerTTL(-time.Minute); got != defaultWorkerTTL {
		t.Fatalf("expected negative hot-idle TTL to fall back to default, got %s", got)
	}
}

// TestEffectiveDrainingInstanceExpiry guards the draining-CP-row reaper against
// ever being disabled by an unset/zero cap — the exact regression where wiring
// janitor.maxDrainTimeout to HandoverDrainTimeout (0 in remote mode) silently
// turned the reaper off (`if maxDrainTimeout > 0` in the janitor), stranding a
// draining CP's orphaned workers. The resolver must NEVER return 0.
func TestEffectiveDrainingInstanceExpiry(t *testing.T) {
	cases := []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{"unset falls back to default", 0, defaultDrainingInstanceExpiry},
		{"negative falls back to default", -time.Minute, defaultDrainingInstanceExpiry},
		{"configured value wins", 6 * time.Hour, 6 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := effectiveDrainingInstanceExpiry(tc.configured)
			if got != tc.want {
				t.Fatalf("effectiveDrainingInstanceExpiry(%s) = %s, want %s", tc.configured, got, tc.want)
			}
			if got <= 0 {
				t.Fatalf("effectiveDrainingInstanceExpiry(%s) returned non-positive %s — reaper would be disabled", tc.configured, got)
			}
		})
	}
}
