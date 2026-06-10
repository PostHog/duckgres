//go:build kubernetes

package controlplane

import (
	"testing"
	"time"
)

func TestEffectiveHotIdleTTL(t *testing.T) {
	if got := effectiveHotIdleTTL(0); got != defaultHotIdleTTL {
		t.Fatalf("expected unset hot-idle TTL to fall back to %s, got %s", defaultHotIdleTTL, got)
	}
	if got := effectiveHotIdleTTL(70 * time.Minute); got != 70*time.Minute {
		t.Fatalf("expected configured hot-idle TTL 70m to win, got %s", got)
	}
	if got := effectiveHotIdleTTL(-time.Minute); got != defaultHotIdleTTL {
		t.Fatalf("expected negative hot-idle TTL to fall back to default, got %s", got)
	}
}
