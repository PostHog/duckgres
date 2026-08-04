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
