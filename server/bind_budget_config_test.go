package server

import "testing"

func TestInitMinimalServerDefaultsBindPortalBudgets(t *testing.T) {
	s := &Server{}
	InitMinimalServer(s, Config{}, nil)

	if got, want := s.cfg.MaxRetainedBindBytes, int64(64<<20); got != want {
		t.Fatalf("MaxRetainedBindBytes = %d, want %d", got, want)
	}
	if got, want := s.cfg.MaxOpenPortals, 1024; got != want {
		t.Fatalf("MaxOpenPortals = %d, want %d", got, want)
	}
}

func TestNormalizeBindPortalBudgetsPreservesPositiveValues(t *testing.T) {
	cfg := Config{MaxRetainedBindBytes: 1 << 20, MaxOpenPortals: 32}
	normalizeBindPortalBudgets(&cfg)

	if got, want := cfg.MaxRetainedBindBytes, int64(1<<20); got != want {
		t.Fatalf("MaxRetainedBindBytes = %d, want %d", got, want)
	}
	if got, want := cfg.MaxOpenPortals, 32; got != want {
		t.Fatalf("MaxOpenPortals = %d, want %d", got, want)
	}
}

func TestNormalizeBindPortalBudgetsReplacesNonPositiveValues(t *testing.T) {
	cfg := Config{MaxRetainedBindBytes: -1, MaxOpenPortals: 0}
	normalizeBindPortalBudgets(&cfg)

	if got, want := cfg.MaxRetainedBindBytes, DefaultMaxRetainedBindBytes; got != want {
		t.Fatalf("MaxRetainedBindBytes = %d, want default %d", got, want)
	}
	if got, want := cfg.MaxOpenPortals, DefaultMaxOpenPortals; got != want {
		t.Fatalf("MaxOpenPortals = %d, want default %d", got, want)
	}
}
