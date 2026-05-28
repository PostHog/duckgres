package controlplane

import "testing"

func TestEffectiveSessionSearchPathUsesClientBeforeConfiguredDefault(t *testing.T) {
	got, source := effectiveSessionSearchPath("ducklake.main", "iceberg.main")
	if got != "ducklake.main,memory.main" {
		t.Fatalf("search path = %q, want ducklake.main,memory.main", got)
	}
	if source != sessionSearchPathSourceClient {
		t.Fatalf("source = %q, want %q", source, sessionSearchPathSourceClient)
	}
}

func TestEffectiveSessionSearchPathUsesConfiguredDefaultWhenClientOmitted(t *testing.T) {
	got, source := effectiveSessionSearchPath("", "iceberg.main,memory.main")
	if got != "iceberg.main,memory.main" {
		t.Fatalf("search path = %q, want iceberg.main,memory.main", got)
	}
	if source != sessionSearchPathSourceConfiguredDefault {
		t.Fatalf("source = %q, want %q", source, sessionSearchPathSourceConfiguredDefault)
	}
}

func TestEffectiveSessionSearchPathReturnsEmptyWhenUnset(t *testing.T) {
	got, source := effectiveSessionSearchPath("", "")
	if got != "" {
		t.Fatalf("search path = %q, want empty", got)
	}
	if source != "" {
		t.Fatalf("source = %q, want empty", source)
	}
}
