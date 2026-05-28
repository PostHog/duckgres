package controlplane

import "testing"

func TestEffectiveSessionDefaultCommandUsesClientSearchPathBeforeConfiguredCatalog(t *testing.T) {
	got, source := effectiveSessionDefaultCommand("ducklake.main", "iceberg")
	if got != "SET search_path = 'ducklake.main,memory.main'" {
		t.Fatalf("command = %q, want SET search_path = 'ducklake.main,memory.main'", got)
	}
	if source != sessionSearchPathSourceClient {
		t.Fatalf("source = %q, want %q", source, sessionSearchPathSourceClient)
	}
}

func TestEffectiveSessionDefaultCommandUsesConfiguredIcebergCatalogWhenClientOmitted(t *testing.T) {
	got, source := effectiveSessionDefaultCommand("", "iceberg")
	if got != "USE iceberg.public" {
		t.Fatalf("command = %q, want USE iceberg.public", got)
	}
	if source != sessionDefaultSourceConfiguredCatalog {
		t.Fatalf("source = %q, want %q", source, sessionDefaultSourceConfiguredCatalog)
	}
}

func TestEffectiveSessionDefaultCommandReturnsEmptyWhenUnset(t *testing.T) {
	got, source := effectiveSessionDefaultCommand("", "")
	if got != "" {
		t.Fatalf("command = %q, want empty", got)
	}
	if source != "" {
		t.Fatalf("source = %q, want empty", source)
	}
}
