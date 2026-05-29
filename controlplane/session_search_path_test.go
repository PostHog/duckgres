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

func TestPassthroughSessionDefaultCatalogCommand(t *testing.T) {
	tests := []struct {
		name             string
		defaultCatalog   string
		duckLakeAttached bool
		want             string
	}{
		{name: "ducklake attached defaults to ducklake", defaultCatalog: "", duckLakeAttached: true, want: "USE ducklake"},
		{name: "iceberg-default user prefers iceberg over ducklake", defaultCatalog: "iceberg", duckLakeAttached: true, want: "USE iceberg.public"},
		{name: "iceberg-default user with no ducklake", defaultCatalog: "iceberg", duckLakeAttached: false, want: "USE iceberg.public"},
		{name: "no ducklake and no configured catalog leaves session as-is", defaultCatalog: "", duckLakeAttached: false, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := passthroughSessionDefaultCatalogCommand(tt.defaultCatalog, tt.duckLakeAttached); got != tt.want {
				t.Fatalf("command = %q, want %q", got, tt.want)
			}
		})
	}
}
