package controlplane

import "testing"

func TestEffectiveSessionDefaultCommandUsesClientSearchPathBeforeCatalog(t *testing.T) {
	got, source := effectiveSessionDefaultCommand("ducklake.main", "iceberg")
	if got != "SET search_path = 'ducklake.main,memory.main'" {
		t.Fatalf("command = %q, want SET search_path = 'ducklake.main,memory.main'", got)
	}
	if source != sessionSearchPathSourceClient {
		t.Fatalf("source = %q, want %q", source, sessionSearchPathSourceClient)
	}
}

func TestEffectiveSessionDefaultCommandUsesIcebergCatalogWhenClientOmitted(t *testing.T) {
	got, source := effectiveSessionDefaultCommand("", "iceberg")
	if got != "USE iceberg.public" {
		t.Fatalf("command = %q, want USE iceberg.public", got)
	}
	if source != sessionDefaultSourceConfiguredCatalog {
		t.Fatalf("source = %q, want %q", source, sessionDefaultSourceConfiguredCatalog)
	}
}

func TestEffectiveSessionDefaultCommandEmptyForDuckLake(t *testing.T) {
	// DuckLake's catalog switch is owned by InitSessionDatabaseMetadata's defer,
	// so the connect-time command for a ducklake session is empty.
	got, source := effectiveSessionDefaultCommand("", "ducklake")
	if got != "" {
		t.Fatalf("command = %q, want empty", got)
	}
	if source != "" {
		t.Fatalf("source = %q, want empty", source)
	}
}

func TestPassthroughSessionDefaultCatalogCommand(t *testing.T) {
	tests := []struct {
		name            string
		effectiveCatalog string
		want            string
	}{
		{name: "ducklake selected", effectiveCatalog: "ducklake", want: "USE ducklake"},
		{name: "iceberg selected", effectiveCatalog: "iceberg", want: "USE iceberg.public"},
		{name: "nothing resolved leaves session as-is", effectiveCatalog: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := passthroughSessionDefaultCatalogCommand(tt.effectiveCatalog); got != tt.want {
				t.Fatalf("command = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveEffectiveCatalog(t *testing.T) {
	tests := []struct {
		name           string
		requested      string
		defaultCatalog string
		duckLake       bool
		iceberg        bool
		want           string
		wantOK         bool
	}{
		{name: "explicit ducklake attached", requested: "ducklake", duckLake: true, iceberg: true, want: "ducklake", wantOK: true},
		{name: "explicit iceberg attached", requested: "iceberg", duckLake: true, iceberg: true, want: "iceberg", wantOK: true},
		{name: "explicit ducklake not attached", requested: "ducklake", duckLake: false, iceberg: true, want: "", wantOK: false},
		{name: "explicit iceberg not attached", requested: "iceberg", duckLake: true, iceberg: false, want: "", wantOK: false},
		{name: "default prefers ducklake", requested: "", duckLake: true, iceberg: true, want: "ducklake", wantOK: true},
		{name: "default honors per-user iceberg", requested: "", defaultCatalog: "iceberg", duckLake: true, iceberg: true, want: "iceberg", wantOK: true},
		{name: "configured iceberg default not attached fails closed", requested: "", defaultCatalog: "iceberg", duckLake: true, iceberg: false, want: "", wantOK: false},
		{name: "default falls back to iceberg-only", requested: "", duckLake: false, iceberg: true, want: "iceberg", wantOK: true},
		{name: "nothing attached fails", requested: "", duckLake: false, iceberg: false, want: "", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := resolveEffectiveCatalog(tt.requested, tt.defaultCatalog, tt.duckLake, tt.iceberg)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("resolveEffectiveCatalog = (%q, %v), want (%q, %v)", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}
