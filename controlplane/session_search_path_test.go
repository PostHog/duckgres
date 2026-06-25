package controlplane

import "testing"

func TestEffectiveSessionDefaultCommandDuckLakeClientSearchPathOnly(t *testing.T) {
	// DuckLake's catalog switch is owned by InitSessionDatabaseMetadata's defer,
	// so a client search_path is applied alone and best-effort.
	got, source := effectiveSessionDefaultCommand("analytics", "ducklake")
	if got != "SET search_path = 'analytics,memory.main'" {
		t.Fatalf("command = %q, want SET search_path = 'analytics,memory.main'", got)
	}
	if source != sessionSearchPathSourceClient {
		t.Fatalf("source = %q, want %q", source, sessionSearchPathSourceClient)
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
		name             string
		effectiveCatalog string
		want             string
	}{
		{name: "ducklake selected", effectiveCatalog: "ducklake", want: "USE ducklake"},
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
		want           string
		wantOK         bool
	}{
		{name: "explicit ducklake attached", requested: "ducklake", duckLake: true, want: "ducklake", wantOK: true},
		{name: "explicit ducklake not attached", requested: "ducklake", duckLake: false, want: "", wantOK: false},
		{name: "default uses ducklake", requested: "", duckLake: true, want: "ducklake", wantOK: true},
		{name: "configured ducklake default attached", requested: "", defaultCatalog: "ducklake", duckLake: true, want: "ducklake", wantOK: true},
		{name: "configured ducklake default not attached fails closed", requested: "", defaultCatalog: "ducklake", duckLake: false, want: "", wantOK: false},
		{name: "nothing attached fails", requested: "", duckLake: false, want: "", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := resolveEffectiveCatalog(tt.requested, tt.defaultCatalog, tt.duckLake)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("resolveEffectiveCatalog = (%q, %v), want (%q, %v)", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}
