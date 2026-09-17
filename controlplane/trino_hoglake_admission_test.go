//go:build kubernetes

package controlplane

import (
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTrinoManagedHoglakeBackendAdmission(t *testing.T) {
	t.Setenv("DUCKGRES_TRINO_MANAGED_HOGLAKE_URI", "")
	t.Setenv("DUCKGRES_TRINO_HOGLAKE_DATA_PATH", "")
	t.Setenv("DUCKGRES_TRINO_HOGLAKE_NAMESPACE", "")
	if err := validateTrinoBackendAvailability(configstore.TrinoBackendDuckLake); err != nil {
		t.Fatalf("default DuckLake backend unavailable: %v", err)
	}
	if err := validateTrinoBackendAvailability(configstore.TrinoBackendHoglake); err == nil {
		t.Fatal("Hoglake admission succeeded without managed configuration")
	}
	t.Setenv("DUCKGRES_TRINO_MANAGED_HOGLAKE_URI", "https://lake.example")
	t.Setenv("DUCKGRES_TRINO_HOGLAKE_DATA_PATH", "s3://example-lake/trino/")
	if err := validateTrinoBackendAvailability(configstore.TrinoBackendHoglake); err != nil {
		t.Fatalf("configured Hoglake backend unavailable: %v", err)
	}
	t.Setenv("DUCKGRES_TRINO_HOGLAKE_DATA_PATH", "s3://example-lake/")
	if err := validateTrinoBackendAvailability(configstore.TrinoBackendHoglake); err == nil {
		t.Fatal("Hoglake admission accepted invalid configuration")
	}
}
