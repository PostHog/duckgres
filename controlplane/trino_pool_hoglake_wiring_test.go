//go:build kubernetes

package controlplane

import (
	"context"
	"strconv"
	"testing"

	"github.com/posthog/duckgres/controlplane/provisioner"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// A pooled cell receives the managed Hoglake service and storage inputs.
//
// Managed Hoglake needs two things the pool has no opinion about: the service
// configuration, and the resolver that reads the tenant's storage identity.
// They are wired once, for every cell, and a pooled cell that silently lost
// either would build, start and reconcile normally - right up to the first
// Hoglake tenant, which would then sit pending citing configuration nobody
// changed.
func TestPooledCellCarriesTheManagedHoglakeInputs(t *testing.T) {
	t.Setenv(envTrinoFilesystemCacheEnabled, "false")
	t.Setenv(envTrinoHoglakeFilesystemCacheEnabled, "false")
	t.Setenv(envTrinoManagedHoglakeURI, "http://hoglake.example:8080")
	t.Setenv(envTrinoHoglakeDataPath, "s3://example-bucket/trino/")
	t.Setenv(envTrinoHoglakeNamespace, "")

	store := &poolObserverWiringStore{fleetBootstrapStore: &fleetBootstrapStore{initialized: map[string]bool{}}}
	kc := kubefake.NewClientset()
	ducklings := func(context.Context, string) (*provisioner.DucklingStatus, error) { return nil, nil }
	storage := func(context.Context, string) (*provisioner.DucklingStatus, error) { return nil, nil }

	for _, cell := range []trinoCell{
		{ID: registeredTrinoCellPrefix + "cell-pool", PublicID: "cell-pool", Namespace: "pooled", Mode: trinoPoolModeShared},
	} {
		wire, err := buildTrinoCellWiring(store, kc, ducklings, cell, storage)
		if err != nil {
			t.Fatalf("wire cell %s: %v", cell.ID, err)
		}
		if !wire.Provisioner.ManagedHoglakeConfigured() {
			t.Fatalf("cell %s cannot provision a managed Hoglake tenant: the service configuration or the storage resolver was dropped", cell.ID)
		}
	}
}

// A pooled cell carries each filesystem cache setting from its own variable.
//
// DuckLake catalogs follow DUCKGRES_TRINO_FILESYSTEM_CACHE_ENABLED and managed
// Hoglake catalogs follow DUCKGRES_TRINO_HOGLAKE_FILESYSTEM_CACHE_ENABLED. A
// wiring that crossed, merged or dropped either would build and reconcile
// normally, then create every new catalog of that backend in the wrong cache
// mode. Opposite values in the two variables catch each of those.
func TestPooledCellCarriesBothFilesystemCacheSettings(t *testing.T) {
	for _, tc := range []struct{ ducklake, hoglake bool }{
		{ducklake: true, hoglake: false},
		{ducklake: false, hoglake: true},
	} {
		t.Run("ducklake="+strconv.FormatBool(tc.ducklake)+",hoglake="+strconv.FormatBool(tc.hoglake), func(t *testing.T) {
			t.Setenv(envTrinoFilesystemCacheEnabled, strconv.FormatBool(tc.ducklake))
			t.Setenv(envTrinoHoglakeFilesystemCacheEnabled, strconv.FormatBool(tc.hoglake))
			t.Setenv(envTrinoManagedHoglakeURI, "http://hoglake.example:8080")
			t.Setenv(envTrinoHoglakeDataPath, "s3://example-bucket/trino/")
			t.Setenv(envTrinoHoglakeNamespace, "")

			store := &poolObserverWiringStore{fleetBootstrapStore: &fleetBootstrapStore{initialized: map[string]bool{}}}
			kc := kubefake.NewClientset()
			ducklings := func(context.Context, string) (*provisioner.DucklingStatus, error) { return nil, nil }
			storage := func(context.Context, string) (*provisioner.DucklingStatus, error) { return nil, nil }
			cell := trinoCell{ID: registeredTrinoCellPrefix + "cell-pool", PublicID: "cell-pool", Namespace: "pooled", Mode: trinoPoolModeShared}

			wire, err := buildTrinoCellWiring(store, kc, ducklings, cell, storage)
			if err != nil {
				t.Fatalf("wire cell %s: %v", cell.ID, err)
			}
			ducklake, hoglake := wire.Provisioner.FilesystemCacheSettings()
			if ducklake != tc.ducklake || hoglake != tc.hoglake {
				t.Fatalf("cell %s cache settings: DuckLake %t, Hoglake %t; want DuckLake %t, Hoglake %t", cell.ID, ducklake, hoglake, tc.ducklake, tc.hoglake)
			}
		})
	}
}
