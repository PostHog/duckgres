//go:build kubernetes

package controlplane

import (
	"context"
	"testing"

	"github.com/posthog/duckgres/controlplane/provisioner"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// A pooled cell gets the SAME storage inputs a legacy cell gets.
//
// Managed Hoglake needs two things the pool has no opinion about: the service
// configuration, and the resolver that reads the tenant's storage identity.
// They are wired once, for every cell, and a pooled cell that silently lost
// either would build, start and reconcile normally - right up to the first
// Hoglake tenant, which would then sit pending citing configuration nobody
// changed.
func TestPooledCellCarriesTheManagedHoglakeInputs(t *testing.T) {
	t.Setenv(envTrinoFilesystemCacheEnabled, "false")
	t.Setenv(envTrinoManagedHoglakeURI, "http://hoglake.example:8080")
	t.Setenv(envTrinoHoglakeDataPath, "s3://example-bucket/trino/")
	t.Setenv(envTrinoHoglakeNamespace, "")

	store := &poolObserverWiringStore{fleetBootstrapStore: &fleetBootstrapStore{initialized: map[string]bool{}}}
	kc := kubefake.NewClientset()
	ducklings := func(context.Context, string) (*provisioner.DucklingStatus, error) { return nil, nil }
	storage := func(context.Context, string) (*provisioner.DucklingStatus, error) { return nil, nil }

	for _, cell := range []trinoCell{
		{ID: "cell-legacy", Namespace: "legacy", CoordinatorURL: "https://legacy.example.test"},
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
