//go:build linux || darwin

package configstore_test

import (
	"errors"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioning"
)

func TestHoglakeLifecycleBlocksDeprovisionAndReplacement(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		store := newIsolatedConfigStore(t)
		seedTrinoOrg(t, store, "tenant")
		if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: "tenant", DucklingName: "tenant", State: configstore.ManagedWarehouseStateReady}).Error; err != nil {
			t.Fatal(err)
		}
		if err := store.EnableTrino("tenant", configstore.TrinoSettings{}); err != nil {
			t.Fatal(err)
		}
		if !enabled {
			if err := store.DisableTrino("tenant"); err != nil {
				t.Fatal(err)
			}
		}
		pstore := provisioning.NewGormStore(store)
		if err := pstore.SetWarehouseDeleting("tenant", configstore.ManagedWarehouseStateReady); !errors.Is(err, configstore.ErrHoglakeLifecycleProtected) {
			t.Fatalf("Hoglake warehouse deletion: %v", err)
		}
		if err := store.DB().Model(&configstore.ManagedWarehouse{}).Where("org_id = ?", "tenant").Update("state", configstore.ManagedWarehouseStateFailed).Error; err != nil {
			t.Fatal(err)
		}
		if err := pstore.CreatePendingWarehouse("tenant", "renamed", &configstore.ManagedWarehouse{DucklingName: "replacement"}); !errors.Is(err, configstore.ErrHoglakeLifecycleProtected) {
			t.Fatalf("Hoglake warehouse replacement: %v", err)
		}
	}
}

func TestHoglakeEnableRacesDeprovisionPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "racing-tenant")
	if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: "racing-tenant", DucklingName: "racing-tenant", State: configstore.ManagedWarehouseStateReady}).Error; err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	out := make(chan error, 2)
	go func() { <-start; out <- store.EnableTrino("racing-tenant", configstore.TrinoSettings{}) }()
	go func() {
		<-start
		out <- provisioning.NewGormStore(store).SetWarehouseDeleting("racing-tenant", configstore.ManagedWarehouseStateReady)
	}()
	close(start)
	a, b := <-out, <-out
	if (a == nil) == (b == nil) {
		t.Fatalf("exactly one lifecycle must win: %v / %v", a, b)
	}
	if a != nil && !errors.Is(a, configstore.ErrHoglakeLifecycleProtected) {
		t.Fatal(a)
	}
	if b != nil && !errors.Is(b, configstore.ErrHoglakeLifecycleProtected) {
		t.Fatal(b)
	}
}

func TestDuckLakeDeprovisionRemainsSupportedPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "ducklake-tenant")
	if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: "ducklake-tenant", DucklingName: "ducklake-tenant", State: configstore.ManagedWarehouseStateReady}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.DB().Create(&configstore.ManagedWarehouseTrino{OrgID: "ducklake-tenant", Backend: configstore.TrinoBackendDuckLake, BackendSelected: true}).Error; err != nil {
		t.Fatal(err)
	}
	if err := provisioning.NewGormStore(store).SetWarehouseDeleting("ducklake-tenant", configstore.ManagedWarehouseStateReady); err != nil {
		t.Fatal(err)
	}
}
