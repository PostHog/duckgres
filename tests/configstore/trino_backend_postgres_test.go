//go:build linux || darwin

package configstore_test

import (
	"errors"
	"github.com/posthog/duckgres/controlplane/configstore"
	"sync"
	"testing"
)

func TestTrinoBackendSelectionSurvivesDisablePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "tenant")
	if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: "tenant", DucklingName: "tenant"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.SelectTrinoCell("tenant", "registered:test-cell"); err != nil {
		t.Fatal(err)
	}
	if row := trinoRow(t, store, "tenant"); row.BackendSelected {
		t.Fatal("cell selection must not choose backend")
	}
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{}); err != nil {
		t.Fatal(err)
	}
	if err := store.DisableTrino("tenant"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{Backend: configstore.TrinoBackendDuckLake}); !errors.Is(err, configstore.ErrTrinoBackendSelectionConflict) {
		t.Fatalf("backend change: %v", err)
	}
	if row := trinoRow(t, store, "tenant"); row.Enabled {
		t.Fatal("conflicting enable changed row")
	}
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{Tier: "premium"}); err != nil {
		t.Fatal(err)
	}
	if row := trinoRow(t, store, "tenant"); row.Backend != configstore.TrinoBackendHoglake || !row.BackendSelected || row.Tier != "premium" {
		t.Fatalf("lost selection: %+v", row)
	}
	rows, err := store.ListTrinoEnabledOrgs()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Backend != configstore.TrinoBackendHoglake {
		t.Fatalf("listing: %+v", rows)
	}
}

func TestTrinoBackendConcurrentSelectionPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "tenant")
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, backend := range []configstore.TrinoBackend{configstore.TrinoBackendDuckLake, configstore.TrinoBackendHoglake} {
		wg.Add(1)
		go func(b configstore.TrinoBackend) {
			defer wg.Done()
			<-start
			results <- store.EnableTrino("tenant", configstore.TrinoSettings{Backend: b})
		}(backend)
	}
	close(start)
	wg.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, configstore.ErrTrinoBackendSelectionConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestTrinoBackendOldBinaryEnablePinsNewCellSelectionPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "mixed-version")
	if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: "mixed-version", DucklingName: "mixed-version"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.SelectTrinoCell("mixed-version", "registered:test-cell"); err != nil {
		t.Fatal(err)
	}
	if trinoRow(t, store, "mixed-version").BackendSelected {
		t.Fatal("initial cell selection chose a backend")
	}
	// A replica running the previous binary knows only enabled/tier. Then
	// disable through the new replica, leaving no enabled flag as evidence.
	if err := store.DB().Exec(`UPDATE duckgres_managed_warehouse_trino SET enabled=TRUE, tier='free' WHERE org_id='mixed-version'`).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.DisableTrino("mixed-version"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnableTrino("mixed-version", configstore.TrinoSettings{Backend: configstore.TrinoBackendHoglake}); !errors.Is(err, configstore.ErrTrinoBackendSelectionConflict) {
		t.Fatalf("mixed-version backend change: %v", err)
	}
	if row := trinoRow(t, store, "mixed-version"); row.Enabled || row.Backend != configstore.TrinoBackendDuckLake || !row.BackendSelected {
		t.Fatalf("lost old binary backend ownership: %+v", row)
	}
}

func TestTrinoBackendNewClientsUseHoglakePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "new-tenant")
	if err := store.EnableTrino("new-tenant", configstore.TrinoSettings{}); err != nil {
		t.Fatal(err)
	}
	if row := trinoRow(t, store, "new-tenant"); row.Backend != configstore.TrinoBackendHoglake || !row.BackendSelected {
		t.Fatalf("new client did not use Hoglake: %+v", row)
	}
	seedTrinoOrg(t, store, "rejected-tenant")
	if err := store.EnableTrino("rejected-tenant", configstore.TrinoSettings{Backend: configstore.TrinoBackendDuckLake}); !errors.Is(err, configstore.ErrTrinoBackendSelectionConflict) {
		t.Fatalf("new DuckLake client accepted: %v", err)
	}
	if row, err := store.GetManagedWarehouseTrino("rejected-tenant"); err != nil || row != nil {
		t.Fatalf("rejection persisted a row: %+v %v", row, err)
	}
}

func TestTrinoBackendExistingDuckLakePreservedPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "existing-tenant")
	if err := store.DB().Create(&configstore.ManagedWarehouseTrino{OrgID: "existing-tenant", Enabled: true, Backend: configstore.TrinoBackendDuckLake, BackendSelected: true, State: configstore.ManagedWarehouseStateReady}).Error; err != nil {
		t.Fatal(err)
	}
	for _, backend := range []configstore.TrinoBackend{"", configstore.TrinoBackendDuckLake} {
		if err := store.DisableTrino("existing-tenant"); err != nil {
			t.Fatal(err)
		}
		if err := store.EnableTrino("existing-tenant", configstore.TrinoSettings{Backend: backend}); err != nil {
			t.Fatal(err)
		}
		if row := trinoRow(t, store, "existing-tenant"); row.Backend != configstore.TrinoBackendDuckLake || !row.BackendSelected {
			t.Fatalf("existing client changed: %+v", row)
		}
	}
	if err := store.EnableTrino("existing-tenant", configstore.TrinoSettings{Backend: configstore.TrinoBackendHoglake}); !errors.Is(err, configstore.ErrTrinoBackendSelectionConflict) {
		t.Fatalf("existing client switched: %v", err)
	}
}
