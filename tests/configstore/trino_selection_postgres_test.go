//go:build linux || darwin

package configstore_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTrinoInitialSelectionPreservesOwnershipPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "tenant")
	if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: "tenant", DucklingName: "tenant"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.SelectTrinoCell("tenant", "registered:cell-001"); err != nil {
		t.Fatal(err)
	}
	if row := trinoRow(t, store, "tenant"); row.Enabled || row.TrinoCellID != "registered:cell-001" {
		t.Fatalf("selection enabled or misassigned tenant: %+v", row)
	}
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{}); err != nil {
		t.Fatal(err)
	}
	if claimed, err := store.ClaimTrinoCell("tenant", "cell-001"); err != nil || claimed {
		t.Fatalf("legacy stole selected tenant: %v %v", claimed, err)
	}
	if err := store.DisableTrino("tenant"); err != nil {
		t.Fatal(err)
	}
	if err := store.SelectTrinoCell("tenant", "cell-001"); !errors.Is(err, configstore.ErrTrinoCellSelectionConflict) {
		t.Fatalf("disabled ownership must be immutable: %v", err)
	}
	if err := store.SelectTrinoCell("tenant", "registered:cell-001"); err != nil {
		t.Fatalf("same selection must be idempotent: %v", err)
	}
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{}); err != nil {
		t.Fatal(err)
	}
	if row := trinoRow(t, store, "tenant"); !row.Enabled || row.TrinoCellID != "registered:cell-001" {
		t.Fatalf("reenable lost selection: %+v", row)
	}
}

func TestTrinoSelectionRacesFirstLegacyClaimPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	for i := 0; i < 20; i++ {
		org := fmt.Sprintf("tenant-%d", i)
		seedTrinoOrg(t, store, org)
		if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: org, DucklingName: org}).Error; err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		var selectionErr, claimErr error
		var claimed bool
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			selectionErr = store.SelectTrinoCell(org, "registered:cell-001")
		}()
		go func() {
			defer wg.Done()
			<-start
			if claimErr = store.EnableTrino(org, configstore.TrinoSettings{}); claimErr == nil {
				claimed, claimErr = store.ClaimTrinoCell(org, "cell-001")
			}
		}()
		close(start)
		wg.Wait()
		if claimErr != nil || (selectionErr != nil && !errors.Is(selectionErr, configstore.ErrTrinoCellSelectionConflict)) {
			t.Fatalf("race failed: claim=%v selection=%v", claimErr, selectionErr)
		}
		row := trinoRow(t, store, org)
		if selectionErr == nil {
			if claimed || row.TrinoCellID != "registered:cell-001" {
				t.Fatalf("successful selection lost ownership: claim=%v row=%+v", claimed, row)
			}
		} else if !claimed || row.TrinoCellID != "cell-001" {
			t.Fatalf("legacy winner not authoritative: claim=%v row=%+v", claimed, row)
		}
	}
}

func TestTrinoSelectionRejectsUnknownWarehouseAndEnabledRowPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	if err := store.SelectTrinoCell("missing", "cell-001"); !errors.Is(err, configstore.ErrTrinoWarehouseNotFound) {
		t.Fatalf("unknown warehouse: %v", err)
	}
	seedTrinoOrg(t, store, "tenant")
	if err := store.SelectTrinoCell("tenant", "cell-001"); !errors.Is(err, configstore.ErrTrinoWarehouseNotFound) {
		t.Fatalf("org without warehouse: %v", err)
	}
	if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: "tenant", DucklingName: "tenant"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{}); err != nil {
		t.Fatal(err)
	}
	if err := store.SelectTrinoCell("tenant", "registered:cell-001"); !errors.Is(err, configstore.ErrTrinoCellSelectionConflict) {
		t.Fatalf("selection after enable must fail: %v", err)
	}
	if claimed, err := store.ClaimTrinoCell("tenant", "cell-001"); err != nil || !claimed {
		t.Fatalf("first claim: %v %v", claimed, err)
	}
	if claimed, err := store.ClaimTrinoCell("tenant", "registered:cell-001"); err != nil || claimed {
		t.Fatalf("second claim: %v %v", claimed, err)
	}
}
