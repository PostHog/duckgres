//go:build linux || darwin

package configstore_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{DefaultCellID: "registered:default-pool"}); err != nil {
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

func TestTrinoSelectionRacesEnablementPostgres(t *testing.T) {
	for _, defaultCell := range []string{"", "registered:default-pool"} {
		t.Run("default="+defaultCell, func(t *testing.T) {
			testTrinoSelectionRace(t, defaultCell)
		})
	}
}

func testTrinoSelectionRace(t *testing.T, defaultCell string) {
	t.Helper()
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
			if claimErr = store.EnableTrino(org, configstore.TrinoSettings{DefaultCellID: defaultCell}); claimErr == nil {
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
		} else {
			wantOwner, wantClaim := defaultCell, false
			if defaultCell == "" {
				wantOwner, wantClaim = "cell-001", true
			}
			if claimed != wantClaim || row.TrinoCellID != wantOwner {
				t.Fatalf("enablement winner not authoritative: claim=%v row=%+v", claimed, row)
			}
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

// A move is a compare-and-swap on the owner: it applies only while the org is
// on the named source, resets readiness so nothing reports the org ready
// before the destination provisions it, and is idempotent when repeated.
func TestTrinoMoveCellPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "tenant")
	if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: "tenant", DucklingName: "tenant"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{}); err != nil {
		t.Fatal(err)
	}
	if claimed, err := store.ClaimTrinoCell("tenant", "cell-001"); err != nil || !claimed {
		t.Fatalf("legacy claim: %v %v", claimed, err)
	}
	now := time.Now().UTC()
	if err := store.UpdateTrinoState("tenant", configstore.TrinoStateUpdate{State: configstore.ManagedWarehouseStateReady, ReadyAt: &now}); err != nil {
		t.Fatal(err)
	}

	if err := store.MoveTrinoCell("tenant", "registered:other", "registered:cell-001"); !errors.Is(err, configstore.ErrTrinoCellMoveConflict) {
		t.Fatalf("move from a cell that does not own the org: %v, want conflict", err)
	}
	if err := store.MoveTrinoCell("missing", "cell-001", "registered:cell-001"); !errors.Is(err, configstore.ErrTrinoWarehouseNotFound) {
		t.Fatalf("move of an org with no Trino row: %v, want not found", err)
	}
	if err := store.MoveTrinoCell("tenant", "cell-001", "registered:cell-001"); err != nil {
		t.Fatalf("move: %v", err)
	}
	row := trinoRow(t, store, "tenant")
	if row.TrinoCellID != "registered:cell-001" || !row.Enabled || row.State != configstore.ManagedWarehouseStatePending || row.ReadyAt != nil || row.FailedAt != nil {
		t.Fatalf("moved row = %+v, want enabled, on the destination, pending with readiness cleared", row)
	}
	if err := store.MoveTrinoCell("tenant", "cell-001", "registered:cell-001"); err != nil {
		t.Fatalf("repeated move must be a no-op: %v", err)
	}

	// The source cell's reconcile tick may have listed the org before the
	// move. Its fenced state write must not land on the moved row.
	if err := store.UpdateTrinoState("tenant", configstore.TrinoStateUpdate{State: configstore.ManagedWarehouseStateReady, ReadyAt: &now, CellID: "cell-001"}); err != nil {
		t.Fatal(err)
	}
	if row := trinoRow(t, store, "tenant"); row.State != configstore.ManagedWarehouseStatePending || row.ReadyAt != nil {
		t.Fatalf("stale source write reached the moved row: %+v", row)
	}
	// The destination's write does.
	if err := store.UpdateTrinoState("tenant", configstore.TrinoStateUpdate{State: configstore.ManagedWarehouseStateReady, ReadyAt: &now, CellID: "registered:cell-001"}); err != nil {
		t.Fatal(err)
	}
	if row := trinoRow(t, store, "tenant"); row.State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("destination write did not land: %+v", row)
	}

	// The legacy claim path cannot take the moved org back.
	if claimed, err := store.ClaimTrinoCell("tenant", "cell-001"); err != nil || claimed {
		t.Fatalf("legacy reclaimed a moved org: %v %v", claimed, err)
	}
	// And moving back is the same operation in the other direction.
	if err := store.MoveTrinoCell("tenant", "registered:cell-001", "cell-001"); err != nil {
		t.Fatalf("move back: %v", err)
	}
	if row := trinoRow(t, store, "tenant"); row.TrinoCellID != "cell-001" || row.State != configstore.ManagedWarehouseStatePending {
		t.Fatalf("moved-back row = %+v", row)
	}
}

// Moves racing each other: exactly one of two moves from the same source to
// different destinations wins, and the loser sees a conflict.
func TestTrinoMoveCellRacesPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	for i := 0; i < 10; i++ {
		org := fmt.Sprintf("tenant-%d", i)
		seedTrinoOrg(t, store, org)
		if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: org, DucklingName: org}).Error; err != nil {
			t.Fatal(err)
		}
		if err := store.EnableTrino(org, configstore.TrinoSettings{}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimTrinoCell(org, "cell-001"); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for j, to := range []string{"registered:a", "registered:b"} {
			wg.Add(1)
			go func(j int, to string) {
				defer wg.Done()
				<-start
				errs[j] = store.MoveTrinoCell(org, "cell-001", to)
			}(j, to)
		}
		close(start)
		wg.Wait()
		wins := 0
		for _, err := range errs {
			switch {
			case err == nil:
				wins++
			case errors.Is(err, configstore.ErrTrinoCellMoveConflict):
			default:
				t.Fatalf("%s: unexpected error %v", org, err)
			}
		}
		if wins != 1 {
			t.Fatalf("%s: %d moves won, want exactly one (%v)", org, wins, errs)
		}
	}
}
