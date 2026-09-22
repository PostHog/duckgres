//go:build linux || darwin

package configstore_test

import (
	"errors"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
)

func TestTrinoDefaultCellIsVisibleWithEnablePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "tenant")
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{DefaultCellID: "registered:pool-a"}); err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListTrinoEnabledOrgs()
	if err != nil || len(rows) != 1 {
		t.Fatalf("enabled listing: rows=%+v err=%v", rows, err)
	}
	if rows[0].CellID != "registered:pool-a" {
		t.Fatalf("reconciler observed enabled tenant without default owner: %+v", rows[0])
	}
	if claimed, err := store.ClaimTrinoCell("tenant", "legacy-cell"); err != nil || claimed {
		t.Fatalf("legacy claimed default-placed tenant: claimed=%v err=%v", claimed, err)
	}
	if err := store.DisableTrino("tenant"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{DefaultCellID: "registered:pool-b"}); err != nil {
		t.Fatal(err)
	}
	if row := trinoRow(t, store, "tenant"); !row.Enabled || row.TrinoCellID != "registered:pool-a" {
		t.Fatalf("changed default moved existing tenant: %+v", row)
	}
}

func TestTrinoDefaultCellRollsBackWithProvisionTransactionPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "tenant")
	abort := errors.New("later provisioning step failed")
	err := store.DB().Transaction(func(tx *gorm.DB) error {
		if err := configstore.EnableTrinoInTransaction(tx, "tenant", configstore.TrinoSettings{DefaultCellID: "registered:pool-a"}); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	row, err := store.GetManagedWarehouseTrino("tenant")
	if err != nil || row != nil {
		t.Fatalf("failed transaction leaked enabled tenant/placement: row=%+v err=%v", row, err)
	}
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{DefaultCellID: "registered:pool-b"}); err != nil {
		t.Fatal(err)
	}
	if row := trinoRow(t, store, "tenant"); row.TrinoCellID != "registered:pool-b" {
		t.Fatalf("retry retained rolled-back placement: %+v", row)
	}
}
