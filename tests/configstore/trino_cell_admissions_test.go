//go:build linux || darwin

package configstore_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func seedTrinoAdmission(t *testing.T, store *configstore.ConfigStore, org, cell string, state configstore.ManagedWarehouseProvisioningState, previouslyReady bool) {
	t.Helper()
	seedTrinoOrg(t, store, org)
	if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: org, DucklingName: org, State: configstore.ManagedWarehouseStateReady}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.SelectTrinoCell(org, cell); err != nil {
		t.Fatal(err)
	}
	if err := store.EnableTrino(org, configstore.TrinoSettings{}); err != nil {
		t.Fatal(err)
	}
	update := configstore.TrinoStateUpdate{State: state}
	if previouslyReady {
		now := time.Now().UTC()
		update.ReadyAt = &now
	}
	if err := store.UpdateTrinoState(org, update); err != nil {
		t.Fatal(err)
	}
}

func TestTrinoAdmittedRosterHistoryAndMembershipPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	const cell = "registered:cell-test"
	seedTrinoAdmission(t, store, "historical", cell, configstore.ManagedWarehouseStateFailed, true)
	seedTrinoAdmission(t, store, "current", cell, configstore.ManagedWarehouseStateReady, false)
	seedTrinoAdmission(t, store, "pending", cell, configstore.ManagedWarehouseStatePending, false)
	seedTrinoAdmission(t, store, "never-ready-failed", cell, configstore.ManagedWarehouseStateFailed, false)
	seedTrinoAdmission(t, store, "disabled", cell, configstore.ManagedWarehouseStateReady, true)
	seedTrinoAdmission(t, store, "other", "registered:other-cell", configstore.ManagedWarehouseStateReady, true)
	if err := store.DisableTrino("disabled"); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().Exec("DELETE FROM duckgres_org_users WHERE org_id = ?", "historical").Error; err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListAdmittedTrinoOrgs(context.Background(), cell)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, row := range rows {
		names = append(names, row.OrgID)
		if row.CellID != cell || row.DatabaseName != row.OrgID+"db" {
			t.Fatalf("roster lost assignment or principal: %+v", row)
		}
	}
	if !reflect.DeepEqual(names, []string{"current", "historical"}) || rows[0].State != configstore.ManagedWarehouseStateReady || rows[1].State != configstore.ManagedWarehouseStateFailed {
		t.Fatalf("admission history or membership changed: %+v", rows)
	}
	other, err := store.ListAdmittedTrinoOrgs(context.Background(), "registered:other-cell")
	if err != nil || len(other) != 1 || other[0].OrgID != "other" {
		t.Fatalf("roster crossed logical cells: %+v %v", other, err)
	}
}

func TestTrinoAdmittedRosterMissingPrincipalPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	const cell = "registered:cell-test"
	seedTrinoAdmission(t, store, "admitted", cell, configstore.ManagedWarehouseStateReady, true)
	if err := store.DB().Exec("UPDATE duckgres_orgs SET database_name = '' WHERE name = ?", "admitted").Error; err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListAdmittedTrinoOrgs(context.Background(), cell)
	if err != nil || len(rows) != 1 || rows[0].OrgID != "admitted" || rows[0].DatabaseName != "" {
		t.Fatalf("invalid principal disappeared instead of blocking certification: %+v %v", rows, err)
	}
}

func TestTrinoAdmittedRosterOrphanIsRetainedPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	const cell = "registered:cell-test"
	seedTrinoAdmission(t, store, "orphan", cell, configstore.ManagedWarehouseStateReady, true)
	// Normal deletion cascades. Remove only this isolated schema's constraint to model an inconsistent restore.
	if err := store.DB().Exec("ALTER TABLE duckgres_managed_warehouse_trino DROP CONSTRAINT fk_duckgres_managed_warehouse_trino_org").Error; err != nil {
		t.Fatal(err)
	}
	if err := store.DB().Exec("DELETE FROM duckgres_org_users WHERE org_id = ?", "orphan").Error; err != nil {
		t.Fatal(err)
	}
	if err := store.DB().Exec("DELETE FROM duckgres_orgs WHERE name = ?", "orphan").Error; err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListAdmittedTrinoOrgs(context.Background(), cell)
	if err != nil || len(rows) != 1 || rows[0].OrgID != "orphan" || rows[0].DatabaseName != "" {
		t.Fatalf("orphaned admission disappeared instead of blocking certification: %+v %v", rows, err)
	}
}

func TestTrinoAdmittedRosterReenablePreservesHistoryNotReadyPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	const cell = "registered:cell-test"
	seedTrinoAdmission(t, store, "returning", cell, configstore.ManagedWarehouseStateReady, true)
	if err := store.DisableTrino("returning"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnableTrino("returning", configstore.TrinoSettings{}); err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListAdmittedTrinoOrgs(context.Background(), cell)
	if err != nil || len(rows) != 1 || rows[0].OrgID != "returning" || rows[0].State != configstore.ManagedWarehouseStatePending {
		t.Fatalf("re-enable lost admission history or granted Ready: %+v %v", rows, err)
	}
	if err := store.DB().Exec("DELETE FROM duckgres_org_users WHERE org_id = ?", "returning").Error; err != nil {
		t.Fatal(err)
	}
	if err := store.DB().Exec("DELETE FROM duckgres_orgs WHERE name = ?", "returning").Error; err != nil {
		t.Fatal(err)
	}
	rows, err = store.ListAdmittedTrinoOrgs(context.Background(), cell)
	if err != nil || len(rows) != 0 {
		t.Fatalf("normal org deletion did not cascade its admission: %+v %v", rows, err)
	}
}
