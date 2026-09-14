//go:build linux || darwin

package configstore_test

import (
	"context"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTrinoRoutingPrincipalsPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	for _, name := range []string{"ready", "disabled", "trino-pending", "warehouse-pending", "missing-root", "disabled-root", "empty-password", "unassigned"} {
		seedTrinoOrg(t, store, name)
		if err := store.DB().Model(&configstore.Org{}).Where("name = ?", name).Update("database_name", "principal_"+name).Error; err != nil {
			t.Fatal(err)
		}
		if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: name, State: configstore.ManagedWarehouseStateReady}).Error; err != nil {
			t.Fatal(err)
		}
		if err := store.EnableTrino(name, configstore.TrinoSettings{}); err != nil {
			t.Fatal(err)
		}
		if err := store.AssignTrinoCell(name, "registered:cell-a"); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateTrinoState(name, configstore.TrinoStateUpdate{State: configstore.ManagedWarehouseStateReady}); err != nil {
			t.Fatal(err)
		}
	}
	changes := []struct {
		model  any
		where  string
		values map[string]any
	}{
		{&configstore.ManagedWarehouseTrino{}, "org_id = 'disabled'", map[string]any{"enabled": false}},
		{&configstore.ManagedWarehouseTrino{}, "org_id = 'trino-pending'", map[string]any{"state": "pending"}},
		{&configstore.ManagedWarehouse{}, "org_id = 'warehouse-pending'", map[string]any{"state": "pending"}},
		{&configstore.OrgUser{}, "org_id = 'disabled-root'", map[string]any{"disabled": true}},
		{&configstore.OrgUser{}, "org_id = 'empty-password'", map[string]any{"password": ""}},
		{&configstore.ManagedWarehouseTrino{}, "org_id = 'unassigned'", map[string]any{"trino_cell_id": ""}},
	}
	for _, change := range changes {
		if err := store.DB().Model(change.model).Where(change.where).Updates(change.values).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DB().Where("org_id = ?", "missing-root").Delete(&configstore.OrgUser{}).Error; err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListTrinoRoutingPrincipals(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Principal != "principal_ready" || rows[0].CellID != "registered:cell-a" {
		t.Fatalf("unexpected routes: %+v", rows)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.ListTrinoRoutingPrincipals(ctx); err == nil {
		t.Fatal("canceled database read must fail")
	}
}
