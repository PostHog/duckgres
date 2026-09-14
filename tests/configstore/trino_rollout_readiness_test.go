//go:build linux || darwin

package configstore_test

import (
	"context"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTrinoRolloutCanaryEligibilityPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "canary")
	if err := store.DB().Model(&configstore.Org{}).Where("name = ?", "canary").Update("database_name", "canary-test").Error; err != nil {
		t.Fatal(err)
	}
	if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: "canary", DucklingName: "canary", State: configstore.ManagedWarehouseStateReady}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.SelectTrinoCell("canary", "registered:cell-test"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnableTrino("canary", configstore.TrinoSettings{}); err != nil {
		t.Fatal(err)
	}
	check := func(cell, principal string, wanted bool) {
		t.Helper()
		got, err := store.TrinoRolloutCanaryEligible(context.Background(), "canary", cell, principal)
		if err != nil || got != wanted {
			t.Fatalf("eligibility %v, %v; wanted %v", got, err, wanted)
		}
	}
	check("registered:cell-test", "canary-test", false)
	if err := store.UpdateTrinoState("canary", configstore.TrinoStateUpdate{State: configstore.ManagedWarehouseStateReady}); err != nil {
		t.Fatal(err)
	}
	check("registered:cell-test", "canary-test", true)
	check("registered:other", "canary-test", false)
	check("registered:cell-test", "impersonator", false)
	if err := store.DB().Model(&configstore.OrgUser{}).Where("org_id = ?", "canary").Update("disabled", true).Error; err != nil {
		t.Fatal(err)
	}
	check("registered:cell-test", "canary-test", false)
	if err := store.DB().Model(&configstore.OrgUser{}).Where("org_id = ?", "canary").Update("disabled", false).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.DisableTrino("canary"); err != nil {
		t.Fatal(err)
	}
	check("registered:cell-test", "canary-test", false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.TrinoRolloutCanaryEligible(ctx, "canary", "registered:cell-test", "canary-test"); err == nil {
		t.Fatal("canceled request succeeded")
	}
}
