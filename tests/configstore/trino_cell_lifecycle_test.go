//go:build linux || darwin

package configstore_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTrinoCellReconcileOwnershipAndUncertainIntentPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()
	var successes atomic.Int32
	var winner *configstore.TrinoCellLease
	var lock sync.Mutex
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			lease, acquired, err := store.BeginTrinoCellReconcile(ctx, "registered:cell-test", uuid.NewString())
			if err != nil {
				t.Error(err)
				return
			}
			if acquired {
				successes.Add(1)
				lock.Lock()
				winner = lease
				lock.Unlock()
			}
		})
	}
	workers.Wait()
	if successes.Load() != 1 || winner == nil {
		t.Fatal("cell must have exactly one durable owner")
	}
	intent := configstore.TrinoCatalogIntent{ID: uuid.NewString(), Sequence: winner.IntentSequence + 1, Backend: "group-blue", Action: "create", Catalog: "org_example"}
	if err := store.SetTrinoCellIntent(ctx, *winner, intent); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTrinoCellIntent(ctx, *winner, intent); err == nil {
		t.Fatal("duplicate intent must not authorize a second remote submission")
	}
	if err := store.FinishTrinoCellReconcile(ctx, *winner); err == nil {
		t.Fatal("unknown remote outcome released cell ownership")
	}
	if _, acquired, err := store.BeginTrinoCellReconcile(ctx, winner.CellID, uuid.NewString()); err != nil || acquired {
		t.Fatal("another replica took an uncertain operation")
	}
	if err := store.ClearTrinoCellIntent(ctx, *winner, intent.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTrinoCellIntent(ctx, *winner, intent); err == nil {
		t.Fatal("cleared intent authorized a duplicate remote submission")
	}
	nextIntent := intent
	nextIntent.ID = uuid.NewString()
	nextIntent.Sequence++
	if err := store.SetTrinoCellIntent(ctx, *winner, nextIntent); err != nil {
		t.Fatal(err)
	}
	if err := store.ClearTrinoCellIntent(ctx, *winner, nextIntent.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTrinoCellIntent(ctx, *winner, intent); err == nil {
		t.Fatal("intervening intent allowed old submission replay")
	}
	if err := store.FinishTrinoCellReconcile(ctx, *winner); err != nil {
		t.Fatal(err)
	}
	next, acquired, err := store.BeginTrinoCellReconcile(ctx, winner.CellID, uuid.NewString())
	if err != nil || !acquired || next.ReconcileEpoch <= winner.ReconcileEpoch {
		t.Fatal("known completion did not allow a fresh fenced owner")
	}
	if err := store.SetTrinoCellIntent(ctx, *winner, intent); err == nil {
		t.Fatal("old owner submitted after a newer pass acquired the cell")
	}
	if err := store.ClearTrinoCellIntent(ctx, *winner, intent.ID); err == nil {
		t.Fatal("old owner cleared a newer pass")
	}
	if err := store.FinishTrinoCellReconcile(ctx, *winner); err == nil {
		t.Fatal("old owner finished a newer pass")
	}
	if _, acquired, err := store.BeginTrinoCellReconcile(ctx, "registered:other-cell", uuid.NewString()); err != nil || !acquired {
		t.Fatal("a held cell blocked another cell")
	}
}

func TestTrinoCellAdmissionEpochBlocksLateReadyPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()
	seedTrinoOrg(t, store, "example")
	if err := store.DB().Create(&configstore.ManagedWarehouse{OrgID: "example", DucklingName: "example", State: configstore.ManagedWarehouseStateReady}).Error; err != nil {
		t.Fatal(err)
	}
	cell := "registered:cell-test"
	if err := store.SelectTrinoCell("example", cell); err != nil {
		t.Fatal(err)
	}
	if err := store.EnableTrino("example", configstore.TrinoSettings{}); err != nil {
		t.Fatal(err)
	}
	lease, acquired, err := store.BeginTrinoCellReconcile(ctx, cell, uuid.NewString())
	if err != nil || !acquired {
		t.Fatal("could not acquire initial pass")
	}
	freeze, err := store.FreezeTrinoCellAdmissions(ctx, cell, "operation-test", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "group-green", lease.AdmissionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if freeze.Stable {
		t.Fatal("freeze acknowledged stable while an earlier owner still runs")
	}
	retried, err := store.FreezeTrinoCellAdmissions(ctx, cell, freeze.OperationID, freeze.PlanHash, freeze.TargetBackend, lease.AdmissionEpoch)
	if err != nil || retried.AdmissionEpoch != freeze.AdmissionEpoch {
		t.Fatal("same-operation freeze retry changed epoch")
	}
	if updated, err := store.UpdateManagedTrinoState(ctx, *lease, "example", configstore.TrinoStateUpdate{State: configstore.ManagedWarehouseStateReady}); err != nil || updated {
		t.Fatal("a pre-freeze blue pass admitted a new warehouse")
	}
	if err := store.FinishTrinoCellReconcile(ctx, *lease); err != nil {
		t.Fatal(err)
	}
	during, acquired, err := store.BeginTrinoCellReconcile(ctx, cell, uuid.NewString())
	if err != nil || !acquired {
		t.Fatal("freeze must allow target preparation")
	}
	if updated, err := store.UpdateManagedTrinoState(ctx, *during, "example", configstore.TrinoStateUpdate{State: configstore.ManagedWarehouseStateReady}); err != nil || updated {
		t.Fatal("frozen preparation admitted a new warehouse")
	}
	if err := store.ReleaseTrinoCellAdmissions(ctx, cell, freeze.OperationID, freeze.AdmissionEpoch); err == nil {
		t.Fatal("uncertified target released admissions")
	}
	certificate := configstore.TrinoCellCertificate{TargetBackend: "group-green", NodeID: "node-test", CoordinatorID: "abcde", RosterHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	if err := store.CertifyTrinoCellTarget(ctx, *during, freeze.OperationID, certificate); err != nil {
		t.Fatal(err)
	}
	changed := certificate
	changed.NodeID = "replacement-node"
	if err := store.CertifyTrinoCellTarget(ctx, *during, freeze.OperationID, changed); err == nil {
		t.Fatal("certificate process identity was overwritten")
	}
	if err := store.ReleaseTrinoCellAdmissions(ctx, cell, freeze.OperationID, freeze.AdmissionEpoch); err != nil {
		t.Fatal(err)
	}
	if updated, err := store.UpdateManagedTrinoState(ctx, *during, "example", configstore.TrinoStateUpdate{State: configstore.ManagedWarehouseStateReady}); err != nil || updated {
		t.Fatal("during-freeze pass admitted after release")
	}
	if err := store.SetTrinoCellIntent(ctx, *during, configstore.TrinoCatalogIntent{ID: uuid.NewString(), Sequence: during.IntentSequence + 1, Backend: "group-blue", Action: "create", Catalog: "org_example"}); err == nil {
		t.Fatal("old blue pass wrote after cutover release")
	}
	status, err := store.GetTrinoCellLifecycle(ctx, cell)
	if err != nil || status.Freeze != nil || status.AdmissionEpoch != freeze.AdmissionEpoch+1 || status.ReleasedOperationID != freeze.OperationID {
		t.Fatal("released lifecycle lost its discoverable admission epoch")
	}
	if err := store.FinishTrinoCellReconcile(ctx, *during); err != nil {
		t.Fatal(err)
	}
	after, acquired, err := store.BeginTrinoCellReconcile(ctx, cell, uuid.NewString())
	if err != nil || !acquired {
		t.Fatal("could not begin post-cutover pass")
	}
	if updated, err := store.UpdateManagedTrinoState(ctx, *after, "example", configstore.TrinoStateUpdate{State: configstore.ManagedWarehouseStateReady}); err != nil || !updated {
		t.Fatal("fresh post-cutover pass did not admit")
	}
	if updated, err := store.UpdateManagedTrinoState(ctx, *lease, "example", configstore.TrinoStateUpdate{State: configstore.ManagedWarehouseStateReady}); err == nil || updated {
		t.Fatal("original owner changed later admission")
	}
	if err := store.FinishTrinoCellReconcile(ctx, *after); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FreezeTrinoCellAdmissions(ctx, cell, "next-operation", freeze.PlanHash, "group-blue", after.AdmissionEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FreezeTrinoCellAdmissions(ctx, cell, freeze.OperationID, freeze.PlanHash, freeze.TargetBackend, lease.AdmissionEpoch); err == nil {
		t.Fatal("delayed prior operation reopened admissions freeze")
	}
}
