//go:build linux || darwin

package configstore_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

func recoveryStore(t *testing.T) (*cpconfigstore.ConfigStore, cpconfigstore.TrinoPoolLease, cpconfigstore.TrinoPoolRecovery) {
	t.Helper()
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "controller-example")
	for i := range 3 {
		if err := store.CreateTrinoPoolInstance(ctx, lease, newInstance(fmt.Sprintf("serving-%d", i), trinopool.PhaseServing)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.CreateTrinoPoolInstance(ctx, lease, newInstance("draining-example", trinopool.PhaseDraining)); err != nil {
		t.Fatal(err)
	}
	req := cpconfigstore.TrinoPoolRecovery{
		OperationID: "recovery-example", PoolID: poolID, InstanceID: "draining-example",
		ExpectedGeneration: 7, Incarnation: "incarnation-example", PodUID: "pod-example",
		BootID: "boot-example", NodeID: "node-example", CoordinatorID: "coordinator-example",
		RequestedBy: "operator@example.com", Reason: "Discard retained work for this instance",
		DestructiveAuthorization: true,
	}
	err := store.RecordTrinoPoolInstanceFields(ctx, lease, req.InstanceID, map[string]any{
		"gateway_generation": req.ExpectedGeneration, "gateway_incarnation": req.Incarnation,
		"coordinator_pod_uid": req.PodUID, "coordinator_boot_id": req.BootID,
		"coordinator_node_id": req.NodeID, "coordinator_id": req.CoordinatorID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, lease, req
}

func TestTrinoPoolRecoverySubmissionAndReplay(t *testing.T) {
	ctx := context.Background()
	store, lease, req := recoveryStore(t)
	first, err := store.RequestTrinoPoolRecovery(ctx, poolID, req.InstanceID, req)
	if err != nil || first == nil || first.CreatedAt.IsZero() {
		t.Fatalf("submit = %+v, %v", first, err)
	}
	instance, err := store.GetTrinoPoolInstance(ctx, req.InstanceID)
	if err != nil || instance.Phase != string(trinopool.PhaseDraining) {
		t.Fatalf("submission changed instance: %+v, %v", instance, err)
	}
	if err := store.AdvanceTrinoPoolInstance(ctx, lease, req.InstanceID, trinopool.PhaseDraining, trinopool.PhaseSuspect, nil); err != nil {
		t.Fatal(err)
	}
	currentLease := claimPool(t, store, "next-controller")
	replayed, err := store.RequestTrinoPoolRecovery(ctx, poolID, req.InstanceID, req)
	if err != nil || replayed == nil || !replayed.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("retry after advancement = %+v, %v", replayed, err)
	}
	changed := req
	changed.Reason = "Different authorization"
	if _, err := store.RequestTrinoPoolRecovery(ctx, poolID, req.InstanceID, changed); !errors.Is(err, cpconfigstore.ErrTrinoPoolRecoveryConflict) {
		t.Fatalf("changed intent = %v", err)
	}
	got, err := store.GetTrinoPoolRecovery(ctx, poolID, req.InstanceID)
	if err != nil || got == nil || got.RequestedBy != req.RequestedBy {
		t.Fatalf("read request = %+v, %v", got, err)
	}
	rows, err := store.ListTrinoPoolRecoveries(ctx, poolID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list = %+v, %v", rows, err)
	}
	got, err = store.GetTrinoPoolRecovery(ctx, "different-pool", req.InstanceID)
	if err != nil || got != nil {
		t.Fatalf("cross-pool read = %+v, %v", got, err)
	}
	for _, transition := range [][2]trinopool.Phase{
		{trinopool.PhaseSuspect, trinopool.PhaseLost},
		{trinopool.PhaseLost, trinopool.PhaseFailureRetired},
	} {
		if err := store.AdvanceTrinoPoolInstance(ctx, currentLease, req.InstanceID, transition[0], transition[1], nil); err != nil {
			t.Fatal(err)
		}
	}
	rows, err = store.ListTrinoPoolRecoveries(ctx, poolID)
	if err != nil || len(rows) != 0 {
		t.Fatalf("completed recovery remains in operator listing: %+v, %v", rows, err)
	}
	got, err = store.GetTrinoPoolRecovery(ctx, poolID, req.InstanceID)
	if err != nil || got == nil || !got.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("completed recovery audit missing: %+v, %v", got, err)
	}
}

func TestTrinoPoolRecoveryRejectsUnapprovedOrStaleIntent(t *testing.T) {
	ctx := context.Background()
	store, _, req := recoveryStore(t)
	for name, mutate := range map[string]func(*cpconfigstore.TrinoPoolRecovery){
		"authorization":       func(r *cpconfigstore.TrinoPoolRecovery) { r.DestructiveAuthorization = false },
		"actor":               func(r *cpconfigstore.TrinoPoolRecovery) { r.RequestedBy = " " },
		"reason":              func(r *cpconfigstore.TrinoPoolRecovery) { r.Reason = " " },
		"long reason":         func(r *cpconfigstore.TrinoPoolRecovery) { r.Reason = strings.Repeat("a", 257) },
		"operation":           func(r *cpconfigstore.TrinoPoolRecovery) { r.OperationID = "" },
		"operation spaces":    func(r *cpconfigstore.TrinoPoolRecovery) { r.OperationID = "invalid operation" },
		"operation slash":     func(r *cpconfigstore.TrinoPoolRecovery) { r.OperationID = "invalid/operation" },
		"operation unicode":   func(r *cpconfigstore.TrinoPoolRecovery) { r.OperationID = "récovery" },
		"generation overflow": func(r *cpconfigstore.TrinoPoolRecovery) { r.ExpectedGeneration = math.MaxInt64 - 3 },
		"generation":          func(r *cpconfigstore.TrinoPoolRecovery) { r.ExpectedGeneration++ },
		"incarnation":         func(r *cpconfigstore.TrinoPoolRecovery) { r.Incarnation = "changed" },
		"pod":                 func(r *cpconfigstore.TrinoPoolRecovery) { r.PodUID = "changed" },
		"boot":                func(r *cpconfigstore.TrinoPoolRecovery) { r.BootID = "changed" },
		"node":                func(r *cpconfigstore.TrinoPoolRecovery) { r.NodeID = "changed" },
		"coordinator":         func(r *cpconfigstore.TrinoPoolRecovery) { r.CoordinatorID = "changed" },
		"pool":                func(r *cpconfigstore.TrinoPoolRecovery) { r.PoolID = "different-pool" },
		"instance":            func(r *cpconfigstore.TrinoPoolRecovery) { r.InstanceID = "different-instance" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := req
			mutate(&changed)
			_, err := store.RequestTrinoPoolRecovery(ctx, poolID, req.InstanceID, changed)
			if err == nil {
				t.Fatal("unsafe request accepted")
			}
			if (strings.HasPrefix(name, "operation") || name == "generation overflow") && !errors.Is(err, cpconfigstore.ErrTrinoPoolRecoveryInvalid) {
				t.Fatalf("malformed intent error = %v, want ErrTrinoPoolRecoveryInvalid", err)
			}
		})
	}
	rows, err := store.ListTrinoPoolRecoveries(ctx, poolID)
	if err != nil || len(rows) != 0 {
		t.Fatalf("rejected intent persisted: %+v, %v", rows, err)
	}
}

func TestTrinoPoolRecoveryPreservesServingFloorAndRejectsOtherPhases(t *testing.T) {
	ctx := context.Background()
	store, lease, req := recoveryStore(t)
	if err := store.AdvanceTrinoPoolInstance(ctx, lease, "serving-0", trinopool.PhaseServing, trinopool.PhaseSuspect, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RequestTrinoPoolRecovery(ctx, poolID, req.InstanceID, req); !errors.Is(err, cpconfigstore.ErrTrinoPoolRecoveryConflict) {
		t.Fatalf("below serving floor = %v", err)
	}
	if err := store.CreateTrinoPoolInstance(ctx, lease, newInstance("serving-replacement", trinopool.PhaseServing)); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceTrinoPoolInstance(ctx, lease, req.InstanceID, trinopool.PhaseDraining, trinopool.PhaseSealed, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RequestTrinoPoolRecovery(ctx, poolID, req.InstanceID, req); !errors.Is(err, cpconfigstore.ErrTrinoPoolRecoveryConflict) {
		t.Fatalf("no longer draining = %v", err)
	}
}

func TestTrinoPoolRecoveryConcurrentSubmissionIsOneIntent(t *testing.T) {
	ctx := context.Background()
	store, _, req := recoveryStore(t)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := store.RequestTrinoPoolRecovery(ctx, poolID, req.InstanceID, req); err != nil {
				t.Errorf("concurrent exact retry: %v", err)
			}
		})
	}
	wg.Wait()
	changed := req
	changed.OperationID = "second-recovery"
	if _, err := store.RequestTrinoPoolRecovery(ctx, poolID, req.InstanceID, changed); !errors.Is(err, cpconfigstore.ErrTrinoPoolRecoveryConflict) {
		t.Fatalf("different operation for same instance = %v", err)
	}
	rows, err := store.ListTrinoPoolRecoveries(ctx, poolID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("concurrent requests = %+v, %v", rows, err)
	}
}
