//go:build kubernetes

package controlplane

import (
	"context"
	"testing"

	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

func TestTrinoPoolRepairsRemainDistinctAfterTargetsStartDraining(t *testing.T) {
	h := newOperatorHarness(t)
	h.servingPool(t)
	h.operator.config.Spec.MaxRepair = 3
	h.store.pool.MaxRepair = 3
	oldIDs := append([]string(nil), h.store.order...)
	for _, id := range oldIDs {
		h.placeInstance(t, id, trinopool.PhaseSuspect, "SUSPECT")
		h.gateway.obligations[id] = trinogateway.Obligations{ActiveQueries: 1}
	}
	instances, err := h.store.ListTrinoPoolInstances(context.Background(), h.store.pool.PoolID)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.operator.applyPlan(context.Background(), h.store.pool, instances); err != nil {
		t.Fatal(err)
	}
	if len(h.store.order) != len(oldIDs)+1 {
		t.Fatal("suspected capacity did not allocate its first replacement")
	}
	firstRepair := h.store.instances[h.store.order[len(oldIDs)]]
	if !firstRepair.Repair || firstRepair.RepairFor != oldIDs[0] {
		t.Fatalf("first repair lost its original target: %+v", firstRepair.View())
	}
	for _, id := range oldIDs {
		h.placeInstance(t, id, trinopool.PhaseDraining, "DRAINING")
	}
	h.tick(t, 30)
	targets := make(map[string]bool)
	serving := 0
	for _, instance := range h.store.instances {
		if instance.Phase != string(trinopool.PhaseServing) {
			continue
		}
		serving++
		if !instance.Repair || instance.RepairFor == "" || targets[instance.RepairFor] {
			t.Fatalf("replacement did not preserve a unique durable target: %+v", instance.View())
		}
		targets[instance.RepairFor] = true
	}
	if serving != 3 {
		t.Fatalf("serving = %d, want 3; phases = %v", serving, h.phases())
	}
	for _, id := range oldIDs {
		if h.store.instances[id].Phase != string(trinopool.PhaseDraining) || h.kube.deleted[id] {
			t.Fatalf("replacement removed the pinned instance %s", id)
		}
	}
}
