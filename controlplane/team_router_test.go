//go:build kubernetes

package controlplane

import (
	"context"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTeamRouterReconcileWarmCapacityUsesExplicitSharedWarmTarget(t *testing.T) {
	sharedPool, _ := newTestK8sPool(t, 10)
	sharedPool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		sharedPool.mu.Lock()
		defer sharedPool.mu.Unlock()
		sharedPool.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		return nil
	}
	tr := &TeamRouter{
		sharedPool: sharedPool,
		globalCfg: ControlPlaneConfig{
			K8s: K8sConfig{
				SharedWarmTarget: 4,
			},
		},
	}

	snap := &configstore.Snapshot{
		Teams: map[string]*configstore.TeamConfig{
			"analytics": {Name: "analytics"},
			"billing":   {Name: "billing"},
		},
	}

	tr.reconcileWarmCapacity(snap)

	if got := sharedPool.minWorkers; got != 4 {
		t.Fatalf("expected shared warm target 4, got %d", got)
	}
}

func TestTeamRouterHandleConfigChangeRefreshesRuntimeOnlyUpdates(t *testing.T) {
	sharedPool, _ := newTestK8sPool(t, 10)
	pool := NewTeamReservedWorkerPool(sharedPool, "analytics", 2)

	oldTC := &configstore.TeamConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "old-metadata.internal",
			},
		},
	}
	newTC := &configstore.TeamConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "new-metadata.internal",
			},
		},
	}

	tr := &TeamRouter{
		teams: map[string]*TeamStack{
			"analytics": {
				Config: oldTC,
				Pool:   pool,
			},
		},
		baseCfg:   K8sWorkerPoolConfig{MaxWorkers: 2},
		globalCfg: ControlPlaneConfig{},
	}

	tr.HandleConfigChange(
		&configstore.Snapshot{Teams: map[string]*configstore.TeamConfig{"analytics": oldTC}},
		&configstore.Snapshot{Teams: map[string]*configstore.TeamConfig{"analytics": newTC}},
	)

	if got := tr.teams["analytics"].Config.Warehouse.MetadataStore.Endpoint; got != "new-metadata.internal" {
		t.Fatalf("expected runtime-only update to refresh stack config, got %q", got)
	}
}
