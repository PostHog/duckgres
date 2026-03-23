//go:build kubernetes

package controlplane

import (
	"context"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestOrgRouterReconcileWarmCapacityUsesExplicitSharedWarmTarget(t *testing.T) {
	sharedPool, _ := newTestK8sPool(t, 10)
	sharedPool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		sharedPool.mu.Lock()
		defer sharedPool.mu.Unlock()
		sharedPool.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		return nil
	}
	tr := &OrgRouter{
		sharedPool: sharedPool,
		globalCfg: ControlPlaneConfig{
			K8s: K8sConfig{
				SharedWarmTarget: 4,
			},
		},
	}

	snap := &configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{
			"analytics": {Name: "analytics"},
			"billing":   {Name: "billing"},
		},
	}

	tr.reconcileWarmCapacity(snap)

	if got := sharedPool.minWorkers; got != 4 {
		t.Fatalf("expected shared warm target 4, got %d", got)
	}
}
