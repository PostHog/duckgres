//go:build kubernetes

package controlplane

import (
	"testing"
	"time"
)

// Audit H1 regression test.
//
// The detached spawn+activate path budgets workerSpawnActivateTimeout
// (pod-ready wait of workerPodReadyTimeout — which may include Karpenter
// provisioning a fresh node — plus gRPC connect and activation). During that
// whole window the worker's config-store row sits in state=spawning with an
// untouched updated_at: nothing heartbeats it until the Reserved record is
// persisted after pod-ready + connect succeed (spawnReservedWorkerForSlot).
//
// The janitor's stuck-spawning sweep retires any spawning row older than its
// spawnTimeout cutoff, and ListStuckWorkers does NOT exclude rows whose owner
// CP is alive. So if the cutoff is shorter than the spawn budget, the janitor
// leader deletes legitimately in-flight pods mid-spawn (cold-node spawns are
// exactly the slow ones), recreating the doomed-spawn thrash the detached
// spawn design exists to prevent.
//
// This test pins the relation: the janitor must not consider a spawning row
// stuck while the detached spawn budget for it has not yet expired. If the
// fix instead heartbeats the spawning row or makes ListStuckWorkers exclude
// live-owner rows, relax this assertion accordingly — but only together with
// a test of that mechanism.
func TestJanitorStuckSpawningCutoffCoversDetachedSpawnBudget(t *testing.T) {
	store := &captureControlPlaneExpiryStore{}
	now := time.Date(2026, time.June, 12, 12, 0, 0, 0, time.UTC)
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }
	janitor.lifecycle = NewWorkerLifecycle(&fakeLifecycleStore{}, &fakePhysicalCleanup{})

	janitor.runOnce()

	if len(store.stuckSpawningBefore) == 0 {
		t.Fatal("janitor did not run the stuck-spawning sweep")
	}
	cutoff := store.stuckSpawningBefore[0]
	tolerated := now.Sub(cutoff)
	if tolerated < workerSpawnActivateTimeout {
		t.Fatalf("janitor retires spawning workers after %s, but a legitimate detached spawn may take up to %s (workerSpawnActivateTimeout: pod-ready incl. node provisioning + connect + activate) — in-flight cold spawns get their pods deleted mid-spawn",
			tolerated, workerSpawnActivateTimeout)
	}

	// Reserved/Activating rows have their updated_at bumped at each lifecycle
	// transition, so the activating cutoff only needs to cover the
	// connect+activate tail of the spawn budget.
	if len(store.stuckActivatingBefore) == 0 {
		t.Fatal("janitor did not run the stuck-activating sweep")
	}
	activateTail := workerSpawnActivateTimeout - workerPodReadyTimeout
	if got := now.Sub(store.stuckActivatingBefore[0]); got < activateTail {
		t.Fatalf("janitor retires reserved/activating workers after %s, but the connect+activate tail of a legitimate spawn may take up to %s", got, activateTail)
	}
}
