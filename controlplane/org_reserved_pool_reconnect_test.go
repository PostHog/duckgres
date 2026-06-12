//go:build kubernetes

package controlplane

import (
	"context"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// Audit H2 regression test.
//
// Every acquisition path pre-claims the session on the pool-side worker
// accounting (claimSessionLocked / activeSessions++) before handing the
// worker out — that counter is what keeps a busy worker out of
// findIdleAssignedWorkerLocked, and what makes ShutdownAll preserve workers
// with live sessions. The Flight reconnect path (ReconnectFlightWorker →
// claimSpecificWorker → reserveClaimedWorker) must do the same: a reconnected
// Flight session is a live session like any other.
//
// Without the claim, the reconnected worker looks idle (activeSessions==0),
// so the org's next connection is co-assigned onto it; the worker-side
// MaxSessions=1 cap rejects that, and the cap-drift recovery then RETIRES the
// worker — killing the live reconnected query.
func TestReconnectFlightWorkerClaimsSession(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	const workerID = 3

	// A Flight session that disconnected with a reconnect token released its
	// worker (ReleaseWorker → TransitionToHotIdleIfNoSessions), so the worker
	// the reconnect claims back is hot-idle in this CP's memory.
	worker := &ManagedWorker{ID: workerID, image: shared.workerImage, done: make(chan struct{})}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleHotIdle,
		Assignment: &WorkerAssignment{
			OrgID: "analytics",
		},
	}); err != nil {
		t.Fatalf("SetSharedState(worker): %v", err)
	}
	shared.workers[workerID] = worker

	store := &captureRuntimeWorkerStore{
		takenOver: &configstore.WorkerRecord{
			WorkerID:          workerID,
			PodName:           "duckgres-worker-3",
			Image:             shared.workerImage,
			State:             configstore.WorkerStateHotIdle,
			OwnerCPInstanceID: shared.cpInstanceID,
			OwnerEpoch:        2,
		},
	}
	shared.runtimeStore = store
	shared.healthCheckFunc = func(ctx context.Context, w *ManagedWorker) error {
		return nil
	}

	pool := NewOrgReservedPool(shared, "analytics", 2, shared.workerImage, nil)
	pool.activateReservedWorker = func(ctx context.Context, w *ManagedWorker) error {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := pool.ReconnectFlightWorker(ctx, workerID, 1)
	if err != nil {
		t.Fatalf("ReconnectFlightWorker: %v", err)
	}
	if got.ID != workerID {
		t.Fatalf("expected worker %d, got %d", workerID, got.ID)
	}

	shared.mu.Lock()
	sessions := got.activeSessions
	idle := pool.findIdleAssignedWorkerLocked(nil)
	shared.mu.Unlock()

	if sessions != 1 {
		t.Fatalf("reconnected Flight session holds no pool-side session claim (activeSessions=%d, want 1) — the worker looks idle while serving a live query and will be co-assigned, then retired by cap-drift recovery", sessions)
	}
	if idle != nil {
		t.Fatalf("worker %d serving a reconnected Flight session is offered for idle reuse — a concurrent same-org connection would be co-assigned onto it", idle.ID)
	}
}
