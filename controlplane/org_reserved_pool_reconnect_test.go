//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type reconnectAdmissionLimiter struct {
	request connectionAdmissionRequest
	lease   *reconnectAdmissionLease
	err     error
}

func (l *reconnectAdmissionLimiter) Acquire(ctx context.Context, request connectionAdmissionRequest, limits func(string) configstore.OrgResourceLimits) (connectionLease, error) {
	l.request = request
	if l.lease != nil {
		return l.lease, l.err
	}
	return nil, l.err
}

type reconnectAdmissionLease struct {
	released bool
}

func (l *reconnectAdmissionLease) Release(context.Context) error {
	l.released = true
	return nil
}

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
// worker — killing the live reconnected session.
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
		preloadedRecords: map[int]*configstore.WorkerRecord{
			workerID: {
				WorkerID:          workerID,
				PodName:           "duckgres-worker-3",
				Image:             shared.workerImage,
				State:             configstore.WorkerStateHotIdle,
				OwnerCPInstanceID: shared.cpInstanceID,
				OwnerEpoch:        1,
			},
		},
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

func TestReconnectFlightWorkerUsesPersistedProfile(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	const workerID = 4

	worker := &ManagedWorker{ID: workerID, image: shared.workerImage, done: make(chan struct{})}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleHotIdle,
		Assignment: &WorkerAssignment{
			OrgID: "analytics",
			Profile: &WorkerProfile{
				CPU:    "4",
				Memory: "16Gi",
			},
		},
	}); err != nil {
		t.Fatalf("SetSharedState(worker): %v", err)
	}
	shared.workers[workerID] = worker

	store := &captureRuntimeWorkerStore{
		preloadedRecords: map[int]*configstore.WorkerRecord{
			workerID: {
				WorkerID:          workerID,
				PodName:           "duckgres-worker-4",
				Image:             shared.workerImage,
				State:             configstore.WorkerStateHotIdle,
				OrgID:             "analytics",
				OwnerCPInstanceID: shared.cpInstanceID,
				OwnerEpoch:        1,
				ProfileCPU:        "4",
				ProfileMemory:     "16Gi",
			},
		},
		takenOver: &configstore.WorkerRecord{
			WorkerID:          workerID,
			PodName:           "duckgres-worker-4",
			Image:             shared.workerImage,
			State:             configstore.WorkerStateHotIdle,
			OrgID:             "analytics",
			OwnerCPInstanceID: shared.cpInstanceID,
			OwnerEpoch:        2,
			ProfileCPU:        "4",
			ProfileMemory:     "16Gi",
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

	state := got.SharedState()
	if state.Assignment == nil || state.Assignment.Profile == nil {
		t.Fatalf("expected reconnected worker assignment to retain persisted profile, got %#v", state.Assignment)
	}
	if state.Assignment.Profile.CPU != "4" || state.Assignment.Profile.Memory != "16Gi" {
		t.Fatalf("expected profile 4/16Gi, got %s/%s", state.Assignment.Profile.CPU, state.Assignment.Profile.Memory)
	}
	if got.profile.CPU != "4" || got.profile.Memory != "16Gi" {
		t.Fatalf("expected worker profile 4/16Gi, got %s/%s", got.profile.CPU, got.profile.Memory)
	}
}

func TestReconnectFlightSessionStaleOwnerEpochReleasesAdmittedVCPU(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	const workerID = 5
	shared.runtimeStore = &captureRuntimeWorkerStore{
		preloadedRecords: map[int]*configstore.WorkerRecord{
			workerID: {
				WorkerID:          workerID,
				PodName:           "duckgres-worker-5",
				Image:             shared.workerImage,
				State:             configstore.WorkerStateHotIdle,
				OrgID:             "analytics",
				OwnerCPInstanceID: shared.cpInstanceID,
				OwnerEpoch:        2,
				ProfileCPU:        "8",
				ProfileMemory:     "32Gi",
			},
		},
		takeOverErr: configstore.ErrWorkerOwnerEpochMismatch,
	}

	pool := NewOrgReservedPool(shared, "analytics", 2, shared.workerImage, nil)
	lease := &reconnectAdmissionLease{}
	limiter := &reconnectAdmissionLimiter{lease: lease}
	sm := NewSessionManager(pool, nil)
	sm.SetRequestedVCPUsResolver(func(profile *WorkerProfile) (int, error) {
		return requestedWorkerVCPUs(profile, "4")
	})
	sm.SetConnectionLimiter(limiter)

	_, _, err := sm.ReconnectFlightSession(context.Background(), "root", workerID, 1)
	if !errors.Is(err, configstore.ErrWorkerOwnerEpochMismatch) {
		t.Fatalf("expected stale reconnect to fail with owner epoch mismatch after vCPU admission, got %v", err)
	}
	if limiter.request.Username != "root" || limiter.request.Protocol != "flight" || limiter.request.RequestedVCPUs != 8 {
		t.Fatalf("expected stale reconnect to use normal vCPU admission with persisted profile, got request %#v", limiter.request)
	}
	if !lease.released {
		t.Fatal("expected failed stale reconnect to release its admitted vCPU lease")
	}
}
