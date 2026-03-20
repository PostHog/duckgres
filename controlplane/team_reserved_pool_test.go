//go:build kubernetes

package controlplane

import (
	"context"
	"testing"
	"time"
)

func TestTeamReservedWorkerPoolAcquireReservesTeamWorker(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewTeamReservedWorkerPool(shared, "analytics", 2)
	worker, err := pool.AcquireWorker(context.Background())
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if worker.activeSessions != 1 {
		t.Fatalf("expected active session claim, got %d", worker.activeSessions)
	}

	state := worker.SharedState()
	if state.Assignment == nil || state.Assignment.TeamName != "analytics" {
		t.Fatalf("expected analytics assignment, got %#v", state.Assignment)
	}
	if state.Lifecycle != WorkerLifecycleReserved {
		t.Fatalf("expected reserved lifecycle, got %q", state.Lifecycle)
	}
}

func TestTeamReservedWorkerPoolAcquireSkipsOtherTeamsWorkers(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	other := &ManagedWorker{ID: 1, done: make(chan struct{})}
	if err := other.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			TeamName:       "billing",
			LeaseExpiresAt: time.Now().Add(time.Hour),
		},
	}); err != nil {
		t.Fatalf("SetSharedState(other): %v", err)
	}
	shared.workers[other.ID] = other

	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewTeamReservedWorkerPool(shared, "analytics", 2)
	worker, err := pool.AcquireWorker(context.Background())
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if worker.ID == other.ID {
		t.Fatal("expected analytics pool to reserve its own worker, not borrow another team's worker")
	}
	if state := worker.SharedState(); state.Assignment == nil || state.Assignment.TeamName != "analytics" {
		t.Fatalf("expected analytics assignment, got %#v", state.Assignment)
	}
}

func TestTeamReservedWorkerPoolReleaseWorkerRetiresOnLastSession(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	worker := &ManagedWorker{ID: 9, activeSessions: 1, done: make(chan struct{})}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			TeamName:       "analytics",
			LeaseExpiresAt: time.Now().Add(time.Hour),
		},
	}); err != nil {
		t.Fatalf("SetSharedState(worker): %v", err)
	}
	shared.workers[worker.ID] = worker

	pool := NewTeamReservedWorkerPool(shared, "analytics", 1)
	pool.ReleaseWorker(worker.ID)

	time.Sleep(100 * time.Millisecond)
	if _, ok := shared.Worker(worker.ID); ok {
		t.Fatal("expected worker to be retired after last session release")
	}
}
