//go:build kubernetes

package controlplane

import (
	"context"
	"testing"
	"time"
)

func TestOrgReservedPoolAcquireReservesOrgWorker(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewOrgReservedPool(shared, "analytics", 2, nil)
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	worker, err := pool.AcquireWorker(ctx)
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if worker.activeSessions != 1 {
		t.Fatalf("expected active session claim, got %d", worker.activeSessions)
	}

	state := worker.SharedState()
	if state.Assignment == nil || state.Assignment.OrgID != "analytics" {
		t.Fatalf("expected analytics assignment, got %#v", state.Assignment)
	}
	if state.Lifecycle != WorkerLifecycleHot {
		t.Fatalf("expected hot lifecycle after activation, got %q", state.Lifecycle)
	}
}

func TestOrgReservedPoolAcquireSkipsOtherOrgsWorkers(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	other := &ManagedWorker{ID: 1, done: make(chan struct{})}
	if err := other.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			OrgID:          "billing",
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

	pool := NewOrgReservedPool(shared, "analytics", 2, nil)
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	worker, err := pool.AcquireWorker(ctx)
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if worker.ID == other.ID {
		t.Fatal("expected analytics pool to reserve its own worker, not borrow another org's worker")
	}
	if state := worker.SharedState(); state.Assignment == nil || state.Assignment.OrgID != "analytics" {
		t.Fatalf("expected analytics assignment, got %#v", state.Assignment)
	}
}

func TestOrgReservedPoolReleaseWorkerRetiresWorkerOnLastSession(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	worker := &ManagedWorker{ID: 9, activeSessions: 1, done: make(chan struct{})}
	worker.SetOwnerCPInstanceID(shared.cpInstanceID)
	worker.SetOwnerEpoch(3)
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleHot,
		Assignment: &WorkerAssignment{
			OrgID:          "analytics",
			LeaseExpiresAt: time.Now().Add(time.Hour),
		},
	}); err != nil {
		t.Fatalf("SetSharedState(worker): %v", err)
	}
	shared.workers[worker.ID] = worker

	pool := NewOrgReservedPool(shared, "analytics", 1, nil)
	pool.ReleaseWorker(worker.ID)

	if _, ok := shared.Worker(worker.ID); ok {
		t.Fatal("expected worker to be retired after last session release")
	}
}

func TestOrgReservedWorkerPoolAcquireActivatesReservedWorkerWhenEnabledWithOrgConfig(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	activated := false
	pool := NewOrgReservedPool(shared, "analytics", 2, nil)
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker) error {
		activated = true
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	worker, err := pool.AcquireWorker(ctx)
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if !activated {
		t.Fatal("expected reserved worker activation")
	}
	if got := worker.SharedState().Lifecycle; got != WorkerLifecycleHot {
		t.Fatalf("expected hot lifecycle after activation, got %q", got)
	}
}

func TestOrgReservedWorkerPoolAcquireDelegatesActivationWithoutCachedTenantRuntime(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewOrgReservedPool(shared, "analytics", 2, nil)
	activated := 0
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker) error {
		activated++
		if state := worker.SharedState(); state.Assignment == nil || state.Assignment.OrgID != "analytics" {
			t.Fatalf("expected delegated activation to use worker assignment only, got %#v", state.Assignment)
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	worker, err := pool.AcquireWorker(ctx)
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if got := worker.SharedState().Lifecycle; got != WorkerLifecycleHot {
		t.Fatalf("expected hot lifecycle after activation, got %q", got)
	}
	if activated != 1 {
		t.Fatalf("expected delegated activation to run once, got %d", activated)
	}
}

func TestOrgReservedPoolAcquireWaitsWhenSharedWarmWorkerBusyAtCapacity(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	worker := &ManagedWorker{ID: 3, activeSessions: 1, done: make(chan struct{})}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleHot,
		Assignment: &WorkerAssignment{
			OrgID:          "analytics",
			LeaseExpiresAt: time.Now().Add(time.Hour),
		},
	}); err != nil {
		t.Fatalf("SetSharedState(worker): %v", err)
	}
	shared.workers[worker.ID] = worker

	pool := NewOrgReservedPool(shared, "analytics", 1, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	got, err := pool.AcquireWorker(ctx)
	if err == nil {
		t.Fatalf("expected AcquireWorker to wait instead of reusing busy worker, got worker %d", got.ID)
	}
	if got != nil {
		t.Fatalf("expected no worker on timeout, got %v", got)
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if worker.activeSessions != 1 {
		t.Fatalf("expected busy worker session count to stay at 1, got %d", worker.activeSessions)
	}
}
