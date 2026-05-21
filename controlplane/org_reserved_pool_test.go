//go:build kubernetes

package controlplane

import (
	"context"
	"testing"
	"time"
)

func addNeutralWarmWorker(shared *K8sWorkerPool, id int) *ManagedWorker {
	worker := &ManagedWorker{ID: id, image: shared.workerImage, done: make(chan struct{})}
	shared.workers[id] = worker
	return worker
}

func TestOrgReservedPoolAcquireReservesOrgWorker(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	addNeutralWarmWorker(shared, 1)
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		// Mirror production SpawnWorker behavior: the spawned worker carries
		// the image it was built from. Required since findReservableWarmWorkerLocked
		// filters by assignment.Image.
		shared.workers[id] = &ManagedWorker{ID: id, image: shared.workerImage, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewOrgReservedPool(shared, "analytics", 2, shared.workerImage, nil)
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
			OrgID: "billing",
		},
	}); err != nil {
		t.Fatalf("SetSharedState(other): %v", err)
	}
	shared.workers[other.ID] = other
	addNeutralWarmWorker(shared, 2)

	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		// Mirror production SpawnWorker behavior: the spawned worker carries
		// the image it was built from. Required since findReservableWarmWorkerLocked
		// filters by assignment.Image.
		shared.workers[id] = &ManagedWorker{ID: id, image: shared.workerImage, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewOrgReservedPool(shared, "analytics", 2, shared.workerImage, nil)
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

func TestOrgReservedPoolReleaseWorkerTransitionsToHotIdleOnLastSession(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	worker := &ManagedWorker{ID: 9, activeSessions: 1, done: make(chan struct{})}
	worker.SetOwnerCPInstanceID(shared.cpInstanceID)
	worker.SetOwnerEpoch(3)
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleHot,
		Assignment: &WorkerAssignment{
			OrgID: "analytics",
		},
	}); err != nil {
		t.Fatalf("SetSharedState(worker): %v", err)
	}
	shared.workers[worker.ID] = worker

	pool := NewOrgReservedPool(shared, "analytics", 1, shared.workerImage, nil)
	pool.ReleaseWorker(worker.ID)

	w, ok := shared.Worker(worker.ID)
	if !ok {
		t.Fatal("expected worker to still exist after hot-idle transition")
	}
	if got := w.SharedState().NormalizedLifecycle(); got != WorkerLifecycleHotIdle {
		t.Fatalf("expected hot_idle lifecycle, got %q", got)
	}
	if w.SharedState().Assignment == nil || w.SharedState().Assignment.OrgID != "analytics" {
		t.Fatal("expected org assignment to be retained in hot_idle state")
	}
}

func TestOrgReservedWorkerPoolAcquireActivatesReservedWorkerWhenEnabledWithOrgConfig(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	addNeutralWarmWorker(shared, 1)
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		// Mirror production SpawnWorker behavior: the spawned worker carries
		// the image it was built from. Required since findReservableWarmWorkerLocked
		// filters by assignment.Image.
		shared.workers[id] = &ManagedWorker{ID: id, image: shared.workerImage, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	activated := false
	pool := NewOrgReservedPool(shared, "analytics", 2, shared.workerImage, nil)
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
	addNeutralWarmWorker(shared, 1)
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		// Mirror production SpawnWorker behavior: the spawned worker carries
		// the image it was built from. Required since findReservableWarmWorkerLocked
		// filters by assignment.Image.
		shared.workers[id] = &ManagedWorker{ID: id, image: shared.workerImage, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewOrgReservedPool(shared, "analytics", 2, shared.workerImage, nil)
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

// TestOrgReservedPoolAcquireUnboundedWhenMaxWorkersZero confirms that
// MaxWorkers == 0 means "no per-org cap" in K8s mode. The load test that
// motivated the cap removal stabilised at 11 workers because the CP
// derived MaxWorkers from CP host memory; the only thing keeping us from
// scaling further was this synthetic cap. With MaxWorkers == 0, the pool
// must keep handing out new workers as long as the shared (cluster) pool
// has room.
func TestOrgReservedPoolAcquireUnboundedWhenMaxWorkersZero(t *testing.T) {
	shared, _ := newTestK8sPool(t, 0) // shared pool also unbounded
	shared.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	// Pre-seed many neutral warm workers so AcquireWorker can reserve
	// each one in turn without blocking on a real spawn path.
	const target = 30
	for i := 1; i <= target; i++ {
		addNeutralWarmWorker(shared, i)
	}
	shared.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, image: shared.workerImage, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	// maxWorkers = 0 — the change under test. AcquireWorker must NOT
	// reject on max-workers grounds.
	pool := NewOrgReservedPool(shared, "analytics", 0, shared.workerImage, nil)
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	seen := make(map[int]struct{}, target)
	for i := 0; i < target; i++ {
		w, err := pool.AcquireWorker(ctx)
		if err != nil {
			t.Fatalf("AcquireWorker[%d] failed with maxWorkers=0: %v", i, err)
		}
		if _, dup := seen[w.ID]; dup {
			t.Fatalf("AcquireWorker[%d] returned duplicate worker ID %d", i, w.ID)
		}
		seen[w.ID] = struct{}{}
	}

	if got := pool.assignedWorkerCountLocked(); got < target {
		t.Fatalf("expected at least %d assigned workers, got %d", target, got)
	}
}

func TestOrgReservedPoolAcquireWaitsWhenSharedWarmWorkerBusyAtCapacity(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	worker := &ManagedWorker{ID: 3, activeSessions: 1, done: make(chan struct{})}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleHot,
		Assignment: &WorkerAssignment{
			OrgID: "analytics",
		},
	}); err != nil {
		t.Fatalf("SetSharedState(worker): %v", err)
	}
	shared.workers[worker.ID] = worker

	pool := NewOrgReservedPool(shared, "analytics", 1, shared.workerImage, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	got, err := pool.AcquireWorker(ctx)
	if err == nil {
		t.Fatalf("expected AcquireWorker to wait instead of reusing busy worker, got worker %d", got.ID)
		return
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
