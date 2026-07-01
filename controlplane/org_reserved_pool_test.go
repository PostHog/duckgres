//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestOrgReservedPoolAcquireReservesOrgWorker(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	shared.spawnWorkerFunc = func(ctx context.Context, id int, image string, profile WorkerProfile) error {
		shared.mu.Lock()
		// Mirror production SpawnWorker: the spawned worker carries the image it was
		// built from, so the image-mismatch retire check in reserveClaimedWorker sees it.
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

	worker, err := pool.AcquireWorker(ctx, nil)
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

	shared.spawnWorkerFunc = func(ctx context.Context, id int, image string, profile WorkerProfile) error {
		shared.mu.Lock()
		// Mirror production SpawnWorker: the spawned worker carries the image it was
		// built from, so the image-mismatch retire check in reserveClaimedWorker sees it.
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

	worker, err := pool.AcquireWorker(ctx, nil)
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

// TestOrgReservedPoolWorkerCount covers the per-org worker count surfaced to
// the admin Overview load bars: it counts this org's cap-counting (Hot)
// workers, excludes hot-idle (parked, not against cap), and ignores workers
// assigned to a different org sharing the pool.
func TestOrgReservedPoolWorkerCount(t *testing.T) {
	shared, _ := newTestK8sPool(t, 10)

	mk := func(id int, org string, lc WorkerLifecycleState) {
		w := &ManagedWorker{ID: id, activeSessions: 1, done: make(chan struct{})}
		w.SetOwnerCPInstanceID(shared.cpInstanceID)
		w.SetOwnerEpoch(1)
		if err := w.SetSharedState(SharedWorkerState{
			Lifecycle:  lc,
			Assignment: &WorkerAssignment{OrgID: org},
		}); err != nil {
			t.Fatalf("SetSharedState(%d): %v", id, err)
		}
		shared.workers[id] = w
	}
	// Two Hot workers for analytics (counted), one analytics hot-idle (excluded),
	// one Hot worker for another org (excluded).
	mk(1, "analytics", WorkerLifecycleHot)
	mk(2, "analytics", WorkerLifecycleHot)
	mk(3, "analytics", WorkerLifecycleHotIdle)
	mk(4, "other", WorkerLifecycleHot)

	pool := NewOrgReservedPool(shared, "analytics", 5, shared.workerImage, nil)
	if got := pool.WorkerCount(); got != 2 {
		t.Fatalf("WorkerCount() = %d, want 2 (two Hot analytics workers; hot-idle + other-org excluded)", got)
	}

	// Empty org → 0, never negative.
	empty := NewOrgReservedPool(shared, "nobody", 5, shared.workerImage, nil)
	if got := empty.WorkerCount(); got != 0 {
		t.Fatalf("WorkerCount() for org with no workers = %d, want 0", got)
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
	shared.spawnWorkerFunc = func(ctx context.Context, id int, image string, profile WorkerProfile) error {
		shared.mu.Lock()
		// Mirror production SpawnWorker: the spawned worker carries the image it was
		// built from, so the image-mismatch retire check in reserveClaimedWorker sees it.
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

	worker, err := pool.AcquireWorker(ctx, nil)
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
	shared.spawnWorkerFunc = func(ctx context.Context, id int, image string, profile WorkerProfile) error {
		shared.mu.Lock()
		// Mirror production SpawnWorker: the spawned worker carries the image it was
		// built from, so the image-mismatch retire check in reserveClaimedWorker sees it.
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

	worker, err := pool.AcquireWorker(ctx, nil)
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
	const target = 30
	shared.spawnWorkerFunc = func(ctx context.Context, id int, image string, profile WorkerProfile) error {
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
		w, err := pool.AcquireWorker(ctx, nil)
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

// At the org's max concurrent workers with all of them busy, AcquireWorker must
// fail FAST with the clear org-cap message — not busy-wait until the client's
// deadline, and not reuse the busy worker (one session per worker).
func TestOrgReservedPoolAcquireFailsClearlyAtOrgCap(t *testing.T) {
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

	// Generous deadline: the call must return promptly on its own, well before this.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	got, err := pool.AcquireWorker(ctx, nil)
	if err == nil {
		t.Fatalf("expected AcquireWorker to fail at org cap, got worker %d", got.ID)
	}
	if got != nil {
		t.Fatalf("expected no worker at org cap, got %v", got)
	}
	var capErr *WorkerCapacityExhaustedError
	if !errors.As(err, &capErr) || capErr.missReason() != configstore.WorkerClaimMissReasonOrgCap {
		t.Fatalf("expected org-cap WorkerCapacityExhaustedError, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("expected fast failure at org cap, took %s", elapsed)
	}
	if worker.activeSessions != 1 {
		t.Fatalf("expected busy worker session count to stay at 1, got %d", worker.activeSessions)
	}
}

// A requester that gives up (ctx cancelled) mid-spawn must NOT doom the spawn:
// the spawn+activate continues on its detached context, the finished worker is
// parked hot-idle (with its hot_idle record persisted), and a subsequent
// AcquireWorker for the org reclaims it through the hot-idle claim path without
// spawning a second pod. Regression net for the doomed-spawn thrash where every
// retry deleted the in-flight pod and re-spawned from scratch.
func TestOrgReservedPoolAbandonedSpawnParksHotIdleAndIsReclaimed(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	const workerID = 21
	store := &captureRuntimeWorkerStore{
		spawned: &configstore.WorkerRecord{WorkerID: workerID},
	}
	shared.runtimeStore = store
	shared.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}

	spawnStarted := make(chan struct{})
	releaseSpawn := make(chan struct{})
	var spawnPodCalls atomic.Int32
	shared.spawnWorkerFunc = func(ctx context.Context, id int, image string, profile WorkerProfile) error {
		if spawnPodCalls.Add(1) == 1 {
			close(spawnStarted)
		}
		// Simulate a slow pod spawn (e.g. Karpenter provisioning a node) that
		// outlives the requester. The detached spawn ctx must stay live after the
		// requester cancels.
		select {
		case <-releaseSpawn:
		case <-ctx.Done():
			return ctx.Err()
		}
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, image: shared.workerImage, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewOrgReservedPool(shared, "analytics", 2, shared.workerImage, nil)
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}

	reqCtx, cancelReq := context.WithCancel(context.Background())
	defer cancelReq()
	acquireErr := make(chan error, 1)
	go func() {
		_, err := pool.AcquireWorker(reqCtx, nil)
		acquireErr <- err
	}()

	select {
	case <-spawnStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("spawn never started")
	}

	// Requester gives up mid-spawn.
	cancelReq()
	select {
	case err := <-acquireErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled for the abandoning requester, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AcquireWorker did not return after requester cancellation")
	}

	// Let the detached spawn finish; the abandoned worker must be parked
	// hot-idle with no sessions, not left Reserved/Activating or retired.
	close(releaseSpawn)
	deadline := time.Now().Add(5 * time.Second)
	for {
		shared.mu.RLock()
		w, ok := shared.workers[workerID]
		var lifecycle WorkerLifecycleState
		sessions := -1
		if ok {
			lifecycle = w.SharedState().NormalizedLifecycle()
			sessions = w.activeSessions
		}
		shared.mu.RUnlock()
		if ok && lifecycle == WorkerLifecycleHotIdle && sessions == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("abandoned spawn was not parked hot-idle (exists=%v lifecycle=%q sessions=%d)", ok, lifecycle, sessions)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The hot_idle record must be persisted (same as ReleaseWorker /
	// TransitionToHotIdleIfNoSessions) so peers/janitor see the parked worker.
	foundHotIdleRecord := false
	for _, rec := range store.snapshot() {
		if rec.WorkerID == workerID && rec.State == configstore.WorkerStateHotIdle {
			foundHotIdleRecord = true
		}
	}
	if !foundHotIdleRecord {
		t.Fatal("expected a persisted hot_idle worker record for the parked worker")
	}

	// The org's next connection reclaims the parked worker via the hot-idle
	// claim — no second pod spawn.
	store.mu.Lock()
	store.hotIdleClaimResult = &configstore.WorkerRecord{
		WorkerID:          workerID,
		State:             configstore.WorkerStateHotIdle,
		OrgID:             "analytics",
		Image:             shared.workerImage,
		OwnerCPInstanceID: shared.cpInstanceID,
		OwnerEpoch:        2,
	}
	store.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	worker, err := pool.AcquireWorker(ctx, nil)
	if err != nil {
		t.Fatalf("AcquireWorker (reclaim): %v", err)
	}
	if worker.ID != workerID {
		t.Fatalf("expected to reclaim parked worker %d, got %d", workerID, worker.ID)
	}
	if got := spawnPodCalls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 pod spawn (reclaim must not respawn), got %d", got)
	}
	if worker.activeSessions != 1 {
		t.Fatalf("expected reclaimed worker to carry the new session, got %d", worker.activeSessions)
	}
	if got := worker.SharedState().NormalizedLifecycle(); got != WorkerLifecycleHot {
		t.Fatalf("expected reclaimed worker to be hot, got %q", got)
	}
}

// A cold burst of N waiters under the org cap must ramp N spawns IN PARALLEL:
// the per-org FIFO gate covers only the short claim/slot decision, not the
// multi-minute spawn+activate. Each spawn blocks on a barrier that opens only
// once all N spawns have started — with the old gate-held-across-spawn behavior
// this deadlocks (waiter k can't start its spawn until k-1 finished) and the
// test times out.
func TestOrgReservedPoolColdBurstSpawnsInParallel(t *testing.T) {
	shared, _ := newTestK8sPool(t, 0)
	shared.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}

	const n = 3
	var mu sync.Mutex
	started := 0
	allStarted := make(chan struct{})
	shared.spawnWorkerFunc = func(ctx context.Context, id int, image string, profile WorkerProfile) error {
		mu.Lock()
		started++
		if started == n {
			close(allStarted)
		}
		mu.Unlock()
		select {
		case <-allStarted: // barrier: requires n CONCURRENT spawns
		case <-ctx.Done():
			return ctx.Err()
		}
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, image: shared.workerImage, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewOrgReservedPool(shared, "analytics", n, shared.workerImage, nil)
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type result struct {
		worker *ManagedWorker
		err    error
	}
	results := make(chan result, n)
	for i := 0; i < n; i++ {
		go func() {
			w, err := pool.AcquireWorker(ctx, nil)
			results <- result{worker: w, err: err}
		}()
	}

	seen := make(map[int]struct{}, n)
	for i := 0; i < n; i++ {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("cold-burst AcquireWorker failed (spawns serialized?): %v", r.err)
			}
			if _, dup := seen[r.worker.ID]; dup {
				t.Fatalf("two cold-burst waiters got the same worker %d", r.worker.ID)
			}
			seen[r.worker.ID] = struct{}{}
		case <-time.After(10 * time.Second):
			t.Fatal("cold-burst waiters did not all complete — spawns did not run in parallel")
		}
	}
}

// If the requester abandons mid-spawn and the detached activation then FAILS,
// the worker must be retired exactly like the synchronous activation-failure
// path — nothing may leak in Reserved/Activating state.
func TestOrgReservedPoolAbandonedSpawnActivationFailureRetiresWorker(t *testing.T) {
	shared, _ := newTestK8sPool(t, 5)
	shared.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}

	spawnStarted := make(chan struct{})
	releaseSpawn := make(chan struct{})
	var spawnedID atomic.Int32
	shared.spawnWorkerFunc = func(ctx context.Context, id int, image string, profile WorkerProfile) error {
		spawnedID.Store(int32(id))
		close(spawnStarted)
		select {
		case <-releaseSpawn:
		case <-ctx.Done():
			return ctx.Err()
		}
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, image: shared.workerImage, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewOrgReservedPool(shared, "analytics", 2, shared.workerImage, nil)
	activationAttempted := make(chan struct{})
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker) error {
		close(activationAttempted)
		return errors.New("tenant activation exploded")
	}

	reqCtx, cancelReq := context.WithCancel(context.Background())
	defer cancelReq()
	acquireErr := make(chan error, 1)
	go func() {
		_, err := pool.AcquireWorker(reqCtx, nil)
		acquireErr <- err
	}()

	select {
	case <-spawnStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("spawn never started")
	}
	cancelReq()
	select {
	case err := <-acquireErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AcquireWorker did not return after requester cancellation")
	}

	close(releaseSpawn)
	select {
	case <-activationAttempted:
	case <-time.After(5 * time.Second):
		t.Fatal("detached activation never ran after the requester abandoned")
	}

	// The failed worker must be retired (removed from the pool), not parked and
	// not left Reserved/Activating.
	id := int(spawnedID.Load())
	deadline := time.Now().Add(5 * time.Second)
	for {
		shared.mu.RLock()
		w, ok := shared.workers[id]
		var lifecycle WorkerLifecycleState
		if ok {
			lifecycle = w.SharedState().NormalizedLifecycle()
		}
		shared.mu.RUnlock()
		if !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("activation-failed abandoned worker %d leaked in lifecycle %q", id, lifecycle)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
