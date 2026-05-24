package controlplane

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type captureControlPlaneExpiryStore struct {
	mu                     sync.Mutex
	cutoffs                []time.Time
	count                  int64
	expireErr              error
	drainingCutoffs        []time.Time
	drainingCount          int64
	orphanedWorkers        []configstore.WorkerRecord
	stuckSpawningBefore    []time.Time
	stuckActivatingBefore  []time.Time
	stuckWorkers           []configstore.WorkerRecord
	expiredSessionsBefore  []time.Time
	expiredHotIdleWorkers  []configstore.WorkerRecord
	pruneMissBucketsBefore []time.Time
	prunedMissBucketCount  int64
	pruneMissBucketErr     error
}

func (s *captureControlPlaneExpiryStore) ExpireControlPlaneInstances(cutoff time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cutoffs = append(s.cutoffs, cutoff)
	return s.count, s.expireErr
}

func (s *captureControlPlaneExpiryStore) ExpireDrainingControlPlaneInstances(before time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.drainingCutoffs = append(s.drainingCutoffs, before)
	return s.drainingCount, nil
}

func (s *captureControlPlaneExpiryStore) snapshot() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Time, len(s.cutoffs))
	copy(out, s.cutoffs)
	return out
}

// ListOrphanedWorkerSnapshots is the lifecycle-typed counterpart used
// by the migrated janitor orphan path. The mock wraps the same orphan
// slice through NewWorkerSnapshot so existing fixtures that
// populate orphanedWorkers also drive the lifecycle path when a
// lifecycle is wired.
func (s *captureControlPlaneExpiryStore) ListOrphanedWorkerSnapshots(before time.Time) ([]configstore.WorkerSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.orphanedWorkers) == 0 {
		return nil, nil
	}
	out := make([]configstore.WorkerSnapshot, 0, len(s.orphanedWorkers))
	for _, rec := range s.orphanedWorkers {
		out = append(out, configstore.NewWorkerSnapshot(rec))
	}
	return out, nil
}

func (s *captureControlPlaneExpiryStore) ListStuckWorkerSnapshots(spawningBefore, activatingBefore time.Time) ([]configstore.WorkerSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stuckSpawningBefore = append(s.stuckSpawningBefore, spawningBefore)
	s.stuckActivatingBefore = append(s.stuckActivatingBefore, activatingBefore)
	if len(s.stuckWorkers) == 0 {
		return nil, nil
	}
	out := make([]configstore.WorkerSnapshot, 0, len(s.stuckWorkers))
	for _, rec := range s.stuckWorkers {
		out = append(out, configstore.NewWorkerSnapshot(rec))
	}
	return out, nil
}

func (s *captureControlPlaneExpiryStore) ExpireFlightSessionRecords(before time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expiredSessionsBefore = append(s.expiredSessionsBefore, before)
	return 0, nil
}

// ListExpiredHotIdleSnapshots is the lifecycle-typed counterpart used
// by the migrated janitor hot-idle path. The mock wraps the same
// underlying slice through NewWorkerSnapshot so existing
// tests can opt into the lifecycle path by setting expiredHotIdleWorkers.
func (s *captureControlPlaneExpiryStore) ListExpiredHotIdleSnapshots(before time.Time) ([]configstore.WorkerSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.expiredHotIdleWorkers) == 0 {
		return nil, nil
	}
	out := make([]configstore.WorkerSnapshot, 0, len(s.expiredHotIdleWorkers))
	for _, rec := range s.expiredHotIdleWorkers {
		out = append(out, configstore.NewWorkerSnapshot(rec))
	}
	return out, nil
}

func (s *captureControlPlaneExpiryStore) PruneWarmCapacityMissBuckets(before time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneMissBucketsBefore = append(s.pruneMissBucketsBefore, before)
	return s.prunedMissBucketCount, s.pruneMissBucketErr
}

func TestControlPlaneJanitorRunExpiresStaleInstances(t *testing.T) {
	store := &captureControlPlaneExpiryStore{}
	now := time.Date(2026, time.March, 26, 15, 0, 0, 0, time.UTC)
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }
	janitor.maxDrainTimeout = 15 * time.Minute

	janitor.runOnce()
	janitor.runOnce()

	calls := store.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected janitor to run exactly twice, got %d", len(calls))
	}
	wantCutoff := now.Add(-20 * time.Second)
	for i, cutoff := range calls {
		if !cutoff.Equal(wantCutoff) {
			t.Fatalf("call %d expected cutoff %v, got %v", i, wantCutoff, cutoff)
		}
	}
	wantDrainCutoff := now.Add(-15 * time.Minute)
	if len(store.drainingCutoffs) != 2 {
		t.Fatalf("expected janitor to expire overdue draining instances exactly twice, got %d", len(store.drainingCutoffs))
	}
	for i, cutoff := range store.drainingCutoffs {
		if !cutoff.Equal(wantDrainCutoff) {
			t.Fatalf("draining call %d expected cutoff %v, got %v", i, wantDrainCutoff, cutoff)
		}
	}
}

func TestControlPlaneJanitorRunPrunesWarmCapacityMissBuckets(t *testing.T) {
	store := &captureControlPlaneExpiryStore{}
	now := time.Date(2026, time.March, 26, 15, 0, 0, 0, time.UTC)
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }

	janitor.runOnce()

	if len(store.pruneMissBucketsBefore) != 1 {
		t.Fatalf("expected one warm capacity miss bucket prune call, got %d", len(store.pruneMissBucketsBefore))
	}
	want := now.Add(-defaultWarmCapacityMissBucketTTL)
	if got := store.pruneMissBucketsBefore[0]; !got.Equal(want) {
		t.Fatalf("expected warm capacity miss bucket prune cutoff %v, got %v", want, got)
	}
}

func TestControlPlaneJanitorRunPrunesWarmCapacityMissBucketsWithConfiguredTTL(t *testing.T) {
	store := &captureControlPlaneExpiryStore{}
	now := time.Date(2026, time.March, 26, 15, 0, 5, 0, time.UTC)
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }
	janitor.warmCapacityMissBucketTTL = 7 * time.Minute

	janitor.runOnce()

	if len(store.pruneMissBucketsBefore) != 1 {
		t.Fatalf("expected one warm capacity miss bucket prune call, got %d", len(store.pruneMissBucketsBefore))
	}
	want := now.Add(-7 * time.Minute).Truncate(configstore.WarmCapacityMissBucketSize)
	if got := store.pruneMissBucketsBefore[0]; !got.Equal(want) {
		t.Fatalf("expected warm capacity miss bucket prune cutoff %v, got %v", want, got)
	}
}

func TestControlPlaneJanitorRunRetiresOrphanedAndStuckWorkers(t *testing.T) {
	store := &captureControlPlaneExpiryStore{
		orphanedWorkers: []configstore.WorkerRecord{
			{WorkerID: 7, PodName: "duckgres-worker-7", State: configstore.WorkerStateHot, OwnerCPInstanceID: "cp-expired:boot-a", OwnerEpoch: 2},
		},
		stuckWorkers: []configstore.WorkerRecord{
			{WorkerID: 9, PodName: "duckgres-worker-9", State: configstore.WorkerStateActivating, OwnerCPInstanceID: "cp-me:boot-b", OwnerEpoch: 1},
		},
	}
	lifecycleStore := &fakeLifecycleStore{orphanReturn: true, terminalReturn: true}
	cleanup := &fakePhysicalCleanup{}
	now := time.Date(2026, time.March, 26, 16, 0, 0, 0, time.UTC)
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }
	janitor.lifecycle = NewWorkerLifecycle(lifecycleStore, cleanup)

	janitor.runOnce()

	if got := lifecycleStore.orphanTransitions; len(got) != 1 || got[0].workerID != 7 || got[0].reason != janitorRetireReasonOrphaned {
		t.Fatalf("expected one orphan CAS via lifecycle for worker 7, got %#v", got)
	}
	if got := lifecycleStore.terminalTransitions; len(got) != 1 || got[0].workerID != 9 || got[0].reason != janitorRetireReasonStuckActivating || got[0].target != configstore.WorkerStateRetired {
		t.Fatalf("expected one terminal CAS via lifecycle for stuck worker 9 → retired, got %#v", got)
	}
	if got := cleanup.snapshot(); len(got) != 2 {
		t.Fatalf("expected two cleanup calls (orphan + stuck), got %#v", got)
	}
	if len(store.stuckSpawningBefore) == 0 || len(store.stuckActivatingBefore) == 0 {
		t.Fatal("expected stuck worker cutoff lookup")
	}
	if len(store.expiredSessionsBefore) == 0 {
		t.Fatal("expected expired flight session cleanup")
	}
}

func TestControlPlaneJanitorRunReconcilesWarmCapacity(t *testing.T) {
	store := &captureControlPlaneExpiryStore{}
	now := time.Date(2026, time.March, 27, 14, 0, 0, 0, time.UTC)
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }

	var mu sync.Mutex
	calls := 0
	janitor.reconcileWarmCapacity = func() {
		mu.Lock()
		defer mu.Unlock()
		calls++
	}

	janitor.runOnce()

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected janitor to reconcile warm capacity exactly once, got %d", calls)
	}
}

func TestControlPlaneJanitorSkipsHotIdlePodDeleteAfterLifecycleCASMiss(t *testing.T) {
	// When the lifecycle's terminal CAS misses (the worker was reclaimed
	// between the list and the CAS), the lifecycle does NOT schedule
	// cleanup. The janitor must not paper over that by invoking any
	// other cleanup path — observability of the miss is the desired
	// signal.
	now := time.Date(2026, time.May, 22, 14, 5, 0, 0, time.UTC)
	listed := configstore.WorkerRecord{
		WorkerID:          12,
		PodName:           "duckgres-worker-12",
		State:             configstore.WorkerStateHotIdle,
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		UpdatedAt:         now.Add(-2 * time.Minute),
	}
	store := &captureControlPlaneExpiryStore{
		expiredHotIdleWorkers: []configstore.WorkerRecord{listed},
	}
	lifecycleStore := &fakeLifecycleStore{terminalReturn: false}
	cleanup := &fakePhysicalCleanup{}
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }
	janitor.hotIdleTTL = time.Minute
	janitor.lifecycle = NewWorkerLifecycle(lifecycleStore, cleanup)

	janitor.runOnce()

	if got := lifecycleStore.terminalTransitions; len(got) != 1 || got[0].workerID != 12 {
		t.Fatalf("expected one terminal CAS attempt for worker 12, got %#v", got)
	}
	if got := cleanup.snapshot(); len(got) != 0 {
		t.Fatalf("expected no cleanup after hot-idle CAS miss, got %#v", got)
	}
}

func TestControlPlaneJanitorHotIdleGoesThroughLifecycleWhenWired(t *testing.T) {
	now := time.Date(2026, time.May, 22, 14, 10, 0, 0, time.UTC)
	listed := configstore.WorkerRecord{
		WorkerID:          21,
		PodName:           "duckgres-worker-21",
		State:             configstore.WorkerStateHotIdle,
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		UpdatedAt:         now.Add(-2 * time.Minute),
	}
	store := &captureControlPlaneExpiryStore{
		expiredHotIdleWorkers: []configstore.WorkerRecord{listed},
	}
	lifecycleStore := &fakeLifecycleStore{terminalReturn: true}
	cleanup := &fakePhysicalCleanup{}
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }
	janitor.hotIdleTTL = time.Minute
	janitor.lifecycle = NewWorkerLifecycle(lifecycleStore, cleanup)

	janitor.runOnce()

	if got := lifecycleStore.terminalTransitions; len(got) != 1 || got[0].workerID != 21 || got[0].target != configstore.WorkerStateRetired {
		t.Fatalf("expected one terminal CAS via lifecycle for worker 21 → retired, got %#v", got)
	}
	if got := cleanup.snapshot(); len(got) != 1 || got[0].workerID != 21 || got[0].reason != "hot_idle_ttl_expired" {
		t.Fatalf("expected lifecycle cleanup for worker 21, got %#v", got)
	}
}

func TestControlPlaneJanitorOrphanGoesThroughLifecycleWhenWired(t *testing.T) {
	// Symmetric to the hot-idle lifecycle test: when a lifecycle service is
	// wired, the orphan path must flow through lifecycle.RetireOrphanFromSnapshot
	// rather than the legacy retireOrphanWorker / retireWorker lambda chain.
	// Guards against the orphan migration silently falling back to legacy
	// because a future refactor accidentally clears the lifecycle field or
	// adds a new fallback branch above the lifecycle check.
	now := time.Date(2026, time.May, 22, 15, 20, 0, 0, time.UTC)
	listed := configstore.WorkerRecord{
		WorkerID:          31,
		PodName:           "duckgres-worker-31",
		State:             configstore.WorkerStateHot,
		OwnerCPInstanceID: "cp-expired:boot-a",
		OwnerEpoch:        2,
		UpdatedAt:         now.Add(-2 * time.Minute),
	}
	store := &captureControlPlaneExpiryStore{
		orphanedWorkers: []configstore.WorkerRecord{listed},
	}
	lifecycleStore := &fakeLifecycleStore{orphanReturn: true}
	cleanup := &fakePhysicalCleanup{}
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }
	janitor.lifecycle = NewWorkerLifecycle(lifecycleStore, cleanup)

	janitor.runOnce()

	if got := lifecycleStore.orphanTransitions; len(got) != 1 || got[0].workerID != 31 || got[0].reason != janitorRetireReasonOrphaned {
		t.Fatalf("expected one orphan CAS via lifecycle for worker 31, got %#v", got)
	}
	if got := cleanup.snapshot(); len(got) != 1 || got[0].workerID != 31 || got[0].reason != janitorRetireReasonOrphaned {
		t.Fatalf("expected lifecycle cleanup for orphan worker 31, got %#v", got)
	}
}

func TestControlPlaneJanitorRunInvokesStrandedPodReconciler(t *testing.T) {
	// Every tick the janitor must invoke the stranded-pod reconciler so that
	// pods leaked by a previous CP (which marked its workers retired but
	// failed to delete the K8s pod) get cleaned up automatically.
	store := &captureControlPlaneExpiryStore{}
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)

	var mu sync.Mutex
	calls := 0
	janitor.cleanupOrphanedWorkerPods = func() {
		mu.Lock()
		defer mu.Unlock()
		calls++
	}

	janitor.runOnce()

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected janitor to invoke stranded-pod reconciler exactly once, got %d", calls)
	}
}

func TestControlPlaneJanitorRunInvokesReconcilerAfterVersionReaper(t *testing.T) {
	// Ordering matters: retireMismatchedVersionWorker retires an idle worker
	// (setting its row to retired) and deletes the pod. If the pod delete
	// fails, the stranded-pod reconciler in the next tick's pass must catch
	// it. Running them in order within the same tick ensures a delete failure
	// gets a retry within ~5s instead of waiting for the next full tick.
	store := &captureControlPlaneExpiryStore{}
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)

	var mu sync.Mutex
	var order []string
	janitor.retireMismatchedVersionWorker = func() {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, "version_reap")
	}
	janitor.cleanupOrphanedWorkerPods = func() {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, "reconcile_stranded")
	}

	janitor.runOnce()

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "version_reap" || order[1] != "reconcile_stranded" {
		t.Fatalf("expected version_reap → reconcile_stranded ordering, got %v", order)
	}
}

func TestControlPlaneJanitorRunInvokesVersionReaperBeforeReconcile(t *testing.T) {
	// The version-aware reaper must run before reconcileWarmCapacity in the
	// same tick so that a retired-this-tick worker's warm slot is replenished
	// immediately rather than waiting a full interval.
	store := &captureControlPlaneExpiryStore{}
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)

	var mu sync.Mutex
	var order []string
	janitor.retireMismatchedVersionWorker = func() {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, "reap")
	}
	janitor.reconcileWarmCapacity = func() {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, "reconcile")
	}

	janitor.runOnce()

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "reap" || order[1] != "reconcile" {
		t.Fatalf("expected reap→reconcile ordering, got %v", order)
	}
}

func TestControlPlaneJanitorRunOnceContinuesAfterExpireError(t *testing.T) {
	store := &captureControlPlaneExpiryStore{
		expireErr: errors.New("boom"),
		orphanedWorkers: []configstore.WorkerRecord{
			{WorkerID: 7, PodName: "duckgres-worker-7", State: configstore.WorkerStateHot, OwnerCPInstanceID: "cp-expired:boot-a", OwnerEpoch: 2},
		},
		stuckWorkers: []configstore.WorkerRecord{
			{WorkerID: 9, PodName: "duckgres-worker-9", State: configstore.WorkerStateActivating, OwnerCPInstanceID: "cp-me:boot-b", OwnerEpoch: 1},
		},
	}
	lifecycleStore := &fakeLifecycleStore{orphanReturn: true, terminalReturn: true}
	cleanup := &fakePhysicalCleanup{}
	now := time.Date(2026, time.March, 27, 18, 0, 0, 0, time.UTC)
	janitor := NewControlPlaneJanitor(store, time.Second, 20*time.Second)
	janitor.now = func() time.Time { return now }
	janitor.lifecycle = NewWorkerLifecycle(lifecycleStore, cleanup)

	var mu sync.Mutex
	reconciled := 0
	janitor.reconcileWarmCapacity = func() {
		mu.Lock()
		defer mu.Unlock()
		reconciled++
	}

	janitor.runOnce()

	if len(store.cutoffs) != 1 {
		t.Fatalf("expected stale control-plane expiry to run once, got %d", len(store.cutoffs))
	}
	if len(store.stuckSpawningBefore) != 1 || len(store.stuckActivatingBefore) != 1 {
		t.Fatalf("expected stuck worker lookup despite expiry error, got spawning=%d activating=%d", len(store.stuckSpawningBefore), len(store.stuckActivatingBefore))
	}
	if len(store.expiredSessionsBefore) != 1 {
		t.Fatalf("expected flight session expiry despite expiry error, got %d", len(store.expiredSessionsBefore))
	}
	if got := lifecycleStore.orphanTransitions; len(got) != 1 || got[0].workerID != 7 {
		t.Fatalf("expected one orphan CAS for worker 7 despite expiry error, got %#v", got)
	}
	if got := lifecycleStore.terminalTransitions; len(got) != 1 || got[0].workerID != 9 {
		t.Fatalf("expected one stuck-worker terminal CAS for worker 9 despite expiry error, got %#v", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if reconciled != 1 {
		t.Fatalf("expected warm capacity reconciliation despite expiry error, got %d", reconciled)
	}
}
