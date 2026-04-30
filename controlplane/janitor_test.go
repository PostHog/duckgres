package controlplane

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type captureControlPlaneExpiryStore struct {
	mu                    sync.Mutex
	cutoffs               []time.Time
	count                 int64
	expireErr             error
	drainingCutoffs       []time.Time
	drainingCount         int64
	orphanedBefore        []time.Time
	orphanedWorkers       []configstore.WorkerRecord
	stuckSpawningBefore   []time.Time
	stuckActivatingBefore []time.Time
	stuckWorkers          []configstore.WorkerRecord
	expiredSessionsBefore []time.Time
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

func (s *captureControlPlaneExpiryStore) ListOrphanedWorkers(before time.Time) ([]configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orphanedBefore = append(s.orphanedBefore, before)
	out := make([]configstore.WorkerRecord, len(s.orphanedWorkers))
	copy(out, s.orphanedWorkers)
	return out, nil
}

func (s *captureControlPlaneExpiryStore) ListStuckWorkers(spawningBefore, activatingBefore time.Time) ([]configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stuckSpawningBefore = append(s.stuckSpawningBefore, spawningBefore)
	s.stuckActivatingBefore = append(s.stuckActivatingBefore, activatingBefore)
	out := make([]configstore.WorkerRecord, len(s.stuckWorkers))
	copy(out, s.stuckWorkers)
	return out, nil
}

func (s *captureControlPlaneExpiryStore) ExpireFlightSessionRecords(before time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expiredSessionsBefore = append(s.expiredSessionsBefore, before)
	return 0, nil
}

func (s *captureControlPlaneExpiryStore) ListExpiredHotIdleWorkers(before time.Time) ([]configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nil, nil
}

func (s *captureControlPlaneExpiryStore) RetireHotIdleWorker(workerID int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return true, nil
}

func (s *captureControlPlaneExpiryStore) ListWorkersDueForCredentialRefresh(ownerCPInstanceID string, cutoff time.Time) ([]configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nil, nil
}

func (s *captureControlPlaneExpiryStore) MarkCredentialsRefreshed(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64, expiresAt time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return true, nil
}

// TestControlPlaneJanitorRunCallsRefreshExpiringCredentials proves the
// scheduler hook fires every tick on the leader. The actual STS / RPC
// work is exercised by the postgres-backed and integration tests; here
// we just verify wiring so a future regression in runOnce can't silently
// drop the credential refresh.
func TestControlPlaneJanitorRunCallsRefreshExpiringCredentials(t *testing.T) {
	store := &captureControlPlaneExpiryStore{}
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC) }

	var calls int
	janitor.refreshExpiringCredentials = func() { calls++ }

	janitor.runOnce()
	janitor.runOnce()

	if calls != 2 {
		t.Fatalf("expected refreshExpiringCredentials to fire on each runOnce; got %d calls in 2 ticks", calls)
	}
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

func TestControlPlaneJanitorRunRetiresOrphanedAndStuckWorkers(t *testing.T) {
	store := &captureControlPlaneExpiryStore{
		orphanedWorkers: []configstore.WorkerRecord{
			{WorkerID: 7, PodName: "duckgres-worker-7"},
		},
		stuckWorkers: []configstore.WorkerRecord{
			{WorkerID: 9, PodName: "duckgres-worker-9", State: configstore.WorkerStateActivating},
		},
	}
	now := time.Date(2026, time.March, 26, 16, 0, 0, 0, time.UTC)
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }

	var mu sync.Mutex
	var retired []struct {
		id     int
		reason string
	}
	janitor.retireWorker = func(record configstore.WorkerRecord, reason string) {
		mu.Lock()
		defer mu.Unlock()
		retired = append(retired, struct {
			id     int
			reason string
		}{id: record.WorkerID, reason: reason})
	}

	janitor.runOnce()

	mu.Lock()
	defer mu.Unlock()
	if len(retired) != 2 {
		t.Fatalf("expected janitor to retire exactly two workers, got %d", len(retired))
	}
	if retired[0].id != 7 || retired[0].reason != janitorRetireReasonOrphaned {
		t.Fatalf("expected orphaned worker 7 with orphaned reason, got %+v", retired[0])
	}
	if retired[1].id != 9 || retired[1].reason != janitorRetireReasonStuckActivating {
		t.Fatalf("expected stuck worker 9 with stuck_activating, got %+v", retired[1])
	}
	if len(store.orphanedBefore) == 0 {
		t.Fatal("expected orphaned worker cutoff lookup")
	}
	wantOrphanedBefore := now.Add(-30 * time.Second)
	for i, cutoff := range store.orphanedBefore {
		if !cutoff.Equal(wantOrphanedBefore) {
			t.Fatalf("orphaned cutoff call %d expected %v, got %v", i, wantOrphanedBefore, cutoff)
		}
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
			{WorkerID: 7, PodName: "duckgres-worker-7"},
		},
		stuckWorkers: []configstore.WorkerRecord{
			{WorkerID: 9, PodName: "duckgres-worker-9", State: configstore.WorkerStateActivating},
		},
	}
	now := time.Date(2026, time.March, 27, 18, 0, 0, 0, time.UTC)
	janitor := NewControlPlaneJanitor(store, time.Second, 20*time.Second)
	janitor.now = func() time.Time { return now }

	var mu sync.Mutex
	var retired []struct {
		id     int
		reason string
	}
	reconciled := 0
	janitor.retireWorker = func(record configstore.WorkerRecord, reason string) {
		mu.Lock()
		defer mu.Unlock()
		retired = append(retired, struct {
			id     int
			reason string
		}{id: record.WorkerID, reason: reason})
	}
	janitor.reconcileWarmCapacity = func() {
		mu.Lock()
		defer mu.Unlock()
		reconciled++
	}

	janitor.runOnce()

	if len(store.cutoffs) != 1 {
		t.Fatalf("expected stale control-plane expiry to run once, got %d", len(store.cutoffs))
	}
	if len(store.orphanedBefore) != 1 {
		t.Fatalf("expected orphaned worker lookup despite expiry error, got %d", len(store.orphanedBefore))
	}
	if len(store.stuckSpawningBefore) != 1 || len(store.stuckActivatingBefore) != 1 {
		t.Fatalf("expected stuck worker lookup despite expiry error, got spawning=%d activating=%d", len(store.stuckSpawningBefore), len(store.stuckActivatingBefore))
	}
	if len(store.expiredSessionsBefore) != 1 {
		t.Fatalf("expected flight session expiry despite expiry error, got %d", len(store.expiredSessionsBefore))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(retired) != 2 {
		t.Fatalf("expected orphaned and stuck workers to be retired, got %+v", retired)
	}
	if reconciled != 1 {
		t.Fatalf("expected warm capacity reconciliation despite expiry error, got %d", reconciled)
	}
}
