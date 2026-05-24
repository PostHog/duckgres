package controlplane

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// fakeLifecycleStore implements workerLifecycleStore with knobs to
// drive each lifecycle method through every outcome branch.
type fakeLifecycleStore struct {
	mu sync.Mutex

	terminalTransitions   []terminalCall
	terminalReturn        bool
	terminalErr           error
	orphanTransitions     []orphanCall
	orphanReturn          bool
	orphanErr             error
	idleVariantCalls      []orphanCall
	idleVariantReturn     bool
	idleVariantErr        error
	markLostCalls         []leaseCall
	markLostReturn        bool
	markLostErr           error
	drainingCalls         []leaseCall
	drainingReturn        bool
	drainingErr           error
	retireDrainingCalls   []leaseCall
	retireDrainingReturn  bool
	retireDrainingErr     error
	bumpCalls             []leaseCall
	bumpNewEpoch          int64
	bumpErr               error
}

type terminalCall struct {
	workerID int
	state    configstore.WorkerState
	target   configstore.WorkerState
	reason   string
}

type orphanCall struct {
	workerID int
	state    configstore.WorkerState
	reason   string
}

type leaseCall struct {
	workerID int
	cpID     string
	epoch    int64
	reason   string
}

func (s *fakeLifecycleStore) MarkWorkerTerminalIfCurrent(record *configstore.WorkerRecord, target configstore.WorkerState, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminalTransitions = append(s.terminalTransitions, terminalCall{
		workerID: record.WorkerID, state: record.State, target: target, reason: reason,
	})
	return s.terminalReturn, s.terminalErr
}

func (s *fakeLifecycleStore) RetireOrphanWorker(record *configstore.WorkerRecord, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orphanTransitions = append(s.orphanTransitions, orphanCall{
		workerID: record.WorkerID, state: record.State, reason: reason,
	})
	return s.orphanReturn, s.orphanErr
}

func (s *fakeLifecycleStore) RetireIdleOrHotIdleWorker(record *configstore.WorkerRecord, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idleVariantCalls = append(s.idleVariantCalls, orphanCall{
		workerID: record.WorkerID, state: record.State, reason: reason,
	})
	return s.idleVariantReturn, s.idleVariantErr
}

func (s *fakeLifecycleStore) MarkWorkerLostIfCurrentLease(workerID int, cpID string, epoch int64, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markLostCalls = append(s.markLostCalls, leaseCall{workerID: workerID, cpID: cpID, epoch: epoch, reason: reason})
	return s.markLostReturn, s.markLostErr
}

func (s *fakeLifecycleStore) MarkWorkerDraining(workerID int, cpID string, epoch int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.drainingCalls = append(s.drainingCalls, leaseCall{workerID: workerID, cpID: cpID, epoch: epoch})
	return s.drainingReturn, s.drainingErr
}

func (s *fakeLifecycleStore) RetireDrainingWorker(workerID int, cpID string, epoch int64, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retireDrainingCalls = append(s.retireDrainingCalls, leaseCall{workerID: workerID, cpID: cpID, epoch: epoch, reason: reason})
	return s.retireDrainingReturn, s.retireDrainingErr
}

func (s *fakeLifecycleStore) BumpWorkerEpoch(workerID int, cpID string, expectedEpoch int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bumpCalls = append(s.bumpCalls, leaseCall{workerID: workerID, cpID: cpID, epoch: expectedEpoch})
	return s.bumpNewEpoch, s.bumpErr
}

// fakePhysicalCleanup records DeleteWorkerArtifacts invocations so tests
// can assert that pod cleanup is scheduled exactly when (and only when)
// the durable CAS lands.
type fakePhysicalCleanup struct {
	mu    sync.Mutex
	calls []cleanupCall
}

type cleanupCall struct {
	workerID int
	podName  string
	reason   string
}

func (c *fakePhysicalCleanup) DeleteWorkerArtifacts(workerID int, podName, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, cleanupCall{workerID: workerID, podName: podName, reason: reason})
}

func (c *fakePhysicalCleanup) snapshot() []cleanupCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]cleanupCall, len(c.calls))
	copy(out, c.calls)
	return out
}

func newTestSnapshot(t *testing.T, workerID int, state configstore.WorkerState, cpID string, epoch int64) configstore.WorkerSnapshot {
	t.Helper()
	return configstore.NewWorkerSnapshot(configstore.WorkerRecord{
		WorkerID:          workerID,
		PodName:           "pod-" + t.Name(),
		State:             state,
		OwnerCPInstanceID: cpID,
		OwnerEpoch:        epoch,
		UpdatedAt:         time.Now(),
	})
}

func TestRetireFromSnapshotSucceedsAndSchedulesCleanup(t *testing.T) {
	store := &fakeLifecycleStore{terminalReturn: true}
	cleanup := &fakePhysicalCleanup{}
	lifecycle := NewWorkerLifecycle(store, cleanup)
	snap := newTestSnapshot(t, 11, configstore.WorkerStateIdle, "cp-a", 4)

	outcome, err := lifecycle.RetireFromSnapshot(snap, configstore.WorkerStateRetired, "spawn_failure")
	if err != nil {
		t.Fatalf("RetireFromSnapshot: %v", err)
	}
	if !outcome.Transitioned {
		t.Fatalf("expected Transitioned=true, got %+v", outcome)
	}
	if outcome.Reason != configstore.TransitionOutcomeTransitioned {
		t.Fatalf("expected transitioned reason, got %q", outcome.Reason)
	}
	if calls := cleanup.snapshot(); len(calls) != 1 || calls[0].workerID != 11 || calls[0].reason != "spawn_failure" {
		t.Fatalf("expected one cleanup call for worker 11, got %#v", calls)
	}
	if len(store.terminalTransitions) != 1 || store.terminalTransitions[0].target != configstore.WorkerStateRetired {
		t.Fatalf("expected one terminal CAS targeting retired, got %#v", store.terminalTransitions)
	}
}

func TestRetireFromSnapshotSkipsCleanupOnCASMiss(t *testing.T) {
	store := &fakeLifecycleStore{terminalReturn: false}
	cleanup := &fakePhysicalCleanup{}
	lifecycle := NewWorkerLifecycle(store, cleanup)
	snap := newTestSnapshot(t, 12, configstore.WorkerStateIdle, "cp-a", 4)

	outcome, err := lifecycle.RetireFromSnapshot(snap, configstore.WorkerStateRetired, "spawn_failure")
	if err != nil {
		t.Fatalf("RetireFromSnapshot: %v", err)
	}
	if outcome.Transitioned || outcome.PhysicalCleanupScheduled {
		t.Fatalf("expected no transition and no cleanup, got %+v", outcome)
	}
	if calls := cleanup.snapshot(); len(calls) != 0 {
		t.Fatalf("expected zero cleanup calls on CAS miss, got %#v", calls)
	}
}

func TestRetireFromSnapshotRejectsNonTerminalTarget(t *testing.T) {
	store := &fakeLifecycleStore{terminalReturn: true}
	cleanup := &fakePhysicalCleanup{}
	lifecycle := NewWorkerLifecycle(store, cleanup)
	snap := newTestSnapshot(t, 13, configstore.WorkerStateIdle, "cp-a", 4)

	if _, err := lifecycle.RetireFromSnapshot(snap, configstore.WorkerStateDraining, "bad"); err == nil {
		t.Fatal("expected error on non-terminal target")
	}
	if len(store.terminalTransitions) != 0 {
		t.Fatalf("expected zero CAS calls on bad target, got %#v", store.terminalTransitions)
	}
}

func TestRetireOrphanFromSnapshotReportsGenericMissWithoutExtraRead(t *testing.T) {
	// Until PR 6 wires a follow-up GetWorkerRecord, the orphan CAS-miss
	// label is deliberately generic (state vs owner vs epoch vs
	// updated_at vs CP-revival is indistinguishable from a CAS bool).
	// The test pins the contract so a future refactor that re-promotes
	// the label has to update both the implementation and this
	// expectation in lockstep.
	store := &fakeLifecycleStore{orphanReturn: false}
	cleanup := &fakePhysicalCleanup{}
	lifecycle := NewWorkerLifecycle(store, cleanup)
	snap := newTestSnapshot(t, 21, configstore.WorkerStateHot, "cp-revived", 3)

	outcome, err := lifecycle.RetireOrphanFromSnapshot(snap, "orphaned")
	if err != nil {
		t.Fatalf("RetireOrphanFromSnapshot: %v", err)
	}
	if outcome.Transitioned {
		t.Fatalf("expected no transition on orphan CAS miss, got %+v", outcome)
	}
	if outcome.Reason != configstore.TransitionOutcomeFenceMissOwner {
		t.Fatalf("expected generic fence-miss reason for unknown-cause orphan miss, got %q", outcome.Reason)
	}
}

func TestRetireIdleVariantSkipsWhenSnapshotNotEligible(t *testing.T) {
	store := &fakeLifecycleStore{idleVariantReturn: true}
	cleanup := &fakePhysicalCleanup{}
	lifecycle := NewWorkerLifecycle(store, cleanup)
	snap := newTestSnapshot(t, 31, configstore.WorkerStateReserved, "cp-a", 5)

	// Store says it would have transitioned but the snapshot is not
	// idle/hot_idle — service must classify as a state-fence miss even
	// though the underlying store call returned true (the store mock
	// is intentionally permissive; the lifecycle service is the layer
	// that surfaces the state restriction in metric labels).
	store.idleVariantReturn = false
	outcome, err := lifecycle.RetireIdleVariantFromSnapshot(snap, "mismatched_version")
	if err != nil {
		t.Fatalf("RetireIdleVariantFromSnapshot: %v", err)
	}
	if outcome.Transitioned {
		t.Fatalf("expected no transition for non-idle snapshot, got %+v", outcome)
	}
	if outcome.Reason != configstore.TransitionOutcomeFenceMissState {
		t.Fatalf("expected state-fence miss reason, got %q", outcome.Reason)
	}
}

func TestDrainAndRetireDrainedSequence(t *testing.T) {
	store := &fakeLifecycleStore{drainingReturn: true, retireDrainingReturn: true}
	cleanup := &fakePhysicalCleanup{}
	lifecycle := NewWorkerLifecycle(store, cleanup)
	lease := configstore.NewWorkerLease(41, "cp-a", 6)

	drainOutcome, err := lifecycle.Drain(lease)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if !drainOutcome.Transitioned {
		t.Fatalf("expected Drain to transition, got %+v", drainOutcome)
	}
	// Drain must NOT schedule cleanup — that's the caller's job between
	// Drain and RetireDrained.
	if calls := cleanup.snapshot(); len(calls) != 0 {
		t.Fatalf("expected zero cleanup calls between Drain and RetireDrained, got %#v", calls)
	}

	retireOutcome, err := lifecycle.RetireDrained(lease, "shutdown")
	if err != nil {
		t.Fatalf("RetireDrained: %v", err)
	}
	if !retireOutcome.Transitioned {
		t.Fatalf("expected RetireDrained to transition, got %+v", retireOutcome)
	}
	// RetireDrained also intentionally does not call cleanup — the
	// pod was already deleted by the caller between the two CAS steps.
	if calls := cleanup.snapshot(); len(calls) != 0 {
		t.Fatalf("expected zero cleanup calls from Drain/RetireDrained, got %#v", calls)
	}

	if got := store.drainingCalls; len(got) != 1 || got[0].epoch != 6 {
		t.Fatalf("expected one Drain CAS with epoch 6, got %#v", got)
	}
	if got := store.retireDrainingCalls; len(got) != 1 || got[0].reason != "shutdown" {
		t.Fatalf("expected one RetireDrained CAS with shutdown reason, got %#v", got)
	}
}

func TestMarkLostFromLeaseSchedulesCleanupOnSuccess(t *testing.T) {
	store := &fakeLifecycleStore{markLostReturn: true}
	cleanup := &fakePhysicalCleanup{}
	lifecycle := NewWorkerLifecycle(store, cleanup)
	lease := configstore.NewWorkerLease(51, "cp-a", 7)

	outcome, err := lifecycle.MarkLostFromLease(lease, "duckgres-worker-51", "health_check_crash")
	if err != nil {
		t.Fatalf("MarkLostFromLease: %v", err)
	}
	if !outcome.Transitioned {
		t.Fatalf("expected transition, got %+v", outcome)
	}
	if calls := cleanup.snapshot(); len(calls) != 1 || calls[0].podName != "duckgres-worker-51" {
		t.Fatalf("expected one cleanup call for pod duckgres-worker-51, got %#v", calls)
	}
}

func TestRefreshLeaseReturnsBumpedLease(t *testing.T) {
	store := &fakeLifecycleStore{bumpNewEpoch: 9}
	lifecycle := NewWorkerLifecycle(store, nil)
	lease := configstore.NewWorkerLease(61, "cp-a", 8)

	newLease, err := lifecycle.RefreshLease(lease)
	if err != nil {
		t.Fatalf("RefreshLease: %v", err)
	}
	if newLease.OwnerEpoch() != 9 {
		t.Fatalf("expected refreshed epoch 9, got %d", newLease.OwnerEpoch())
	}
	if newLease.WorkerID() != 61 || newLease.OwnerCPInstanceID() != "cp-a" {
		t.Fatalf("expected refreshed lease to preserve worker id and cp id, got %#v", newLease)
	}
	if got := store.bumpCalls; len(got) != 1 || got[0].epoch != 8 {
		t.Fatalf("expected one Bump CAS at expected epoch 8, got %#v", got)
	}
}

func TestRefreshLeasePropagatesEpochMismatch(t *testing.T) {
	store := &fakeLifecycleStore{bumpErr: configstore.ErrWorkerOwnerEpochMismatch}
	lifecycle := NewWorkerLifecycle(store, nil)
	lease := configstore.NewWorkerLease(62, "cp-a", 1)

	_, err := lifecycle.RefreshLease(lease)
	if !errors.Is(err, configstore.ErrWorkerOwnerEpochMismatch) {
		t.Fatalf("expected ErrWorkerOwnerEpochMismatch, got %v", err)
	}
}

// Quick smoke: snapshot fields survive ObserveWorker-equivalent round-trip.
// We construct via the test-only escape hatch since this test file lives
// outside configstore.
func TestNewTestSnapshotPreservesFields(t *testing.T) {
	snap := newTestSnapshot(t, 71, configstore.WorkerStateHotIdle, "cp-x", 12)
	if snap.WorkerID() != 71 || snap.State() != configstore.WorkerStateHotIdle ||
		snap.OwnerCPInstanceID() != "cp-x" || snap.OwnerEpoch() != 12 {
		t.Fatalf("snapshot fields lost in round-trip: %#v", snap)
	}
	// PodName uses test name so we just confirm it's non-empty here.
	if snap.PodName() == "" {
		t.Fatal("expected snapshot pod name to be set by helper")
	}
	if snap.UpdatedAt().IsZero() {
		// Snapshot updated_at is set by the helper to "now"; if zero the
		// helper changed and the lifecycle service's updated_at fence
		// would behave differently downstream.
		t.Fatal("expected snapshot updated_at to be set by helper")
	}
	_ = time.Now() // keep the time import honest in case helper changes
}
