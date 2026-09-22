package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// The orphaned-draining sweep retires exactly the stale `draining` rows whose
// pod is verifiably gone: a present pod, a fresh row, and an unverifiable pod
// are all left alone, and the listing cutoff is now - grace.
func TestControlPlaneJanitorOrphanedDrainingSweep(t *testing.T) {
	now := time.Date(2026, time.September, 22, 12, 0, 0, 0, time.UTC)
	draining := func(id int, pod string, updatedAt time.Time) configstore.WorkerRecord {
		return configstore.WorkerRecord{
			WorkerID: id, PodName: pod, State: configstore.WorkerStateDraining, OrgID: "acme",
			OwnerCPInstanceID: "cp-a", OwnerEpoch: 3, UpdatedAt: updatedAt,
		}
	}
	stale := now.Add(-time.Hour)
	store := &captureControlPlaneExpiryStore{
		staleByState: []configstore.WorkerRecord{
			draining(11, "w-gone", stale),       // pod NotFound → retire
			draining(12, "w-present", stale),    // pod still running → keep
			draining(13, "w-fresh", now),        // inside grace → not even listed
			draining(14, "w-unverified", stale), // pod check errors → keep
			{WorkerID: 15, PodName: "w-hot", State: configstore.WorkerStateHot, OwnerCPInstanceID: "cp-a", OwnerEpoch: 1, UpdatedAt: stale}, // wrong state → not listed
		},
	}
	lifecycleStore := &fakeLifecycleStore{terminalReturn: true}
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }
	janitor.lifecycle = NewWorkerLifecycle(lifecycleStore, &fakePhysicalCleanup{})
	var checked []string
	janitor.workerPodGone = func(_ context.Context, podName string) (bool, error) {
		checked = append(checked, podName)
		switch podName {
		case "w-gone":
			return true, nil
		case "w-present":
			return false, nil
		default:
			return false, errors.New("api unavailable")
		}
	}

	janitor.runOnce()

	if len(store.staleByStateCutoffs) != 1 || !store.staleByStateCutoffs[0].Equal(now.Add(-defaultDrainingOrphanGrace)) {
		t.Fatalf("expected one listing at now-%s, got %v", defaultDrainingOrphanGrace, store.staleByStateCutoffs)
	}
	if len(checked) != 3 {
		t.Fatalf("expected the three stale draining pods to be verified, got %v", checked)
	}
	if len(lifecycleStore.terminalTransitions) != 1 {
		t.Fatalf("expected exactly one retire, got %#v", lifecycleStore.terminalTransitions)
	}
	tr := lifecycleStore.terminalTransitions[0]
	if tr.workerID != 11 || tr.target != configstore.WorkerStateRetired || tr.reason != janitorRetireReasonDrainingOrphan || tr.state != configstore.WorkerStateDraining {
		t.Fatalf("unexpected retire: %#v", tr)
	}
}

// A retire error stops the sweep for the tick (never marches on past a
// failing store), and a listing error retires nothing.
func TestControlPlaneJanitorOrphanedDrainingSweepStopsOnRetireError(t *testing.T) {
	now := time.Now()
	stale := now.Add(-time.Hour)
	store := &captureControlPlaneExpiryStore{
		staleByState: []configstore.WorkerRecord{
			{WorkerID: 21, PodName: "w-1", State: configstore.WorkerStateDraining, OwnerCPInstanceID: "cp-a", OwnerEpoch: 1, UpdatedAt: stale},
			{WorkerID: 22, PodName: "w-2", State: configstore.WorkerStateDraining, OwnerCPInstanceID: "cp-a", OwnerEpoch: 1, UpdatedAt: stale},
		},
	}
	lifecycleStore := &fakeLifecycleStore{terminalErr: errors.New("store down")}
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }
	janitor.lifecycle = NewWorkerLifecycle(lifecycleStore, &fakePhysicalCleanup{})
	janitor.workerPodGone = func(context.Context, string) (bool, error) { return true, nil }

	janitor.runOnce()
	if len(lifecycleStore.terminalTransitions) != 1 {
		t.Fatalf("sweep must stop after the first retire error, got %d attempts", len(lifecycleStore.terminalTransitions))
	}

	store.staleByStateErr = errors.New("list failed")
	lifecycleStore.terminalTransitions = nil
	lifecycleStore.terminalErr = nil
	janitor.runOnce()
	if len(lifecycleStore.terminalTransitions) != 0 {
		t.Fatalf("listing error must retire nothing, got %#v", lifecycleStore.terminalTransitions)
	}
}

// Unwired (no clientset → nil workerPodGone) or grace 0 disables the sweep
// entirely: no listing, no retires.
func TestControlPlaneJanitorOrphanedDrainingSweepDisabledWhenUnwired(t *testing.T) {
	stale := time.Now().Add(-time.Hour)
	store := &captureControlPlaneExpiryStore{
		staleByState: []configstore.WorkerRecord{
			{WorkerID: 31, PodName: "w-1", State: configstore.WorkerStateDraining, OwnerCPInstanceID: "cp-a", OwnerEpoch: 1, UpdatedAt: stale},
		},
	}
	lifecycleStore := &fakeLifecycleStore{terminalReturn: true}
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.lifecycle = NewWorkerLifecycle(lifecycleStore, &fakePhysicalCleanup{})

	janitor.runOnce() // workerPodGone nil
	janitor.workerPodGone = func(context.Context, string) (bool, error) { return true, nil }
	janitor.drainingOrphanGrace = 0
	janitor.runOnce() // grace disabled

	if len(store.staleByStateCutoffs) != 0 {
		t.Fatalf("disabled sweep must not list, got %v", store.staleByStateCutoffs)
	}
	if len(lifecycleStore.terminalTransitions) != 0 {
		t.Fatalf("disabled sweep must not retire, got %#v", lifecycleStore.terminalTransitions)
	}
}
