package controlplane

import (
	"testing"
	"time"
)

func TestSharedWorkerStateZeroValueDefaultsToIdle(t *testing.T) {
	var state SharedWorkerState

	if got := state.NormalizedLifecycle(); got != WorkerLifecycleIdle {
		t.Fatalf("expected zero value lifecycle to normalize to %q, got %q", WorkerLifecycleIdle, got)
	}
	if err := state.Validate(); err != nil {
		t.Fatalf("expected zero value state to validate: %v", err)
	}
	if state.Assignment != nil {
		t.Fatalf("expected zero value state to be unassigned, got %#v", state.Assignment)
	}
}

func TestSharedWorkerStateTransitionLifecycle(t *testing.T) {
	leaseExpiry := time.Date(2026, time.March, 20, 16, 0, 0, 0, time.UTC)

	state, err := (SharedWorkerState{}).Transition(WorkerLifecycleReserved, &WorkerAssignment{
		OrgID:          "analytics",
		LeaseExpiresAt: leaseExpiry,
	})
	if err != nil {
		t.Fatalf("reserve worker: %v", err)
	}
	if got := state.NormalizedLifecycle(); got != WorkerLifecycleReserved {
		t.Fatalf("expected reserved lifecycle, got %q", got)
	}
	if state.Assignment == nil || state.Assignment.OrgID != "analytics" {
		t.Fatalf("expected analytics assignment, got %#v", state.Assignment)
	}
	if !state.Assignment.LeaseExpiresAt.Equal(leaseExpiry) {
		t.Fatalf("expected lease expiry %v, got %v", leaseExpiry, state.Assignment.LeaseExpiresAt)
	}

	for _, next := range []WorkerLifecycleState{
		WorkerLifecycleActivating,
		WorkerLifecycleHot,
		WorkerLifecycleDraining,
		WorkerLifecycleRetired,
	} {
		state, err = state.Transition(next, nil)
		if err != nil {
			t.Fatalf("transition to %q: %v", next, err)
		}
	}

	if got := state.NormalizedLifecycle(); got != WorkerLifecycleRetired {
		t.Fatalf("expected retired lifecycle, got %q", got)
	}
	if state.Assignment == nil || state.Assignment.OrgID != "analytics" {
		t.Fatalf("expected retired worker to retain last assignment metadata, got %#v", state.Assignment)
	}
}

func TestSharedWorkerStateTransitionRejectsMissingOrInvalidAssignment(t *testing.T) {
	if _, err := (SharedWorkerState{}).Transition(WorkerLifecycleReserved, nil); err == nil {
		t.Fatal("expected reserve transition without assignment to fail")
	}

	if _, err := (SharedWorkerState{}).Transition(WorkerLifecycleReserved, &WorkerAssignment{
		LeaseExpiresAt: time.Now().Add(time.Hour),
	}); err == nil {
		t.Fatal("expected reserve transition without org ID to fail")
	}

	if _, err := (SharedWorkerState{}).Transition(WorkerLifecycleReserved, &WorkerAssignment{
		OrgID: "analytics",
	}); err == nil {
		t.Fatal("expected reserve transition without lease expiry to fail")
	}
}

func TestSharedWorkerStateTransitionRejectsInvalidLifecycleMoves(t *testing.T) {
	leaseExpiry := time.Date(2026, time.March, 20, 16, 0, 0, 0, time.UTC)
	state, err := (SharedWorkerState{}).Transition(WorkerLifecycleReserved, &WorkerAssignment{
		OrgID:          "analytics",
		LeaseExpiresAt: leaseExpiry,
	})
	if err != nil {
		t.Fatalf("reserve worker: %v", err)
	}

	if _, err := state.Transition(WorkerLifecycleHot, nil); err == nil {
		t.Fatal("expected reserved -> hot transition to fail")
	}

	state, err = state.Transition(WorkerLifecycleActivating, nil)
	if err != nil {
		t.Fatalf("transition to activating: %v", err)
	}

	if _, err := state.Transition(WorkerLifecycleHot, &WorkerAssignment{
		OrgID:          "billing",
		LeaseExpiresAt: leaseExpiry.Add(time.Hour),
	}); err == nil {
		t.Fatal("expected activating -> hot transition to reject assignment changes")
	}

	state, err = state.Transition(WorkerLifecycleHot, nil)
	if err != nil {
		t.Fatalf("transition to hot: %v", err)
	}

	if _, err := state.Transition(WorkerLifecycleReserved, nil); err == nil {
		t.Fatal("expected hot -> reserved transition to fail")
	}
}
