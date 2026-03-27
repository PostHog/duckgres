package controlplane

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type captureControlPlaneExpiryStore struct {
	mu                    sync.Mutex
	cutoffs               []time.Time
	count                 int64
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
	return s.count, nil
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

func TestControlPlaneJanitorRunExpiresStaleInstances(t *testing.T) {
	store := &captureControlPlaneExpiryStore{}
	now := time.Date(2026, time.March, 26, 15, 0, 0, 0, time.UTC)
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }
	janitor.maxDrainTimeout = 15 * time.Minute

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		janitor.Run(ctx)
	}()

	time.Sleep(25 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("janitor did not stop after context cancellation")
	}

	calls := store.snapshot()
	if len(calls) < 2 {
		t.Fatalf("expected janitor to run at least twice, got %d", len(calls))
	}
	wantCutoff := now.Add(-20 * time.Second)
	for i, cutoff := range calls {
		if !cutoff.Equal(wantCutoff) {
			t.Fatalf("call %d expected cutoff %v, got %v", i, wantCutoff, cutoff)
		}
	}
	wantDrainCutoff := now.Add(-15 * time.Minute)
	if len(store.drainingCutoffs) < 2 {
		t.Fatalf("expected janitor to expire overdue draining instances at least twice, got %d", len(store.drainingCutoffs))
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

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		janitor.Run(ctx)
	}()

	time.Sleep(25 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("janitor did not stop after context cancellation")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(retired) < 2 {
		t.Fatalf("expected janitor to retire at least two workers, got %d", len(retired))
	}
	if retired[0].id != 7 || retired[0].reason != janitorRetireReasonLeaseExpiry {
		t.Fatalf("expected orphaned worker 7 with lease_expiry, got %+v", retired[0])
	}
	if retired[1].id != 9 || retired[1].reason != janitorRetireReasonStuckActivating {
		t.Fatalf("expected stuck worker 9 with stuck_activating, got %+v", retired[1])
	}
	if len(store.orphanedBefore) == 0 {
		t.Fatal("expected orphaned worker cutoff lookup")
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

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		janitor.Run(ctx)
	}()

	time.Sleep(25 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("janitor did not stop after context cancellation")
	}

	mu.Lock()
	defer mu.Unlock()
	if calls == 0 {
		t.Fatal("expected janitor to reconcile warm capacity")
	}
}
