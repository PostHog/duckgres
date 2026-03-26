package controlplane

import (
	"context"
	"sync"
	"testing"
	"time"
)

type captureControlPlaneExpiryStore struct {
	mu      sync.Mutex
	cutoffs []time.Time
	count   int64
}

func (s *captureControlPlaneExpiryStore) ExpireControlPlaneInstances(cutoff time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cutoffs = append(s.cutoffs, cutoff)
	return s.count, nil
}

func (s *captureControlPlaneExpiryStore) snapshot() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Time, len(s.cutoffs))
	copy(out, s.cutoffs)
	return out
}

func TestControlPlaneJanitorRunExpiresStaleInstances(t *testing.T) {
	store := &captureControlPlaneExpiryStore{}
	now := time.Date(2026, time.March, 26, 15, 0, 0, 0, time.UTC)
	janitor := NewControlPlaneJanitor(store, 10*time.Millisecond, 20*time.Second)
	janitor.now = func() time.Time { return now }

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
}
