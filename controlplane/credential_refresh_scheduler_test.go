//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type fakeCredRefreshStore struct {
	mu      sync.Mutex
	due     []configstore.WorkerRecord
	listErr error
	calls   int
	cutoffs []time.Time
	owners  []string
}

func (f *fakeCredRefreshStore) ListWorkersDueForCredentialRefresh(ownerCPInstanceID string, cutoff time.Time) ([]configstore.WorkerRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.cutoffs = append(f.cutoffs, cutoff)
	f.owners = append(f.owners, ownerCPInstanceID)
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]configstore.WorkerRecord, len(f.due))
	copy(out, f.due)
	return out, nil
}

// TestCredentialRefreshSchedulerTickRefreshesDueWorkers proves the scheduler
// looks each due worker up in the local pool and fires the refresh hook for
// each one it finds.
func TestCredentialRefreshSchedulerTickRefreshesDueWorkers(t *testing.T) {
	w1 := &ManagedWorker{ID: 101}
	w2 := &ManagedWorker{ID: 102}
	store := &fakeCredRefreshStore{
		due: []configstore.WorkerRecord{
			{WorkerID: 101, OrgID: "org-a"},
			{WorkerID: 102, OrgID: "org-b"},
		},
	}

	pool := map[int]*ManagedWorker{101: w1, 102: w2}
	var refreshed []int
	var mu sync.Mutex
	scheduler := &credentialRefreshScheduler{
		interval:     time.Millisecond,
		lookahead:    30 * time.Minute,
		cpInstanceID: "cp-me",
		store:        store,
		now:          func() time.Time { return time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC) },
		workerByID: func(id int) (*ManagedWorker, bool) {
			w, ok := pool[id]
			return w, ok
		},
		refresh: func(ctx context.Context, w *ManagedWorker) error {
			mu.Lock()
			defer mu.Unlock()
			refreshed = append(refreshed, w.ID)
			return nil
		},
	}

	scheduler.tick(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(refreshed) != 2 || refreshed[0] != 101 || refreshed[1] != 102 {
		t.Fatalf("expected workers 101 and 102 to be refreshed in order, got %v", refreshed)
	}
	if len(store.owners) == 0 || store.owners[0] != "cp-me" {
		t.Fatalf("expected query to filter on cpInstanceID 'cp-me', got %v", store.owners)
	}
	wantCutoff := time.Date(2026, time.April, 30, 12, 30, 0, 0, time.UTC)
	if !store.cutoffs[0].Equal(wantCutoff) {
		t.Fatalf("expected cutoff %v, got %v", wantCutoff, store.cutoffs[0])
	}
}

// TestCredentialRefreshSchedulerTickSkipsMissingFromPool ensures workers we
// no longer hold in the in-memory pool are silently skipped — we'll catch
// them on a later tick if they come back.
func TestCredentialRefreshSchedulerTickSkipsMissingFromPool(t *testing.T) {
	store := &fakeCredRefreshStore{
		due: []configstore.WorkerRecord{{WorkerID: 999, OrgID: "org-x"}},
	}
	var refreshed int
	scheduler := &credentialRefreshScheduler{
		lookahead:    30 * time.Minute,
		cpInstanceID: "cp-me",
		store:        store,
		now:          time.Now,
		workerByID:   func(int) (*ManagedWorker, bool) { return nil, false },
		refresh: func(context.Context, *ManagedWorker) error {
			refreshed++
			return nil
		},
	}

	scheduler.tick(context.Background())

	if refreshed != 0 {
		t.Fatalf("expected refresh hook not to fire when worker is missing from pool, got %d calls", refreshed)
	}
}

// TestCredentialRefreshSchedulerTickContinuesAfterRefreshError makes sure a
// refresh failure for one worker does not abort the rest of the batch — each
// worker is independent and a single STS hiccup should not block the others.
func TestCredentialRefreshSchedulerTickContinuesAfterRefreshError(t *testing.T) {
	w1 := &ManagedWorker{ID: 1}
	w2 := &ManagedWorker{ID: 2}
	store := &fakeCredRefreshStore{
		due: []configstore.WorkerRecord{
			{WorkerID: 1, OrgID: "org-a"},
			{WorkerID: 2, OrgID: "org-b"},
		},
	}
	pool := map[int]*ManagedWorker{1: w1, 2: w2}
	var refreshed []int
	scheduler := &credentialRefreshScheduler{
		lookahead:    30 * time.Minute,
		cpInstanceID: "cp-me",
		store:        store,
		now:          time.Now,
		workerByID: func(id int) (*ManagedWorker, bool) {
			w, ok := pool[id]
			return w, ok
		},
		refresh: func(ctx context.Context, w *ManagedWorker) error {
			refreshed = append(refreshed, w.ID)
			if w.ID == 1 {
				return errors.New("sts boom")
			}
			return nil
		},
	}

	scheduler.tick(context.Background())

	if len(refreshed) != 2 || refreshed[0] != 1 || refreshed[1] != 2 {
		t.Fatalf("expected scheduler to keep going after worker 1 errored, got %v", refreshed)
	}
}

// TestCredentialRefreshSchedulerTickHandlesListError ensures a transient DB
// error doesn't crash the scheduler — the next tick will retry.
func TestCredentialRefreshSchedulerTickHandlesListError(t *testing.T) {
	store := &fakeCredRefreshStore{listErr: errors.New("db down")}
	scheduler := &credentialRefreshScheduler{
		lookahead:    30 * time.Minute,
		cpInstanceID: "cp-me",
		store:        store,
		now:          time.Now,
		workerByID:   func(int) (*ManagedWorker, bool) { return nil, false },
		refresh:      func(context.Context, *ManagedWorker) error { return nil },
	}

	scheduler.tick(context.Background()) // would panic on a nil deref if not handled
}

// TestCredentialRefreshSchedulerRunStopsOnContextCancel confirms ctx
// cancellation actually unblocks Run — required so SetupMultiTenant doesn't
// leak the goroutine on shutdown.
func TestCredentialRefreshSchedulerRunStopsOnContextCancel(t *testing.T) {
	store := &fakeCredRefreshStore{}
	scheduler := &credentialRefreshScheduler{
		interval:     10 * time.Millisecond,
		lookahead:    30 * time.Minute,
		cpInstanceID: "cp-me",
		store:        store,
		now:          time.Now,
		workerByID:   func(int) (*ManagedWorker, bool) { return nil, false },
		refresh:      func(context.Context, *ManagedWorker) error { return nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		scheduler.Run(ctx)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop after context cancellation")
	}

	store.mu.Lock()
	if store.calls < 1 {
		t.Errorf("expected scheduler to tick at least once before cancel, got %d", store.calls)
	}
	store.mu.Unlock()
}
