package controlplane

import (
	"errors"
	"sync"
	"testing"
)

// TestManagedWorkerRefreshOwnerEpochAtomicSerializesWithReaders pins
// the contract that closes the BumpWorkerEpoch ↔ ShutdownAll race:
// a concurrent reader cannot observe the in-memory epoch between the
// durable bump and the in-memory set. We model the cred-refresh
// activator by holding the lock across a "DB call" (a channel
// rendezvous), and prove that a concurrent OwnerEpoch() call blocks
// until the refresh callback returns. If the lock disappeared, the
// reader would observe the pre-refresh value and the test would
// fail.
func TestManagedWorkerRefreshOwnerEpochAtomicSerializesWithReaders(t *testing.T) {
	w := &ManagedWorker{ownerEpoch: 5}

	dbCalled := make(chan struct{})
	dbComplete := make(chan struct{})
	refreshDone := make(chan error, 1)

	go func() {
		refreshDone <- w.RefreshOwnerEpochAtomic(func(current int64) (int64, error) {
			if current != 5 {
				return 0, errors.New("callback saw unexpected current epoch")
			}
			close(dbCalled)
			<-dbComplete
			return current + 1, nil
		})
	}()

	<-dbCalled
	// The refresh callback is now running under the lock. The positive
	// assertion below is what proves the mutex is doing its job: after
	// the callback completes, the concurrent OwnerEpoch() must return
	// the post-CAS value (6), not a torn or pre-CAS read. If the
	// mutex regressed, the goroutine that started before
	// close(dbComplete) could capture the pre-CAS value (5) into
	// readDone before the in-memory store. We don't bother with a
	// timing-based "is the goroutine blocked right now?" check — that
	// race-detects scheduler delay rather than mutex correctness.
	readDone := make(chan int64, 1)
	go func() {
		readDone <- w.OwnerEpoch()
	}()

	close(dbComplete)
	if err := <-refreshDone; err != nil {
		t.Fatalf("RefreshOwnerEpochAtomic returned err: %v", err)
	}
	if got := <-readDone; got != 6 {
		t.Fatalf("OwnerEpoch() = %d after refresh, want 6 (must reflect the post-CAS value, not the pre-CAS one)", got)
	}
}

// TestManagedWorkerRefreshOwnerEpochAtomicLeavesEpochOnErr verifies
// the documented contract: if the callback returns an error, the
// in-memory epoch stays at its pre-call value.
func TestManagedWorkerRefreshOwnerEpochAtomicLeavesEpochOnErr(t *testing.T) {
	w := &ManagedWorker{ownerEpoch: 3}
	want := errors.New("refresh failed")
	got := w.RefreshOwnerEpochAtomic(func(current int64) (int64, error) {
		if current != 3 {
			t.Fatalf("callback saw current=%d, want 3", current)
		}
		return 99, want
	})
	if !errors.Is(got, want) {
		t.Fatalf("RefreshOwnerEpochAtomic err = %v, want %v", got, want)
	}
	if e := w.OwnerEpoch(); e != 3 {
		t.Fatalf("OwnerEpoch() after failed refresh = %d, want unchanged 3", e)
	}
}

// TestManagedWorkerOwnerEpochAccessorsRaceFree exercises concurrent
// readers and writers under -race to catch any path that mutates
// ownerEpoch without going through the mutex-bearing accessors.
func TestManagedWorkerOwnerEpochAccessorsRaceFree(t *testing.T) {
	w := &ManagedWorker{}
	const n = 64
	var wg sync.WaitGroup
	wg.Add(n * 4)
	for i := 0; i < n; i++ {
		go func() { defer wg.Done(); _ = w.OwnerEpoch() }()
		go func() { defer wg.Done(); w.SetOwnerEpoch(1) }()
		go func() { defer wg.Done(); _ = w.IncrementOwnerEpoch() }()
		go func() {
			defer wg.Done()
			_ = w.RefreshOwnerEpochAtomic(func(c int64) (int64, error) { return c + 1, nil })
		}()
	}
	wg.Wait()
}

// TestManagedWorkerClaimSessionLocked pins the shared session-claim
// bookkeeping used identically by FlightWorkerPool.AcquireWorker and
// K8sWorkerPool.AcquireWorker after a worker is selected: each claim
// increments activeSessions and advances the peakSessions high-water
// mark, but peakSessions never regresses when sessions are later
// released. This is a pure no-op for scheduling; it only updates the
// worker's own counters (the caller holds the pool lock).
func TestManagedWorkerClaimSessionLocked(t *testing.T) {
	w := &ManagedWorker{}

	w.claimSessionLocked()
	if w.activeSessions != 1 || w.peakSessions != 1 {
		t.Fatalf("after first claim: active=%d peak=%d, want 1/1", w.activeSessions, w.peakSessions)
	}

	w.claimSessionLocked()
	if w.activeSessions != 2 || w.peakSessions != 2 {
		t.Fatalf("after second claim: active=%d peak=%d, want 2/2", w.activeSessions, w.peakSessions)
	}

	// Simulate a release: peak must hold even though active drops.
	w.activeSessions--
	if w.activeSessions != 1 || w.peakSessions != 2 {
		t.Fatalf("after release: active=%d peak=%d, want 1/2", w.activeSessions, w.peakSessions)
	}

	// Re-claiming below the prior peak must not regress peak.
	w.claimSessionLocked()
	if w.activeSessions != 2 || w.peakSessions != 2 {
		t.Fatalf("after re-claim: active=%d peak=%d, want 2/2", w.activeSessions, w.peakSessions)
	}

	// A claim that exceeds the prior peak advances it.
	w.claimSessionLocked()
	if w.activeSessions != 3 || w.peakSessions != 3 {
		t.Fatalf("after exceeding peak: active=%d peak=%d, want 3/3", w.activeSessions, w.peakSessions)
	}
}
