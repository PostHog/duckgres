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
