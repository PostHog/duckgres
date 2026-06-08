//go:build kubernetes

package controlplane

import (
	"context"
	"sync"
	"testing"
	"time"
)

// The gate must grant ownership to one holder at a time and release to waiters
// in FIFO arrival order — this is what stops a later connection from snatching a
// worker a longer-waiting one is owed.
func TestOrgAcquireGateFIFOOrder(t *testing.T) {
	g := newOrgAcquireGate()

	// First acquire wins immediately.
	if err := g.acquire(context.Background()); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	const n = 5
	entered := make([]int, 0, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	starts := make([]chan struct{}, n)

	for i := 0; i < n; i++ {
		starts[i] = make(chan struct{})
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-starts[idx] // queue in deterministic order
			if err := g.acquire(context.Background()); err != nil {
				t.Errorf("waiter %d acquire: %v", idx, err)
				return
			}
			mu.Lock()
			entered = append(entered, idx)
			mu.Unlock()
			g.release()
		}(i)
	}

	// Release each waiter onto the queue one at a time so arrival order is 0..n-1.
	for i := 0; i < n; i++ {
		close(starts[i])
		time.Sleep(10 * time.Millisecond)
	}

	g.release() // hand the gate to the FIFO queue head
	wg.Wait()

	for i := 0; i < n; i++ {
		if entered[i] != i {
			t.Fatalf("gate granted out of FIFO order: got %v want [0 1 2 3 4]", entered)
		}
	}
}

// A waiter whose context is cancelled while queued must not deadlock the gate:
// release() skips it and grants to the next live waiter.
func TestOrgAcquireGateCancelledWaiterIsSkipped(t *testing.T) {
	g := newOrgAcquireGate()
	if err := g.acquire(context.Background()); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// Waiter A queues, then cancels.
	ctxA, cancelA := context.WithCancel(context.Background())
	aDone := make(chan error, 1)
	go func() { aDone <- g.acquire(ctxA) }()
	time.Sleep(20 * time.Millisecond)

	// Waiter B queues behind A.
	bGot := make(chan struct{}, 1)
	go func() {
		if err := g.acquire(context.Background()); err == nil {
			bGot <- struct{}{}
		}
	}()
	time.Sleep(20 * time.Millisecond)

	cancelA()
	if err := <-aDone; err == nil {
		t.Fatal("expected cancelled waiter A to return an error")
	}

	// Hand off the gate: A is cancelled, so B must get it.
	g.release()
	select {
	case <-bGot:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter B did not acquire the gate after A cancelled (gate leaked)")
	}
	g.release()
}
