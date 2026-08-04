//go:build kubernetes

package controlplane

import (
	"context"
	"sync"
	"testing"
	"time"
)

type captureLeaderElector struct {
	ctxDone chan struct{}
}

func (e *captureLeaderElector) Run(ctx context.Context) {
	<-ctx.Done()
	close(e.ctxDone)
}

// countingLeaderElector models client-go's LeaderElector after a lost lease:
// Run records the call and returns immediately rather than blocking. A correct
// manager must re-invoke Run (re-contend) rather than calling it once.
type countingLeaderElector struct {
	mu    sync.Mutex
	calls int
	runCh chan struct{}
}

func (e *countingLeaderElector) Run(ctx context.Context) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	select {
	case e.runCh <- struct{}{}:
	default:
	}
}

func (e *countingLeaderElector) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func TestJanitorLeaderManagerStopCancelsLeaderElection(t *testing.T) {
	elector := &captureLeaderElector{ctxDone: make(chan struct{})}
	manager := &JanitorLeaderManager{
		elector:    elector,
		leaderLoop: newLeaderOnlyLoop(func(context.Context) {}),
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	manager.Stop()

	select {
	case <-elector.ctxDone:
	case <-time.After(time.Second):
		t.Fatal("expected Stop to cancel leader-election context")
	}
}

// Regression for the elector-never-restarts bug: when Run returns (lease lost),
// the manager must re-contend by invoking Run again, not stay out of the
// election. Without the re-contend loop Run is called exactly once.
func TestJanitorLeaderManagerReContendsAfterLosingLeadership(t *testing.T) {
	elector := &countingLeaderElector{runCh: make(chan struct{}, 100)}
	manager := &JanitorLeaderManager{
		elector:          elector,
		leaderLoop:       newLeaderOnlyLoop(func(context.Context) {}),
		recontendBackoff: time.Millisecond,
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer manager.Stop()

	for i := 0; i < 3; i++ {
		select {
		case <-elector.runCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("expected re-contention; Run invoked only %d times", elector.count())
		}
	}
}

// After Stop, the re-contend loop must terminate (no further Run invocations).
func TestJanitorLeaderManagerStopHaltsReContendLoop(t *testing.T) {
	elector := &countingLeaderElector{runCh: make(chan struct{}, 1000)}
	manager := &JanitorLeaderManager{
		elector:          elector,
		leaderLoop:       newLeaderOnlyLoop(func(context.Context) {}),
		recontendBackoff: time.Millisecond,
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-elector.runCh // ensure the loop is spinning
	manager.Stop()

	time.Sleep(20 * time.Millisecond) // let any in-flight iteration drain
	before := elector.count()
	time.Sleep(50 * time.Millisecond)
	if after := elector.count(); after != before {
		t.Fatalf("expected loop to stop after Stop; Run calls grew %d -> %d", before, after)
	}
}
