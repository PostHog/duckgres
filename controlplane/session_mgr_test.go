//go:build !kubernetes

package controlplane

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/posthog/duckgres/server/flightclient"
)

// mockCloser tracks whether Close was called.
type mockCloser struct {
	closed atomic.Bool
}

func (m *mockCloser) Close() error {
	m.closed.Store(true)
	return nil
}

type grantRaceContext struct {
	context.Context

	enteredDone chan struct{}
	allowDone   chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
}

func newGrantRaceContext() *grantRaceContext {
	return &grantRaceContext{
		Context:     context.Background(),
		enteredDone: make(chan struct{}),
		allowDone:   make(chan struct{}),
		done:        make(chan struct{}),
	}
}

func (c *grantRaceContext) Done() <-chan struct{} {
	c.closeOnce.Do(func() {
		close(c.enteredDone)
		<-c.allowDone
		close(c.done)
	})
	return c.done
}

func (c *grantRaceContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func TestOnWorkerCrash_MarksExecutorsDead(t *testing.T) {
	pool := &FlightWorkerPool{
		workers: make(map[int]*ManagedWorker),
	}
	sm := NewSessionManager(pool, nil)

	executor := &flightclient.FlightExecutor{}
	pid := int32(1001)

	sm.mu.Lock()
	sm.sessions[pid] = &ManagedSession{
		PID:      pid,
		WorkerID: 5,
		Executor: executor,
	}
	sm.byWorker[5] = []int32{pid}
	sm.mu.Unlock()

	var notifiedPIDs []int32
	sm.OnWorkerCrash(5, func(pid int32) {
		notifiedPIDs = append(notifiedPIDs, pid)
	})

	// Executor should be marked dead
	if !executor.IsDead() {
		t.Fatal("expected executor to be marked dead after OnWorkerCrash")
	}

	// errorFn should have been called
	if len(notifiedPIDs) != 1 || notifiedPIDs[0] != pid {
		t.Fatalf("expected errorFn called with pid %d, got %v", pid, notifiedPIDs)
	}

	// Session should be removed
	if sm.SessionCount() != 0 {
		t.Fatalf("expected 0 sessions after crash, got %d", sm.SessionCount())
	}
}

func TestOnWorkerCrash_ClosesConnections(t *testing.T) {
	pool := &FlightWorkerPool{
		workers: make(map[int]*ManagedWorker),
	}
	sm := NewSessionManager(pool, nil)

	conn := &mockCloser{}
	executor := &flightclient.FlightExecutor{}
	pid := int32(1002)

	sm.mu.Lock()
	sm.sessions[pid] = &ManagedSession{
		PID:        pid,
		WorkerID:   7,
		Executor:   executor,
		connCloser: conn,
	}
	sm.byWorker[7] = []int32{pid}
	sm.mu.Unlock()

	sm.OnWorkerCrash(7, func(pid int32) {})

	if !conn.closed.Load() {
		t.Fatal("expected TCP connection to be closed on worker crash")
	}
}

func TestOnWorkerCrash_MultipleSessions(t *testing.T) {
	pool := &FlightWorkerPool{
		workers: make(map[int]*ManagedWorker),
	}
	sm := NewSessionManager(pool, nil)

	exec1 := &flightclient.FlightExecutor{}
	exec2 := &flightclient.FlightExecutor{}
	conn1 := &mockCloser{}
	conn2 := &mockCloser{}

	sm.mu.Lock()
	sm.sessions[1001] = &ManagedSession{PID: 1001, WorkerID: 3, Executor: exec1, connCloser: conn1}
	sm.sessions[1002] = &ManagedSession{PID: 1002, WorkerID: 3, Executor: exec2, connCloser: conn2}
	sm.byWorker[3] = []int32{1001, 1002}
	sm.mu.Unlock()

	sm.OnWorkerCrash(3, func(pid int32) {})

	if !exec1.IsDead() || !exec2.IsDead() {
		t.Fatal("expected both executors to be marked dead")
	}
	if !conn1.closed.Load() || !conn2.closed.Load() {
		t.Fatal("expected both connections to be closed")
	}
	if sm.SessionCount() != 0 {
		t.Fatalf("expected 0 sessions, got %d", sm.SessionCount())
	}
}

func TestSetConnCloser(t *testing.T) {
	pool := &FlightWorkerPool{
		workers: make(map[int]*ManagedWorker),
	}
	sm := NewSessionManager(pool, nil)

	pid := int32(1003)
	sm.mu.Lock()
	sm.sessions[pid] = &ManagedSession{PID: pid, WorkerID: 1}
	sm.byWorker[1] = []int32{pid}
	sm.mu.Unlock()

	conn := &mockCloser{}
	sm.SetConnCloser(pid, conn)

	// Verify it was set by triggering a crash
	sm.OnWorkerCrash(1, func(pid int32) {})

	if !conn.closed.Load() {
		t.Fatal("expected connection registered via SetConnCloser to be closed on crash")
	}
}

func TestSetConnCloser_UnknownPID(t *testing.T) {
	pool := &FlightWorkerPool{
		workers: make(map[int]*ManagedWorker),
	}
	sm := NewSessionManager(pool, nil)

	// Should not panic when PID doesn't exist
	conn := &mockCloser{}
	sm.SetConnCloser(9999, conn)

	if conn.closed.Load() {
		t.Fatal("connection should not be closed for unknown PID")
	}
}

func TestRecoverWorkerPanic_NilPointer(t *testing.T) {
	var err error
	func() {
		defer recoverWorkerPanic(&err)
		var i *int
		_ = *i //nolint:govet
	}()

	if err == nil {
		t.Fatal("expected error from recovered nil pointer panic")
		return
	}
	if !strings.Contains(err.Error(), "worker likely crashed") {
		t.Fatalf("expected crash message, got: %v", err)
	}
}

func TestRecoverWorkerPanic_NonNilPointerRePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected re-panic for non-nil-pointer panic")
			return
		}
		if s, ok := r.(string); !ok || s != "unrelated panic" {
			t.Fatalf("expected original panic value, got: %v", r)
		}
	}()

	var err error
	func() {
		defer recoverWorkerPanic(&err)
		panic("unrelated panic")
	}()

	t.Fatal("should not reach here")
}

func TestRecoverWorkerPanic_RuntimeErrorRePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected re-panic for non-nil-pointer runtime error")
			return
		}
		if re, ok := r.(runtime.Error); !ok {
			t.Fatalf("expected runtime.Error, got %T: %v", r, r)
		} else if strings.Contains(re.Error(), "nil pointer") {
			t.Fatal("this test should use a non-nil-pointer runtime error")
		}
	}()

	var err error
	func() {
		defer recoverWorkerPanic(&err)
		s := []int{}
		_ = s[1] //nolint:govet
	}()

	t.Fatal("should not reach here")
}

func TestDestroySessionAfterOnWorkerCrash(t *testing.T) {
	// Verify that DestroySession is a safe no-op when OnWorkerCrash already
	// cleaned up the session. This is the exact production sequence:
	// OnWorkerCrash runs from the health check, then the deferred
	// DestroySession runs when handleConnection returns.
	pool := &FlightWorkerPool{
		workers: make(map[int]*ManagedWorker),
	}
	sm := NewSessionManager(pool, nil)

	conn := &mockCloser{}
	executor := &flightclient.FlightExecutor{}
	pid := int32(1010)

	sm.mu.Lock()
	sm.sessions[pid] = &ManagedSession{
		PID:        pid,
		WorkerID:   9,
		Executor:   executor,
		connCloser: conn,
	}
	sm.byWorker[9] = []int32{pid}
	sm.mu.Unlock()

	// Simulate crash cleanup
	sm.OnWorkerCrash(9, func(pid int32) {})

	if sm.SessionCount() != 0 {
		t.Fatal("expected 0 sessions after OnWorkerCrash")
	}

	// Now DestroySession runs (from deferred call in handleConnection).
	// Should be a no-op — no panic, no double-close of worker resources.
	sm.DestroySession(pid)

	if sm.SessionCount() != 0 {
		t.Fatal("expected 0 sessions after DestroySession")
	}
}

func TestResolveSessionLimits_UsesRebalancerDefaultsWhenUnset(t *testing.T) {
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, nil, false)
	sm := NewSessionManager(&FlightWorkerPool{workers: make(map[int]*ManagedWorker)}, r)

	mem, threads := sm.resolveSessionLimits("", 0)
	if mem != "24576MB" {
		t.Fatalf("expected memory limit 24576MB, got %q", mem)
	}
	if threads != 8 {
		t.Fatalf("expected threads 8, got %d", threads)
	}
}

func TestResolveSessionLimits_PreservesExplicitValues(t *testing.T) {
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, nil, false)
	sm := NewSessionManager(&FlightWorkerPool{workers: make(map[int]*ManagedWorker)}, r)

	mem, threads := sm.resolveSessionLimits("1024MB", 2)
	if mem != "1024MB" {
		t.Fatalf("expected memory limit 1024MB, got %q", mem)
	}
	if threads != 2 {
		t.Fatalf("expected threads 2, got %d", threads)
	}
}

func TestSessionManager_ConnectionLimits_Basic(t *testing.T) {
	ctx := context.Background()
	sm := NewSessionManager(nil, nil)
	sm.SetOrgID("test-org")
	sm.SetMaxConnections(2)

	// Acquire slot 1
	if err := sm.acquireSlot(ctx); err != nil {
		t.Fatalf("unexpected error acquiring slot 1: %v", err)
	}
	if sm.activeSlots != 1 {
		t.Fatalf("expected activeSlots=1, got %d", sm.activeSlots)
	}

	// Acquire slot 2
	if err := sm.acquireSlot(ctx); err != nil {
		t.Fatalf("unexpected error acquiring slot 2: %v", err)
	}
	if sm.activeSlots != 2 {
		t.Fatalf("expected activeSlots=2, got %d", sm.activeSlots)
	}

	// Release slot 1
	sm.releaseSlot()
	if sm.activeSlots != 1 {
		t.Fatalf("expected activeSlots=1, got %d", sm.activeSlots)
	}
}

func TestSessionManager_ConnectionLimits_Queuing(t *testing.T) {
	ctx := context.Background()
	sm := NewSessionManager(nil, nil)
	sm.SetOrgID("test-org")
	sm.SetMaxConnections(1)

	// Acquire slot 1
	if err := sm.acquireSlot(ctx); err != nil {
		t.Fatalf("unexpected error acquiring slot 1: %v", err)
	}

	// Acquire slot 2 (should block since max is 1)
	blockedCh := make(chan struct{})
	doneCh := make(chan error, 1)
	go func() {
		close(blockedCh)
		err := sm.acquireSlot(ctx)
		doneCh <- err
	}()

	<-blockedCh
	// Sleep briefly to ensure the goroutine has called acquireSlot and is waiting
	time.Sleep(50 * time.Millisecond)

	sm.mu.Lock()
	waitersLen := len(sm.waiters)
	sm.mu.Unlock()
	if waitersLen != 1 {
		t.Fatalf("expected 1 waiter, got %d", waitersLen)
	}

	// Release slot 1, should unblock waiter 2
	sm.releaseSlot()

	select {
	case err := <-doneCh:
		if err != nil {
			t.Fatalf("waiter received error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for queued request to be unblocked")
	}

	if sm.activeSlots != 1 {
		t.Fatalf("expected activeSlots=1, got %d", sm.activeSlots)
	}
}

func TestSessionManager_ConnectionLimits_Timeout(t *testing.T) {
	sm := NewSessionManager(nil, nil)
	sm.SetOrgID("test-org")
	sm.SetMaxConnections(1)

	// Acquire slot 1
	if err := sm.acquireSlot(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Acquire slot 2 with a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	blockedCh := make(chan struct{})
	doneCh := make(chan error, 1)
	go func() {
		close(blockedCh)
		err := sm.acquireSlot(ctx)
		doneCh <- err
	}()

	<-blockedCh
	time.Sleep(50 * time.Millisecond)

	// Cancel context of waiter
	cancel()

	select {
	case err := <-doneCh:
		if err == nil || !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("expected context canceled error, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for canceled waiter to exit")
	}

	sm.mu.Lock()
	waitersLen := len(sm.waiters)
	sm.mu.Unlock()
	if waitersLen != 0 {
		t.Fatalf("expected 0 waiters after cancel, got %d", waitersLen)
	}

	// Slot count should still be 1 (held by first session)
	if sm.activeSlots != 1 {
		t.Fatalf("expected activeSlots=1, got %d", sm.activeSlots)
	}
}

func TestSessionManager_ConnectionLimits_CancelAfterGrantDoesNotLeakSlot(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)

	for attempt := 0; attempt < 100; attempt++ {
		sm := NewSessionManager(nil, nil)
		sm.SetOrgID("test-org")
		sm.SetMaxConnections(1)

		if err := sm.acquireSlot(context.Background()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := newGrantRaceContext()
		doneCh := make(chan error, 1)
		go func() {
			doneCh <- sm.acquireSlot(ctx)
		}()

		select {
		case <-ctx.enteredDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for queued request")
		}

		// Simulate the release/limit-change grant path while the waiter is
		// blocked evaluating ctx.Done(). When allowDone closes, both select cases
		// are ready; if cancellation wins, the pre-accounted slot must be released.
		sm.mu.Lock()
		next := sm.waiters[0]
		sm.waiters = sm.waiters[1:]
		sm.activeSlots++
		close(next)
		sm.mu.Unlock()
		close(ctx.allowDone)

		select {
		case err := <-doneCh:
			if err != nil {
				sm.mu.Lock()
				activeSlots := sm.activeSlots
				waitersLen := len(sm.waiters)
				sm.mu.Unlock()
				if activeSlots != 1 {
					t.Fatalf("canceled granted waiter leaked slot: activeSlots=%d waiters=%d", activeSlots, waitersLen)
				}
				return
			}
			sm.releaseSlot()
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for queued request")
		}
	}

	t.Fatal("test did not exercise cancellation after grant")
}

func TestSessionManager_ConnectionLimits_DynamicLimit(t *testing.T) {
	ctx := context.Background()
	sm := NewSessionManager(nil, nil)
	sm.SetOrgID("test-org")
	sm.SetMaxConnections(1)

	// Acquire slot 1
	if err := sm.acquireSlot(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Waiters 1 & 2
	done1 := make(chan error, 1)
	go func() {
		done1 <- sm.acquireSlot(ctx)
	}()
	done2 := make(chan error, 1)
	go func() {
		done2 <- sm.acquireSlot(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	sm.mu.Lock()
	waitersLen := len(sm.waiters)
	sm.mu.Unlock()
	if waitersLen != 2 {
		t.Fatalf("expected 2 waiters, got %d", waitersLen)
	}

	// Dynamically increase MaxConnections to 3, should wake up both waiters
	sm.SetMaxConnections(3)

	for i := 0; i < 2; i++ {
		select {
		case err := <-done1:
			if err != nil {
				t.Fatalf("waiter 1 failed: %v", err)
			}
		case err := <-done2:
			if err != nil {
				t.Fatalf("waiter 2 failed: %v", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for waiters to be unblocked by dynamic limit increase")
		}
	}

	if sm.activeSlots != 3 {
		t.Fatalf("expected activeSlots=3, got %d", sm.activeSlots)
	}
}
