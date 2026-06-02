//go:build !kubernetes

package controlplane

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/posthog/duckgres/server/flightclient"
	dto "github.com/prometheus/client_model/go"
)

// mockCloser tracks whether Close was called.
type mockCloser struct {
	closed atomic.Bool
}

func (m *mockCloser) Close() error {
	m.closed.Store(true)
	return nil
}

type acquireErrorPool struct {
	err error
}

func (p *acquireErrorPool) AcquireWorker(ctx context.Context, _ *WorkerProfile) (*ManagedWorker, error) {
	return nil, p.err
}

func (p *acquireErrorPool) ReleaseWorker(id int) {}

func (p *acquireErrorPool) RetireWorker(id int) {}

func (p *acquireErrorPool) RetireWorkerIfNoSessions(id int) bool {
	return false
}

func (p *acquireErrorPool) Worker(id int) (*ManagedWorker, bool) {
	return nil, false
}

func (p *acquireErrorPool) SpawnMinWorkers(count int) error {
	return nil
}

func (p *acquireErrorPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
}

func (p *acquireErrorPool) SetMaxWorkers(n int) {}

func (p *acquireErrorPool) ShutdownAll() {}

func TestCreateSessionObservesWarmCapacityExhaustion(t *testing.T) {
	controlPlaneWorkerAcquireFailuresCounter.Reset()
	sm := NewSessionManager(&acquireErrorPool{
		err: NewWarmCapacityExhaustedError(30 * time.Second),
	}, nil)

	_, _, err := sm.CreateSession(context.Background(), "root", 1001, "", 0, nil)
	var capacityErr *WarmCapacityExhaustedError
	if !errors.As(err, &capacityErr) {
		t.Fatalf("expected warm capacity error, got %v", err)
	}

	counter, counterErr := controlPlaneWorkerAcquireFailuresCounter.GetMetricWithLabelValues("warm_capacity_exhausted")
	if counterErr != nil {
		t.Fatalf("failed to read warm capacity counter: %v", counterErr)
	}
	metric := &dto.Metric{}
	if err := counter.Write(metric); err != nil {
		t.Fatalf("failed to write warm capacity counter: %v", err)
	}
	if got := metric.GetCounter().GetValue(); got != 1 {
		t.Fatalf("expected one warm capacity acquisition failure, got %v", got)
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

func TestSessionManager_ConnectionLimits_WaitsUntilSlotReleased(t *testing.T) {
	ctx := context.Background()
	sm := NewSessionManager(nil, nil)
	sm.SetMaxConnections(1)

	if err := sm.acquireSlot(ctx); err != nil {
		t.Fatalf("unexpected error acquiring slot 1: %v", err)
	}

	acquired := make(chan error, 1)
	go func() {
		acquired <- sm.acquireSlot(ctx)
	}()

	select {
	case err := <-acquired:
		t.Fatalf("expected second acquire to wait, got %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	if sm.activeSlots != 1 {
		t.Fatalf("expected activeSlots=1, got %d", sm.activeSlots)
	}

	sm.releaseSlot()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("expected waiting acquire to succeed after release, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued acquire")
	}
	if sm.activeSlots != 1 {
		t.Fatalf("expected activeSlots=1 after queued acquire, got %d", sm.activeSlots)
	}
}

func TestSessionManager_ConnectionLimits_DynamicLimitIncreaseAdmitsNewSession(t *testing.T) {
	ctx := context.Background()
	sm := NewSessionManager(nil, nil)
	sm.SetMaxConnections(1)

	// Acquire slot 1
	if err := sm.acquireSlot(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sm.SetMaxConnections(3)
	if err := sm.acquireSlot(ctx); err != nil {
		t.Fatalf("expected new slot after dynamic limit increase, got %v", err)
	}
	if sm.activeSlots != 2 {
		t.Fatalf("expected activeSlots=2, got %d", sm.activeSlots)
	}
}

func waitForConnectionWaiters(t *testing.T, sm *SessionManager, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		sm.mu.RLock()
		got := len(sm.waiters)
		sm.mu.RUnlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	sm.mu.RLock()
	got := len(sm.waiters)
	sm.mu.RUnlock()
	t.Fatalf("timed out waiting for %d connection waiters, got %d", want, got)
}

func TestSessionManager_ConnectionLimits_FIFO(t *testing.T) {
	ctx := context.Background()
	sm := NewSessionManager(nil, nil)
	sm.SetMaxConnections(1)

	if err := sm.acquireSlot(ctx); err != nil {
		t.Fatalf("unexpected error acquiring initial slot: %v", err)
	}

	const waiters = 3
	ready := make(chan struct{}, waiters)
	order := make(chan int, waiters)
	var wg sync.WaitGroup
	for i := 0; i < waiters; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			if err := sm.acquireSlot(ctx); err != nil {
				t.Errorf("waiter %d acquire: %v", i, err)
				return
			}
			order <- i
		}()
		<-ready
		waitForConnectionWaiters(t, sm, i+1)
	}
	for i := 0; i < waiters; i++ {
		sm.releaseSlot()
		select {
		case got := <-order:
			if got != i {
				t.Fatalf("grant order[%d] = %d, want %d", i, got, i)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for waiter %d", i)
		}
	}
	wg.Wait()
}

func TestSessionManager_ConnectionLimits_DeadlineWhileQueuedReturnsTooManyConnections(t *testing.T) {
	sm := NewSessionManager(nil, nil)
	sm.SetMaxConnections(1)

	if err := sm.acquireSlot(context.Background()); err != nil {
		t.Fatalf("unexpected initial acquire error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := sm.acquireSlot(ctx)
	if !errors.Is(err, ErrTooManyConnections) {
		t.Fatalf("expected ErrTooManyConnections, got %v", err)
	}
	if sm.activeSlots != 1 {
		t.Fatalf("expected activeSlots=1 after deadline, got %d", sm.activeSlots)
	}

	sm.releaseSlot()
	if sm.activeSlots != 0 {
		t.Fatalf("expected activeSlots=0 after release, got %d", sm.activeSlots)
	}
}

func TestSessionManager_ConnectionLimits_CancelWhileQueuedDoesNotBlockNextWaiter(t *testing.T) {
	sm := NewSessionManager(nil, nil)
	sm.SetMaxConnections(1)

	if err := sm.acquireSlot(context.Background()); err != nil {
		t.Fatalf("unexpected initial acquire error: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelErr := make(chan error, 1)
	go func() {
		cancelErr <- sm.acquireSlot(cancelCtx)
	}()
	waitForConnectionWaiters(t, sm, 1)

	nextAcquired := make(chan error, 1)
	go func() {
		nextAcquired <- sm.acquireSlot(context.Background())
	}()
	waitForConnectionWaiters(t, sm, 2)

	cancel()
	select {
	case err := <-cancelErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for canceled waiter")
	}

	sm.releaseSlot()
	select {
	case err := <-nextAcquired:
		if err != nil {
			t.Fatalf("expected next waiter to acquire after canceled waiter, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for next waiter")
	}
}

func TestSessionManager_ConnectionLimits_DestroySessionGrantsQueuedWaiter(t *testing.T) {
	pool := &FlightWorkerPool{workers: make(map[int]*ManagedWorker)}
	sm := NewSessionManager(pool, nil)
	sm.SetMaxConnections(1)

	if err := sm.acquireSlot(context.Background()); err != nil {
		t.Fatalf("unexpected initial acquire error: %v", err)
	}
	sm.mu.Lock()
	sm.sessions[101] = &ManagedSession{PID: 101, WorkerID: 7}
	sm.byWorker[7] = []int32{101}
	sm.mu.Unlock()

	acquired := make(chan error, 1)
	go func() {
		acquired <- sm.acquireSlot(context.Background())
	}()
	waitForConnectionWaiters(t, sm, 1)

	sm.DestroySession(101)
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("expected queued waiter to acquire after DestroySession, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued waiter after DestroySession")
	}
}

func TestSessionManager_ConnectionLimits_WorkerCrashGrantsQueuedWaiter(t *testing.T) {
	pool := &FlightWorkerPool{workers: make(map[int]*ManagedWorker)}
	sm := NewSessionManager(pool, nil)
	sm.SetMaxConnections(1)

	if err := sm.acquireSlot(context.Background()); err != nil {
		t.Fatalf("unexpected initial acquire error: %v", err)
	}
	sm.mu.Lock()
	sm.sessions[101] = &ManagedSession{PID: 101, WorkerID: 7, Executor: &flightclient.FlightExecutor{}}
	sm.byWorker[7] = []int32{101}
	sm.mu.Unlock()

	acquired := make(chan error, 1)
	go func() {
		acquired <- sm.acquireSlot(context.Background())
	}()
	waitForConnectionWaiters(t, sm, 1)

	sm.OnWorkerCrash(7, func(pid int32) {})
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("expected queued waiter to acquire after OnWorkerCrash, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued waiter after OnWorkerCrash")
	}
}
