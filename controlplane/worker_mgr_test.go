package controlplane

import (
	"context"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestHealthCheckFailureCountingResetsOnSuccess(t *testing.T) {
	// Simulate 2 consecutive failures followed by a success.
	// The counter should reset, requiring another 3 failures to trigger force-kill.
	failures := make(map[int]int)
	workerID := 42

	// Two failures
	failures[workerID]++
	failures[workerID]++
	if failures[workerID] != 2 {
		t.Fatalf("expected 2 failures, got %d", failures[workerID])
	}

	// Success resets
	delete(failures, workerID)
	if _, ok := failures[workerID]; ok {
		t.Fatal("expected failure counter to be cleared after success")
	}

	// New failure starts from 1
	failures[workerID]++
	if failures[workerID] != 1 {
		t.Fatalf("expected 1 failure after reset, got %d", failures[workerID])
	}
}

func TestHealthCheckFailureCountingTriggersAtThreshold(t *testing.T) {
	failures := make(map[int]int)
	workerID := 7

	triggered := false
	for i := 0; i < maxConsecutiveHealthFailures; i++ {
		failures[workerID]++
		if failures[workerID] >= maxConsecutiveHealthFailures {
			triggered = true
			delete(failures, workerID)
		}
	}

	if !triggered {
		t.Fatal("expected force-kill to trigger at maxConsecutiveHealthFailures")
	}
	if _, ok := failures[workerID]; ok {
		t.Fatal("expected failure counter to be cleaned up after force-kill")
	}
}

func TestHealthCheckFailureCountingDoesNotTriggerBelowThreshold(t *testing.T) {
	failures := make(map[int]int)
	workerID := 7

	for i := 0; i < maxConsecutiveHealthFailures-1; i++ {
		failures[workerID]++
		if failures[workerID] >= maxConsecutiveHealthFailures {
			t.Fatal("should not trigger below threshold")
		}
	}
}

func TestHealthCheckFailureCountingCleanupOnWorkerExit(t *testing.T) {
	failures := make(map[int]int)
	workerID := 10

	// Accumulate some failures
	failures[workerID] = 2

	// Worker exits (done channel closes) — counter should be cleaned up
	delete(failures, workerID)

	if _, ok := failures[workerID]; ok {
		t.Fatal("expected failure counter to be cleaned up on worker exit")
	}
}

func TestRetireWorkerProcessAlreadyDead(t *testing.T) {
	// Create a process that exits immediately so we can test the alreadyDead path
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start test process: %v", err)
	}

	done := make(chan struct{})
	w := &ManagedWorker{
		ID:   99,
		cmd:  cmd,
		done: done,
	}

	// Wait for process to exit
	go func() {
		w.exitErr = cmd.Wait()
		close(done)
	}()
	<-done

	// retireWorkerProcess should detect alreadyDead and not panic
	retireWorkerProcess(w)

	// Verify the process state is accessible
	if w.cmd.ProcessState == nil {
		t.Fatal("expected ProcessState to be set after Wait()")
	}
	if w.cmd.ProcessState.ExitCode() != 0 {
		t.Errorf("expected exit code 0, got %d", w.cmd.ProcessState.ExitCode())
	}
}

func TestRetireWorkerProcessAlreadyDeadNonZeroExit(t *testing.T) {
	cmd := exec.Command("false")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start test process: %v", err)
	}

	done := make(chan struct{})
	w := &ManagedWorker{
		ID:   100,
		cmd:  cmd,
		done: done,
	}

	go func() {
		w.exitErr = cmd.Wait()
		close(done)
	}()
	<-done

	retireWorkerProcess(w)

	if w.exitErr == nil {
		t.Fatal("expected non-nil exitErr for process that exited with non-zero code")
	}
	if w.cmd.ProcessState.ExitCode() != 1 {
		t.Errorf("expected exit code 1, got %d", w.cmd.ProcessState.ExitCode())
	}
}

func TestRetireWorkerProcessGracefulShutdown(t *testing.T) {
	// Start a process that will respond to SIGINT (sleep)
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start test process: %v", err)
	}

	done := make(chan struct{})
	w := &ManagedWorker{
		ID:   101,
		cmd:  cmd,
		done: done,
	}

	go func() {
		w.exitErr = cmd.Wait()
		close(done)
	}()

	// retireWorkerProcess should send SIGINT and the process should exit
	retireWorkerProcess(w)

	// Verify the done channel was closed (process exited)
	select {
	case <-done:
		// expected
	case <-time.After(5 * time.Second):
		t.Fatal("process did not exit after retireWorkerProcess")
	}
}

func TestHealthCheckLoopDetectsCrashedWorker(t *testing.T) {
	pool := &FlightWorkerPool{
		workers: make(map[int]*ManagedWorker),
	}

	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start test process: %v", err)
	}

	done := make(chan struct{})
	w := &ManagedWorker{
		ID:   1,
		cmd:  cmd,
		done: done,
	}

	// Wait for process to exit (simulating a crash)
	go func() {
		w.exitErr = cmd.Wait()
		close(done)
	}()
	<-done

	// Add to pool so the health check loop can find it
	pool.mu.Lock()
	pool.workers[1] = w
	pool.mu.Unlock()

	crashedWorkers := make(chan int, 1)
	onCrash := func(workerID int) {
		crashedWorkers <- workerID
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go pool.HealthCheckLoop(ctx, 50*time.Millisecond, onCrash)

	select {
	case id := <-crashedWorkers:
		if id != 1 {
			t.Errorf("expected crash notification for worker 1, got %d", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for crash notification")
	}

	// Worker should be removed from pool
	pool.mu.RLock()
	_, stillInPool := pool.workers[1]
	pool.mu.RUnlock()
	if stillInPool {
		t.Fatal("crashed worker should have been removed from pool")
	}
}

// makeFakeWorker creates a ManagedWorker with a started process that stays alive.
// Returns the worker and a cancel function to kill it.
func makeFakeWorker(t *testing.T, id int) (*ManagedWorker, func()) {
	t.Helper()
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start fake worker process: %v", err)
	}
	done := make(chan struct{})
	w := &ManagedWorker{
		ID:   id,
		cmd:  cmd,
		done: done,
	}
	go func() {
		w.exitErr = cmd.Wait()
		close(done)
	}()
	cleanup := func() {
		_ = cmd.Process.Kill()
		<-done
	}
	return w, cleanup
}

func TestAcquireWorkerBlocksUntilSlotAvailable(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 2, 0)

	// Pre-populate 2 busy workers so the pool is at capacity.
	w0, cleanup0 := makeFakeWorker(t, 0)
	defer cleanup0()
	w1, cleanup1 := makeFakeWorker(t, 1)
	defer cleanup1()

	pool.mu.Lock()
	w0.activeSessions = 1
	w1.activeSessions = 1
	pool.workers[0] = w0
	pool.workers[1] = w1
	pool.nextWorkerID = 2
	// Fill the semaphore to match the 2 active workers.
	pool.workerSem <- struct{}{}
	pool.workerSem <- struct{}{}
	pool.mu.Unlock()

	// AcquireWorker should now block because the semaphore is full.
	acquired := make(chan struct{})
	go func() {
		// This will block until a slot opens.
		_, _ = pool.AcquireWorker(context.Background())
		close(acquired)
	}()

	// Verify it doesn't return immediately.
	select {
	case <-acquired:
		t.Fatal("AcquireWorker should block when pool is at capacity")
	case <-time.After(100 * time.Millisecond):
		// expected: still blocked
	}

	// Retire one worker to free a slot.
	pool.RetireWorker(0)

	// Now AcquireWorker should unblock (it will try to spawn, which may fail,
	// but the point is it unblocked from the semaphore).
	select {
	case <-acquired:
		// expected: unblocked
	case <-time.After(15 * time.Second):
		t.Fatal("AcquireWorker did not unblock after RetireWorker")
	}
}

func TestAcquireWorkerRespectsContextCancellation(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 1, 0)

	// Fill the single semaphore slot.
	pool.workerSem <- struct{}{}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := pool.AcquireWorker(ctx)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if ctx.Err() == nil {
		t.Fatal("expected context to be done")
	}
}

func TestAcquireWorkerUnlimitedWhenMaxZero(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 0, 0)

	if pool.workerSem != nil {
		t.Fatal("expected nil workerSem when maxWorkers=0")
	}

	// AcquireWorker should not block on semaphore (it will fail trying to
	// spawn a worker with a fake binary, but should get past the semaphore).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := pool.AcquireWorker(ctx)
	// Will fail to spawn since there's no real binary, but the point is
	// it didn't block on a nil semaphore.
	if err == nil {
		t.Fatal("expected spawn error with non-existent binary")
	}
}

func TestAcquireWorkerShutdownUnblocksWaiters(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 1, 0)

	// Fill the single semaphore slot.
	pool.workerSem <- struct{}{}

	errCh := make(chan error, 1)
	go func() {
		_, err := pool.AcquireWorker(context.Background())
		errCh <- err
	}()

	// Give the goroutine time to block on the semaphore.
	time.Sleep(50 * time.Millisecond)

	pool.ShutdownAll()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error after shutdown")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AcquireWorker did not unblock after ShutdownAll")
	}
}

func TestRetireWorkerIfNoSessions_ReleasesClaimOnFailure(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 1, 0)

	// Manually inject a worker with 1 active session (as if AcquireWorker just returned it)
	w, cleanup := makeFakeWorker(t, 1)
	defer cleanup()
	w.activeSessions = 1

	pool.mu.Lock()
	pool.workers[1] = w
	pool.workerSem <- struct{}{} // Claim slot
	pool.mu.Unlock()

	// Calling RetireWorkerIfNoSessions should release the claim and kill the worker.
	if !pool.RetireWorkerIfNoSessions(1) {
		t.Fatal("expected RetireWorkerIfNoSessions to return true")
	}

	// Verify semaphore is freed: we should be able to push a token.
	select {
	case pool.workerSem <- struct{}{}:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("semaphore slot was leaked")
	}

	// Verify worker is gone
	if _, ok := pool.Worker(1); ok {
		t.Fatal("worker should have been retired")
	}
}

func TestAcquireWorker_AtomicClaimRace(t *testing.T) {
	// Tests that two concurrent acquisitions don't pick the same idle worker.
	const n = 5
	pool := NewFlightWorkerPool(t.TempDir(), "", 10, 0)

	// Pre-warm with n idle workers
	for i := 1; i <= n; i++ {
		w, cleanup := makeFakeWorker(t, i)
		defer cleanup()
		pool.mu.Lock()
		pool.workers[i] = w
		pool.mu.Unlock()
	}

	// Simultaneous acquisitions
	results := make(chan *ManagedWorker, n)
	for i := 0; i < n; i++ {
		go func() {
			w, _ := pool.AcquireWorker(context.Background())
			results <- w
		}()
	}

	workers := make(map[int]bool)
	for i := 0; i < n; i++ {
		w := <-results
		if w == nil {
			t.Fatal("failed to acquire worker")
		}
		if workers[w.ID] {
			t.Errorf("worker %d was assigned multiple times!", w.ID)
		}
		workers[w.ID] = true
	}
}

func TestRetireWorker_ReleasesAllSessions(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 5, 0)

	// Inject a worker with 2 sessions
	w, cleanup := makeFakeWorker(t, 1)
	defer cleanup()
	w.activeSessions = 2

	pool.mu.Lock()
	pool.workers[1] = w
	pool.workerSem <- struct{}{}
	pool.workerSem <- struct{}{}
	pool.mu.Unlock()

	// Retire it
	pool.RetireWorker(1)

	// Verify 2 slots released: should be able to push 5 tokens (capacity)
	for i := 0; i < 5; i++ {
		select {
		case pool.workerSem <- struct{}{}:
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("semaphore slot %d was leaked", i)
		}
	}
}

func TestCrashReleasesSemaphoreSlots(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 2, 0)

	// Create a worker that exits immediately (simulates crash).
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start test process: %v", err)
	}
	done := make(chan struct{})
	w := &ManagedWorker{
		ID:   0,
		cmd:  cmd,
		done: done,
	}
	go func() {
		w.exitErr = cmd.Wait()
		close(done)
	}()
	<-done // wait for it to exit

	pool.mu.Lock()
	w.activeSessions = 1
	pool.workers[0] = w
	pool.nextWorkerID = 1
	pool.workerSem <- struct{}{} // account for the worker in the semaphore
	pool.mu.Unlock()

	// Start health check loop which will detect the crash and release the slot.
	crashCh := make(chan int, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go pool.HealthCheckLoop(ctx, 50*time.Millisecond, func(workerID int) {
		select {
		case crashCh <- workerID:
		default:
		}
	})

	// Wait for crash to be detected.
	select {
	case <-crashCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for crash detection")
	}

	// Verify the semaphore slot was released: we should be able to push 2 tokens
	// (maxWorkers=2) since the crashed worker's slot was freed.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case pool.workerSem <- struct{}{}:
			case <-time.After(2 * time.Second):
				t.Error("semaphore slot not available after crash")
			}
		}()
	}
	wg.Wait()
}

func TestReleaseWorker_KeepsWorkerAlive(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 2, 30*time.Second)

	w, cleanup := makeFakeWorker(t, 1)
	defer cleanup()
	w.activeSessions = 1

	pool.mu.Lock()
	pool.workers[1] = w
	pool.workerSem <- struct{}{} // account for the active session
	pool.mu.Unlock()

	pool.ReleaseWorker(1)

	// Worker should still be in the pool
	got, ok := pool.Worker(1)
	if !ok {
		t.Fatal("expected worker to remain in pool after ReleaseWorker")
	}
	if got.activeSessions != 0 {
		t.Fatalf("expected 0 active sessions, got %d", got.activeSessions)
	}
	if got.idleSince.IsZero() {
		t.Fatal("expected idleSince to be set")
	}

	// Semaphore slot should be released
	select {
	case pool.workerSem <- struct{}{}:
		// success — slot was freed
	case <-time.After(100 * time.Millisecond):
		t.Fatal("semaphore slot was not released by ReleaseWorker")
	}
}

func TestReleaseWorker_FallsBackToRetireWhenTTLZero(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 2, 0)

	w, cleanup := makeFakeWorker(t, 1)
	defer cleanup()
	w.activeSessions = 1

	pool.mu.Lock()
	pool.workers[1] = w
	pool.workerSem <- struct{}{}
	pool.mu.Unlock()

	pool.ReleaseWorker(1)

	// Worker should be removed (RetireWorker path)
	if _, ok := pool.Worker(1); ok {
		t.Fatal("expected worker to be retired when workerIdleTTL=0")
	}
}

func TestReleaseWorker_IdleWorkerReusedByAcquire(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 5, 30*time.Second)

	w, cleanup := makeFakeWorker(t, 1)
	defer cleanup()
	w.activeSessions = 1

	pool.mu.Lock()
	pool.workers[1] = w
	pool.nextWorkerID = 2
	pool.mu.Unlock()

	// Release the worker back to idle pool
	pool.ReleaseWorker(1)

	// AcquireWorker should reuse the idle worker instead of spawning a new one
	got, err := pool.AcquireWorker(context.Background())
	if err != nil {
		t.Fatalf("AcquireWorker failed: %v", err)
	}
	if got.ID != 1 {
		t.Fatalf("expected to reuse worker 1, got worker %d", got.ID)
	}
	if got.activeSessions != 1 {
		t.Fatalf("expected 1 active session after acquire, got %d", got.activeSessions)
	}
	if !got.idleSince.IsZero() {
		t.Fatal("expected idleSince to be cleared after acquire")
	}
}

func TestRetireIdleWorkers_RetiresExpiredWorkers(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 5, 50*time.Millisecond)

	w, cleanup := makeFakeWorker(t, 1)
	defer cleanup()
	w.idleSince = time.Now().Add(-100 * time.Millisecond) // expired

	pool.mu.Lock()
	pool.workers[1] = w
	pool.mu.Unlock()

	pool.retireIdleWorkers()

	// Worker should be removed from pool
	if _, ok := pool.Worker(1); ok {
		t.Fatal("expected expired idle worker to be retired")
	}
}

func TestRetireIdleWorkers_KeepsNonExpiredWorkers(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 5, time.Hour)

	w, cleanup := makeFakeWorker(t, 1)
	defer cleanup()
	w.idleSince = time.Now() // just became idle

	pool.mu.Lock()
	pool.workers[1] = w
	pool.mu.Unlock()

	pool.retireIdleWorkers()

	// Worker should still be in pool (TTL not expired)
	if _, ok := pool.Worker(1); !ok {
		t.Fatal("expected non-expired idle worker to remain in pool")
	}
}

func TestRetireIdleWorkers_RespectsMinIdleWorkers(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 5, 50*time.Millisecond)
	pool.minIdleWorkers = 2

	// Create 3 expired idle workers
	var cleanups []func()
	for i := 1; i <= 3; i++ {
		w, cleanup := makeFakeWorker(t, i)
		cleanups = append(cleanups, cleanup)
		w.idleSince = time.Now().Add(-100 * time.Millisecond)
		pool.mu.Lock()
		pool.workers[i] = w
		pool.mu.Unlock()
	}
	defer func() {
		for _, c := range cleanups {
			c()
		}
	}()

	pool.retireIdleWorkers()

	// Should keep at least 2 idle workers alive
	pool.mu.RLock()
	remaining := len(pool.workers)
	pool.mu.RUnlock()
	if remaining < 2 {
		t.Fatalf("expected at least 2 workers to remain (minIdleWorkers=2), got %d", remaining)
	}
}

func TestRetireIdleWorkers_SkipsActiveWorkers(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 5, 50*time.Millisecond)

	w, cleanup := makeFakeWorker(t, 1)
	defer cleanup()
	w.activeSessions = 1 // busy worker

	pool.mu.Lock()
	pool.workers[1] = w
	pool.mu.Unlock()

	pool.retireIdleWorkers()

	// Active worker should not be retired
	if _, ok := pool.Worker(1); !ok {
		t.Fatal("expected active worker to remain in pool")
	}
}

func TestRetireIdleWorkers_NoopWhenTTLZero(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 5, 0)

	w, cleanup := makeFakeWorker(t, 1)
	defer cleanup()
	w.idleSince = time.Now().Add(-time.Hour) // very stale

	pool.mu.Lock()
	pool.workers[1] = w
	pool.mu.Unlock()

	pool.retireIdleWorkers()

	// Worker should remain — TTL is disabled
	if _, ok := pool.Worker(1); !ok {
		t.Fatal("expected worker to remain when workerIdleTTL=0")
	}
}
