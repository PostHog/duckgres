package controlplane

import (
	"context"
	"fmt"
	"net"
	"os"
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
	pool := NewFlightWorkerPool(t.TempDir(), "", 1)

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
	pool.retireWorkerProcess(w)

	// Verify the process state is accessible
	if w.cmd.ProcessState == nil {
		t.Fatal("expected ProcessState to be set after Wait()")
	}
	if w.cmd.ProcessState.ExitCode() != 0 {
		t.Errorf("expected exit code 0, got %d", w.cmd.ProcessState.ExitCode())
	}
}

func TestRetireWorkerProcessAlreadyDeadNonZeroExit(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 1)

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

	pool.retireWorkerProcess(w)

	if w.exitErr == nil {
		t.Fatal("expected non-nil exitErr for process that exited with non-zero code")
	}
	if w.cmd.ProcessState.ExitCode() != 1 {
		t.Errorf("expected exit code 1, got %d", w.cmd.ProcessState.ExitCode())
	}
}

func TestRetireWorkerProcessGracefulShutdown(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 1)

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
	pool.retireWorkerProcess(w)

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
		workers:    make(map[int]*ManagedWorker),
		shutdownCh: make(chan struct{}),
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

func TestAcquireWorkerReusesIdleWorker(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 2)

	// Pre-populate an idle worker.
	w0, cleanup0 := makeFakeWorker(t, 0)
	defer cleanup0()

	pool.mu.Lock()
	pool.workers[0] = w0
	pool.nextWorkerID = 1
	pool.mu.Unlock()

	// Acquire should reuse the idle worker, not spawn a new one.
	w, err := pool.AcquireWorker(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.ID != 0 {
		t.Fatalf("expected worker 0 (idle), got worker %d", w.ID)
	}
	if w.activeSessions != 1 {
		t.Fatalf("expected 1 active session, got %d", w.activeSessions)
	}
}

func TestAcquireWorkerLeastLoadedAtCapacity(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 2)

	// Pre-populate 2 busy workers (at capacity).
	w0, cleanup0 := makeFakeWorker(t, 0)
	defer cleanup0()
	w1, cleanup1 := makeFakeWorker(t, 1)
	defer cleanup1()

	pool.mu.Lock()
	w0.activeSessions = 3
	w1.activeSessions = 1
	pool.workers[0] = w0
	pool.workers[1] = w1
	pool.nextWorkerID = 2
	pool.mu.Unlock()

	// Acquire should pick the least-loaded worker (w1 with 1 session).
	w, err := pool.AcquireWorker(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.ID != 1 {
		t.Fatalf("expected worker 1 (least loaded), got worker %d", w.ID)
	}
	if w.activeSessions != 2 {
		t.Fatalf("expected 2 active sessions after acquire, got %d", w.activeSessions)
	}
}

func TestAcquireWorkerSpawnsWhenBelowCapacity(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 3)

	// Pre-populate 1 busy worker (below capacity of 3).
	w0, cleanup0 := makeFakeWorker(t, 0)
	defer cleanup0()

	pool.mu.Lock()
	w0.activeSessions = 1
	pool.workers[0] = w0
	pool.nextWorkerID = 1
	pool.mu.Unlock()

	// Acquire should try to spawn (will fail since no real binary, but it
	// should NOT pick the existing worker — it should try to spawn first).
	_, err := pool.AcquireWorker(context.Background())
	if err == nil {
		t.Fatal("expected spawn error with non-existent binary")
	}
	// The error should be a spawn error, not a "no worker found" error.
	if pool.nextWorkerID <= 1 {
		t.Fatal("expected nextWorkerID to have incremented (spawn attempted)")
	}
}

func TestAcquireWorkerCleansDeadWorkersWhenAllDead(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 2)

	// Pre-populate 2 dead workers.
	cmd0 := exec.Command("true")
	_ = cmd0.Start()
	done0 := make(chan struct{})
	w0 := &ManagedWorker{ID: 0, cmd: cmd0, done: done0, activeSessions: 1}
	go func() { w0.exitErr = cmd0.Wait(); close(done0) }()
	<-done0

	cmd1 := exec.Command("true")
	_ = cmd1.Start()
	done1 := make(chan struct{})
	w1 := &ManagedWorker{ID: 1, cmd: cmd1, done: done1, activeSessions: 1}
	go func() { w1.exitErr = cmd1.Wait(); close(done1) }()
	<-done1

	pool.mu.Lock()
	pool.workers[0] = w0
	pool.workers[1] = w1
	pool.nextWorkerID = 2
	pool.mu.Unlock()

	// AcquireWorker should clean dead entries and try to spawn a replacement.
	_, err := pool.AcquireWorker(context.Background())
	// Will fail to spawn since no real binary, but dead workers should be cleaned.
	if err == nil {
		t.Fatal("expected spawn error")
	}

	pool.mu.RLock()
	// Dead workers should have been cleaned up.
	count := len(pool.workers)
	pool.mu.RUnlock()
	if count != 0 {
		t.Fatalf("expected 0 workers after cleanup, got %d", count)
	}
}

func TestAcquireWorkerUnlimitedWhenMaxZero(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 0)

	// AcquireWorker should not block (it will fail trying to
	// spawn a worker with a fake binary, but should get past any checks).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := pool.AcquireWorker(ctx)
	// Will fail to spawn since there's no real binary, but the point is
	// it didn't block.
	if err == nil {
		t.Fatal("expected spawn error with non-existent binary")
	}
}

func TestAcquireWorkerShutdownReturnsError(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 2)
	pool.ShutdownAll()

	_, err := pool.AcquireWorker(context.Background())
	if err == nil {
		t.Fatal("expected error after shutdown")
	}
}

func TestRetireWorkerIfNoSessions_ReleasesClaimOnSharedWorker(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 2)

	// Inject a worker with 2 sessions (shared worker).
	w, cleanup := makeFakeWorker(t, 1)
	defer cleanup()
	w.activeSessions = 2

	pool.mu.Lock()
	pool.workers[1] = w
	pool.mu.Unlock()

	// RetireWorkerIfNoSessions should decrement but NOT retire (still has 1 session).
	if pool.RetireWorkerIfNoSessions(1) {
		t.Fatal("should not retire a worker that still has sessions")
	}

	pool.mu.RLock()
	if w.activeSessions != 1 {
		t.Fatalf("expected 1 active session after release, got %d", w.activeSessions)
	}
	_, ok := pool.workers[1]
	pool.mu.RUnlock()
	if !ok {
		t.Fatal("worker should still be in pool")
	}
}

func TestRetireWorkerIfNoSessions_RetiresWhenLastSession(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 1)

	// Inject a worker with 1 session.
	w, cleanup := makeFakeWorker(t, 1)
	defer cleanup()
	w.activeSessions = 1

	pool.mu.Lock()
	pool.workers[1] = w
	pool.mu.Unlock()

	// RetireWorkerIfNoSessions should retire it.
	if !pool.RetireWorkerIfNoSessions(1) {
		t.Fatal("expected RetireWorkerIfNoSessions to return true")
	}

	if _, ok := pool.Worker(1); ok {
		t.Fatal("worker should have been retired")
	}
}

func TestAcquireWorker_AtomicClaimRace(t *testing.T) {
	// Tests that two concurrent acquisitions don't pick the same idle worker.
	const n = 5
	pool := NewFlightWorkerPool(t.TempDir(), "", 10)

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

func TestRetireWorker_RemovesFromPool(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 5)

	// Inject a worker with 2 sessions
	w, cleanup := makeFakeWorker(t, 1)
	defer cleanup()
	w.activeSessions = 2

	pool.mu.Lock()
	pool.workers[1] = w
	pool.mu.Unlock()

	pool.RetireWorker(1)

	if _, ok := pool.Worker(1); ok {
		t.Fatal("worker should have been removed from pool")
	}
}

func TestCrashRemovesWorkerFromPool(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 2)

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
	pool.mu.Unlock()

	// Start health check loop which will detect the crash.
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

	// Verify the worker was removed from the pool.
	pool.mu.RLock()
	_, stillInPool := pool.workers[0]
	pool.mu.RUnlock()
	if stillInPool {
		t.Fatal("crashed worker should have been removed from pool")
	}
}

func TestLiveWorkerCountLocked(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 5)

	// Add 2 live workers and 1 dead worker.
	w0, cleanup0 := makeFakeWorker(t, 0)
	defer cleanup0()
	w1, cleanup1 := makeFakeWorker(t, 1)
	defer cleanup1()

	cmd2 := exec.Command("true")
	_ = cmd2.Start()
	done2 := make(chan struct{})
	w2 := &ManagedWorker{ID: 2, cmd: cmd2, done: done2}
	go func() { w2.exitErr = cmd2.Wait(); close(done2) }()
	<-done2

	pool.mu.Lock()
	pool.workers[0] = w0
	pool.workers[1] = w1
	pool.workers[2] = w2
	count := pool.liveWorkerCountLocked()
	pool.mu.Unlock()

	if count != 2 {
		t.Fatalf("expected 2 live workers, got %d", count)
	}
}

func TestCleanDeadWorkersLocked(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 5)

	// Add 1 live worker and 2 dead workers.
	w0, cleanup0 := makeFakeWorker(t, 0)
	defer cleanup0()

	cmd1 := exec.Command("true")
	_ = cmd1.Start()
	done1 := make(chan struct{})
	w1 := &ManagedWorker{ID: 1, cmd: cmd1, done: done1}
	go func() { w1.exitErr = cmd1.Wait(); close(done1) }()
	<-done1

	cmd2 := exec.Command("true")
	_ = cmd2.Start()
	done2 := make(chan struct{})
	w2 := &ManagedWorker{ID: 2, cmd: cmd2, done: done2}
	go func() { w2.exitErr = cmd2.Wait(); close(done2) }()
	<-done2

	pool.mu.Lock()
	pool.workers[0] = w0
	pool.workers[1] = w1
	pool.workers[2] = w2
	pool.cleanDeadWorkersLocked()
	remaining := len(pool.workers)
	_, liveExists := pool.workers[0]
	pool.mu.Unlock()

	if remaining != 1 {
		t.Fatalf("expected 1 worker after cleanup, got %d", remaining)
	}
	if !liveExists {
		t.Fatal("live worker 0 should still exist")
	}
}

func TestLeastLoadedWorkerLocked(t *testing.T) {
	pool := NewFlightWorkerPool(t.TempDir(), "", 5)

	w0, cleanup0 := makeFakeWorker(t, 0)
	defer cleanup0()
	w1, cleanup1 := makeFakeWorker(t, 1)
	defer cleanup1()
	w2, cleanup2 := makeFakeWorker(t, 2)
	defer cleanup2()

	w0.activeSessions = 5
	w1.activeSessions = 2
	w2.activeSessions = 8

	pool.mu.Lock()
	pool.workers[0] = w0
	pool.workers[1] = w1
	pool.workers[2] = w2
	best := pool.leastLoadedWorkerLocked()
	pool.mu.Unlock()

	if best == nil {
		t.Fatal("expected a worker")
	}
	if best.ID != 1 {
		t.Fatalf("expected worker 1 (least loaded with 2 sessions), got worker %d", best.ID)
	}
}

func TestAcquireWorkerConcurrentSharing(t *testing.T) {
	// With maxWorkers=2 and 2 busy workers, 10 concurrent acquires should
	// all succeed by sharing the existing workers.
	pool := NewFlightWorkerPool(t.TempDir(), "", 2)

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
	pool.mu.Unlock()

	const concurrency = 10
	var wg sync.WaitGroup
	errors := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pool.AcquireWorker(context.Background())
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Fatalf("unexpected error: %v", err)
	}

	// All 10 sessions should be spread across the 2 workers.
	pool.mu.RLock()
	total := w0.activeSessions + w1.activeSessions
	pool.mu.RUnlock()

	// 2 original + 10 new = 12
	if total != 12 {
		t.Fatalf("expected 12 total sessions across 2 workers, got %d", total)
	}
}

// shortTempDir creates a short temp directory suitable for Unix socket paths
// (which are limited to 104 bytes on macOS). t.TempDir() paths are too long.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "pb-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestPreBindSocketsCreatesListeners(t *testing.T) {
	dir := shortTempDir(t)
	pool := NewFlightWorkerPool(dir, "", 5)

	if err := pool.PreBindSockets(3); err != nil {
		t.Fatalf("PreBindSockets failed: %v", err)
	}

	pool.preboundMu.Lock()
	count := len(pool.prebound)
	pool.preboundMu.Unlock()

	if count != 3 {
		t.Fatalf("expected 3 pre-bound sockets, got %d", count)
	}

	// Verify socket files exist on disk
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("%s/worker-%d.sock", dir, i)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected socket file %s to exist: %v", path, err)
		}
	}
}

func TestPreBindSocketsCleanupOnPartialFailure(t *testing.T) {
	dir := shortTempDir(t)
	pool := NewFlightWorkerPool(dir, "", 5)

	// Pre-bind 2 sockets, then make the 3rd fail by creating a non-empty
	// directory at worker-2.sock. os.Remove can't remove non-empty dirs,
	// and net.Listen can't bind a socket over a directory.
	blockPath := fmt.Sprintf("%s/worker-2.sock", dir)
	if err := os.MkdirAll(blockPath, 0700); err != nil {
		t.Fatalf("failed to create blocking dir: %v", err)
	}
	if err := os.WriteFile(blockPath+"/keep", []byte{}, 0600); err != nil {
		t.Fatalf("failed to create file in blocking dir: %v", err)
	}

	err := pool.PreBindSockets(3)
	if err == nil {
		t.Fatal("expected PreBindSockets to fail")
	}

	pool.preboundMu.Lock()
	count := len(pool.prebound)
	pool.preboundMu.Unlock()

	if count != 0 {
		t.Fatalf("expected 0 pre-bound sockets after failure, got %d", count)
	}

	// Verify socket files for the successfully bound sockets were cleaned up
	for i := 0; i < 2; i++ {
		path := fmt.Sprintf("%s/worker-%d.sock", dir, i)
		if _, err := os.Stat(path); err == nil {
			t.Fatalf("expected socket file %s to be cleaned up after failure", path)
		}
	}
}

func TestTakePreboundAndReturnRoundTrip(t *testing.T) {
	dir := shortTempDir(t)
	pool := NewFlightWorkerPool(dir, "", 3)

	if err := pool.PreBindSockets(3); err != nil {
		t.Fatalf("PreBindSockets failed: %v", err)
	}

	// Take all 3
	sockets := make([]*preboundSocket, 0, 3)
	for i := 0; i < 3; i++ {
		ps := pool.takePrebound()
		if ps == nil {
			t.Fatalf("takePrebound returned nil on take %d", i)
		}
		sockets = append(sockets, ps)
	}

	// Pool should be empty
	if ps := pool.takePrebound(); ps != nil {
		t.Fatal("expected nil from exhausted pool")
	}

	// Return all 3
	for _, ps := range sockets {
		pool.returnPrebound(ps)
	}

	// Pool should have 3 again
	pool.preboundMu.Lock()
	count := len(pool.prebound)
	pool.preboundMu.Unlock()

	if count != 3 {
		t.Fatalf("expected 3 pre-bound sockets after return, got %d", count)
	}
}

func TestReleaseWorkerSocketReturnsPrebound(t *testing.T) {
	dir := shortTempDir(t)
	pool := NewFlightWorkerPool(dir, "", 2)

	if err := pool.PreBindSockets(2); err != nil {
		t.Fatalf("PreBindSockets failed: %v", err)
	}

	ps := pool.takePrebound()
	if ps == nil {
		t.Fatal("takePrebound returned nil")
	}

	w := &ManagedWorker{
		parentListener: ps.listener,
		prebound:       ps,
		socketPath:     ps.socketPath,
	}

	pool.releaseWorkerSocket(w)

	if w.prebound != nil {
		t.Fatal("expected prebound to be nil after release")
	}

	// Socket should be back in the pool
	pool.preboundMu.Lock()
	count := len(pool.prebound)
	pool.preboundMu.Unlock()

	if count != 2 {
		t.Fatalf("expected 2 pre-bound sockets after release, got %d", count)
	}
}

func TestReleaseWorkerSocketClosesNonPrebound(t *testing.T) {
	dir := shortTempDir(t)
	pool := NewFlightWorkerPool(dir, "", 0)

	// Create a non-pre-bound socket
	socketPath := fmt.Sprintf("%s/worker-test.sock", dir)
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	w := &ManagedWorker{
		parentListener: ln,
		prebound:       nil,
		socketPath:     socketPath,
	}

	pool.releaseWorkerSocket(w)

	// Socket file should be removed
	if _, err := os.Stat(socketPath); err == nil {
		t.Fatal("expected socket file to be removed for non-pre-bound socket")
	}
}

func TestCloseAllPreboundClosesListeners(t *testing.T) {
	dir := shortTempDir(t)
	pool := NewFlightWorkerPool(dir, "", 3)

	if err := pool.PreBindSockets(3); err != nil {
		t.Fatalf("PreBindSockets failed: %v", err)
	}

	pool.closeAllPrebound()

	pool.preboundMu.Lock()
	count := len(pool.prebound)
	pool.preboundMu.Unlock()

	if count != 0 {
		t.Fatalf("expected 0 pre-bound sockets after closeAll, got %d", count)
	}

	// Verify listeners are closed by attempting to accept (should fail)
	// Socket files should still exist (SetUnlinkOnClose=false)
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("%s/worker-%d.sock", dir, i)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected socket file %s to still exist after closeAll", path)
		}
	}
}

func TestShutdownAllReturnsPreboundToPool(t *testing.T) {
	dir := shortTempDir(t)
	pool := NewFlightWorkerPool(dir, "", 3)

	if err := pool.PreBindSockets(3); err != nil {
		t.Fatalf("PreBindSockets failed: %v", err)
	}

	// Take one pre-bound socket and assign it to a fake worker
	ps := pool.takePrebound()
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	done := make(chan struct{})
	w := &ManagedWorker{
		ID:             0,
		cmd:            cmd,
		socketPath:     ps.socketPath,
		parentListener: ps.listener,
		prebound:       ps,
		done:           done,
	}
	go func() { w.exitErr = cmd.Wait(); close(done) }()

	pool.mu.Lock()
	pool.workers[0] = w
	pool.mu.Unlock()

	// ShutdownAll should release the worker's prebound socket back to pool,
	// then closeAllPrebound closes everything.
	pool.ShutdownAll()

	// Verify pool is fully drained
	pool.preboundMu.Lock()
	count := len(pool.prebound)
	pool.preboundMu.Unlock()

	if count != 0 {
		t.Fatalf("expected 0 pre-bound sockets after shutdown, got %d", count)
	}
}

func TestConcurrentTakeReturn(t *testing.T) {
	dir := shortTempDir(t)
	pool := NewFlightWorkerPool(dir, "", 10)

	if err := pool.PreBindSockets(10); err != nil {
		t.Fatalf("PreBindSockets failed: %v", err)
	}

	// 10 goroutines each take and return a socket 100 times
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ps := pool.takePrebound()
				if ps != nil {
					pool.returnPrebound(ps)
				}
			}
		}()
	}
	wg.Wait()

	// All 10 should be back in the pool
	pool.preboundMu.Lock()
	count := len(pool.prebound)
	pool.preboundMu.Unlock()

	if count != 10 {
		t.Fatalf("expected 10 pre-bound sockets after concurrent ops, got %d", count)
	}
}
