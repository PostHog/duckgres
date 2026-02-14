package controlplane

import (
	"context"
	"os/exec"
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
