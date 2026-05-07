//go:build !kubernetes

package controlplane

import (
	"bytes"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/posthog/duckgres/server/flightclient"
)

// mockCloser stands in for the client TCP/TLS conn — captures bytes written
// (so tests can assert a FATAL ErrorResponse was delivered) and tracks whether
// Close was called.
type mockCloser struct {
	closed  atomic.Bool
	writeMu sync.Mutex
	written []byte
	events  []string
}

func (m *mockCloser) Write(p []byte) (int, error) {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	if m.closed.Load() {
		return 0, errors.New("write after close")
	}
	m.events = append(m.events, "write")
	m.written = append(m.written, p...)
	return len(p), nil
}

func (m *mockCloser) Bytes() []byte {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	out := make([]byte, len(m.written))
	copy(out, m.written)
	return out
}

func (m *mockCloser) Close() error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	m.events = append(m.events, "close")
	m.closed.Store(true)
	return nil
}

func (m *mockCloser) Events() []string {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	out := make([]string, len(m.events))
	copy(out, m.events)
	return out
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
		PID:      pid,
		WorkerID: 7,
		Executor: executor,
		conn:     conn,
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
	sm.sessions[1001] = &ManagedSession{PID: 1001, WorkerID: 3, Executor: exec1, conn: conn1}
	sm.sessions[1002] = &ManagedSession{PID: 1002, WorkerID: 3, Executor: exec2, conn: conn2}
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

func TestOnWorkerCrash_WritesFATALBeforeClose(t *testing.T) {
	// Asserts the new behavior: when a worker is reaped, the CP delivers a
	// pgwire FATAL ErrorResponse on the client conn before closing it. This
	// is the difference between psql cleanly surfacing "connection lost" and
	// dbt's libpq state machine hanging silently on a half-open socket.
	pool := &FlightWorkerPool{workers: make(map[int]*ManagedWorker)}
	sm := NewSessionManager(pool, nil)

	conn := &mockCloser{}
	pid := int32(1500)

	sm.mu.Lock()
	sm.sessions[pid] = &ManagedSession{
		PID:      pid,
		WorkerID: 42,
		Executor: &flightclient.FlightExecutor{},
		conn:     conn,
	}
	sm.byWorker[42] = []int32{pid}
	sm.mu.Unlock()

	sm.OnWorkerCrash(42, func(int32) {})

	if !conn.closed.Load() {
		t.Fatal("conn was not closed on worker crash")
	}

	// Inspect the bytes the crash handler wrote: pgwire ErrorResponse starts
	// with the byte 'E', followed by a 4-byte length, then field-tagged
	// strings. We just assert FATAL + the worker ID appear so we know a
	// FATAL packet was emitted before the close — not testing the wire
	// encoding in detail (server/wire owns that).
	got := conn.Bytes()
	if len(got) == 0 {
		t.Fatal("no bytes written before close — FATAL not delivered")
	}
	if got[0] != 'E' {
		t.Errorf("expected first byte 'E' (ErrorResponse), got %q", got[0])
	}
	if !bytes.Contains(got, []byte("FATAL")) {
		t.Errorf("expected 'FATAL' in payload, got %q", got)
	}
	if !bytes.Contains(got, []byte("42")) {
		t.Errorf("expected worker id '42' in payload, got %q", got)
	}
	if got := conn.Events(); !slices.Equal(got, []string{"write", "write", "write", "close"}) {
		t.Fatalf("expected FATAL message writes before close, got event order %v", got)
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
		PID:      pid,
		WorkerID: 9,
		Executor: executor,
		conn:     conn,
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
