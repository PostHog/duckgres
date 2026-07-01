package server

import (
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/server/usersecrets"
)

// TestConnSummariesByWorkerID covers the live-list state snapshot: an active
// session reports "active" + a query-start; an idle-in-transaction session
// reports the idle state + zero query-start; worker-less conns are omitted.
func TestConnSummariesByWorkerID(t *testing.T) {
	s := &Server{conns: map[int32]*clientConn{}}

	active := &clientConn{pid: 1, workerID: 100}
	active.currentQuery.Store("SELECT 1")
	active.queryStart.Store(time.Now())
	s.conns[1] = active

	idleTx := &clientConn{pid: 2, workerID: 200, txStatus: txStatusTransaction}
	idleTx.currentQuery.Store("")
	s.conns[2] = idleTx

	// Worker-less conn (standalone / transient) must be omitted.
	noWorker := &clientConn{pid: 3, workerID: -1}
	noWorker.currentQuery.Store("")
	s.conns[3] = noWorker

	sums := s.ConnSummariesByWorkerID()
	if len(sums) != 2 {
		t.Fatalf("expected 2 worker-bound summaries, got %d", len(sums))
	}
	if sums[100].State != "active" || sums[100].QueryStart.IsZero() {
		t.Fatalf("worker 100 should be active with a query-start, got %+v", sums[100])
	}
	if sums[200].State != "idle in transaction" {
		t.Fatalf("worker 200 should be 'idle in transaction', got %q", sums[200].State)
	}
	if !sums[200].QueryStart.IsZero() {
		t.Fatalf("idle session should have zero query-start, got %v", sums[200].QueryStart)
	}
	if _, ok := sums[-1]; ok {
		t.Fatal("worker-less conn must be omitted")
	}

	// Nil server is safe.
	var ns *Server
	if ns.ConnSummariesByWorkerID() != nil {
		t.Fatal("nil server should return nil")
	}
}

// TestConnDetailByPID covers the admin live-query detail snapshot: registry
// lookup, the (load-bearing) redaction guarantee, and state derivation.
func TestConnDetailByPID(t *testing.T) {
	s := &Server{conns: map[int32]*clientConn{}}

	// An active connection running a CREATE SECRET — currentQuery holds the
	// already-redacted form, exactly as the query path stores it.
	const raw = "CREATE SECRET leak (TYPE s3, KEY_ID 'AKIAEXAMPLE', SECRET 'topsecretmaterial')"
	active := &clientConn{
		pid:             42,
		orgID:           "acme",
		username:        "bob",
		database:        "main",
		applicationName: "psql",
		workerID:        7,
		workerPod:       "duckling-acme-7",
		backendStart:    time.Now(),
	}
	active.currentQuery.Store(usersecrets.RedactForLog(raw))
	active.queryStart.Store(time.Now())
	s.conns[42] = active

	d, ok := s.ConnDetailByPID(42)
	if !ok {
		t.Fatal("expected ConnDetailByPID to find pid 42")
	}
	if strings.Contains(d.Query, "topsecretmaterial") || strings.Contains(d.Query, "AKIAEXAMPLE") {
		t.Fatalf("redaction breached — credential material leaked into detail: %q", d.Query)
	}
	if !strings.Contains(d.Query, "redacted") {
		t.Fatalf("expected redacted placeholder, got %q", d.Query)
	}
	if d.State != "active" {
		t.Fatalf("expected state=active, got %q", d.State)
	}
	if d.OrgID != "acme" || d.Username != "bob" || d.WorkerID != 7 || d.WorkerPod != "duckling-acme-7" {
		t.Fatalf("metadata mismatch: %+v", d)
	}
	if d.QueryStart.IsZero() {
		t.Fatal("expected non-zero QueryStart for an active query")
	}

	// An idle-in-transaction connection: no current query, txn open.
	idle := &clientConn{pid: 7, txStatus: txStatusTransaction}
	idle.currentQuery.Store("")
	s.conns[7] = idle
	di, ok := s.ConnDetailByPID(7)
	if !ok {
		t.Fatal("expected to find pid 7")
	}
	if di.State != "idle in transaction" {
		t.Fatalf("expected state=idle in transaction, got %q", di.State)
	}
	if di.Query != "" {
		t.Fatalf("expected empty query for idle conn, got %q", di.Query)
	}

	// Missing pid → not found.
	if _, ok := s.ConnDetailByPID(999); ok {
		t.Fatal("expected ConnDetailByPID to miss for unknown pid")
	}

	// Nil receiver is safe (defensive — callers may hold a nil server).
	var ns *Server
	if _, ok := ns.ConnDetailByPID(1); ok {
		t.Fatal("expected nil server to report not found")
	}
}
