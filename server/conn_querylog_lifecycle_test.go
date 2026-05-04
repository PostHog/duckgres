package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
)

// newLifecycleClientConn builds a clientConn with both reader and writer
// wired so the disconnect monitor's bufio.Reader.Peek doesn't dereference
// nil. The clientSide pipe end stays open for the duration of the test;
// disconnect monitoring will Peek from it harmlessly until the test ends.
func newLifecycleClientConn(t *testing.T) (*clientConn, func()) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	out := &bytes.Buffer{}
	ql := &QueryLogger{ch: make(chan QueryLogEntry, 100)}
	srv := &Server{activeQueries: make(map[BackendKey]context.CancelFunc), queryLogger: ql}
	c := &clientConn{
		server:   srv,
		conn:     serverSide,
		reader:   bufio.NewReader(serverSide),
		writer:   bufio.NewWriter(out),
		txStatus: txStatusIdle,
		cursors:  map[string]*cursorState{},
		portals:  map[string]*portal{},
		stmts:    map[string]*preparedStmt{},
		ctx:      context.Background(),
	}
	cleanup := func() {
		_ = serverSide.Close()
		_ = clientSide.Close()
	}
	return c, cleanup
}

// lifecycleExecutor is a stub executor that records every Query/Exec invocation
// and returns either a configured RowSet/ExecResult or an error. Used by the
// lifecycle tests to drive each entrypoint without needing real DuckDB.
type lifecycleExecutor struct {
	noopProfiling
	queryRows  RowSet
	queryErr   error
	execResult ExecResult
	execErr    error
	queryCalls atomic.Int32
	execCalls  atomic.Int32
}

func (e *lifecycleExecutor) QueryContext(_ context.Context, _ string, _ ...any) (RowSet, error) {
	e.queryCalls.Add(1)
	return e.queryRows, e.queryErr
}
func (e *lifecycleExecutor) ExecContext(_ context.Context, _ string, _ ...any) (ExecResult, error) {
	e.execCalls.Add(1)
	return e.execResult, e.execErr
}
func (e *lifecycleExecutor) Query(_ string, _ ...any) (RowSet, error) {
	e.queryCalls.Add(1)
	return e.queryRows, e.queryErr
}
func (e *lifecycleExecutor) Exec(_ string, _ ...any) (ExecResult, error) {
	e.execCalls.Add(1)
	return e.execResult, e.execErr
}
func (e *lifecycleExecutor) ConnContext(_ context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}
func (e *lifecycleExecutor) PingContext(_ context.Context) error { return nil }
func (e *lifecycleExecutor) Close() error                        { return nil }

// emptyExecResult is an ExecResult that reports 0 rows affected — sufficient
// for these tests, which assert lifecycle log presence not row-count math.
type emptyExecResult struct{}

func (emptyExecResult) RowsAffected() (int64, error) { return 0, nil }

// captureSlog redirects slog.Default to a buffer, returns the buffer and a
// restore function. Each test in this file uses it to assert the presence
// of "Query started." / "Query finished." log lines per entrypoint.
func captureSlog(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	prev := slog.Default()
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return &buf, func() { slog.SetDefault(prev) }
}

// assertLifecyclePair asserts that the captured slog output contains both
// a "Query started." line and a "Query finished." line — the invariant
// PR #519 enforces: every query that runs on a worker, regardless of which
// pgwire protocol path serviced it, gets a matched start/finish pair so a
// LogQL filter on the message catches all of them.
func assertLifecyclePair(t *testing.T, buf *bytes.Buffer, label string) {
	t.Helper()
	out := buf.String()
	if !strings.Contains(out, `msg="Query started."`) {
		t.Errorf("[%s] missing 'Query started.' in:\n%s", label, out)
	}
	if !strings.Contains(out, `msg="Query finished."`) {
		t.Errorf("[%s] missing 'Query finished.' in:\n%s", label, out)
	}
}

// TestLifecyclePairFiresOnExecuteQueryDirect covers the simple-query DML
// entrypoint (BEGIN, INSERT, UPDATE, DELETE, etc.). Pre-PR #519 this path
// only logged on failure via logQueryError; success was silent.
func TestLifecyclePairFiresOnExecuteQueryDirect(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
	c.username = "alice"
	c.workerID = 7777

	if err := c.executeQueryDirect("UPDATE foo SET x = 1", "UPDATE"); err != nil {
		t.Fatalf("executeQueryDirect: %v", err)
	}
	assertLifecyclePair(t, buf, "executeQueryDirect")
	if !strings.Contains(buf.String(), `worker=7777`) {
		t.Errorf("expected worker=7777 attr in lifecycle logs:\n%s", buf.String())
	}
}

// TestLifecyclePairFiresOnExecuteQueryDirectOnError verifies the deferred
// logQueryFinished fires even when the executor returns an error — pre-PR #519
// the error path emitted only logQueryError, so a LogQL filter on
// "Query finished." would miss failed queries entirely.
func TestLifecyclePairFiresOnExecuteQueryDirectOnError(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.executor = &lifecycleExecutor{execErr: errors.New("Catalog Error: table does not exist")}
	c.username = "alice"
	c.workerID = 7777

	if err := c.executeQueryDirect("UPDATE missing SET x = 1", "UPDATE"); err != nil {
		t.Fatalf("executeQueryDirect returned non-nil err: %v", err)
	}
	assertLifecyclePair(t, buf, "executeQueryDirect-error")
	if !strings.Contains(buf.String(), `error=`) {
		t.Errorf("expected error= attr on Finished log for failed query:\n%s", buf.String())
	}
}

// TestLifecyclePairFiresOnExecuteSelectQuery covers the simple-query SELECT
// path. This path already had logQueryStarted pre-#519, but error returns
// (Scan errors, Columns errors, rows.Err()) skipped logQueryFinished —
// verify the deferred close pattern now balances every Started.
func TestLifecyclePairFiresOnExecuteSelectQuery(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.executor = &lifecycleExecutor{queryErr: errors.New("Catalog Error: table does not exist")}
	c.username = "alice"
	c.workerID = 7777

	_, _, _, _ = c.executeSelectQuery("SELECT * FROM missing", "SELECT")
	assertLifecyclePair(t, buf, "executeSelectQuery-error")
}

// TestLifecyclePairFiresOnHandleExecuteExec covers the extended-query Exec
// path (handleExecute → executor.Exec). This is the prod-traffic path that
// drove the worker-41827 investigation: every modern pg driver (psycopg, pgx,
// JDBC) issues queries via Bind/Execute, so missing lifecycle logs here
// invisibilised the credential-refresh deadlock impact.
func TestLifecyclePairFiresOnHandleExecuteExec(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.executor = &lifecycleExecutor{execErr: errors.New("worker is dead")}
	c.username = "alice"
	c.workerID = 41827

	stmt := &preparedStmt{
		query:          "UPDATE foo SET x = 1",
		convertedQuery: "UPDATE foo SET x = 1",
	}
	port := &portal{stmt: stmt}
	c.portals["p1"] = port

	// Execute body: portal name "p1" + max rows 0
	body := append([]byte("p1"), 0)
	body = append(body, 0, 0, 0, 0)

	c.handleExecute(body)
	assertLifecyclePair(t, buf, "handleExecute-Exec")
}

// TestLifecyclePairFiresOnHandleExecuteQuery covers the extended-query Query
// path (handleExecute → executor.Query for SELECTs). Same rationale as the
// Exec test above — modern drivers go through this path for every SELECT.
func TestLifecyclePairFiresOnHandleExecuteQuery(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.executor = &lifecycleExecutor{queryErr: errors.New("worker is dead")}
	c.username = "alice"
	c.workerID = 41827

	stmt := &preparedStmt{
		query:          "SELECT * FROM foo",
		convertedQuery: "SELECT * FROM foo",
	}
	port := &portal{stmt: stmt}
	c.portals["p1"] = port

	body := append([]byte("p1"), 0)
	body = append(body, 0, 0, 0, 0)

	c.handleExecute(body)
	assertLifecyclePair(t, buf, "handleExecute-Query")
}
