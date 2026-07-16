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
	"time"

	"github.com/posthog/duckgres/server/sqlcore"
	"github.com/posthog/duckgres/server/wire"
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

type boundLifecycleExecutor struct {
	lifecycleExecutor
	boundCalls atomic.Int32
}

func (e *boundLifecycleExecutor) QueryWithBoundParams(_ string, _ sqlcore.SQLLiteralAppender) (RowSet, error) {
	e.boundCalls.Add(1)
	return e.queryRows, e.queryErr
}

func (e *boundLifecycleExecutor) ExecWithBoundParams(_ string, _ sqlcore.SQLLiteralAppender) (ExecResult, error) {
	e.boundCalls.Add(1)
	return e.execResult, e.execErr
}

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

func TestLifecycleLogsBoundOversizedQueryText(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	query := "SELECT " + strings.Repeat("q", maxQueryLength) + " query-tail-marker"
	c.logQueryStarted(query)
	c.logQueryFinished(query, time.Now(), 0, errors.New("engine error: "+query))

	out := buf.String()
	assertLifecyclePair(t, buf, "oversized-query")
	if strings.Contains(out, "query-tail-marker") {
		t.Fatal("lifecycle logs retained query text beyond the control-plane bound")
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

// streamingRowSet is a fake RowSet that yields a fixed number of single-column
// string rows. Used to drive executeSelectQuery past the rows.Next() gate so
// the test can exercise mid-stream wire-write failures.
type streamingRowSet struct {
	rows      [][]any
	idx       int
	cols      []string
	colTypers []ColumnTyper
	closed    bool
}

func (s *streamingRowSet) Columns() ([]string, error)          { return s.cols, nil }
func (s *streamingRowSet) ColumnTypes() ([]ColumnTyper, error) { return s.colTypers, nil }
func (s *streamingRowSet) Next() bool {
	if s.idx >= len(s.rows) {
		return false
	}
	s.idx++
	return true
}
func (s *streamingRowSet) Scan(dest ...any) error {
	row := s.rows[s.idx-1]
	for i, v := range row {
		if p, ok := dest[i].(*interface{}); ok {
			*p = v
		}
	}
	return nil
}
func (s *streamingRowSet) Close() error { s.closed = true; return nil }
func (s *streamingRowSet) Err() error   { return nil }

// stringColumnTyper is a tiny ColumnTyper that reports VARCHAR. Sufficient for
// the streaming-write test, which only cares that pgwire encodes a column.
type stringColumnTyper struct{}

func (stringColumnTyper) DatabaseTypeName() string { return "VARCHAR" }

// failingWriter returns a fixed error on every Write. Wrapped by a small
// bufio.Writer so a wire-message Write triggers an underlying flush that
// surfaces the error to the caller — simulating the prod symptom where the
// AWS NLB tore down a stalled pgwire socket and the pgwire send-row write
// failed mid-stream.
type failingWriter struct{ err error }

func (f failingWriter) Write(_ []byte) (int, error) { return 0, f.err }

// TestExecuteSelectQuery_LogsErrorOnWireWriteFailure verifies the regression
// from the posthog-mw-prod-us TCP-write-timeout incident: when a mid-stream
// wire write fails (sendRowDescription / sendDataRowWithFormats), the CP must
// emit Error-level "Query execution errored." so alerts fire. Pre-fix only
// the deferred Info-level "Query finished." was emitted, hiding the failure
// below alerting thresholds.
func TestExecuteSelectQuery_LogsErrorOnWireWriteFailure(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	// 16-byte buffer + failing underlying writer: any wire message larger
	// than 16 bytes (every real pgwire message) triggers an underlying Write
	// that returns the configured error.
	c.writer = bufio.NewWriterSize(failingWriter{err: errors.New("write tcp: broken pipe")}, 16)

	c.executor = &lifecycleExecutor{
		queryRows: &streamingRowSet{
			cols:      []string{"c"},
			colTypers: []ColumnTyper{stringColumnTyper{}},
			rows:      [][]any{{"hello"}},
		},
	}
	c.username = "alice"
	c.workerID = 7777

	_, _, _, _ = c.executeSelectQuery("SELECT 'hello'", "SELECT")

	out := buf.String()
	if !strings.Contains(out, `level=ERROR msg="Query execution errored."`) {
		t.Errorf("expected Error-level 'Query execution errored.' in:\n%s", out)
	}
	// Error message must explicitly identify this as a pgwire client write
	// failure, not a generic "write tcp …" string. Operators reading the
	// alert shouldn't have to know that port 5432 means pgwire.
	if !strings.Contains(out, "pgwire client write failed") {
		t.Errorf("expected wrapped 'pgwire client write failed' prefix in error attr:\n%s", out)
	}
	// Lifecycle pair must still fire so Loki filters on Started/Finished
	// catch the failed query.
	assertLifecyclePair(t, buf, "executeSelectQuery-wire-error")
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

func TestPortalLifecycleTerminalPortalsReleaseLargeStatementsAfterReparse(t *testing.T) {
	const (
		largeQuerySize = 256 << 10
		largeParamOIDs = 27_000
	)
	for _, tc := range []struct {
		name   string
		binary bool
	}{
		{name: "success"},
		{name: "binary failure", binary: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, cleanup := newLifecycleClientConn(t)
			defer cleanup()
			var out bytes.Buffer
			c.writer = bufio.NewWriter(&out)
			c.passthrough = true

			var (
				executor QueryExecutor
				bound    *boundLifecycleExecutor
			)
			if tc.binary {
				bound = &boundLifecycleExecutor{}
				executor = bound
			} else {
				executor = &lifecycleExecutor{execResult: emptyExecResult{}}
			}
			c.executor = executor

			paramTypes := make([]int32, largeParamOIDs)
			paramFormats := []int16(nil)
			value := bindTestValue{data: []byte("value")}
			wantState := portalStateDone
			if tc.binary {
				paramTypes[0] = OidInt4
				paramFormats = []int16{1}
				value = bindTestValue{data: []byte{0, 0}}
				wantState = portalStateFailed
			}
			largeQuery := "INSERT INTO t VALUES ($1) /* " + strings.Repeat("x", largeQuerySize) + " */"
			c.stmts["large"] = &preparedStmt{
				query:          largeQuery,
				convertedQuery: "INSERT INTO t VALUES (?) /* " + strings.Repeat("x", largeQuerySize) + " */",
				paramTypes:     paramTypes,
				numParams:      1,
			}

			p := bindPortalForTest(t, c, "terminal", "large", paramFormats, []bindTestValue{value}, nil)
			out.Reset()
			c.handleExecute(executeTestBody("terminal"))
			if err := c.writer.Flush(); err != nil {
				t.Fatalf("flush Execute: %v", err)
			}
			if p.state != wantState {
				t.Fatalf("terminal state = %v, want %v", p.state, wantState)
			}
			requireReleasedBindPayload(t, p)
			if p.stmt != nil {
				t.Fatalf("terminal portal retained prepared statement %p", p.stmt)
			}
			if tc.binary {
				if !bytes.Contains(out.Bytes(), []byte("08P01")) {
					t.Fatalf("malformed binary Execute response = %q, want 08P01", out.Bytes())
				}
				if got := bound.boundCalls.Load(); got != 0 {
					t.Fatalf("malformed binary Bind reached Flight executor %d times", got)
				}
			}

			out.Reset()
			c.handleExecute(executeTestBody("terminal"))
			if err := c.writer.Flush(); err != nil {
				t.Fatalf("flush repeated Execute: %v", err)
			}
			if !bytes.Contains(out.Bytes(), []byte("55000")) {
				t.Fatalf("repeated terminal Execute response = %q, want 55000", out.Bytes())
			}

			c.handleClose([]byte{'S', 'm', 'i', 's', 's', 'i', 'n', 'g', 0})
			if c.portals["terminal"] != p {
				t.Fatal("Close(S) for a missing statement dropped terminal portal")
			}
			if err := c.writer.Flush(); err != nil {
				t.Fatalf("flush missing Close(S): %v", err)
			}
			out.Reset()

			replacementQuery := "SELECT 1 /* " + strings.Repeat("r", largeQuerySize) + " */"
			for i := 0; i < 3; i++ {
				previous := c.stmts["large"]
				c.handleParse(parseTestBody("large", replacementQuery, nil))
				if err := c.writer.Flush(); err != nil {
					t.Fatalf("flush re-Parse %d: %v", i, err)
				}
				frames := scanWireFrames(t, out.Bytes())
				if len(frames) != 1 || frames[0].msgType != wire.MsgParseComplete {
					t.Fatalf("re-Parse %d frames = %q, want ParseComplete", i, frameTypes(frames))
				}
				replacement := c.stmts["large"]
				if replacement == nil || replacement == previous || replacement.query != replacementQuery {
					t.Fatalf("re-Parse %d did not replace large statement: got %p, previous %p", i, replacement, previous)
				}
				out.Reset()
			}

			c.handleClose([]byte{'S', 'l', 'a', 'r', 'g', 'e', 0})
			if _, ok := c.portals["terminal"]; ok {
				t.Fatal("Close(S) did not remove terminal portal after re-Parse")
			}
		})
	}
}

func TestPortalLifecycleMaxRowsKeepsLegacyReadyPortal(t *testing.T) {
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()
	var out bytes.Buffer
	c.writer = bufio.NewWriter(&out)
	executor := &lifecycleExecutor{
		queryRows: &streamingRowSet{
			cols:      []string{"value"},
			colTypers: []ColumnTyper{stringColumnTyper{}},
			rows:      [][]any{{"one"}, {"two"}, {"three"}, {"four"}},
		},
	}
	c.executor = executor
	c.stmts["select"] = &preparedStmt{
		query:          "SELECT $1",
		convertedQuery: "SELECT ?",
		numParams:      1,
	}
	p := bindPortalForTest(t, c, "limited", "select", nil, []bindTestValue{{data: []byte("value")}}, nil)

	for i := 0; i < 2; i++ {
		out.Reset()
		c.handleExecute(executeTestBodyWithMaxRows("limited", 1))
		if err := c.writer.Flush(); err != nil {
			t.Fatalf("flush limited Execute %d: %v", i, err)
		}
		if p.state != portalStateReady {
			t.Fatalf("limited Execute %d state = %v, want ready", i, p.state)
		}
		if p.stmt == nil {
			t.Fatalf("limited Execute %d released the statement", i)
		}
		requireBindPayload(t, p)
	}
	if got := executor.queryCalls.Load(); got != 2 {
		t.Fatalf("limited Execute query calls = %d, want 2", got)
	}
}

func TestPortalLifecycleSimpleQueryDropsUnnamedPortal(t *testing.T) {
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()
	var out bytes.Buffer
	c.writer = bufio.NewWriter(&out)
	c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
	c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
	p := bindPortalForTest(t, c, "", "insert", nil, []bindTestValue{{data: []byte("value")}}, nil)

	if err := c.handleQuery([]byte("UPDATE t SET value = 1\x00")); err != nil {
		t.Fatalf("handle simple query: %v", err)
	}
	if _, ok := c.portals[""]; ok {
		t.Fatal("simple Query did not remove unnamed portal")
	}
	requireReleasedBindPayload(t, p)
}
