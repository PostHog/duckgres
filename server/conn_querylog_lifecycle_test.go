package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	queryRows        RowSet
	queryErr         error
	execResult       ExecResult
	execErr          error
	queryEntered     chan struct{}
	queryRelease     chan struct{}
	queryEnteredOnce sync.Once
	queryCalls       atomic.Int32
	execCalls        atomic.Int32
}

func (e *lifecycleExecutor) QueryContext(_ context.Context, _ string, _ ...any) (RowSet, error) {
	e.queryCalls.Add(1)
	e.blockQuery()
	return e.queryRows, e.queryErr
}
func (e *lifecycleExecutor) ExecContext(_ context.Context, _ string, _ ...any) (ExecResult, error) {
	e.execCalls.Add(1)
	return e.execResult, e.execErr
}
func (e *lifecycleExecutor) Query(_ string, _ ...any) (RowSet, error) {
	e.queryCalls.Add(1)
	e.blockQuery()
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

func (e *lifecycleExecutor) blockQuery() {
	if e.queryEntered != nil {
		e.queryEnteredOnce.Do(func() { close(e.queryEntered) })
	}
	if e.queryRelease != nil {
		<-e.queryRelease
	}
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

func capturedLogLine(t *testing.T, output, message string) string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, message) {
			return line
		}
	}
	t.Fatalf("missing captured log line containing %q in:\n%s", message, output)
	return ""
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

type columnErrorRowSet struct {
	err error
}

func (r *columnErrorRowSet) Columns() ([]string, error)          { return nil, r.err }
func (r *columnErrorRowSet) ColumnTypes() ([]ColumnTyper, error) { return nil, nil }
func (r *columnErrorRowSet) Next() bool                          { return false }
func (r *columnErrorRowSet) Scan(...any) error                   { return nil }
func (r *columnErrorRowSet) Close() error                        { return nil }
func (r *columnErrorRowSet) Err() error                          { return nil }

func TestRowStreamErrorLogBoundsAndRedactsQueryText(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		errMessage string
		forbidden  string
	}{
		{
			name:       "oversized",
			query:      "SELECT " + strings.Repeat("q", maxQueryLength) + " row-query-tail-marker",
			errMessage: "engine error: " + strings.Repeat("e", maxQueryLength) + " row-error-tail-marker",
			forbidden:  "tail-marker",
		},
		{
			name:       "secret",
			query:      "CREATE SECRET s (TYPE S3, SECRET 'row-log-secret-marker')",
			errMessage: "engine error: CREATE SECRET s (TYPE S3, SECRET 'row-log-secret-marker')",
			forbidden:  "row-log-secret-marker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, restore := captureSlog(t)
			defer restore()

			c, cleanup := newLifecycleClientConn(t)
			defer cleanup()

			_ = c.streamRowsToClient(&columnErrorRowSet{err: errors.New(tt.errMessage)}, "SELECT", tt.query)
			line := capturedLogLine(t, buf.String(), `msg="Failed to get column info."`)
			if strings.Contains(line, tt.forbidden) {
				t.Fatalf("row-stream error log retained unsafe query text %q:\n%s", tt.forbidden, line)
			}
		})
	}
}

func TestCopyOutErrorLogBoundsQueryText(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()
	c.passthrough = true
	c.executor = &lifecycleExecutor{
		queryErr: errors.New("engine error: " + strings.Repeat("e", maxQueryLength) + " copy-error-tail-marker"),
	}

	query := "COPY (SELECT '" + strings.Repeat("q", maxQueryLength) + " copy-query-tail-marker') TO STDOUT"
	if err := c.handleCopyOut(query, strings.ToUpper(query)); err != nil {
		t.Fatalf("handleCopyOut returned error: %v", err)
	}
	line := capturedLogLine(t, buf.String(), `msg="COPY TO query failed."`)
	if strings.Contains(line, "tail-marker") {
		t.Fatalf("COPY error log retained query text beyond the control-plane bound:\n%s", line)
	}
}

func TestCopyInTableCheckErrorLogBoundsQueryText(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()
	c.executor = &lifecycleExecutor{
		queryErr: errors.New("engine error: " + strings.Repeat("e", maxQueryLength) + " copy-check-error-tail-marker"),
	}

	query := "COPY " + strings.Repeat("q", maxQueryLength) + "_copy-table-tail-marker FROM STDIN"
	if err := c.handleCopyIn(query, strings.ToUpper(query)); err != nil {
		t.Fatalf("handleCopyIn returned error: %v", err)
	}
	line := capturedLogLine(t, buf.String(), `msg="COPY FROM table check failed."`)
	if strings.Contains(line, "tail-marker") {
		t.Fatalf("COPY table-check error log retained text beyond the control-plane bound:\n%s", line)
	}
}

func assertCurrentQueryBoundWhileRunning(t *testing.T, c *clientConn, exec *lifecycleExecutor, want string, run func()) {
	t.Helper()
	done := make(chan struct{})
	released := false
	waited := false
	finish := func() {
		if !released {
			close(exec.queryRelease)
			released = true
		}
		if waited {
			return
		}
		waited = true
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("query did not finish after executor release")
		}
	}
	go func() {
		defer close(done)
		run()
	}()
	defer finish()

	select {
	case <-exec.queryEntered:
	case <-time.After(time.Second):
		t.Fatal("query did not reach executor")
	}

	query, _ := c.currentQuery.Load().(string)
	if query != want {
		t.Fatalf("active-query snapshot length = %d, want exact prefix length %d", len(query), len(want))
	}

	finish()
}

func TestCurrentQuerySnapshotsBoundOversizedSQL(t *testing.T) {
	query := "SELECT '" + strings.Repeat("q", maxQueryLength) + " active-query-tail-marker'"

	t.Run("simple", func(t *testing.T) {
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()
		release := make(chan struct{})
		exec := &lifecycleExecutor{
			queryRows:    &streamingRowSet{},
			queryEntered: make(chan struct{}),
			queryRelease: release,
		}
		c.executor = exec
		assertCurrentQueryBoundWhileRunning(t, c, exec, query[:maxQueryLength], func() {
			_ = c.handleQuery([]byte(query))
		})
	})

	t.Run("extended", func(t *testing.T) {
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()
		release := make(chan struct{})
		exec := &lifecycleExecutor{
			queryRows:    &streamingRowSet{},
			queryEntered: make(chan struct{}),
			queryRelease: release,
		}
		c.executor = exec
		c.portals["p1"] = &portal{stmt: &preparedStmt{query: query, convertedQuery: "SELECT 1"}}
		body := append([]byte("p1"), 0, 0, 0, 0, 0)
		assertCurrentQueryBoundWhileRunning(t, c, exec, query[:maxQueryLength], func() {
			c.handleExecute(body)
		})
	})
}

func TestExtendedQueryColumnErrorLogBoundsQueryText(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()
	query := "SELECT '" + strings.Repeat("q", maxQueryLength) + " extended-query-tail-marker'"
	c.executor = &lifecycleExecutor{
		queryRows: &columnErrorRowSet{
			err: errors.New("engine error: " + strings.Repeat("e", maxQueryLength) + " extended-error-tail-marker"),
		},
	}
	c.portals["p1"] = &portal{stmt: &preparedStmt{query: query, convertedQuery: query}}
	body := append([]byte("p1"), 0, 0, 0, 0, 0)
	c.handleExecute(body)

	line := capturedLogLine(t, buf.String(), `msg="Columns error."`)
	if strings.Contains(line, "tail-marker") {
		t.Fatalf("extended-query column error log retained text beyond the control-plane bound:\n%s", line)
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
