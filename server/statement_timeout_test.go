package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/posthog/duckgres/server/wire"
)

// deadlineObservingExecutor records whether the context each executor call
// received actually carried a deadline. That is the property the statement
// timeout depends on: PinnedExecutor.Query/Exec run on context.Background(), so
// a path that calls them instead of the *Context variants is silently unbounded.
type deadlineObservingExecutor struct {
	selectOneExecutor
	queryHadDeadline []bool
	execHadDeadline  []bool
}

func (e *deadlineObservingExecutor) QueryContext(ctx context.Context, q string, args ...any) (RowSet, error) {
	_, ok := ctx.Deadline()
	e.queryHadDeadline = append(e.queryHadDeadline, ok)
	return e.selectOneExecutor.QueryContext(ctx, q, args...)
}

func (e *deadlineObservingExecutor) ExecContext(ctx context.Context, q string, args ...any) (ExecResult, error) {
	_, ok := ctx.Deadline()
	e.execHadDeadline = append(e.execHadDeadline, ok)
	return e.selectOneExecutor.ExecContext(ctx, q, args...)
}

func newTimeoutTestConn(t *testing.T, timeout time.Duration) *clientConn {
	t.Helper()
	s := &Server{cfg: Config{StatementTimeout: timeout}}
	s.activeQueries = make(map[BackendKey]context.CancelFunc)
	return &clientConn{server: s, ctx: context.Background()}
}

func TestStatementTimeoutAppliesDeadline(t *testing.T) {
	c := newTimeoutTestConn(t, 50*time.Millisecond)
	ctx, cleanup := c.queryContextInner(false)
	defer cleanup()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("statement context has no deadline; the timeout was not applied")
	}
	if until := time.Until(deadline); until <= 0 || until > time.Second {
		t.Fatalf("deadline %v out of expected range", until)
	}
}

// A zero timeout must preserve today's unbounded behaviour exactly — this knob
// is opt-in, and a stray deadline would start killing legitimate long queries.
func TestStatementTimeoutZeroLeavesStatementsUnbounded(t *testing.T) {
	c := newTimeoutTestConn(t, 0)
	ctx, cleanup := c.queryContextInner(false)
	defer cleanup()

	if _, ok := ctx.Deadline(); ok {
		t.Fatal("statement context has a deadline with StatementTimeout=0")
	}
}

// A cursor takes ONE context at DECLARE and holds it across every FETCH, so the
// timeout bounds the cursor's whole lifetime rather than each FETCH. Stricter
// than PostgreSQL and deliberate; pin it so it cannot change silently.
func TestStatementTimeoutBoundsCursorLifetimeNotEachFetch(t *testing.T) {
	c := newTimeoutTestConn(t, 50*time.Millisecond)
	ctx, cleanup := c.queryContextForCursor()
	defer cleanup()

	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("cursor context has no deadline; the timeout does not reach the cursor path")
	}
}

// REGRESSION (review P0): extended-protocol Execute used context-less
// executor.Query/Exec, so prepared statements — what pgx/psycopg3/JDBC send —
// were reachable by neither the statement timeout nor a CancelRequest. Drive a
// real Parse/Bind/Execute and assert the executor saw a deadline.
func TestStatementTimeoutReachesExtendedProtocolExecute(t *testing.T) {
	exec := &deadlineObservingExecutor{}
	c, _ := newBufferedConn(exec)
	c.server.cfg.StatementTimeout = time.Minute
	c.stmts = make(map[string]*preparedStmt)
	c.portals = make(map[string]*portal)

	// Extended-protocol handlers are void; a failure parks on c.fatalErr.
	c.handleParse(append([]byte("s1\x00SELECT 1\x00"), 0, 0))
	c.handleBind(append([]byte("p1\x00s1\x00"), 0, 0, 0, 0, 0, 0))
	c.handleExecute(append([]byte("p1\x00"), 0, 0, 0, 0))
	if c.fatalErr != nil {
		t.Fatalf("extended flow failed: %v", c.fatalErr)
	}

	if len(exec.queryHadDeadline) == 0 {
		t.Fatal("extended Execute never reached the executor's context-aware path")
	}
	for i, had := range exec.queryHadDeadline {
		if !had {
			t.Fatalf("extended Execute call %d ran without a deadline: prepared statements are unbounded", i)
		}
	}
}

// The classification is driven by the ERROR and gated on the feature, so it
// cannot go sticky the way a stored statement context did.
func TestStatementTimedOutIsErrorDrivenAndFeatureGated(t *testing.T) {
	deadline := context.DeadlineExceeded
	wrapped := errors.New("flight execute: context deadline exceeded")
	other := errors.New("syntax error at or near \"selct\"")

	on := newTimeoutTestConn(t, time.Minute)
	if !on.statementTimedOut(deadline) || !on.statementTimedOut(wrapped) {
		t.Fatal("deadline errors not recognised while the timeout is configured")
	}
	if on.statementTimedOut(other) || on.statementTimedOut(nil) {
		t.Fatal("non-deadline error classified as a statement timeout")
	}
	if got, want := on.cancellationMessage(deadline), "canceling statement due to statement timeout"; got != want {
		t.Fatalf("cancellationMessage = %q, want %q", got, want)
	}
	if got, want := on.cancellationMessage(context.Canceled), "canceling statement due to user request"; got != want {
		t.Fatalf("user-cancel wording = %q, want %q", got, want)
	}

	// Feature off: internal deadlines (attach, exec, worker gRPC) must NOT start
	// surfacing as 57014 just because this code exists.
	off := newTimeoutTestConn(t, 0)
	if off.statementTimedOut(deadline) || off.statementTimedOut(wrapped) {
		t.Fatal("deadline classified as a statement timeout with the feature disabled")
	}
	if got, want := off.cancellationMessage(deadline), "canceling statement due to user request"; got != want {
		t.Fatalf("disabled-path wording = %q, want %q", got, want)
	}
}

func TestIsCallerCancellationCoversStatementTimeout(t *testing.T) {
	c := newTimeoutTestConn(t, time.Minute)
	if !c.isCallerCancellation(context.DeadlineExceeded) {
		t.Fatal("statement timeout not treated as caller cancellation (would log as an infra failure)")
	}
	off := newTimeoutTestConn(t, 0)
	if off.isCallerCancellation(context.DeadlineExceeded) {
		t.Fatal("deadline treated as caller cancellation with the feature disabled")
	}
}

// --- Coverage of the remaining execution paths -------------------------------
//
// #1194 wired the timeout through extended-protocol Execute. The tests below
// pin the paths that stayed context-less: the Describe schema probes (which
// really execute the statement at LIMIT 0), the writable-CTE rewrite paths on
// BOTH protocols, and COPY in both directions. Each fails against a path that
// bypasses the statement context, because the fake executor fails fast on any
// context-less call instead of hanging.

// stmtTimeoutExecutor is a fake engine for statement-timeout tests. Its
// Context-aware methods record the context they were handed; with blockQuery /
// blockExec set they then wait for that context to finish and return its
// error, simulating a wedged statement. Its context-less Query/Exec fail fast
// with a marker error, so a code path that bypasses the statement context
// fails the test immediately.
type stmtTimeoutExecutor struct {
	noopProfiling
	mu         sync.Mutex
	ctxs       []context.Context
	bareCalls  int
	blockQuery bool
	blockExec  bool
	rows       RowSet
	err        error
}

// errBareExecutorCall is returned by the context-less methods so a bypassing
// path surfaces a distinctive error rather than a timeout.
var errBareExecutorCall = errors.New("context-less executor call: statement context bypassed")

func (e *stmtTimeoutExecutor) recordCtx(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ctxs = append(e.ctxs, ctx)
}

func (e *stmtTimeoutExecutor) lastCtx() context.Context {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.ctxs) == 0 {
		return nil
	}
	return e.ctxs[len(e.ctxs)-1]
}

func (e *stmtTimeoutExecutor) bareCallCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.bareCalls
}

func (e *stmtTimeoutExecutor) QueryContext(ctx context.Context, _ string, _ ...any) (RowSet, error) {
	e.recordCtx(ctx)
	if e.blockQuery {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return e.rows, e.err
}

func (e *stmtTimeoutExecutor) ExecContext(ctx context.Context, _ string, _ ...any) (ExecResult, error) {
	e.recordCtx(ctx)
	if e.blockExec {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return emptyExecResult{}, e.err
}

func (e *stmtTimeoutExecutor) Query(_ string, _ ...any) (RowSet, error) {
	e.mu.Lock()
	e.bareCalls++
	e.mu.Unlock()
	return nil, errBareExecutorCall
}

func (e *stmtTimeoutExecutor) Exec(_ string, _ ...any) (ExecResult, error) {
	e.mu.Lock()
	e.bareCalls++
	e.mu.Unlock()
	return nil, errBareExecutorCall
}

func (e *stmtTimeoutExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}
func (e *stmtTimeoutExecutor) PingContext(context.Context) error { return nil }
func (e *stmtTimeoutExecutor) Close() error                      { return nil }

// wireErr is a parsed ErrorResponse.
type wireErr struct{ code, msg string }

// drainWireFull parses buffered wire output into a message-type sequence plus
// any ErrorResponse (code, message) pairs, then resets the buffer.
func drainWireFull(t *testing.T, c *clientConn, out *bytes.Buffer) (string, []wireErr) {
	t.Helper()
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	data := out.Bytes()
	seq := make([]byte, 0, 8)
	var errs []wireErr
	for len(data) > 0 {
		if len(data) < 5 {
			t.Fatalf("truncated wire message: % x", data)
		}
		msgType := data[0]
		msgLen := binary.BigEndian.Uint32(data[1:5])
		if int(msgLen)+1 > len(data) {
			t.Fatalf("wire message length %d exceeds buffer %d", msgLen, len(data))
		}
		payload := data[5 : 1+msgLen]
		if msgType == 'E' {
			we := wireErr{}
			for len(payload) > 1 {
				field := payload[0]
				end := bytes.IndexByte(payload[1:], 0)
				if end < 0 {
					t.Fatalf("unterminated error field %q", field)
				}
				val := string(payload[1 : 1+end])
				switch field {
				case 'C':
					we.code = val
				case 'M':
					we.msg = val
				}
				payload = payload[1+end+1:]
			}
			errs = append(errs, we)
		}
		seq = append(seq, msgType)
		data = data[1+msgLen:]
	}
	out.Reset()
	return string(seq), errs
}

// newStmtTimeoutWireConn builds a clientConn with the timeout configured and
// its wire output captured, ready for handler-level tests.
func newStmtTimeoutWireConn(t *testing.T, timeout time.Duration) (*clientConn, *bytes.Buffer) {
	t.Helper()
	c, out, cleanup := newPortalSuspConn(t)
	t.Cleanup(cleanup)
	c.server.cfg.StatementTimeout = timeout
	return c, out
}

// addTimeoutPortal registers a prepared statement + portal with no parameters.
func addTimeoutPortal(c *clientConn, portalName, query string) {
	stmt := &preparedStmt{query: query, convertedQuery: query}
	c.stmts[portalName+".stmt"] = stmt
	c.portals[portalName] = &portal{stmt: stmt}
}

// A wedged SELECT through extended-protocol Execute must die at the deadline
// with PostgreSQL's exact 57014 timeout wording on the wire, not just carry a
// deadline (the #1194 regression test asserts the deadline; this asserts the
// user-visible outcome).
func TestStatementTimeoutBoundsExtendedExecuteQuery(t *testing.T) {
	c, out := newStmtTimeoutWireConn(t, 50*time.Millisecond)

	ex := &stmtTimeoutExecutor{blockQuery: true}
	c.executor = ex
	addTimeoutPortal(c, "p1", "SELECT 1")

	start := time.Now()
	c.handleExecute(portalExecBody("p1", 0))
	elapsed := time.Since(start)

	if ex.bareCallCount() != 0 {
		t.Fatal("extended Execute used the context-less executor method; the timeout cannot reach it")
	}
	if elapsed > 30*time.Second {
		t.Fatalf("wedged statement was not bounded by the timeout (took %v)", elapsed)
	}
	_, errs := drainWireFull(t, c, out)
	if len(errs) != 1 || errs[0].code != "57014" {
		t.Fatalf("expected one 57014 ErrorResponse, got %v", errs)
	}
	if errs[0].msg != "canceling statement due to statement timeout" {
		t.Fatalf("msg = %q, want PostgreSQL's exact timeout wording", errs[0].msg)
	}
	// A timeout ends one statement, not the session.
	if c.ctx.Err() != nil {
		t.Fatal("connection context cancelled by a statement timeout")
	}
}

// Same for the non-result branch (INSERT/DDL) — the wedged-INSERT shape that
// motivated the knob.
func TestStatementTimeoutBoundsExtendedExecuteExec(t *testing.T) {
	c, out := newStmtTimeoutWireConn(t, 50*time.Millisecond)

	ex := &stmtTimeoutExecutor{blockExec: true}
	c.executor = ex
	addTimeoutPortal(c, "p1", "INSERT INTO t VALUES (1)")

	c.handleExecute(portalExecBody("p1", 0))

	if ex.bareCallCount() != 0 {
		t.Fatal("extended Execute used the context-less executor method; the timeout cannot reach it")
	}
	_, errs := drainWireFull(t, c, out)
	if len(errs) != 1 || errs[0].code != "57014" {
		t.Fatalf("expected one 57014 ErrorResponse, got %v", errs)
	}
	if errs[0].msg != "canceling statement due to statement timeout" {
		t.Fatalf("msg = %q, want PostgreSQL's exact timeout wording", errs[0].msg)
	}
}

// The Describe schema probe really executes the statement (LIMIT 0 bounds
// rows, not planning or side effects), so it must run under the deadline too:
// a statement that wedges in the engine wedges the probe just as it wedges
// Execute.
func TestStatementTimeoutReachesExtendedDescribeProbe(t *testing.T) {
	for _, descType := range []byte{'S', 'P'} {
		t.Run(string(descType), func(t *testing.T) {
			c, _ := newStmtTimeoutWireConn(t, 50*time.Millisecond)

			ex := &stmtTimeoutExecutor{err: errors.New("probe failed")}
			c.executor = ex
			addTimeoutPortal(c, "p1", "SELECT 1")

			var body []byte
			if descType == 'P' {
				body = append([]byte{descType}, []byte("p1")...)
			} else {
				body = append([]byte{descType}, []byte("p1.stmt")...)
			}
			body = append(body, 0)
			c.handleDescribe(body)

			if ex.bareCallCount() != 0 {
				t.Fatal("Describe probe used the context-less executor method; the timeout cannot reach it")
			}
			ctx := ex.lastCtx()
			if ctx == nil {
				t.Fatal("Describe probe never reached the executor")
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("Describe probe context carries no deadline")
			}
		})
	}
}

// The writable-CTE rewrite path runs its steps through executeMultiStatement —
// the shape an incremental INSERT with CTEs compiles to. It must carry the
// deadline like any other simple-protocol statement.
func TestStatementTimeoutReachesMultiStatementSimple(t *testing.T) {
	c, out := newStmtTimeoutWireConn(t, 50*time.Millisecond)

	ex := &stmtTimeoutExecutor{blockExec: true}
	c.executor = ex

	start := time.Now()
	err := c.executeMultiStatement([]string{"INSERT INTO t VALUES (1)"}, nil)
	if err != nil {
		t.Fatalf("executeMultiStatement: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("wedged rewrite step was not bounded by the timeout (took %v)", elapsed)
	}
	if ex.bareCallCount() != 0 {
		t.Fatal("multi-statement rewrite used the context-less executor method; the timeout cannot reach it")
	}
	_, errs := drainWireFull(t, c, out)
	if len(errs) != 1 || errs[0].code != "57014" {
		t.Fatalf("expected one 57014 ErrorResponse, got %v", errs)
	}
	if errs[0].msg != "canceling statement due to statement timeout" {
		t.Fatalf("msg = %q, want PostgreSQL's exact timeout wording", errs[0].msg)
	}
}

// The extended-protocol rewrite path (executeMultiStatementExtended) must share
// the Execute's statement context.
func TestStatementTimeoutReachesMultiStatementExtended(t *testing.T) {
	c, out := newStmtTimeoutWireConn(t, 50*time.Millisecond)

	ex := &stmtTimeoutExecutor{blockExec: true}
	c.executor = ex

	stmt := &preparedStmt{
		query:          "WITH d AS (DELETE FROM t RETURNING *) SELECT count(*) FROM d",
		convertedQuery: "WITH d AS (DELETE FROM t RETURNING *) SELECT count(*) FROM d",
		statements:     []string{"CREATE TEMP TABLE tmp AS DELETE FROM t RETURNING *", "SELECT count(*) FROM tmp"},
	}
	c.stmts["s1"] = stmt
	c.portals["p1"] = &portal{stmt: stmt}

	start := time.Now()
	c.handleExecute(portalExecBody("p1", 0))
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("wedged rewrite step was not bounded by the timeout (took %v)", elapsed)
	}
	if ex.bareCallCount() != 0 {
		t.Fatal("extended multi-statement rewrite used the context-less executor method; the timeout cannot reach it")
	}
	_, errs := drainWireFull(t, c, out)
	if len(errs) != 1 || errs[0].code != "57014" {
		t.Fatalf("expected one 57014 ErrorResponse, got %v", errs)
	}
	if errs[0].msg != "canceling statement due to statement timeout" {
		t.Fatalf("msg = %q, want PostgreSQL's exact timeout wording", errs[0].msg)
	}
}

// COPY TO STDOUT drives its whole result set through one Query call; that call
// must carry the statement deadline or a wedged COPY stays unbounded.
func TestStatementTimeoutReachesCopyOut(t *testing.T) {
	c, out := newStmtTimeoutWireConn(t, 50*time.Millisecond)

	ex := &stmtTimeoutExecutor{blockQuery: true}
	c.executor = ex

	start := time.Now()
	if err := c.handleCopy("COPY t TO STDOUT", "COPY T TO STDOUT"); err != nil {
		t.Fatalf("handleCopy: %v", err)
	}
	if ex.bareCallCount() != 0 {
		t.Fatal("COPY TO STDOUT used the context-less executor method; the timeout cannot reach it")
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("wedged COPY was not bounded by the timeout (took %v)", elapsed)
	}
	_, errs := drainWireFull(t, c, out)
	if len(errs) != 1 || errs[0].code != "57014" {
		t.Fatalf("expected one 57014 ErrorResponse, got %v", errs)
	}
	if errs[0].msg != "canceling statement due to statement timeout" {
		t.Fatalf("msg = %q, want PostgreSQL's exact timeout wording", errs[0].msg)
	}
}

// COPY FROM STDIN reads the wire inline, so it cannot run the disconnect
// monitor, but the statement context — deadline and cancel registration —
// must still reach the engine calls (the schema probe and the load itself).
func TestStatementTimeoutReachesCopyIn(t *testing.T) {
	// An empty COPY stream: CopyDone immediately, so the probe runs, then the
	// load — which wedges here. The conn is a pipe solely for the peer-address
	// and monitor plumbing; input comes from the buffer.
	var input bytes.Buffer
	writePGMessage(&input, wire.MsgCopyDone, nil)
	serverSide, clientSide := net.Pipe()
	defer func() { _ = serverSide.Close(); _ = clientSide.Close() }()

	out := &bytes.Buffer{}
	srv := &Server{cfg: Config{StatementTimeout: 50 * time.Millisecond}}
	srv.activeQueries = make(map[BackendKey]context.CancelFunc)
	srv.queryLogger = &QueryLogger{ch: make(chan QueryLogEntry, 100)}
	c := &clientConn{
		server:   srv,
		conn:     serverSide,
		reader:   bufio.NewReader(&input),
		writer:   bufio.NewWriter(out),
		txStatus: txStatusIdle,
		cursors:  map[string]*cursorState{},
		ctx:      context.Background(),
	}

	ex := &stmtTimeoutExecutor{
		blockExec: true,                   // the load wedges
		rows:      newSuspensionRowSet(0), // the schema probe answers
	}
	c.executor = ex

	start := time.Now()
	if err := c.handleCopyIn("COPY t FROM STDIN", "COPY T FROM STDIN"); err != nil {
		t.Fatalf("handleCopyIn: %v", err)
	}

	if ex.bareCallCount() != 0 {
		t.Fatal("COPY FROM STDIN used a context-less executor method; the timeout cannot reach it")
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("wedged COPY load was not bounded by the timeout (took %v)", elapsed)
	}
	ctx := ex.lastCtx()
	if ctx == nil {
		t.Fatal("COPY FROM STDIN never reached a context-aware executor method")
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("COPY FROM STDIN engine calls carry no deadline")
	}
	_, errs := drainWireFull(t, c, out)
	if len(errs) != 1 || errs[0].code != "57014" {
		t.Fatalf("expected one 57014 ErrorResponse, got %v", errs)
	}
	if errs[0].msg != "canceling statement due to statement timeout" {
		t.Fatalf("msg = %q, want PostgreSQL's exact timeout wording", errs[0].msg)
	}
}

// A suspended portal's rowset outlives its Execute handler, so its statement
// context must outlive it too — but be torn down when the portal completes.
func TestStatementTimeoutSuspendedPortalKeepsContextAlive(t *testing.T) {
	c, out := newStmtTimeoutWireConn(t, time.Hour)

	ex := &stmtTimeoutExecutor{rows: newSuspensionRowSet(3)}
	c.executor = ex
	addTimeoutPortal(c, "p1", "SELECT * FROM t")

	c.handleExecute(portalExecBody("p1", 2))
	seq, _ := drainWireFull(t, c, out)
	if seq != "TDDs" {
		t.Fatalf("first Execute: expected TDDs, got %q", seq)
	}
	ctx := ex.lastCtx()
	if ctx == nil {
		t.Fatal("extended Execute never reached a context-aware executor method")
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("suspended portal statement context has no deadline")
	}
	if ctx.Err() != nil {
		t.Fatal("statement context cancelled while the portal is suspended; the open rowset dies with it")
	}

	c.handleExecute(portalExecBody("p1", 0))
	seq, _ = drainWireFull(t, c, out)
	if seq != "DC" {
		t.Fatalf("resume Execute: expected DC, got %q", seq)
	}
	if ctx.Err() == nil {
		t.Fatal("statement context leaked after the portal completed")
	}
}

// A cursor's deadline bounds its whole lifetime (deliberate — see CLAUDE.md),
// so a FETCH that fails because the cursor's context expired mid-lifetime must
// report the timeout wording, even after other statements ran in between.
func TestStatementTimeoutCursorLifetimeClassifiesAfterLaterStatement(t *testing.T) {
	c, out := newStmtTimeoutWireConn(t, 30*time.Millisecond)

	// The cursor's rowset reports the cursor context's terminal error, the
	// way a worker surfaces a cancelled in-flight scan.
	cursorRows := &ctxErrRowSet{cols: []string{"c"}, colTypers: []ColumnTyper{stringColumnTyper{}}}
	ex := &stmtTimeoutExecutor{rows: cursorRows}
	c.executor = ex

	c.cursors["c1"] = &cursorState{query: "SELECT 1"}
	fetch := &pg_query.FetchStmt{Portalname: "c1", Direction: pg_query.FetchDirection_FETCH_FORWARD, HowMany: 1}

	// First FETCH opens the cursor (its context starts here).
	if err := c.handleFetchCursor("FETCH 1 FROM c1", fetch); err != nil {
		t.Fatalf("first FETCH: %v", err)
	}
	_, _ = drainWireFull(t, c, out)
	ctx := ex.lastCtx()
	if ctx == nil {
		t.Fatal("cursor open never reached a context-aware executor method")
	}
	cursorRows.ctx = ctx // the rowset now fails the way the worker would

	// Wait out the cursor's deadline, then run an unrelated statement — its
	// own context becomes the connection's in-flight one.
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("cursor context carries no deadline")
	}
	time.Sleep(time.Until(deadline) + 20*time.Millisecond)
	interCtx, interCleanup := c.queryContextForCursor()
	interCleanup()
	if interCtx.Err() != context.Canceled {
		t.Fatalf("interleaved statement context state %v, want canceled (cleanup ran)", interCtx.Err())
	}

	// The cursor's context expired mid-lifetime; the rowset surfaces it on
	// the next FETCH.
	if err := c.handleFetchCursor("FETCH 1 FROM c1", fetch); err != nil {
		t.Fatalf("second FETCH: %v", err)
	}
	_, errs := drainWireFull(t, c, out)
	if len(errs) != 1 {
		t.Fatalf("expected one ErrorResponse, got %v", errs)
	}
	if errs[0].code != "57014" || errs[0].msg != "canceling statement due to statement timeout" {
		t.Fatalf("cursor lifetime timeout classified as %q %q, want 57014 timeout wording", errs[0].code, errs[0].msg)
	}
}

// ctxErrRowSet is a RowSet with no rows whose Err() reports the terminal
// error of the context the engine call was handed.
type ctxErrRowSet struct {
	cols      []string
	colTypers []ColumnTyper
	ctx       context.Context
}

func (s *ctxErrRowSet) Columns() ([]string, error)          { return s.cols, nil }
func (s *ctxErrRowSet) ColumnTypes() ([]ColumnTyper, error) { return s.colTypers, nil }
func (s *ctxErrRowSet) Next() bool                          { return false }
func (s *ctxErrRowSet) Scan(...any) error                   { return errors.New("no rows") }
func (s *ctxErrRowSet) Close() error                        { return nil }
func (s *ctxErrRowSet) Err() error {
	if s.ctx == nil {
		return nil
	}
	return s.ctx.Err()
}
