package server

import (
	"context"
	"errors"
	"testing"
	"time"
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
