package server

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newTimeoutTestConn builds the minimum clientConn needed to exercise the
// statement-timeout plumbing: a server carrying the config, plus the query
// registry queryContextInner writes into.
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
	if c.statementTimedOut() {
		t.Fatal("statementTimedOut() true with no timeout configured")
	}
}

func TestStatementTimedOutClassifiesAsTimeoutNotUserCancel(t *testing.T) {
	c := newTimeoutTestConn(t, 10*time.Millisecond)

	ctx, cleanup := c.queryContextInner(false)
	defer cleanup()
	<-ctx.Done()

	if !c.statementTimedOut() {
		t.Fatal("statementTimedOut() = false after the deadline expired")
	}
	if got, want := c.cancellationMessage(), "canceling statement due to statement timeout"; got != want {
		t.Fatalf("cancellationMessage() = %q, want %q", got, want)
	}
	// The deadline sits on the statement context, so the CONNECTION context is
	// still healthy. Without the statementTimedOut() branch this would be
	// classified as an infra failure and surfaced as a bare 42000.
	if c.ctx.Err() != nil {
		t.Fatal("connection context was cancelled; a timeout must end one statement, not the session")
	}
	if !c.isCallerCancellation(context.DeadlineExceeded) {
		t.Fatal("isCallerCancellation() = false for a statement timeout")
	}
}

// A user cancel and a timeout both map to 57014, but the wording differs and
// drivers string-match it, so the two must not be conflated.
func TestUserCancelKeepsUserRequestWording(t *testing.T) {
	c := newTimeoutTestConn(t, 0)

	_, cleanup := c.queryContextInner(false)
	defer cleanup()

	if c.statementTimedOut() {
		t.Fatal("statementTimedOut() true for a user cancel")
	}
	if got, want := c.cancellationMessage(), "canceling statement due to user request"; got != want {
		t.Fatalf("cancellationMessage() = %q, want %q", got, want)
	}
}

// A ctx deadline surfaces as "context deadline exceeded", which the original
// substring check did not match — it only looked for "context canceled".
func TestIsQueryCancelledMatchesDeadlineExceeded(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"deadline sentinel", context.DeadlineExceeded, true},
		{"cancel sentinel", context.Canceled, true},
		{"wrapped deadline", errors.New("rpc error: context deadline exceeded"), true},
		{"wrapped cancel", errors.New("rpc error: context canceled"), true},
		{"unrelated", errors.New("syntax error at or near \"selct\""), false},
		{"nil", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isQueryCancelled(tc.err); got != tc.want {
				t.Fatalf("isQueryCancelled(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// A cursor takes ONE context at DECLARE and holds it across every FETCH, so the
// timeout bounds the cursor's whole lifetime rather than each FETCH. That is
// stricter than PostgreSQL and is deliberate; pin it so it cannot change silently.
func TestStatementTimeoutBoundsCursorLifetimeNotEachFetch(t *testing.T) {
	c := newTimeoutTestConn(t, 50*time.Millisecond)

	ctx, cleanup := c.queryContextForCursor()
	defer cleanup()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("cursor context has no deadline; the timeout does not reach the cursor path")
	}
	if until := time.Until(deadline); until <= 0 || until > time.Second {
		t.Fatalf("cursor deadline %v out of expected range", until)
	}
}
