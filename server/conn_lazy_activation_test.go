package server

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

// --- unit: ensureSessionActive ----------------------------------------

// TestEnsureSessionActiveActivatesOnce pins the core laziness contract: the
// activator runs exactly once, installs the executor + worker identity, and a
// later call — even one that asks for a pinned worker — is a no-op (moving an
// ALREADY-active connection to a bigger worker is escalateWorker's job, not the
// activator's).
func TestEnsureSessionActiveActivatesOnce(t *testing.T) {
	c := &clientConn{}
	fake := &tierExecutor{name: "activated"}
	calls := 0
	sawPinned := false
	c.sessionActivator = func(_ context.Context, pinned bool) (QueryExecutor, int, string, error) {
		calls++
		sawPinned = pinned
		return fake, 7, "pod-7", nil
	}

	if err := c.ensureSessionActive(context.Background(), false); err != nil || calls != 1 || sawPinned {
		t.Fatalf("ensureSessionActive: err=%v calls=%d pinned=%v", err, calls, sawPinned)
	}
	if c.executor != QueryExecutor(fake) || c.workerID != 7 || c.workerPod != "pod-7" {
		t.Fatalf("executor/worker identity not installed: exec=%v worker=%d pod=%q", c.executor, c.workerID, c.workerPod)
	}

	if err := c.ensureSessionActive(context.Background(), true); err != nil || calls != 1 {
		t.Fatalf("second ensureSessionActive re-activated: err=%v calls=%d", err, calls)
	}
}

// TestEnsureSessionActiveNilActivatorNoop covers every eager path (standalone,
// process backend, GUC-sized, tier-disabled): no activator installed means the
// hook must be inert.
func TestEnsureSessionActiveNilActivatorNoop(t *testing.T) {
	exec := &tierExecutor{name: "eager"}
	c := &clientConn{executor: exec}
	if err := c.ensureSessionActive(context.Background(), false); err != nil {
		t.Fatalf("ensureSessionActive: %v", err)
	}
	if c.executor != QueryExecutor(exec) {
		t.Fatal("eager executor was replaced")
	}
	if c.needsActivation() {
		t.Fatal("connection with an executor reports needsActivation")
	}
}

// TestEnsureSessionActiveFailureLeavesConnectionInactive asserts a failed
// activation does not install a half-built session: the executor stays nil so
// the next statement re-attempts rather than dereferencing nothing.
func TestEnsureSessionActiveFailureLeavesConnectionInactive(t *testing.T) {
	c := &clientConn{}
	want := errors.New("worker capacity exhausted for organization")
	c.sessionActivator = func(context.Context, bool) (QueryExecutor, int, string, error) {
		return nil, 0, "", want
	}
	if err := c.ensureSessionActive(context.Background(), false); !errors.Is(err, want) {
		t.Fatalf("ensureSessionActive err = %v, want %v", err, want)
	}
	if c.executor != nil {
		t.Fatal("executor installed despite a failed activation")
	}
}

// --- helpers ----------------------------------------------------------

// lazyConn builds a control-plane-shaped connection with NO executor and the
// tier switcher installed, exactly as the control plane hands one to the
// message loop when it defers worker acquisition. activate hands out `small`
// for an unpinned acquire and `big` for a pinned one, recording the calls.
type lazyConn struct {
	c        *clientConn
	out      *bytes.Buffer
	small    *tierExecutor
	big      *tierExecutor
	pinned   []bool
	switched []string
	order    *[]string
}

func newLazyConn(t *testing.T) *lazyConn {
	t.Helper()
	var order []string
	l := &lazyConn{
		small: &tierExecutor{name: "small", order: &order},
		big:   &tierExecutor{name: "big", order: &order},
		order: &order,
	}
	c, out := newBufferedConn(nil)
	l.c = c
	l.out = out
	c.stmts = make(map[string]*preparedStmt)
	c.portals = make(map[string]*portal)
	c.cursors = make(map[string]*cursorState)
	// The control plane installs BOTH hooks: the connection is nominally on the
	// exploratory tier and has no session yet.
	c.onExploratoryWorker = true
	c.sessionActivator = func(_ context.Context, pinned bool) (QueryExecutor, int, string, error) {
		l.pinned = append(l.pinned, pinned)
		order = append(order, "activate")
		if pinned {
			// Mirrors the control-plane activator: a pinning first statement
			// acquires the standard profile directly and marks the connection
			// off-tier, so no escalation follows.
			MarkConnectionPinned(c)
			return l.big, 9, "worker-9", nil
		}
		return l.small, 3, "worker-3", nil
	}
	c.workerSwitcher = func(_ context.Context, reason string) (QueryExecutor, int, string, error) {
		l.switched = append(l.switched, reason)
		order = append(order, "switch")
		return l.big, 9, "worker-9", nil
	}
	return l
}

// --- lazy activation through the query paths ---------------------------

// TestLazyActivationOnFirstQuery asserts nothing is acquired at connect time:
// the first statement that needs an engine activates (unpinned, so on the
// exploratory worker) and a second statement reuses that session.
func TestLazyActivationOnFirstQuery(t *testing.T) {
	l := newLazyConn(t)
	l.small.queryFn = func(int, string) (RowSet, error) { return &tierRowSet{rows: []int64{1}}, nil }

	if len(l.pinned) != 0 {
		t.Fatalf("activator ran before the first statement: %v", l.pinned)
	}
	if err := l.c.handleQuery([]byte("SELECT 1\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if len(l.pinned) != 1 || l.pinned[0] {
		t.Fatalf("activator calls = %v, want exactly one unpinned", l.pinned)
	}
	if len(l.switched) != 0 {
		t.Fatalf("plain SELECT escalated: %v", l.switched)
	}
	if !l.c.onExploratoryWorker {
		t.Fatal("plain SELECT left the exploratory tier")
	}
	if l.c.workerID != 3 || l.c.workerPod != "worker-3" {
		t.Fatalf("worker identity = %d/%q, want 3/worker-3", l.c.workerID, l.c.workerPod)
	}
	if len(l.small.queryCalls) != 1 {
		t.Fatalf("exploratory worker query calls = %v, want one", l.small.queryCalls)
	}

	if err := l.c.handleQuery([]byte("SELECT 1\x00")); err != nil {
		t.Fatalf("second handleQuery: %v", err)
	}
	if len(l.pinned) != 1 {
		t.Fatalf("second statement re-activated: %v", l.pinned)
	}
	if len(l.small.queryCalls) != 2 {
		t.Fatalf("exploratory worker query calls = %v, want two", l.small.queryCalls)
	}
}

// TestLazyActivationPinnedFirstStatement asserts a pinning FIRST statement
// takes the standard profile in ONE acquire: the activator is called with
// pinned=true and the switcher never runs (no small-acquire-then-escalate).
func TestLazyActivationPinnedFirstStatement(t *testing.T) {
	l := newLazyConn(t)

	if err := l.c.handleQuery([]byte("CREATE TEMP TABLE t (a INT)\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if len(l.pinned) != 1 || !l.pinned[0] {
		t.Fatalf("activator calls = %v, want exactly one pinned", l.pinned)
	}
	if len(l.switched) != 0 {
		t.Fatalf("pinned first statement also escalated (wasted double acquire): %v", l.switched)
	}
	if l.c.onExploratoryWorker {
		t.Fatal("connection still on the exploratory tier after a pinning statement")
	}
	if len(l.small.execCalls) != 0 || len(l.small.queryCalls) != 0 {
		t.Fatalf("exploratory worker saw the pinning statement: exec=%v query=%v", l.small.execCalls, l.small.queryCalls)
	}
	if len(l.big.execCalls) != 1 {
		t.Fatalf("standard worker exec calls = %v, want one", l.big.execCalls)
	}
	want := []string{"activate", "big:exec"}
	if strings.Join(*l.order, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v, want %v", *l.order, want)
	}
}

// TestLazyActivationEngineFreeStatementsDoNotActivate is the point of the whole
// exercise: statements the control plane answers itself must not spend a worker.
func TestLazyActivationEngineFreeStatementsDoNotActivate(t *testing.T) {
	for _, q := range []string{
		"",
		";",
		"SET duckgres.query_source = 'endpoints'",
		"SHOW duckgres.query_source",
		"SET application_name = 'x'",
	} {
		t.Run(q, func(t *testing.T) {
			l := newLazyConn(t)
			if err := l.c.handleQuery([]byte(q + "\x00")); err != nil {
				t.Fatalf("handleQuery(%q): %v", q, err)
			}
			if len(l.pinned) != 0 {
				t.Fatalf("engine-free statement %q activated a worker: %v", q, l.pinned)
			}
			if l.c.executor != nil {
				t.Fatalf("engine-free statement %q installed an executor", q)
			}
		})
	}
}

// TestLazyActivationFailureIsConnectionFatal asserts an activation failure is
// treated exactly like a failed escalation: there is no usable session to
// resynchronize to, so the client gets a FATAL with the mapped SQLSTATE, no
// ReadyForQuery, and the message loop unwinds.
func TestLazyActivationFailureIsConnectionFatal(t *testing.T) {
	cases := []struct {
		name     string
		query    string
		err      error
		wantCode string
	}{
		{"disabled", "SELECT 1", errors.New("this account is disabled; contact your administrator"), "28000"},
		{"org cap", "SELECT 1", errors.New("worker capacity exhausted for organization"), "53300"},
		{"other", "SELECT 1", errors.New("dial worker: connection refused"), "53400"},
		{"pinned", "CREATE TEMP TABLE t (a INT)", errors.New("worker capacity exhausted for organization"), "53300"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := newLazyConn(t)
			l.c.sessionActivator = func(context.Context, bool) (QueryExecutor, int, string, error) {
				return nil, 0, "", tc.err
			}

			err := l.c.handleQuery([]byte(tc.query + "\x00"))
			if err == nil {
				t.Fatal("handleQuery returned nil; a failed activation must terminate the connection")
			}
			if !errors.Is(err, errConnectionFatal) {
				t.Fatalf("error %v does not carry errConnectionFatal, so the message loop would resume", err)
			}
			msgs := parseWireMsgs(t, l.out.Bytes())
			if !hasErrorResponse(msgs, "FATAL", tc.wantCode) {
				t.Fatalf("want FATAL %s, got %s", tc.wantCode, describeErrorResponses(msgs))
			}
			if countMsgs(msgs, 'Z') != 0 {
				t.Fatalf("ReadyForQuery sent after a connection-fatal activation failure: %s", describeMsgs(msgs))
			}
		})
	}
}

// TestLazyActivationExtendedProtocol asserts the extended path activates too,
// and picks the tier from the Parse-time classification in one acquire.
func TestLazyActivationExtendedProtocol(t *testing.T) {
	t.Run("small", func(t *testing.T) {
		l := newLazyConn(t)
		l.small.queryFn = func(int, string) (RowSet, error) { return &tierRowSet{rows: []int64{1}}, nil }
		if err := extRun(t, l.c, "s1", "SELECT n FROM t"); err != nil {
			t.Fatalf("extended run: %v", err)
		}
		if len(l.pinned) != 1 || l.pinned[0] {
			t.Fatalf("activator calls = %v, want one unpinned", l.pinned)
		}
		if len(l.switched) != 0 {
			t.Fatalf("plain SELECT escalated: %v", l.switched)
		}
	})
	t.Run("pinning", func(t *testing.T) {
		l := newLazyConn(t)
		if err := extRun(t, l.c, "s1", "CREATE TEMP TABLE t (a INT)"); err != nil {
			t.Fatalf("extended run: %v", err)
		}
		if len(l.pinned) != 1 || !l.pinned[0] {
			t.Fatalf("activator calls = %v, want one pinned", l.pinned)
		}
		if len(l.switched) != 0 {
			t.Fatalf("pinned first statement also escalated: %v", l.switched)
		}
		if len(l.big.execCalls) != 1 {
			t.Fatalf("standard worker exec calls = %v, want one", l.big.execCalls)
		}
	})
}

// TestLazyActivationBatchedFirstStatement asserts a batch activates on its
// first engine-touching statement and does not re-activate for the rest.
func TestLazyActivationBatchedFirstStatement(t *testing.T) {
	l := newLazyConn(t)
	l.small.queryFn = func(int, string) (RowSet, error) { return &tierRowSet{rows: []int64{1}}, nil }

	if err := l.c.handleQuery([]byte("SELECT 1; CREATE TEMP TABLE t (a INT)\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if len(l.pinned) != 1 || l.pinned[0] {
		t.Fatalf("activator calls = %v, want one unpinned (the leading read)", l.pinned)
	}
	if len(l.switched) != 1 {
		t.Fatalf("switcher calls = %v, want one (the CREATE TEMP TABLE)", l.switched)
	}
	want := []string{"activate", "small:query", "switch", "big:exec"}
	if strings.Join(*l.order, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v, want %v", *l.order, want)
	}
}

// TestLazyActivationCopyPinsInOneAcquire asserts COPY — routed above the
// transpile-time hook — activates directly on the standard profile.
func TestLazyActivationCopyPinsInOneAcquire(t *testing.T) {
	l := newLazyConn(t)
	if err := l.c.handleQuery([]byte("COPY t TO 's3://bucket/o.parquet' (FORMAT parquet)\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if len(l.pinned) != 1 || !l.pinned[0] {
		t.Fatalf("activator calls = %v, want one pinned", l.pinned)
	}
	if len(l.switched) != 0 {
		t.Fatalf("COPY escalated after activating: %v", l.switched)
	}
	if len(l.big.execCalls) != 1 {
		t.Fatalf("standard worker exec calls = %v, want one", l.big.execCalls)
	}
}

// TestLazyActivationDeclareCursorPinsInOneAcquire asserts a DECLARE as the
// FIRST statement acquires the standard profile directly — the cursor's
// worker-side RowSet must never open on a session that is about to be replaced.
func TestLazyActivationDeclareCursorPinsInOneAcquire(t *testing.T) {
	l := newLazyConn(t)
	l.big.queryFn = func(int, string) (RowSet, error) { return &tierRowSet{rows: []int64{1, 2}}, nil }

	if err := l.c.handleQuery([]byte("DECLARE cur CURSOR FOR SELECT n FROM t\x00")); err != nil {
		t.Fatalf("handleQuery(DECLARE): %v", err)
	}
	if len(l.pinned) != 1 || !l.pinned[0] {
		t.Fatalf("activator calls = %v, want one pinned", l.pinned)
	}
	if len(l.switched) != 0 {
		t.Fatalf("DECLARE escalated after activating: %v", l.switched)
	}
	if err := l.c.handleQuery([]byte("FETCH 1 FROM cur\x00")); err != nil {
		t.Fatalf("handleQuery(FETCH): %v", err)
	}
	if len(l.small.queryCalls) != 0 {
		t.Fatalf("cursor opened on the exploratory worker: %v", l.small.queryCalls)
	}
	if len(l.big.queryCalls) != 1 {
		t.Fatalf("standard worker query calls = %v, want one (the cursor open)", l.big.queryCalls)
	}
}
