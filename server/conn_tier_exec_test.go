package server

import (
	"bytes"
	"context"
	"errors"

	"strings"
	"testing"
)

// --- fakes -------------------------------------------------------------

// tierRowSet is a scripted RowSet: it yields `rows` (single BIGINT column)
// and then reports `err` from Err(). Modeled on LocalRowSet's surface
// (server/executor.go) minus the *sql.Rows plumbing.
type tierRowSet struct {
	rows   []int64
	err    error
	idx    int
	closed int
}

func (r *tierRowSet) Columns() ([]string, error) { return []string{"n"}, nil }
func (r *tierRowSet) ColumnTypes() ([]ColumnTyper, error) {
	return []ColumnTyper{describeColumnType("BIGINT")}, nil
}
func (r *tierRowSet) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}
func (r *tierRowSet) Scan(dest ...any) error {
	*(dest[0].(*interface{})) = r.rows[r.idx-1]
	return nil
}
func (r *tierRowSet) Close() error { r.closed++; return nil }
func (r *tierRowSet) Err() error   { return r.err }

// tierExecutor is a scripted QueryExecutor recording what the connection sent
// it. queryFn/execFn let a test script per-call behavior; the zero value
// answers every query with an empty RowSet and every exec with 0 rows.
type tierExecutor struct {
	noopProfiling
	name       string
	queryCalls []string
	execCalls  []string
	queryFn    func(call int, query string) (RowSet, error)
	execFn     func(call int, query string) (ExecResult, error)
	// order records interleaving with the switcher across all executors.
	order *[]string
}

func (e *tierExecutor) note(s string) {
	if e.order != nil {
		*e.order = append(*e.order, s)
	}
}

func (e *tierExecutor) QueryContext(_ context.Context, query string, _ ...any) (RowSet, error) {
	e.queryCalls = append(e.queryCalls, query)
	e.note(e.name + ":query")
	if e.queryFn != nil {
		return e.queryFn(len(e.queryCalls), query)
	}
	return &tierRowSet{}, nil
}

func (e *tierExecutor) ExecContext(_ context.Context, query string, _ ...any) (ExecResult, error) {
	e.execCalls = append(e.execCalls, query)
	e.note(e.name + ":exec")
	if e.execFn != nil {
		return e.execFn(len(e.execCalls), query)
	}
	return &fakeExecResult{}, nil
}

func (e *tierExecutor) Query(query string, args ...any) (RowSet, error) {
	return e.QueryContext(context.Background(), query, args...)
}
func (e *tierExecutor) Exec(query string, args ...any) (ExecResult, error) {
	return e.ExecContext(context.Background(), query, args...)
}
func (e *tierExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}
func (e *tierExecutor) PingContext(context.Context) error { return nil }
func (e *tierExecutor) Close() error                      { return nil }

const tierOOMError = "flight execute: rpc error: code = Internal desc = failed to execute query: " +
	"Out of Memory Error: failed to allocate data of size 1.0 GiB (900.0 MiB/1.0 GiB used)"

// countMsgs returns how many backend messages of the given type byte are present.
func countMsgs(msgs []wireMsg, typ byte) int {
	n := 0
	for _, m := range msgs {
		if m.typ == typ {
			n++
		}
	}
	return n
}

// fatalErrorResponseWith reports whether msgs carry a FATAL-severity
// ErrorResponse containing all of wants. Severity is written both as the
// localized ('M'/'S') fields; matching the literal "FATAL" byte-string in the
// body is enough to distinguish it from "ERROR".
func fatalErrorResponseWith(msgs []wireMsg, wants ...string) bool {
	all := append([]string{"FATAL"}, wants...)
	return errorResponseWith(msgs, all...)
}

// --- 1. pinning statements escalate BEFORE the executor sees them -------

// TestSimpleQueryPinsBeforeExecute asserts a statement that creates session
// state (CREATE TEMP TABLE) escalates the connection to a normal-size worker
// BEFORE it executes — the exploratory worker must stay stateless by
// construction, so the small worker must never see the statement.
func TestSimpleQueryPinsBeforeExecute(t *testing.T) {
	var order []string
	small := &tierExecutor{name: "small", order: &order}
	big := &tierExecutor{name: "big", order: &order}
	c, out := newBufferedConn(small)
	c.onExploratoryWorker = true
	var reasons []string
	c.workerSwitcher = func(_ context.Context, reason string) (QueryExecutor, int, string, error) {
		reasons = append(reasons, reason)
		order = append(order, "switch")
		return big, 9, "worker-9", nil
	}

	if err := c.handleQuery([]byte("CREATE TEMP TABLE t (a INT)\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	if len(reasons) != 1 || reasons[0] != escalateReasonState {
		t.Fatalf("switcher reasons = %v, want [%q]", reasons, escalateReasonState)
	}
	if len(small.execCalls) != 0 || len(small.queryCalls) != 0 {
		t.Fatalf("exploratory worker saw the pinning statement: exec=%v query=%v", small.execCalls, small.queryCalls)
	}
	if len(big.execCalls) != 1 {
		t.Fatalf("standard worker exec calls = %v, want exactly one", big.execCalls)
	}
	if len(order) < 2 || order[0] != "switch" {
		t.Fatalf("escalation did not precede execution: %v", order)
	}
	if c.onExploratoryWorker {
		t.Fatal("connection still on the exploratory tier after a pinning statement")
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if countMsgs(msgs, 'E') != 0 {
		t.Fatalf("unexpected ErrorResponse: %s", describeMsgs(msgs))
	}
}

// TestSimpleQuerySmallOKStatementDoesNotPin is the negative control: a plain
// read must stay on the exploratory worker (a false pin costs a bigger pod on
// every connection).
func TestSimpleQuerySmallOKStatementDoesNotPin(t *testing.T) {
	small := &tierExecutor{name: "small", queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{1}}, nil
	}}
	c, _ := newBufferedConn(small)
	c.onExploratoryWorker = true
	switched := 0
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		switched++
		return &tierExecutor{name: "big"}, 9, "worker-9", nil
	}

	if err := c.handleQuery([]byte("SELECT 1\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if switched != 0 {
		t.Fatalf("plain SELECT escalated (%d switcher calls)", switched)
	}
	if !c.onExploratoryWorker {
		t.Fatal("plain SELECT left the exploratory tier")
	}
	if len(small.queryCalls) != 1 {
		t.Fatalf("exploratory worker query calls = %v, want one", small.queryCalls)
	}
}

// TestWritableCTEPinsBeforeExecute asserts the writable-CTE rewrite branch is
// covered too: classifyStatementTier detects the embedded DML
// (containsMutatingNode), and the rewrite executes it on the worker, so the
// escalation must happen before the rewrite runs.
func TestWritableCTEPinsBeforeExecute(t *testing.T) {
	var order []string
	small := &tierExecutor{name: "small", order: &order}
	big := &tierExecutor{name: "big", order: &order, queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{1}}, nil
	}}
	c, _ := newBufferedConn(small)
	c.onExploratoryWorker = true
	var reasons []string
	c.workerSwitcher = func(_ context.Context, reason string) (QueryExecutor, int, string, error) {
		reasons = append(reasons, reason)
		order = append(order, "switch")
		return big, 9, "worker-9", nil
	}

	const q = "WITH ins AS (INSERT INTO t VALUES (1) RETURNING n) SELECT n FROM ins\x00"
	if err := c.handleQuery([]byte(q)); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if len(reasons) != 1 || reasons[0] != escalateReasonState {
		t.Fatalf("switcher reasons = %v, want [%q]", reasons, escalateReasonState)
	}
	if len(small.execCalls) != 0 || len(small.queryCalls) != 0 {
		t.Fatalf("exploratory worker ran the writable CTE: exec=%v query=%v", small.execCalls, small.queryCalls)
	}
	if len(order) == 0 || order[0] != "switch" {
		t.Fatalf("escalation did not precede execution: %v", order)
	}
}

// TestCopyPinsBeforeExecute asserts COPY escalates before it is handled. COPY
// is routed to handleCopy ABOVE the transpile-time classification hook, so
// without a dedicated hook it would run on the exploratory worker unpinned.
// Every COPY pins in BOTH directions — COPY FROM writes, and COPY TO STDOUT
// streams a whole relation through the worker — matching classifyStatementTier.
func TestCopyPinsBeforeExecute(t *testing.T) {
	// A file COPY (neither TO STDOUT nor FROM STDIN) takes handleCopy's
	// pass-through-to-DuckDB branch: one Exec, and crucially no reads from the
	// client socket, so the assertion is deterministic in unit scope.
	const fileCopy = "COPY t TO 's3://bucket/o.parquet' (FORMAT parquet)"

	var order []string
	small := &tierExecutor{name: "small", order: &order}
	big := &tierExecutor{name: "big", order: &order}
	c, out := newBufferedConn(small)
	c.onExploratoryWorker = true
	var reasons []string
	c.workerSwitcher = func(_ context.Context, reason string) (QueryExecutor, int, string, error) {
		reasons = append(reasons, reason)
		order = append(order, "switch")
		return big, 9, "worker-9", nil
	}

	if err := c.handleQuery([]byte(fileCopy + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if len(reasons) != 1 || reasons[0] != escalateReasonState {
		t.Fatalf("switcher reasons = %v, want [%q]", reasons, escalateReasonState)
	}
	if len(small.execCalls) != 0 || len(small.queryCalls) != 0 {
		t.Fatalf("exploratory worker saw the COPY: exec=%v query=%v", small.execCalls, small.queryCalls)
	}
	if len(big.execCalls) != 1 {
		t.Fatalf("standard worker exec calls = %v, want exactly one", big.execCalls)
	}
	want := []string{"switch", "big:exec"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("execution order = %v, want %v", order, want)
	}
	if c.onExploratoryWorker {
		t.Fatal("connection still on the exploratory tier after a COPY")
	}
	if msgs := parseWireMsgs(t, out.Bytes()); countMsgs(msgs, 'E') != 0 {
		t.Fatalf("unexpected ErrorResponse: %s", describeMsgs(msgs))
	}
}

// TestCopyEscalationFailureIsConnectionFatal asserts a COPY whose escalation
// fails terminates the connection with a FATAL, and — for COPY FROM STDIN —
// never enters handleCopy (which would otherwise start reading CopyData from a
// client whose session is already gone).
func TestCopyEscalationFailureIsConnectionFatal(t *testing.T) {
	for _, q := range []string{
		"COPY t FROM STDIN",
		"COPY t TO STDOUT",
		"COPY t TO 's3://bucket/o.parquet' (FORMAT parquet)",
	} {
		t.Run(q, func(t *testing.T) {
			small := &tierExecutor{name: "small"}
			c, out := newBufferedConn(small)
			c.onExploratoryWorker = true
			c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
				return nil, 0, "", errors.New("worker capacity exhausted for organization")
			}

			err := c.handleQuery([]byte(q + "\x00"))
			if err == nil {
				t.Fatal("handleQuery returned nil; a failed escalation must terminate the connection")
			}
			msgs := parseWireMsgs(t, out.Bytes())
			if !fatalErrorResponseWith(msgs, "53300") {
				t.Fatalf("want FATAL 53300, got %s", describeMsgs(msgs))
			}
			if countMsgs(msgs, 'Z') != 0 {
				t.Fatalf("ReadyForQuery sent after a connection-fatal failure: %s", describeMsgs(msgs))
			}
			if len(small.execCalls) != 0 || len(small.queryCalls) != 0 {
				t.Fatalf("COPY ran on the dead exploratory session: exec=%v query=%v", small.execCalls, small.queryCalls)
			}
		})
	}
}

// TestSimpleQueryEscalationFailureIsConnectionFatal pins the contract recorded
// on escalateWorker: by the time the switcher fails the connection's previous
// session is already destroyed, so a failed escalation cannot be an
// error-and-continue — the client gets a FATAL ErrorResponse and the message
// loop terminates the connection.
func TestSimpleQueryEscalationFailureIsConnectionFatal(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode string
	}{
		{"disabled", errors.New("this account is disabled; contact your administrator"), "28000"},
		{"org cap", errors.New("worker capacity exhausted for organization"), "53300"},
		{"no idle", errors.New("worker capacity exhausted; retry in about 45s"), "53300"},
		{"other", errors.New("dial worker: connection refused"), "53400"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			small := &tierExecutor{name: "small"}
			c, out := newBufferedConn(small)
			c.onExploratoryWorker = true
			c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
				return nil, 0, "", tc.err
			}

			err := c.handleQuery([]byte("CREATE TEMP TABLE t (a INT)\x00"))
			if err == nil {
				t.Fatal("handleQuery returned nil; a failed escalation must terminate the connection")
			}
			msgs := parseWireMsgs(t, out.Bytes())
			if !fatalErrorResponseWith(msgs, tc.wantCode) {
				t.Fatalf("want FATAL ErrorResponse with %s, got %s", tc.wantCode, describeMsgs(msgs))
			}
			if countMsgs(msgs, 'Z') != 0 {
				t.Fatalf("ReadyForQuery sent after a connection-fatal escalation failure: %s", describeMsgs(msgs))
			}
			if len(small.execCalls) != 0 {
				t.Fatalf("statement ran on the dead exploratory session: %v", small.execCalls)
			}
		})
	}
}

// TestBatchedStatementPinsBeforeExecute covers the multi-statement simple
// query path: batches re-classify per statement, so the pinning statement in a
// batch escalates before it runs while the leading read stays on the small
// worker.
func TestBatchedStatementPinsBeforeExecute(t *testing.T) {
	var order []string
	small := &tierExecutor{name: "small", order: &order, queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{1}}, nil
	}}
	big := &tierExecutor{name: "big", order: &order}
	c, _ := newBufferedConn(small)
	c.onExploratoryWorker = true
	var reasons []string
	c.workerSwitcher = func(_ context.Context, reason string) (QueryExecutor, int, string, error) {
		reasons = append(reasons, reason)
		order = append(order, "switch")
		return big, 9, "worker-9", nil
	}

	if err := c.handleQuery([]byte("SELECT 1; CREATE TEMP TABLE t (a INT)\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if len(reasons) != 1 || reasons[0] != escalateReasonState {
		t.Fatalf("switcher reasons = %v, want [%q]", reasons, escalateReasonState)
	}
	want := []string{"small:query", "switch", "big:exec"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("execution order = %v, want %v", order, want)
	}
}

// --- 2. prepare-phase OOM re-executes on the escalated worker ----------

// TestSelectReexecutesOnPrepareOOM asserts a read that blows the small
// worker's memory_limit before any bytes reached the client is transparently
// re-executed on a normal-size worker: the client sees only success.
func TestSelectReexecutesOnPrepareOOM(t *testing.T) {
	small := &tierExecutor{name: "small", queryFn: func(int, string) (RowSet, error) {
		return nil, errors.New(tierOOMError)
	}}
	big := &tierExecutor{name: "big", queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{7}}, nil
	}}
	c, out := newBufferedConn(small)
	c.onExploratoryWorker = true
	var reasons []string
	c.workerSwitcher = func(_ context.Context, reason string) (QueryExecutor, int, string, error) {
		reasons = append(reasons, reason)
		return big, 9, "worker-9", nil
	}

	if err := c.handleQuery([]byte("SELECT n FROM big\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if len(reasons) != 1 || reasons[0] != escalateReasonOOM {
		t.Fatalf("switcher reasons = %v, want [%q]", reasons, escalateReasonOOM)
	}
	if len(big.queryCalls) != 1 {
		t.Fatalf("standard worker query calls = %v, want one (the re-execute)", big.queryCalls)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if countMsgs(msgs, 'E') != 0 {
		t.Fatalf("client saw an error for a successfully re-executed query: %s", describeMsgs(msgs))
	}
	if got := countMsgs(msgs, 'T'); got != 1 {
		t.Fatalf("RowDescription count = %d, want 1: %s", got, describeMsgs(msgs))
	}
	if got := countMsgs(msgs, 'D'); got != 1 {
		t.Fatalf("DataRow count = %d, want 1: %s", got, describeMsgs(msgs))
	}
	if countMsgs(msgs, 'C') != 1 {
		t.Fatalf("want exactly one CommandComplete: %s", describeMsgs(msgs))
	}
}

// TestSelectPrepareOOMEscalationFailureIsConnectionFatal asserts that when the
// escalation itself fails after an OOM, the ORIGINAL query error reaches the
// client as FATAL and the connection terminates (the previous session is gone).
func TestSelectPrepareOOMEscalationFailureIsConnectionFatal(t *testing.T) {
	small := &tierExecutor{name: "small", queryFn: func(int, string) (RowSet, error) {
		return nil, errors.New(tierOOMError)
	}}
	c, out := newBufferedConn(small)
	c.onExploratoryWorker = true
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return nil, 0, "", errors.New("worker capacity exhausted for organization")
	}

	err := c.handleQuery([]byte("SELECT n FROM big\x00"))
	if err == nil {
		t.Fatal("handleQuery returned nil; a failed OOM escalation must terminate the connection")
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !fatalErrorResponseWith(msgs, "53300", "Out of Memory Error") {
		t.Fatalf("want FATAL 53300 carrying the original OOM error, got %s", describeMsgs(msgs))
	}
	if countMsgs(msgs, 'Z') != 0 {
		t.Fatalf("ReadyForQuery sent after a connection-fatal escalation failure: %s", describeMsgs(msgs))
	}
}

// TestSelectPrepareOOMOffTierSurfaces is the negative control for the
// prepare-phase retry: a connection that is not on the exploratory tier must
// surface the OOM unchanged (no switcher, ordinary ERROR + ReadyForQuery).
func TestSelectPrepareOOMOffTierSurfaces(t *testing.T) {
	exec := &tierExecutor{name: "std", queryFn: func(int, string) (RowSet, error) {
		return nil, errors.New(tierOOMError)
	}}
	c, out := newBufferedConn(exec)
	switched := 0
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		switched++
		return nil, 0, "", nil
	}

	if err := c.handleQuery([]byte("SELECT n FROM big\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if switched != 0 {
		t.Fatalf("off-tier connection escalated (%d switcher calls)", switched)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "Out of Memory Error") || fatalErrorResponseWith(msgs) {
		t.Fatalf("want a non-fatal ERROR carrying the OOM: %s", describeMsgs(msgs))
	}
	if countMsgs(msgs, 'Z') != 1 {
		t.Fatalf("want one ReadyForQuery: %s", describeMsgs(msgs))
	}
}

// TestSelectPrepareOOMInTransactionSurfaces asserts the retry never fires
// inside an open transaction: re-executing on a different worker would silently
// drop the transaction's accumulated state.
func TestSelectPrepareOOMInTransactionSurfaces(t *testing.T) {
	small := &tierExecutor{name: "small", queryFn: func(int, string) (RowSet, error) {
		return nil, errors.New(tierOOMError)
	}}
	c, out := newBufferedConn(small)
	c.onExploratoryWorker = true
	c.txStatus = txStatusTransaction
	switched := 0
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		switched++
		return &tierExecutor{name: "big"}, 9, "worker-9", nil
	}

	if err := c.handleQuery([]byte("SELECT n FROM big\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if switched != 0 {
		t.Fatalf("in-transaction OOM escalated (%d switcher calls)", switched)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "Out of Memory Error") {
		t.Fatalf("want the OOM surfaced: %s", describeMsgs(msgs))
	}
}

// --- 3. mid-stream OOM ------------------------------------------------

// TestSelectMidStreamOOMAfterRowsSurfaces asserts an OOM that lands after rows
// were already streamed to the client is surfaced, never re-executed: the
// client has already consumed part of a result set that a retry could not
// reproduce.
func TestSelectMidStreamOOMAfterRowsSurfaces(t *testing.T) {
	small := &tierExecutor{name: "small", queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{1, 2}, err: errors.New(tierOOMError)}, nil
	}}
	c, out := newBufferedConn(small)
	c.onExploratoryWorker = true
	switched := 0
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		switched++
		return &tierExecutor{name: "big"}, 9, "worker-9", nil
	}

	if err := c.handleQuery([]byte("SELECT n FROM big\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if switched != 0 {
		t.Fatalf("mid-stream OOM after %d rows escalated (%d switcher calls)", 2, switched)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if got := countMsgs(msgs, 'D'); got != 2 {
		t.Fatalf("DataRow count = %d, want 2: %s", got, describeMsgs(msgs))
	}
	if !errorResponseWith(msgs, "42000", "Out of Memory Error") {
		t.Fatalf("want ERROR 42000 carrying the OOM: %s", describeMsgs(msgs))
	}
	if countMsgs(msgs, 'Z') != 1 {
		t.Fatalf("want one ReadyForQuery: %s", describeMsgs(msgs))
	}
}

// TestSelectZeroRowMidStreamOOMReexecutes asserts the mid-stream case where
// NOTHING was streamed yet (the OOM arrived from the first Next()/Err()):
// re-execute on the escalated worker and do NOT resend RowDescription, since
// the first attempt already sent one for the identical schema.
func TestSelectZeroRowMidStreamOOMReexecutes(t *testing.T) {
	first := &tierRowSet{err: errors.New(tierOOMError)}
	small := &tierExecutor{name: "small", queryFn: func(int, string) (RowSet, error) {
		return first, nil
	}}
	big := &tierExecutor{name: "big", queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{5, 6}}, nil
	}}
	c, out := newBufferedConn(small)
	c.onExploratoryWorker = true
	var reasons []string
	c.workerSwitcher = func(_ context.Context, reason string) (QueryExecutor, int, string, error) {
		reasons = append(reasons, reason)
		return big, 9, "worker-9", nil
	}

	if err := c.handleQuery([]byte("SELECT n FROM big\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if len(reasons) != 1 || reasons[0] != escalateReasonOOM {
		t.Fatalf("switcher reasons = %v, want [%q]", reasons, escalateReasonOOM)
	}
	if first.closed == 0 {
		t.Fatal("the abandoned first RowSet was never closed")
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if countMsgs(msgs, 'E') != 0 {
		t.Fatalf("client saw an error for a successfully re-executed query: %s", describeMsgs(msgs))
	}
	if got := countMsgs(msgs, 'T'); got != 1 {
		t.Fatalf("RowDescription count = %d, want exactly 1 (never resent on retry): %s", got, describeMsgs(msgs))
	}
	if got := countMsgs(msgs, 'D'); got != 2 {
		t.Fatalf("DataRow count = %d, want 2 from the retry: %s", got, describeMsgs(msgs))
	}
	if !commandCompleteWith(msgs, "SELECT 2") {
		t.Fatalf("want CommandComplete 'SELECT 2': %s", describeMsgs(msgs))
	}
}

// commandCompleteWith reports whether a CommandComplete carries the given tag.
func commandCompleteWith(msgs []wireMsg, tag string) bool {
	for _, m := range msgs {
		if m.typ == 'C' && bytes.Contains(m.body, []byte(tag)) {
			return true
		}
	}
	return false
}

// --- SQLSTATE mapping helper ------------------------------------------

func TestEscalationErrorSQLState(t *testing.T) {
	cases := map[string]string{
		"this account is disabled; contact your administrator":                "28000",
		"switch worker: this account is disabled; contact your administrator": "28000",
		"worker capacity exhausted for organization":                          "53300",
		"worker capacity exhausted; retry in about 45s":                       "53300",
		"worker capacity unavailable while control plane is shutting down":    "53300",
		"dial worker: connection refused":                                     "53400",
		"":                                                                    "53400",
	}
	for msg, want := range cases {
		if got := escalationErrorSQLState(errors.New(msg)); got != want {
			t.Fatalf("escalationErrorSQLState(%q) = %s, want %s", msg, got, want)
		}
	}
	if got := escalationErrorSQLState(nil); got != "53400" {
		t.Fatalf("escalationErrorSQLState(nil) = %s, want 53400", got)
	}
}
