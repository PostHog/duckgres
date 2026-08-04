package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// errorFields is a decoded ErrorResponse: the wire body is a sequence of
// (field-type byte, NUL-terminated value) pairs ending in a zero byte, so the
// severity ('S'), SQLSTATE ('C') and message ('M') are read as FIELDS rather
// than substring-matched against the whole body — otherwise a "FATAL" or a
// SQLSTATE-looking token appearing in the error TEXT would satisfy the
// assertion.
type errorFields struct {
	severity string
	code     string
	message  string
}

func decodeErrorResponses(msgs []wireMsg) []errorFields {
	var out []errorFields
	for _, m := range msgs {
		if m.typ != 'E' {
			continue
		}
		var f errorFields
		for i := 0; i < len(m.body) && m.body[i] != 0; {
			typ := m.body[i]
			end := bytes.IndexByte(m.body[i+1:], 0)
			if end < 0 {
				break
			}
			value := string(m.body[i+1 : i+1+end])
			switch typ {
			case 'S':
				f.severity = value
			case 'C':
				f.code = value
			case 'M':
				f.message = value
			}
			i += 1 + end + 1
		}
		out = append(out, f)
	}
	return out
}

// hasErrorResponse reports whether msgs carry an ErrorResponse with exactly
// this severity and SQLSTATE, whose message contains every wantsInMessage
// substring.
func hasErrorResponse(msgs []wireMsg, severity, code string, wantsInMessage ...string) bool {
	for _, f := range decodeErrorResponses(msgs) {
		if f.severity != severity || f.code != code {
			continue
		}
		ok := true
		for _, w := range wantsInMessage {
			if !strings.Contains(f.message, w) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// hasSeverity reports whether ANY ErrorResponse carries this severity. Used
// for negative assertions ("no FATAL was emitted"), where naming a SQLSTATE
// would weaken the check.
func hasSeverity(msgs []wireMsg, severity string) bool {
	for _, f := range decodeErrorResponses(msgs) {
		if f.severity == severity {
			return true
		}
	}
	return false
}

// describeErrorResponses renders the decoded ErrorResponses for failure output.
func describeErrorResponses(msgs []wireMsg) string {
	var b strings.Builder
	for _, f := range decodeErrorResponses(msgs) {
		fmt.Fprintf(&b, "[%s %s %q]", f.severity, f.code, f.message)
	}
	if b.Len() == 0 {
		return "<no ErrorResponse>"
	}
	return b.String()
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

// TestDeclareCursorPinsBeforeExecute asserts DECLARE escalates at DECLARE
// time. A cursor is session state whose worker-side RowSet outlives the
// statement that opened it, so if DECLARE did not pin, a later pinning
// statement would destroy the session out from under the open cursor and
// strand it. FETCH/CLOSE need no hook of their own — this test proves it by
// running a pinning statement BETWEEN two FETCHes and showing the switcher
// fired exactly once, at DECLARE.
func TestDeclareCursorPinsBeforeExecute(t *testing.T) {
	var order []string
	small := &tierExecutor{name: "small", order: &order}
	big := &tierExecutor{name: "big", order: &order, queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{1, 2}}, nil
	}}
	c, _ := newBufferedConn(small)
	c.cursors = make(map[string]*cursorState)
	c.onExploratoryWorker = true
	var reasons []string
	c.workerSwitcher = func(_ context.Context, reason string) (QueryExecutor, int, string, error) {
		reasons = append(reasons, reason)
		order = append(order, "switch")
		return big, 9, "worker-9", nil
	}

	if err := c.handleQuery([]byte("DECLARE cur CURSOR FOR SELECT n FROM t\x00")); err != nil {
		t.Fatalf("handleQuery(DECLARE): %v", err)
	}
	if len(reasons) != 1 || reasons[0] != escalateReasonState {
		t.Fatalf("DECLARE did not pin: switcher reasons = %v, want [%q]", reasons, escalateReasonState)
	}
	if c.onExploratoryWorker {
		t.Fatal("connection still on the exploratory tier after DECLARE")
	}

	// The cursor's RowSet opens lazily on the first FETCH — on the pinned
	// worker, because DECLARE already escalated.
	if err := c.handleQuery([]byte("FETCH 1 FROM cur\x00")); err != nil {
		t.Fatalf("handleQuery(FETCH): %v", err)
	}
	if len(small.queryCalls) != 0 {
		t.Fatalf("cursor opened on the exploratory worker: %v", small.queryCalls)
	}
	if len(big.queryCalls) != 1 {
		t.Fatalf("standard worker query calls = %v, want one (the cursor open)", big.queryCalls)
	}

	// A pinning statement mid-cursor must not escalate again (the pin is
	// sticky), so the session backing the open cursor survives.
	if err := c.handleQuery([]byte("CREATE TEMP TABLE x (a INT)\x00")); err != nil {
		t.Fatalf("handleQuery(CREATE TEMP TABLE): %v", err)
	}
	if len(reasons) != 1 {
		t.Fatalf("switcher fired again after DECLARE (%v) — the cursor's session would have been destroyed", reasons)
	}
	if err := c.handleQuery([]byte("FETCH 1 FROM cur\x00")); err != nil {
		t.Fatalf("handleQuery(second FETCH): %v", err)
	}
	if len(big.queryCalls) != 1 {
		t.Fatalf("cursor was re-opened (%v); it must still be the original RowSet", big.queryCalls)
	}
}

// TestBatchedDeclareCursorPins is the batched sibling of
// TestDeclareCursorPinsBeforeExecute. executeSingleStatement's cursor switch
// returns from its DECLARE case before reaching the batch classification hook,
// so a single Q message carrying `DECLARE …; FETCH …` would otherwise open the
// cursor's RowSet on the exploratory worker — exactly the stranding the
// DECLARE pin exists to prevent.
func TestBatchedDeclareCursorPins(t *testing.T) {
	var order []string
	small := &tierExecutor{name: "small", order: &order}
	big := &tierExecutor{name: "big", order: &order, queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{1, 2}}, nil
	}}
	c, out := newBufferedConn(small)
	c.cursors = make(map[string]*cursorState)
	c.onExploratoryWorker = true
	var reasons []string
	c.workerSwitcher = func(_ context.Context, reason string) (QueryExecutor, int, string, error) {
		reasons = append(reasons, reason)
		order = append(order, "switch")
		return big, 9, "worker-9", nil
	}

	const batch = "DECLARE cur CURSOR FOR SELECT n FROM t; FETCH 1 FROM cur\x00"
	if err := c.handleQuery([]byte(batch)); err != nil {
		t.Fatalf("handleQuery(batch): %v", err)
	}
	if len(reasons) != 1 || reasons[0] != escalateReasonState {
		t.Fatalf("batched DECLARE did not pin: switcher reasons = %v, want [%q]", reasons, escalateReasonState)
	}
	if len(small.queryCalls) != 0 {
		t.Fatalf("cursor opened on the exploratory worker: %v", small.queryCalls)
	}
	if len(big.queryCalls) != 1 {
		t.Fatalf("standard worker query calls = %v, want one (the cursor open)", big.queryCalls)
	}
	// The escalation must precede the cursor open, not merely coexist with it.
	want := []string{"switch", "big:query"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("execution order = %v, want %v", order, want)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if countMsgs(msgs, 'E') != 0 {
		t.Fatalf("unexpected ErrorResponse: %s", describeErrorResponses(msgs))
	}
}

// TestBatchedDeclareCursorEscalationFailureIsConnectionFatal asserts the batch
// path propagates a failed DECLARE escalation as connection-fatal:
// executeSingleStatement returns (false, err), handleMultiStatementQuery
// returns it WITHOUT writing ReadyForQuery, and handleQuery hands it to the
// message loop.
func TestBatchedDeclareCursorEscalationFailureIsConnectionFatal(t *testing.T) {
	small := &tierExecutor{name: "small"}
	c, out := newBufferedConn(small)
	c.cursors = make(map[string]*cursorState)
	c.onExploratoryWorker = true
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return nil, 0, "", errors.New("worker capacity exhausted for organization")
	}

	err := c.handleQuery([]byte("DECLARE cur CURSOR FOR SELECT n FROM t; FETCH 1 FROM cur\x00"))
	if err == nil {
		t.Fatal("handleQuery returned nil; a failed escalation must terminate the connection")
	}
	if !errors.Is(err, errConnectionFatal) {
		t.Fatalf("batch error %v does not carry errConnectionFatal, so the message loop would resume", err)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !hasErrorResponse(msgs, "FATAL", "53300") {
		t.Fatalf("want FATAL 53300, got %s", describeErrorResponses(msgs))
	}
	if countMsgs(msgs, 'Z') != 0 {
		t.Fatalf("ReadyForQuery sent after a connection-fatal failure: %s", describeMsgs(msgs))
	}
	if len(c.cursors) != 0 {
		t.Fatalf("cursor registered despite the failed escalation: %v", c.cursors)
	}
	if len(small.queryCalls) != 0 {
		t.Fatalf("batch continued on the dead exploratory session: %v", small.queryCalls)
	}
}

// TestDeclareCursorEscalationFailureIsConnectionFatal asserts a DECLARE whose
// escalation fails terminates the connection instead of registering a cursor
// against a session that no longer exists.
func TestDeclareCursorEscalationFailureIsConnectionFatal(t *testing.T) {
	small := &tierExecutor{name: "small"}
	c, out := newBufferedConn(small)
	c.cursors = make(map[string]*cursorState)
	c.onExploratoryWorker = true
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return nil, 0, "", errors.New("worker capacity exhausted for organization")
	}

	err := c.handleQuery([]byte("DECLARE cur CURSOR FOR SELECT n FROM t\x00"))
	if err == nil {
		t.Fatal("handleQuery returned nil; a failed escalation must terminate the connection")
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !hasErrorResponse(msgs, "FATAL", "53300") {
		t.Fatalf("want FATAL 53300, got %s", describeErrorResponses(msgs))
	}
	if countMsgs(msgs, 'Z') != 0 {
		t.Fatalf("ReadyForQuery sent after a connection-fatal failure: %s", describeMsgs(msgs))
	}
	if len(c.cursors) != 0 {
		t.Fatalf("cursor registered despite the failed escalation: %v", c.cursors)
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
			if !hasErrorResponse(msgs, "FATAL", "53300") {
				t.Fatalf("want FATAL 53300, got %s", describeErrorResponses(msgs))
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
			if !hasErrorResponse(msgs, "FATAL", tc.wantCode) {
				t.Fatalf("want FATAL ErrorResponse with %s, got %s", tc.wantCode, describeErrorResponses(msgs))
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
	if !hasErrorResponse(msgs, "FATAL", "53300", "Out of Memory Error") {
		t.Fatalf("want FATAL 53300 carrying the original OOM error, got %s", describeErrorResponses(msgs))
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
	if !hasErrorResponse(msgs, "ERROR", "XX000", "Out of Memory Error") {
		t.Fatalf("want a non-fatal ERROR carrying the OOM: %s", describeErrorResponses(msgs))
	}
	if hasSeverity(msgs, "FATAL") {
		t.Fatalf("off-tier OOM must not terminate the connection: %s", describeErrorResponses(msgs))
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
	if !hasErrorResponse(msgs, "ERROR", "XX000", "Out of Memory Error") {
		t.Fatalf("want the OOM surfaced: %s", describeErrorResponses(msgs))
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
	if !hasErrorResponse(msgs, "ERROR", "42000", "Out of Memory Error") {
		t.Fatalf("want ERROR 42000 carrying the OOM: %s", describeErrorResponses(msgs))
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
