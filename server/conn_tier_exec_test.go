package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/posthog/duckgres/server/auth"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

// --- 4. extended query protocol ---------------------------------------

// newExtendedTierConn builds an exploratory-tier connection with the maps the
// extended-protocol handlers need, plus a switcher that hands out `big` and
// records the reasons it was called with.
func newExtendedTierConn(small, big *tierExecutor) (c *clientConn, out *bytes.Buffer, reasons *[]string) {
	c, out = newBufferedConn(small)
	c.stmts = make(map[string]*preparedStmt)
	c.portals = make(map[string]*portal)
	c.cursors = make(map[string]*cursorState)
	c.onExploratoryWorker = true
	var seen []string
	reasons = &seen
	c.workerSwitcher = func(_ context.Context, reason string) (QueryExecutor, int, string, error) {
		seen = append(seen, reason)
		if big != nil && big.order != nil {
			*big.order = append(*big.order, "switch")
		}
		return big, 9, "worker-9", nil
	}
	return c, out, reasons
}

// The three extended-query messages the tier tests drive, each through
// runExtendedQueryMessage — the same dispatcher the message loop uses, so a
// connection-fatal error propagates exactly as it would in production.
func extParse(c *clientConn, name, query string) error {
	return c.runExtendedQueryMessage(c.handleParse, append([]byte(name+"\x00"+query+"\x00"), 0, 0))
}

func extBind(c *clientConn, portalName, stmtName string) error {
	return c.runExtendedQueryMessage(c.handleBind, append([]byte(portalName+"\x00"+stmtName+"\x00"), 0, 0, 0, 0, 0, 0))
}

func extExecute(c *clientConn, portalName string, maxRows int32) error {
	body := append([]byte(portalName), 0)
	body = append(body, byte(maxRows>>24), byte(maxRows>>16), byte(maxRows>>8), byte(maxRows))
	return c.runExtendedQueryMessage(c.handleExecute, body)
}

func extDescribeStmt(c *clientConn, name string) error {
	return c.runExtendedQueryMessage(c.handleDescribe, append([]byte("S"+name), 0))
}

// extRun drives Parse → Bind → Execute for one statement, failing the test on
// any error that is not connection-fatal, and returning the fatal one.
func extRun(t *testing.T, c *clientConn, name, query string) error {
	t.Helper()
	for _, step := range []struct {
		what string
		run  func() error
	}{
		{"Parse", func() error { return extParse(c, name, query) }},
		{"Bind", func() error { return extBind(c, name, name) }},
		{"Execute", func() error { return extExecute(c, name, 0) }},
	} {
		if err := step.run(); err != nil {
			return fmt.Errorf("%s: %w", step.what, err)
		}
	}
	return nil
}

// TestExtendedExecutePinsBeforeExecute is the extended-protocol sibling of
// TestSimpleQueryPinsBeforeExecute: a statement classified as pinning at Parse
// escalates at Execute BEFORE the executor sees it, so the exploratory worker
// stays stateless.
func TestExtendedExecutePinsBeforeExecute(t *testing.T) {
	var order []string
	small := &tierExecutor{name: "small", order: &order}
	big := &tierExecutor{name: "big", order: &order}
	c, out, reasons := newExtendedTierConn(small, big)

	if err := extRun(t, c, "s1", "CREATE TEMP TABLE t (a INT)"); err != nil {
		t.Fatalf("extended run: %v", err)
	}

	if !c.stmts["s1"].pinsWorker {
		t.Fatal("Parse did not classify CREATE TEMP TABLE as pinning")
	}
	if len(*reasons) != 1 || (*reasons)[0] != escalateReasonState {
		t.Fatalf("switcher reasons = %v, want [%q]", *reasons, escalateReasonState)
	}
	if len(small.execCalls) != 0 || len(small.queryCalls) != 0 {
		t.Fatalf("exploratory worker saw the pinning statement: exec=%v query=%v", small.execCalls, small.queryCalls)
	}
	if len(big.execCalls) != 1 {
		t.Fatalf("standard worker exec calls = %v, want exactly one", big.execCalls)
	}
	want := []string{"switch", "big:exec"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("execution order = %v, want %v", order, want)
	}
	if c.onExploratoryWorker {
		t.Fatal("connection still on the exploratory tier after a pinning statement")
	}
	if msgs := parseWireMsgs(t, out.Bytes()); countMsgs(msgs, 'E') != 0 {
		t.Fatalf("unexpected ErrorResponse: %s", describeErrorResponses(msgs))
	}
}

// TestExtendedExecuteSmallOKDoesNotPin is the negative control: a plain read
// executed through the extended protocol stays on the exploratory worker.
func TestExtendedExecuteSmallOKDoesNotPin(t *testing.T) {
	small := &tierExecutor{name: "small", queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{1}}, nil
	}}
	c, _, reasons := newExtendedTierConn(small, &tierExecutor{name: "big"})

	if err := extRun(t, c, "s1", "SELECT n FROM t"); err != nil {
		t.Fatalf("extended run: %v", err)
	}
	if c.stmts["s1"].pinsWorker {
		t.Fatal("plain SELECT classified as pinning at Parse")
	}
	if len(*reasons) != 0 {
		t.Fatalf("plain SELECT escalated: %v", *reasons)
	}
	if !c.onExploratoryWorker {
		t.Fatal("plain SELECT left the exploratory tier")
	}
	if len(small.queryCalls) != 1 {
		t.Fatalf("exploratory worker query calls = %v, want one", small.queryCalls)
	}
}

// TestExtendedExecuteEscalationFailureIsConnectionFatal asserts the extended
// path enforces the same contract as the simple one: a failed escalation is
// connection-fatal (the previous session is already destroyed), so the client
// gets a FATAL with the mapped SQLSTATE and the dispatcher hands the message
// loop an errConnectionFatal instead of resuming at the next Sync.
func TestExtendedExecuteEscalationFailureIsConnectionFatal(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode string
	}{
		{"disabled", errors.New("this account is disabled; contact your administrator"), "28000"},
		{"org cap", errors.New("worker capacity exhausted for organization"), "53300"},
		{"other", errors.New("dial worker: connection refused"), "53400"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			small := &tierExecutor{name: "small"}
			c, out, _ := newExtendedTierConn(small, nil)
			c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
				return nil, 0, "", tc.err
			}

			err := extRun(t, c, "s1", "CREATE TEMP TABLE t (a INT)")
			if err == nil {
				t.Fatal("extended Execute returned nil; a failed escalation must terminate the connection")
			}
			if !errors.Is(err, errConnectionFatal) {
				t.Fatalf("error %v does not carry errConnectionFatal, so the message loop would resume", err)
			}
			msgs := parseWireMsgs(t, out.Bytes())
			if !hasErrorResponse(msgs, "FATAL", tc.wantCode) {
				t.Fatalf("want FATAL %s, got %s", tc.wantCode, describeErrorResponses(msgs))
			}
			if countMsgs(msgs, 'Z') != 0 {
				t.Fatalf("ReadyForQuery sent after a connection-fatal failure: %s", describeMsgs(msgs))
			}
			if len(small.execCalls) != 0 || len(small.queryCalls) != 0 {
				t.Fatalf("statement ran on the dead exploratory session: exec=%v query=%v", small.execCalls, small.queryCalls)
			}
		})
	}
}

// TestExtendedDeclareCursorPins is the extended sibling of
// TestDeclareCursorPinsBeforeExecute. DECLARE is intercepted at Parse and
// dispatched from handleExecute's cursor switch, above the general pin hook, so
// without a hook of its own the cursor's worker-side RowSet would open on the
// exploratory worker and a later pinning statement would strand it.
func TestExtendedDeclareCursorPins(t *testing.T) {
	var order []string
	small := &tierExecutor{name: "small", order: &order}
	big := &tierExecutor{name: "big", order: &order, queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{1, 2}}, nil
	}}
	c, out, reasons := newExtendedTierConn(small, big)

	if err := extRun(t, c, "d1", "DECLARE cur CURSOR FOR SELECT n FROM t"); err != nil {
		t.Fatalf("extended DECLARE: %v", err)
	}
	if len(*reasons) != 1 || (*reasons)[0] != escalateReasonState {
		t.Fatalf("extended DECLARE did not pin: switcher reasons = %v, want [%q]", *reasons, escalateReasonState)
	}
	if c.onExploratoryWorker {
		t.Fatal("connection still on the exploratory tier after DECLARE")
	}

	// The cursor's RowSet opens lazily on the first FETCH — on the pinned
	// worker, because DECLARE already escalated.
	if err := extRun(t, c, "f1", "FETCH 1 FROM cur"); err != nil {
		t.Fatalf("extended FETCH: %v", err)
	}
	if len(small.queryCalls) != 0 {
		t.Fatalf("cursor opened on the exploratory worker: %v", small.queryCalls)
	}
	if len(big.queryCalls) != 1 {
		t.Fatalf("standard worker query calls = %v, want one (the cursor open)", big.queryCalls)
	}
	want := []string{"switch", "big:query"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("execution order = %v, want %v", order, want)
	}
	if msgs := parseWireMsgs(t, out.Bytes()); countMsgs(msgs, 'E') != 0 {
		t.Fatalf("unexpected ErrorResponse: %s", describeErrorResponses(msgs))
	}
}

// TestExtendedDeclareCursorEscalationFailureIsConnectionFatal asserts a
// DECLARE whose escalation fails terminates the connection instead of
// registering a cursor against a session that no longer exists.
func TestExtendedDeclareCursorEscalationFailureIsConnectionFatal(t *testing.T) {
	small := &tierExecutor{name: "small"}
	c, out, _ := newExtendedTierConn(small, nil)
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return nil, 0, "", errors.New("worker capacity exhausted for organization")
	}

	err := extRun(t, c, "d1", "DECLARE cur CURSOR FOR SELECT n FROM t")
	if err == nil {
		t.Fatal("extended DECLARE returned nil; a failed escalation must terminate the connection")
	}
	if !errors.Is(err, errConnectionFatal) {
		t.Fatalf("error %v does not carry errConnectionFatal", err)
	}
	if len(c.cursors) != 0 {
		t.Fatalf("cursor registered despite the failed escalation: %v", c.cursors)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !hasErrorResponse(msgs, "FATAL", "53300") {
		t.Fatalf("want FATAL 53300, got %s", describeErrorResponses(msgs))
	}
	if countMsgs(msgs, 'Z') != 0 {
		t.Fatalf("ReadyForQuery sent after a connection-fatal failure: %s", describeMsgs(msgs))
	}
}

// TestExtendedDescribeProbePins covers the Describe hazard unique to the
// extended protocol: Describe answers a result-returning statement by EXECUTING
// it (a LIMIT-0 probe). `SELECT … INTO` returns results by prefix and is not
// DML-RETURNING, so it reaches the probe — and the probe creates a table. The
// probe must therefore escalate first, exactly like Execute.
func TestExtendedDescribeProbePins(t *testing.T) {
	var order []string
	small := &tierExecutor{name: "small", order: &order}
	big := &tierExecutor{name: "big", order: &order, queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{1}}, nil
	}}
	c, _, reasons := newExtendedTierConn(small, big)

	if err := extParse(c, "s1", "SELECT n INTO t2 FROM t"); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !c.stmts["s1"].pinsWorker {
		t.Fatal("SELECT INTO not classified as pinning at Parse")
	}
	if err := extDescribeStmt(c, "s1"); err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if len(*reasons) != 1 || (*reasons)[0] != escalateReasonState {
		t.Fatalf("Describe probe did not pin: switcher reasons = %v, want [%q]", *reasons, escalateReasonState)
	}
	if len(small.queryCalls) != 0 || len(small.execCalls) != 0 {
		t.Fatalf("Describe probed the exploratory worker: query=%v exec=%v", small.queryCalls, small.execCalls)
	}
	if len(big.queryCalls) != 1 {
		t.Fatalf("standard worker probe calls = %v, want one (the probe really executes)", big.queryCalls)
	}
	want := []string{"switch", "big:query"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("escalation did not precede the probe: %v, want %v", order, want)
	}
}

// TestExtendedDescribePortalProbePins is the Describe-PORTAL sibling of
// TestExtendedDescribeProbePins — the branch psycopg and JDBC drive after Bind
// runs the same executing probe, so it carries the same hook.
func TestExtendedDescribePortalProbePins(t *testing.T) {
	var order []string
	small := &tierExecutor{name: "small", order: &order}
	big := &tierExecutor{name: "big", order: &order, queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{1}}, nil
	}}
	c, _, reasons := newExtendedTierConn(small, big)

	if err := extParse(c, "s1", "SELECT n INTO t2 FROM t"); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if err := extBind(c, "p1", "s1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := c.runExtendedQueryMessage(c.handleDescribe, append([]byte("Pp1"), 0)); err != nil {
		t.Fatalf("Describe portal: %v", err)
	}
	if len(*reasons) != 1 || (*reasons)[0] != escalateReasonState {
		t.Fatalf("portal Describe probe did not pin: switcher reasons = %v", *reasons)
	}
	if len(small.queryCalls) != 0 {
		t.Fatalf("portal Describe probed the exploratory worker: %v", small.queryCalls)
	}
	want := []string{"switch", "big:query"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("escalation did not precede the probe: %v, want %v", order, want)
	}
}

// TestExtendedDescribeEscalationFailureIsConnectionFatal asserts a Describe
// whose escalation fails terminates the connection instead of falling through
// to a probe on a session that no longer exists. Describe runs outside any
// query-metrics scope, so this also covers the escalation's error reporting on
// that path.
func TestExtendedDescribeEscalationFailureIsConnectionFatal(t *testing.T) {
	small := &tierExecutor{name: "small"}
	c, out, _ := newExtendedTierConn(small, nil)
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return nil, 0, "", errors.New("worker capacity exhausted for organization")
	}

	if err := extParse(c, "s1", "SELECT n INTO t2 FROM t"); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	err := extDescribeStmt(c, "s1")
	if err == nil {
		t.Fatal("Describe returned nil; a failed escalation must terminate the connection")
	}
	if !errors.Is(err, errConnectionFatal) {
		t.Fatalf("error %v does not carry errConnectionFatal", err)
	}
	if len(small.queryCalls) != 0 {
		t.Fatalf("Describe probed the dead exploratory session: %v", small.queryCalls)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !hasErrorResponse(msgs, "FATAL", "53300") {
		t.Fatalf("want FATAL 53300, got %s", describeErrorResponses(msgs))
	}
	if countMsgs(msgs, 'Z') != 0 {
		t.Fatalf("ReadyForQuery sent after a connection-fatal failure: %s", describeMsgs(msgs))
	}
}

// TestExtendedDescribeProbeSmallOKDoesNotPin is the negative control for the
// Describe hook: probing a plain read must stay on the exploratory worker.
func TestExtendedDescribeProbeSmallOKDoesNotPin(t *testing.T) {
	small := &tierExecutor{name: "small", queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{}, nil
	}}
	c, _, reasons := newExtendedTierConn(small, &tierExecutor{name: "big"})

	if err := extParse(c, "s1", "SELECT n FROM t"); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if err := extDescribeStmt(c, "s1"); err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if len(*reasons) != 0 {
		t.Fatalf("Describe of a plain read escalated: %v", *reasons)
	}
	if len(small.queryCalls) != 1 {
		t.Fatalf("exploratory worker probe calls = %v, want one", small.queryCalls)
	}
}

// TestExtendedExecuteReexecutesOnOOM asserts the prepare-phase retry on the
// extended result path: a read that blew the small worker's memory_limit before
// any bytes reached the client is re-executed on the escalated worker, and the
// client sees only success.
func TestExtendedExecuteReexecutesOnOOM(t *testing.T) {
	small := &tierExecutor{name: "small", queryFn: func(int, string) (RowSet, error) {
		return nil, errors.New(tierOOMError)
	}}
	big := &tierExecutor{name: "big", queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{7}}, nil
	}}
	c, out, reasons := newExtendedTierConn(small, big)

	if err := extRun(t, c, "s1", "SELECT n FROM big"); err != nil {
		t.Fatalf("extended run: %v", err)
	}
	if len(*reasons) != 1 || (*reasons)[0] != escalateReasonOOM {
		t.Fatalf("switcher reasons = %v, want [%q]", *reasons, escalateReasonOOM)
	}
	if len(big.queryCalls) != 1 {
		t.Fatalf("standard worker query calls = %v, want one (the re-execute)", big.queryCalls)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if countMsgs(msgs, 'E') != 0 {
		t.Fatalf("client saw an error for a successfully re-executed query: %s", describeErrorResponses(msgs))
	}
	if got := countMsgs(msgs, 'T'); got != 1 {
		t.Fatalf("RowDescription count = %d, want 1: %s", got, describeMsgs(msgs))
	}
	if got := countMsgs(msgs, 'D'); got != 1 {
		t.Fatalf("DataRow count = %d, want 1: %s", got, describeMsgs(msgs))
	}
	if !commandCompleteWith(msgs, "SELECT 1") {
		t.Fatalf("want CommandComplete 'SELECT 1': %s", describeMsgs(msgs))
	}
}

// TestExtendedExecuteOOMEscalationFailureIsConnectionFatal asserts that when
// the escalation itself fails after an OOM, the ORIGINAL query error reaches
// the client as FATAL and the connection terminates.
func TestExtendedExecuteOOMEscalationFailureIsConnectionFatal(t *testing.T) {
	small := &tierExecutor{name: "small", queryFn: func(int, string) (RowSet, error) {
		return nil, errors.New(tierOOMError)
	}}
	c, out, _ := newExtendedTierConn(small, nil)
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return nil, 0, "", errors.New("worker capacity exhausted for organization")
	}

	err := extRun(t, c, "s1", "SELECT n FROM big")
	if err == nil {
		t.Fatal("extended Execute returned nil; a failed OOM escalation must terminate the connection")
	}
	if !errors.Is(err, errConnectionFatal) {
		t.Fatalf("error %v does not carry errConnectionFatal", err)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !hasErrorResponse(msgs, "FATAL", "53300", "Out of Memory Error") {
		t.Fatalf("want FATAL 53300 carrying the original OOM error, got %s", describeErrorResponses(msgs))
	}
}

// TestExtendedExecuteZeroRowMidStreamOOMReexecutes asserts the mid-stream case
// where NOTHING was streamed yet: re-execute on the escalated worker without
// resending RowDescription, since the first attempt already sent one for the
// identical schema.
func TestExtendedExecuteZeroRowMidStreamOOMReexecutes(t *testing.T) {
	first := &tierRowSet{err: errors.New(tierOOMError)}
	small := &tierExecutor{name: "small", queryFn: func(int, string) (RowSet, error) {
		return first, nil
	}}
	big := &tierExecutor{name: "big", queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{5, 6}}, nil
	}}
	c, out, reasons := newExtendedTierConn(small, big)

	if err := extRun(t, c, "s1", "SELECT n FROM big"); err != nil {
		t.Fatalf("extended run: %v", err)
	}
	if len(*reasons) != 1 || (*reasons)[0] != escalateReasonOOM {
		t.Fatalf("switcher reasons = %v, want [%q]", *reasons, escalateReasonOOM)
	}
	if first.closed == 0 {
		t.Fatal("the abandoned first RowSet was never closed")
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if countMsgs(msgs, 'E') != 0 {
		t.Fatalf("client saw an error for a successfully re-executed query: %s", describeErrorResponses(msgs))
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

// TestExtendedExecuteMidStreamOOMAfterRowsSurfaces is the negative control: an
// OOM that lands after rows already reached the client is surfaced, never
// re-executed — a retry cannot un-send them.
func TestExtendedExecuteMidStreamOOMAfterRowsSurfaces(t *testing.T) {
	small := &tierExecutor{name: "small", queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{1, 2}, err: errors.New(tierOOMError)}, nil
	}}
	c, out, reasons := newExtendedTierConn(small, &tierExecutor{name: "big"})

	if err := extRun(t, c, "s1", "SELECT n FROM big"); err != nil {
		t.Fatalf("extended run: %v", err)
	}
	if len(*reasons) != 0 {
		t.Fatalf("mid-stream OOM after rows escalated: %v", *reasons)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if got := countMsgs(msgs, 'D'); got != 2 {
		t.Fatalf("DataRow count = %d, want 2: %s", got, describeMsgs(msgs))
	}
	if !hasErrorResponse(msgs, "ERROR", "42000", "Out of Memory Error") {
		t.Fatalf("want ERROR 42000 carrying the OOM: %s", describeErrorResponses(msgs))
	}
}

// TestExtendedExecuteMaxRowsUnchanged pins the non-tier behavior of the
// Execute row loop through the shared streaming helper: maxRows still caps the
// DataRows sent (portal suspension is not implemented — the surplus row is
// fetched and dropped, as before).
func TestExtendedExecuteMaxRowsUnchanged(t *testing.T) {
	exec := &tierExecutor{name: "std", queryFn: func(int, string) (RowSet, error) {
		return &tierRowSet{rows: []int64{1, 2, 3}}, nil
	}}
	c, out := newBufferedConn(exec)
	c.stmts = make(map[string]*preparedStmt)
	c.portals = make(map[string]*portal)

	if err := extParse(c, "s1", "SELECT n FROM t"); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if err := extBind(c, "s1", "s1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := extExecute(c, "s1", 2); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if got := countMsgs(msgs, 'D'); got != 2 {
		t.Fatalf("DataRow count = %d, want 2 (maxRows): %s", got, describeMsgs(msgs))
	}
	if !commandCompleteWith(msgs, "SELECT 2") {
		t.Fatalf("want CommandComplete 'SELECT 2': %s", describeMsgs(msgs))
	}
}

// --- 5. secret DDL pins before it is intercepted -----------------------

// secretSentinel is a credential-shaped literal that must never appear in a
// log line or an ErrorResponse.
const secretSentinel = "SECRET_VALUE_XYZ"

// secretDDLStatements are the shapes that must pin: the managed persistent
// path (intercepted and EXECUTED above the general pin hook) and the plain
// session-scoped path (whose secret is worker state that escalation drops).
func secretDDLStatements() []string {
	return []string{
		"CREATE PERSISTENT SECRET my_s3 (TYPE s3, KEY_ID 'k', SECRET '" + secretSentinel + "')",
		"CREATE SECRET my_s3 (TYPE s3, KEY_ID 'k', SECRET '" + secretSentinel + "')",
		"DROP PERSISTENT SECRET my_s3",
	}
}

// TestSecretDDLPinsBeforeInterceptionSimple asserts secret DDL escalates BEFORE
// the user-secrets interception executes it. The interception sits above the
// general pin hook (it owns its own execution), so without a hook of its own
// the secret would be created on the exploratory worker — and a plain
// (session-scoped) secret is silently dropped by the very next escalation.
func TestSecretDDLPinsBeforeInterceptionSimple(t *testing.T) {
	for _, stmt := range secretDDLStatements() {
		t.Run(stmt[:12], func(t *testing.T) {
			var order []string
			small := &tierExecutor{name: "small", order: &order}
			big := &tierExecutor{name: "big", order: &order}
			c, _ := newBufferedConn(small)
			c.server.cfg.UserSecrets = &fakeUserSecretMgr{delExist: true}
			c.onExploratoryWorker = true
			var reasons []string
			c.workerSwitcher = func(_ context.Context, reason string) (QueryExecutor, int, string, error) {
				reasons = append(reasons, reason)
				order = append(order, "switch")
				return big, 9, "worker-9", nil
			}

			if err := c.handleQuery(append([]byte(stmt), 0)); err != nil {
				t.Fatalf("handleQuery: %v", err)
			}
			if len(reasons) != 1 || reasons[0] != escalateReasonState {
				t.Fatalf("secret DDL did not pin: switcher reasons = %v", reasons)
			}
			if len(small.execCalls) != 0 || len(small.queryCalls) != 0 {
				t.Fatalf("secret DDL ran on the exploratory worker: exec=%v query=%v", small.execCalls, small.queryCalls)
			}
			if len(order) == 0 || order[0] != "switch" {
				t.Fatalf("escalation did not precede execution: %v", order)
			}
		})
	}
}

// TestSecretDDLPinsBeforeInterceptionExtended is the extended-protocol sibling:
// the interception at Execute likewise runs above the general pin hook.
func TestSecretDDLPinsBeforeInterceptionExtended(t *testing.T) {
	for _, stmt := range secretDDLStatements() {
		t.Run(stmt[:12], func(t *testing.T) {
			var order []string
			small := &tierExecutor{name: "small", order: &order}
			big := &tierExecutor{name: "big", order: &order}
			c, _, reasons := newExtendedTierConn(small, big)
			c.server.cfg.UserSecrets = &fakeUserSecretMgr{delExist: true}

			if err := extRun(t, c, "s1", stmt); err != nil {
				t.Fatalf("extended run: %v", err)
			}
			if len(*reasons) != 1 || (*reasons)[0] != escalateReasonState {
				t.Fatalf("secret DDL did not pin: switcher reasons = %v", *reasons)
			}
			if len(small.execCalls) != 0 || len(small.queryCalls) != 0 {
				t.Fatalf("secret DDL ran on the exploratory worker: exec=%v query=%v", small.execCalls, small.queryCalls)
			}
			if len(order) == 0 || order[0] != "switch" {
				t.Fatalf("escalation did not precede execution: %v", order)
			}
		})
	}
}

// TestSecretDDLEscalationFailureIsConnectionFatalAndRedacted asserts the
// failure path: a FATAL with the mapped SQLSTATE, and — because engine and
// server logs echo SQL — the credential appears in NEITHER the client's
// ErrorResponse NOR any log line.
func TestSecretDDLEscalationFailureIsConnectionFatalAndRedacted(t *testing.T) {
	const stmt = "CREATE PERSISTENT SECRET my_s3 (TYPE s3, KEY_ID 'k', SECRET '" + secretSentinel + "')"
	for _, protocol := range []string{"simple", "extended"} {
		t.Run(protocol, func(t *testing.T) {
			logs, restoreLogs := captureSlog(t)
			defer restoreLogs()
			small := &tierExecutor{name: "small"}
			c, out, _ := newExtendedTierConn(small, nil)
			c.server.cfg.UserSecrets = &fakeUserSecretMgr{}
			c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
				return nil, 0, "", errors.New("worker capacity exhausted for organization")
			}

			var err error
			if protocol == "simple" {
				err = c.handleQuery(append([]byte(stmt), 0))
			} else {
				err = extRun(t, c, "s1", stmt)
			}
			if err == nil {
				t.Fatal("returned nil; a failed escalation must terminate the connection")
			}
			if !errors.Is(err, errConnectionFatal) {
				t.Fatalf("error %v does not carry errConnectionFatal", err)
			}
			msgs := parseWireMsgs(t, out.Bytes())
			if !hasErrorResponse(msgs, "FATAL", "53300") {
				t.Fatalf("want FATAL 53300, got %s", describeErrorResponses(msgs))
			}
			if len(small.execCalls) != 0 {
				t.Fatalf("secret DDL ran on the dead exploratory session: %v", small.execCalls)
			}
			if bytes.Contains(out.Bytes(), []byte(secretSentinel)) {
				t.Fatalf("credential leaked to the client: %s", describeErrorResponses(msgs))
			}
			if strings.Contains(logs.String(), secretSentinel) {
				t.Fatalf("credential leaked to the logs: %s", logs.String())
			}
		})
	}
}

// TestSecretDDLOffTierUnchanged is the negative control: without the
// exploratory tier the statement is intercepted and executed exactly as before,
// with no switcher involvement.
func TestSecretDDLOffTierUnchanged(t *testing.T) {
	const stmt = "CREATE PERSISTENT SECRET my_s3 (TYPE s3, KEY_ID 'k', SECRET 's')"
	mgr := &fakeUserSecretMgr{}
	exec := &tierExecutor{name: "std"}
	c, out := newBufferedConn(exec)
	c.server.cfg.UserSecrets = mgr
	c.orgID, c.username = "org1", "alice"
	switched := 0
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		switched++
		return nil, 0, "", nil
	}

	if err := c.handleQuery(append([]byte(stmt), 0)); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if switched != 0 {
		t.Fatalf("off-tier secret DDL escalated (%d switcher calls)", switched)
	}
	if len(exec.execCalls) != 1 {
		t.Fatalf("executor calls = %v, want the statement executed once", exec.execCalls)
	}
	if len(mgr.putCalls) != 1 {
		t.Fatalf("putCalls = %v, want the secret persisted", mgr.putCalls)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if countMsgs(msgs, 'E') != 0 {
		t.Fatalf("unexpected ErrorResponse: %s", describeErrorResponses(msgs))
	}
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

// --- post-escalation s3_cache re-apply: STATEMENT-scoped, not fatal ----

// TestSimpleQueryS3CacheReapplyFailureIsStatementScoped pins the one
// escalateWorker failure that is NOT connection-fatal. The switcher SUCCEEDED
// (there is a healthy session on the standard worker and the pin stands); only
// the post-swap `duckgres.s3_cache` re-apply failed. So the statement fails
// with a normal ERROR naming the re-apply, the connection is resynchronized
// with a ReadyForQuery instead of terminated, and the session flag is reset to
// the transport the worker is ACTUALLY in (proxied — a fresh session always
// starts on the cache proxy) so SHOW cannot lie.
func TestSimpleQueryS3CacheReapplyFailureIsStatementScoped(t *testing.T) {
	small := &s3CacheRecordingExecutor{}
	c, out := newBufferedConn(small)
	if err := c.handleQuery([]byte("SET duckgres.s3_cache = off\x00")); err != nil {
		t.Fatalf("handleQuery(SET off): %v", err)
	}
	out.Reset()

	big := &s3CacheRecordingExecutor{err: errors.New("secret swap failed")}
	c.onExploratoryWorker = true
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return big, 7, "worker-7", nil
	}

	err := c.handleQuery([]byte("CREATE TEMP TABLE t (a INT)\x00"))
	if !errors.Is(err, errStatementAborted) {
		t.Fatalf("handleQuery error = %v, want errStatementAborted", err)
	}
	if errors.Is(err, errConnectionFatal) {
		t.Fatalf("error %v is connection-fatal; the escalation succeeded and the session is healthy", err)
	}
	if c.fatalErr != nil {
		t.Fatalf("fatalErr parked (%v); the message loop would terminate a healthy connection", c.fatalErr)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if hasSeverity(msgs, "FATAL") {
		t.Fatalf("FATAL emitted for a statement-scoped failure: %s", describeErrorResponses(msgs))
	}
	if !hasErrorResponse(msgs, "ERROR", "XX000", "duckgres.s3_cache") {
		t.Fatalf("want ERROR XX000 naming the GUC, got %s", describeErrorResponses(msgs))
	}
	if countMsgs(msgs, 'Z') != 1 {
		t.Fatalf("want exactly one ReadyForQuery (connection stays alive): %s", describeMsgs(msgs))
	}
	if !c.S3CacheEnabled() {
		t.Fatal("session still reports the bypass; SHOW would name a transport the worker isn't using")
	}
	if c.onExploratoryWorker {
		t.Fatal("pin rolled back; the escalation itself succeeded and must stand")
	}
	if len(big.execCalls) != 0 || len(small.execCalls) != 0 {
		t.Fatalf("statement executed anyway: small=%v big=%v", small.execCalls, big.execCalls)
	}
}

// TestExtendedExecuteS3CacheReapplyFailureIsStatementScoped is the extended
// sibling: same severity contract, except ReadyForQuery belongs to Sync, so the
// handler must NOT write one — it reports the ERROR (arming skip-until-Sync)
// and the dispatcher returns nil rather than a connection-fatal error.
func TestExtendedExecuteS3CacheReapplyFailureIsStatementScoped(t *testing.T) {
	small := &tierExecutor{name: "small"}
	c, out := newBufferedConn(small)
	c.stmts = make(map[string]*preparedStmt)
	c.portals = make(map[string]*portal)
	c.cursors = make(map[string]*cursorState)
	if err := c.handleQuery([]byte("SET duckgres.s3_cache = off\x00")); err != nil {
		t.Fatalf("handleQuery(SET off): %v", err)
	}
	out.Reset()

	big := &s3CacheRecordingExecutor{err: errors.New("secret swap failed")}
	c.onExploratoryWorker = true
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return big, 9, "worker-9", nil
	}

	if err := extRun(t, c, "s1", "CREATE TEMP TABLE t (a INT)"); err != nil {
		t.Fatalf("extended run returned %v; the connection must not be terminated", err)
	}
	if c.fatalErr != nil {
		t.Fatalf("fatalErr parked (%v) for a statement-scoped failure", c.fatalErr)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if hasSeverity(msgs, "FATAL") {
		t.Fatalf("FATAL emitted for a statement-scoped failure: %s", describeErrorResponses(msgs))
	}
	if !hasErrorResponse(msgs, "ERROR", "XX000", "duckgres.s3_cache") {
		t.Fatalf("want ERROR XX000 naming the GUC, got %s", describeErrorResponses(msgs))
	}
	if countMsgs(msgs, 'Z') != 0 {
		t.Fatalf("handler wrote ReadyForQuery; Sync owns it on the extended protocol: %s", describeMsgs(msgs))
	}
	if !c.ignoreTillSync {
		t.Fatal("skip-until-Sync not armed after the ErrorResponse")
	}
	if !c.S3CacheEnabled() {
		t.Fatal("session still reports the bypass after a failed re-apply")
	}
	if len(big.execCalls) != 0 || len(small.execCalls) != 0 {
		t.Fatalf("statement executed anyway: small=%v big=%v", small.execCalls, big.execCalls)
	}
}

// TestS3CacheReapplyFailureKeepsConnectionAlive drives the real message loop:
// the statement that failed its re-apply must not close the connection, and the
// NEXT statement must run normally on the escalated worker. The switcher's
// failure text deliberately contains "connection refused" — the loop has to
// classify errStatementAborted BEFORE isConnectionBroken, or a healthy
// connection is dropped because a worker-side error mentioned a refused socket.
func TestS3CacheReapplyFailureKeepsConnectionAlive(t *testing.T) {
	small := &s3CacheRecordingExecutor{}
	c, out := newPipelineConn(t, small)
	big := &s3CacheRecordingExecutor{err: errors.New("swap secret: dial worker: connection refused")}
	c.onExploratoryWorker = true
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return big, 7, "worker-7", nil
	}

	frames := runPipeline(t, c, out,
		queryFrame("SET duckgres.s3_cache = off"),
		queryFrame("CREATE TEMP TABLE t (a INT)"),
		queryFrame("SELECT 1"),
	)
	types := frameTypes(frames)
	if strings.Count(types, "Z") != 3 {
		t.Fatalf("want a ReadyForQuery per statement, got frames %q", types)
	}
	if !strings.Contains(types, "E") {
		t.Fatalf("no ErrorResponse for the failed re-apply: frames %q", types)
	}
	var sawReapplyError bool
	for _, f := range frames {
		if f.msgType == 'E' && bytes.Contains(f.body, []byte("XX000")) && bytes.Contains(f.body, []byte("duckgres.s3_cache")) {
			sawReapplyError = true
		}
	}
	if !sawReapplyError {
		t.Fatalf("want an ERROR XX000 naming duckgres.s3_cache, got frames %q", types)
	}
	if big.queryCalls != 1 {
		t.Fatalf("SELECT after the aborted statement ran %d times on the escalated worker, want 1", big.queryCalls)
	}
}

// TestPostConnectAuthorizationErrorIsNotAnAuthFailure guards the security
// metric: duckgres_auth_failures_total counts the AUTHENTICATION handshake, so
// the tier's post-connect 28000 (a user disabled between connect and the
// escalation's re-check) must not land in it — otherwise disabling an account
// reads as a brute-force attempt. The second half is the control: the same
// SQLSTATE sent during startup still counts.
func TestPostConnectAuthorizationErrorIsNotAnAuthFailure(t *testing.T) {
	small := &tierExecutor{name: "small"}
	c, out := newPipelineConn(t, small)
	c.onExploratoryWorker = true
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return nil, 0, "", errors.New("this account is disabled; contact your administrator")
	}

	before := testutil.ToFloat64(auth.AuthFailuresCounter)
	c.reader = bufio.NewReader(bytes.NewReader(queryFrame("CREATE TEMP TABLE t (a INT)")))
	err := c.messageLoop()
	if !errors.Is(err, errConnectionFatal) {
		t.Fatalf("messageLoop error = %v, want the connection-fatal escalation failure", err)
	}
	_ = c.writer.Flush()
	if !bytes.Contains(out.Bytes(), []byte("28000")) {
		t.Fatal("no 28000 was sent; the test is not exercising the guarded path")
	}
	if got := testutil.ToFloat64(auth.AuthFailuresCounter); got != before {
		t.Fatalf("duckgres_auth_failures_total = %v after a post-connect 28000, want %v", got, before)
	}

	// Control: a class-28 error raised before the message loop (the real
	// authentication phase) still increments.
	startup, _ := newBufferedConn(small)
	startup.sendError("FATAL", "28P01", "password authentication failed")
	if got := testutil.ToFloat64(auth.AuthFailuresCounter); got != before+1 {
		t.Fatalf("duckgres_auth_failures_total = %v after a startup-phase 28P01, want %v", got, before+1)
	}
}
