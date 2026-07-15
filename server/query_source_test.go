package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// TestQuerySourceGetter covers the session accessor the compute meter reads:
// unset defaults to "standard", a SET value round-trips, and reset to empty
// falls back to the default. Callers of setQuerySource pass already-validated
// canonical values (the transpiler / startup-option validation reject anything
// outside {standard, endpoints} with 22023 before the setter is reached).
func TestQuerySourceGetter(t *testing.T) {
	c := &clientConn{}

	// Unset → default "standard" (never an error for a missing value).
	if got := c.QuerySource(); got != "standard" {
		t.Fatalf("unset QuerySource() = %q, want %q", got, "standard")
	}
	if got := c.QuerySource(); got != defaultQuerySource {
		t.Fatalf("unset QuerySource() = %q, want defaultQuerySource %q", got, defaultQuerySource)
	}

	// SET → value round-trips.
	c.setQuerySource("endpoints")
	if got := c.QuerySource(); got != "endpoints" {
		t.Fatalf("after set, QuerySource() = %q, want %q", got, "endpoints")
	}

	// RESET / empty → back to default.
	c.setQuerySource("")
	if got := c.QuerySource(); got != "standard" {
		t.Fatalf("after reset, QuerySource() = %q, want %q", got, "standard")
	}
}

// TestQuerySourceStartupOption asserts a `-c duckgres.query_source=...` startup
// option is parsed into the map ParseStartupOptions returns and applied to the
// session by applyStartupQuerySource, with the same validation as SET:
// canonical values (case-insensitively, normalized to lowercase) are applied,
// anything else is a 22023 error the startup handler turns into a FATAL
// connection rejection — matching resolveWorkerProfile's handling of invalid
// duckgres.worker_* startup options.
func TestQuerySourceStartupOption(t *testing.T) {
	opts := ParseStartupOptions("-c duckgres.query_source=endpoints")
	if got := opts[querySourceGUCName]; got != "endpoints" {
		t.Fatalf("ParseStartupOptions[%q] = %q, want %q", querySourceGUCName, got, "endpoints")
	}

	valid := map[string]string{
		"endpoints":  "endpoints",
		"standard":   "standard",
		"ENDPOINTS":  "endpoints", // case-insensitive, normalized to lowercase
		" standard ": "standard",  // surrounding whitespace ignored
		"":           "standard",  // empty = default
	}
	for raw, want := range valid {
		c := &clientConn{}
		if err := c.applyStartupQuerySource(raw); err != nil {
			t.Fatalf("applyStartupQuerySource(%q) error: %v", raw, err)
		}
		if got := c.QuerySource(); got != want {
			t.Fatalf("after startup option %q, QuerySource() = %q, want %q", raw, got, want)
		}
	}

	for _, raw := range []string{"garbage", "endpointss", strings.Repeat("x", 10*1024), "café\n\ttab"} {
		c := &clientConn{}
		err := c.applyStartupQuerySource(raw)
		if err == nil {
			t.Fatalf("applyStartupQuerySource(%.40q) = nil error, want 22023 rejection", raw)
		}
		var coded interface{ SQLState() string }
		if !errors.As(err, &coded) || coded.SQLState() != "22023" {
			t.Fatalf("applyStartupQuerySource(%.40q) error = %v, want SQLSTATE 22023", raw, err)
		}
		if !strings.Contains(err.Error(), `"standard"`) || !strings.Contains(err.Error(), `"endpoints"`) {
			t.Fatalf("rejection must name the valid values, got %q", err.Error())
		}
		if got := c.QuerySource(); got != "standard" {
			t.Fatalf("after rejected startup option, QuerySource() = %q, want default %q", got, "standard")
		}
	}
}

// wireMsg is a decoded backend protocol message: a type byte and its body.
type wireMsg struct {
	typ  byte
	body []byte
}

// parseWireMsgs decodes the sequence of length-prefixed backend messages a
// clientConn wrote to its buffer (type byte + 4-byte length incl. length).
func parseWireMsgs(t *testing.T, buf []byte) []wireMsg {
	t.Helper()
	var msgs []wireMsg
	for i := 0; i+5 <= len(buf); {
		typ := buf[i]
		length := int(buf[i+1])<<24 | int(buf[i+2])<<16 | int(buf[i+3])<<8 | int(buf[i+4])
		if length < 4 || i+1+length > len(buf) {
			t.Fatalf("malformed wire message at offset %d (length %d, remaining %d)", i, length, len(buf)-i)
		}
		msgs = append(msgs, wireMsg{typ: typ, body: buf[i+5 : i+1+length]})
		i += 1 + length
	}
	return msgs
}

// newBufferedConn builds a clientConn that writes to an in-memory buffer, for
// asserting the exact backend messages a simple query produces. The returned
// *bytes.Buffer holds the flushed output after handleQuery.
func newBufferedConn(exec QueryExecutor) (*clientConn, *bytes.Buffer) {
	clientSide, serverSide := net.Pipe()
	// The pipe halves are only needed so bufio has something to wrap; the test
	// reads the flushed buffer, not the pipe. Closed by the caller via t.Cleanup.
	_ = serverSide
	var out bytes.Buffer
	c := &clientConn{
		server:   &Server{activeQueries: make(map[BackendKey]context.CancelFunc)},
		conn:     clientSide,
		reader:   bufio.NewReader(clientSide),
		writer:   bufio.NewWriter(&out),
		ctx:      context.Background(),
		cancel:   func() {},
		txStatus: txStatusIdle,
		executor: exec,
	}
	return c, &out
}

// selectOneExecutor answers any QueryContext with a single-row RowSet, so a
// SELECT in a batch alongside the query_source GUC actually executes.
type selectOneExecutor struct {
	noopProfiling
	queryCalls int
}

func (e *selectOneExecutor) QueryContext(context.Context, string, ...any) (RowSet, error) {
	e.queryCalls++
	return &staticCountRowSet{count: 1}, nil
}
func (e *selectOneExecutor) ExecContext(context.Context, string, ...any) (ExecResult, error) {
	return &fakeExecResult{}, nil
}
func (e *selectOneExecutor) Query(string, ...any) (RowSet, error) {
	return nil, errors.New("not implemented")
}
func (e *selectOneExecutor) Exec(string, ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}
func (e *selectOneExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}
func (e *selectOneExecutor) PingContext(context.Context) error { return nil }
func (e *selectOneExecutor) Close() error                      { return nil }

// TestQuerySourceMixedBatchSetThenShow is the regression for the e2e bug: a
// single simple query batching `SET duckgres.query_source='endpoints'; SHOW
// duckgres.query_source` must run BOTH statements in order — the SET
// (session-side) then the SHOW returning the just-set value. Previously the
// whole-batch transpile surfaced QuerySourceSet on the first statement and
// returned early, swallowing the trailing SHOW so it never emitted `endpoints`.
func TestQuerySourceMixedBatchSetThenShow(t *testing.T) {
	c, out := newBufferedConn(&selectOneExecutor{})

	if err := c.handleQuery([]byte("SET duckgres.query_source = 'endpoints'; SHOW duckgres.query_source\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	msgs := parseWireMsgs(t, out.Bytes())

	// Expect: CommandComplete(SET), RowDescription, DataRow(endpoints),
	// CommandComplete(SHOW), ReadyForQuery — in that order.
	var sawSetComplete, sawShowRow, sawShowComplete bool
	for _, m := range msgs {
		switch m.typ {
		case 'C':
			if bytes.Contains(m.body, []byte("SET")) {
				sawSetComplete = true
			}
			if bytes.Contains(m.body, []byte("SHOW")) {
				if !sawShowRow {
					t.Fatalf("SHOW CommandComplete arrived before the SHOW DataRow — the SHOW was dropped")
				}
				sawShowComplete = true
			}
		case 'D':
			if bytes.Contains(m.body, []byte("endpoints")) {
				if !sawSetComplete {
					t.Fatalf("SHOW DataRow arrived before the SET completed — statements ran out of order")
				}
				sawShowRow = true
			}
		}
	}

	if !sawSetComplete {
		t.Fatalf("no CommandComplete(SET) in output; msgs=%s", describeMsgs(msgs))
	}
	if !sawShowRow {
		t.Fatalf("SHOW duckgres.query_source did not return 'endpoints' (trailing statement swallowed); msgs=%s", describeMsgs(msgs))
	}
	if !sawShowComplete {
		t.Fatalf("no CommandComplete(SHOW) in output; msgs=%s", describeMsgs(msgs))
	}
	if got := c.QuerySource(); got != "endpoints" {
		t.Fatalf("session QuerySource() = %q, want %q", got, "endpoints")
	}
}

// TestQuerySourceMixedBatchWithNormalStatement asserts a batch mixing the
// custom GUC with a normal statement (`SET duckgres.query_source='endpoints';
// SELECT 1`) still executes the SELECT — the GUC statement must not swallow the
// rest of the batch.
func TestQuerySourceMixedBatchWithNormalStatement(t *testing.T) {
	exec := &selectOneExecutor{}
	c, out := newBufferedConn(exec)

	if err := c.handleQuery([]byte("SET duckgres.query_source = 'endpoints'; SELECT 1\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	if exec.queryCalls != 1 {
		t.Fatalf("SELECT did not run: QueryContext called %d times, want 1", exec.queryCalls)
	}
	if got := c.QuerySource(); got != "endpoints" {
		t.Fatalf("session QuerySource() = %q, want %q", got, "endpoints")
	}

	msgs := parseWireMsgs(t, out.Bytes())
	var sawSetComplete, sawSelectComplete bool
	for _, m := range msgs {
		if m.typ == 'C' {
			if bytes.Contains(m.body, []byte("SET")) {
				sawSetComplete = true
			}
			if bytes.Contains(m.body, []byte("SELECT")) {
				sawSelectComplete = true
			}
		}
	}
	if !sawSetComplete {
		t.Fatalf("no CommandComplete(SET); msgs=%s", describeMsgs(msgs))
	}
	if !sawSelectComplete {
		t.Fatalf("SELECT 1 after the GUC did not complete; msgs=%s", describeMsgs(msgs))
	}
}

// errorResponseWith reports whether msgs contain an ErrorResponse ('E') whose
// body carries all the given substrings (e.g. the SQLSTATE and message parts).
func errorResponseWith(msgs []wireMsg, wants ...string) bool {
	for _, m := range msgs {
		if m.typ != 'E' {
			continue
		}
		ok := true
		for _, w := range wants {
			if !bytes.Contains(m.body, []byte(w)) {
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

// TestQuerySourceInvalidSimpleSetRejected asserts the simple-protocol SET path
// rejects a value outside {standard, endpoints} with SQLSTATE 22023, names the
// valid values, leaves the session value untouched, and a subsequent SHOW
// still reports the default. The value is the billing bucket key
// (duckgres_org_compute_usage.query_source), so junk here would flow into the
// billing table and its exports with unbounded cardinality.
func TestQuerySourceInvalidSimpleSetRejected(t *testing.T) {
	c, out := newBufferedConn(&selectOneExecutor{})

	if err := c.handleQuery([]byte("SET duckgres.query_source = 'garbage'\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "22023", `"standard"`, `"endpoints"`) {
		t.Fatalf("no 22023 ErrorResponse naming the valid values; msgs=%s", describeMsgs(msgs))
	}
	if errorResponseWith(msgs, "garbage") {
		t.Fatalf("rejection echoes the offending value; msgs=%s", describeMsgs(msgs))
	}
	for _, m := range msgs {
		if m.typ == 'C' && bytes.Contains(m.body, []byte("SET")) {
			t.Fatalf("rejected SET still produced CommandComplete(SET); msgs=%s", describeMsgs(msgs))
		}
	}
	if got := c.QuerySource(); got != "standard" {
		t.Fatalf("after rejected SET, QuerySource() = %q, want default %q", got, "standard")
	}

	// SHOW after the failed SET still reports the default.
	out.Reset()
	if err := c.handleQuery([]byte("SHOW duckgres.query_source\x00")); err != nil {
		t.Fatalf("handleQuery(SHOW): %v", err)
	}
	msgs = parseWireMsgs(t, out.Bytes())
	found := false
	for _, m := range msgs {
		if m.typ == 'D' && bytes.Contains(m.body, []byte("standard")) {
			found = true
		}
	}
	if !found {
		t.Fatalf("SHOW after rejected SET did not report 'standard'; msgs=%s", describeMsgs(msgs))
	}
}

// TestQuerySourceInvalidSetKeepsPreviousValue asserts a rejected SET (here via
// the split multi-statement batch path) does not clobber a previously-set
// valid value.
func TestQuerySourceInvalidSetKeepsPreviousValue(t *testing.T) {
	c, out := newBufferedConn(&selectOneExecutor{})

	if err := c.handleQuery([]byte("SET duckgres.query_source = 'endpoints'\x00")); err != nil {
		t.Fatalf("handleQuery(valid SET): %v", err)
	}
	if got := c.QuerySource(); got != "endpoints" {
		t.Fatalf("QuerySource() = %q, want %q", got, "endpoints")
	}

	// Multi-statement batch: the connection layer splits it and re-transpiles
	// each statement, so the invalid SET is rejected on the per-statement path.
	out.Reset()
	if err := c.handleQuery([]byte("SELECT 1; SET duckgres.query_source = 'garbage'\x00")); err != nil {
		t.Fatalf("handleQuery(batch): %v", err)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "22023", `"standard"`, `"endpoints"`) {
		t.Fatalf("batched invalid SET not rejected with 22023; msgs=%s", describeMsgs(msgs))
	}
	if got := c.QuerySource(); got != "endpoints" {
		t.Fatalf("rejected SET clobbered the session value: QuerySource() = %q, want %q", got, "endpoints")
	}
}

// TestQuerySourceCaseAndEmptySet asserts value normalization on the SET path:
// a mixed-case value is accepted and stored lowercase, and an empty string
// resets to the default (SHOW reports "standard" again).
func TestQuerySourceCaseAndEmptySet(t *testing.T) {
	c, _ := newBufferedConn(&selectOneExecutor{})

	if err := c.handleQuery([]byte("SET duckgres.query_source = 'ENDPOINTS'\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if got := c.QuerySource(); got != "endpoints" {
		t.Fatalf("mixed-case SET: QuerySource() = %q, want normalized %q", got, "endpoints")
	}

	if err := c.handleQuery([]byte("SET duckgres.query_source = ''\x00")); err != nil {
		t.Fatalf("handleQuery(empty): %v", err)
	}
	if got := c.QuerySource(); got != "standard" {
		t.Fatalf("empty SET must reset to default: QuerySource() = %q, want %q", got, "standard")
	}
}

// TestQuerySourceExtendedParseInvalidRejected asserts the extended-protocol
// path rejects an invalid value at Parse time (before anything is stored on
// the session or the statement map).
func TestQuerySourceExtendedParseInvalidRejected(t *testing.T) {
	c, out := newBufferedConn(&selectOneExecutor{})
	c.stmts = make(map[string]*preparedStmt)

	// Parse message body: statement name, query (both NUL-terminated), then
	// int16 parameter-type count.
	body := append([]byte("s1\x00SET duckgres.query_source = 'garbage'\x00"), 0, 0)
	c.handleParse(body)
	_ = c.flushWriter()

	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "22023", `"standard"`, `"endpoints"`) {
		t.Fatalf("extended Parse of invalid SET not rejected with 22023; msgs=%s", describeMsgs(msgs))
	}
	if _, ok := c.stmts["s1"]; ok {
		t.Fatalf("rejected Parse still stored the prepared statement")
	}
	if got := c.QuerySource(); got != "standard" {
		t.Fatalf("after rejected Parse, QuerySource() = %q, want default %q", got, "standard")
	}

	// A valid value still parses and applies at Execute time (portal run).
	out.Reset()
	body = append([]byte("s2\x00SET duckgres.query_source = 'endpoints'\x00"), 0, 0)
	c.handleParse(body)
	_ = c.flushWriter()
	st, ok := c.stmts["s2"]
	if !ok {
		t.Fatalf("valid SET did not parse: msgs=%s", describeMsgs(parseWireMsgs(t, out.Bytes())))
	}
	if st.querySourceSet == nil || *st.querySourceSet != "endpoints" {
		t.Fatalf("prepared stmt querySourceSet = %v, want endpoints", st.querySourceSet)
	}
}

// TestConnectionBillingClampsQuerySource is the defense-in-depth assert: if a
// non-canonical value ever reaches teardown despite SET/startup validation
// (i.e. a future bypass), ConnectionBilling degrades it to "standard" instead
// of writing arbitrary client input into the billing bucket key.
func TestConnectionBillingClampsQuerySource(t *testing.T) {
	cases := map[string]string{
		"":          "standard",
		"standard":  "standard",
		"endpoints": "endpoints",
		"garbage":   "standard", // bypassed junk degrades to the default
		"ENDPOINTS": "standard", // non-canonical case counts as junk here too
	}
	for stored, want := range cases {
		cc := &clientConn{querySource: stored, backendStart: time.Now(), workerMillicores: 1000, workerMiB: 1024}
		_, qs, _, _, _ := ConnectionBilling(cc)
		if qs != want {
			t.Fatalf("ConnectionBilling with stored %q: querySource = %q, want %q", stored, qs, want)
		}
	}
}

func describeMsgs(msgs []wireMsg) string {
	var b bytes.Buffer
	for _, m := range msgs {
		b.WriteByte(m.typ)
		b.WriteByte('(')
		for _, x := range m.body {
			if x >= 0x20 && x < 0x7f {
				b.WriteByte(x)
			} else {
				b.WriteByte('.')
			}
		}
		b.WriteString(") ")
	}
	return b.String()
}
