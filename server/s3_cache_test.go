package server

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

// s3CacheRecordingExecutor is a selectOneExecutor that also implements the
// S3CacheControl capability, recording the worker swap calls the conn issues.
type s3CacheRecordingExecutor struct {
	selectOneExecutor
	calls []bool
	err   error
}

func (e *s3CacheRecordingExecutor) SetS3CacheEnabled(_ context.Context, enabled bool) error {
	e.calls = append(e.calls, enabled)
	return e.err
}

// TestS3CacheSimpleSetAppliesToExecutor asserts the core contract of the SET
// path: `SET duckgres.s3_cache = off` invokes the executor capability with
// enabled=false BEFORE the session state flips, SHOW then reports "off", and
// setting it back to on invokes the capability with enabled=true. Re-setting
// the current value must NOT re-invoke the worker (no redundant secret swaps).
func TestS3CacheSimpleSetAppliesToExecutor(t *testing.T) {
	exec := &s3CacheRecordingExecutor{}
	c, out := newBufferedConn(exec)

	if !c.S3CacheEnabled() {
		t.Fatalf("fresh session S3CacheEnabled() = false, want true (default on)")
	}

	if err := c.handleQuery([]byte("SET duckgres.s3_cache = off\x00")); err != nil {
		t.Fatalf("handleQuery(SET off): %v", err)
	}
	if len(exec.calls) != 1 || exec.calls[0] != false {
		t.Fatalf("executor calls after SET off = %v, want [false]", exec.calls)
	}
	if c.S3CacheEnabled() {
		t.Fatalf("S3CacheEnabled() = true after SET off, want false")
	}

	// SHOW reports the session state.
	out.Reset()
	if err := c.handleQuery([]byte("SHOW duckgres.s3_cache\x00")); err != nil {
		t.Fatalf("handleQuery(SHOW): %v", err)
	}
	sawOff := false
	for _, m := range parseWireMsgs(t, out.Bytes()) {
		if m.typ == 'D' && bytes.Contains(m.body, []byte("off")) {
			sawOff = true
		}
	}
	if !sawOff {
		t.Fatalf("SHOW duckgres.s3_cache did not report 'off'")
	}

	// Redundant SET to the same value: state-only, no worker call.
	if err := c.handleQuery([]byte("SET duckgres.s3_cache = off\x00")); err != nil {
		t.Fatalf("handleQuery(redundant SET off): %v", err)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("redundant SET off re-invoked the worker: calls = %v", exec.calls)
	}

	// Back on (via RESET, which maps to the default).
	if err := c.handleQuery([]byte("RESET duckgres.s3_cache\x00")); err != nil {
		t.Fatalf("handleQuery(RESET): %v", err)
	}
	if len(exec.calls) != 2 || exec.calls[1] != true {
		t.Fatalf("executor calls after RESET = %v, want [false true]", exec.calls)
	}
	if !c.S3CacheEnabled() {
		t.Fatalf("S3CacheEnabled() = false after RESET, want true")
	}
}

// TestS3CacheSetWorkerFailureKeepsState asserts a failed worker swap fails the
// SET (ErrorResponse, no CommandComplete) and leaves the session state on its
// previous value — SHOW must never claim a transport the worker isn't using.
func TestS3CacheSetWorkerFailureKeepsState(t *testing.T) {
	exec := &s3CacheRecordingExecutor{err: errors.New("swap S3 secret transport: boom")}
	c, out := newBufferedConn(exec)

	if err := c.handleQuery([]byte("SET duckgres.s3_cache = off\x00")); err != nil {
		t.Fatalf("handleQuery(SET off): %v", err)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "XX000", s3CacheGUCName) {
		t.Fatalf("failed worker swap did not surface as XX000 ErrorResponse; msgs=%s", describeMsgs(msgs))
	}
	for _, m := range msgs {
		if m.typ == 'C' && bytes.Contains(m.body, []byte("SET")) {
			t.Fatalf("failed SET still produced CommandComplete(SET); msgs=%s", describeMsgs(msgs))
		}
	}
	if !c.S3CacheEnabled() {
		t.Fatalf("failed swap flipped session state: S3CacheEnabled() = false, want true")
	}

	// SHOW after the failure still reports the (unchanged) default.
	out.Reset()
	if err := c.handleQuery([]byte("SHOW duckgres.s3_cache\x00")); err != nil {
		t.Fatalf("handleQuery(SHOW): %v", err)
	}
	sawOn := false
	for _, m := range parseWireMsgs(t, out.Bytes()) {
		if m.typ == 'D' && bytes.Contains(m.body, []byte("on")) {
			sawOn = true
		}
	}
	if !sawOn {
		t.Fatalf("SHOW after failed SET did not report 'on'")
	}
}

// TestS3CacheSetWithoutCapabilityIsStateOnly asserts the standalone/in-process
// case: an executor that does not implement S3CacheControl gets
// session-state-only SET/SHOW (no error) — correct because those deployments
// have no cache proxy to bypass.
func TestS3CacheSetWithoutCapabilityIsStateOnly(t *testing.T) {
	c, out := newBufferedConn(&selectOneExecutor{})

	if err := c.handleQuery([]byte("SET duckgres.s3_cache = off\x00")); err != nil {
		t.Fatalf("handleQuery(SET off): %v", err)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	sawSet := false
	for _, m := range msgs {
		if m.typ == 'C' && bytes.Contains(m.body, []byte("SET")) {
			sawSet = true
		}
	}
	if !sawSet {
		t.Fatalf("SET without capability did not complete; msgs=%s", describeMsgs(msgs))
	}
	if c.S3CacheEnabled() {
		t.Fatalf("S3CacheEnabled() = true after SET off, want false (state-only)")
	}
}

// TestS3CacheInvalidSetRejected asserts the simple-protocol SET path rejects a
// non-boolean value with 22023, names the valid values without echoing the
// client input, and leaves the session state untouched.
func TestS3CacheInvalidSetRejected(t *testing.T) {
	exec := &s3CacheRecordingExecutor{}
	c, out := newBufferedConn(exec)

	if err := c.handleQuery([]byte("SET duckgres.s3_cache = 'garbage'\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "22023", `"on"`, `"off"`) {
		t.Fatalf("no 22023 ErrorResponse naming the valid values; msgs=%s", describeMsgs(msgs))
	}
	if errorResponseWith(msgs, "garbage") {
		t.Fatalf("rejection echoes the offending value; msgs=%s", describeMsgs(msgs))
	}
	if len(exec.calls) != 0 {
		t.Fatalf("rejected SET reached the executor: calls = %v", exec.calls)
	}
	if !c.S3CacheEnabled() {
		t.Fatalf("rejected SET flipped session state")
	}
}

// TestS3CacheMixedBatch asserts the split-batch path: `SET duckgres.s3_cache =
// off; SELECT 1` applies the GUC (worker call included) and still runs the
// trailing SELECT.
func TestS3CacheMixedBatch(t *testing.T) {
	exec := &s3CacheRecordingExecutor{}
	c, out := newBufferedConn(exec)

	if err := c.handleQuery([]byte("SET duckgres.s3_cache = off; SELECT 1\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if len(exec.calls) != 1 || exec.calls[0] != false {
		t.Fatalf("executor calls = %v, want [false]", exec.calls)
	}
	if exec.queryCalls != 1 {
		t.Fatalf("SELECT did not run: QueryContext called %d times, want 1", exec.queryCalls)
	}
	if c.S3CacheEnabled() {
		t.Fatalf("S3CacheEnabled() = true after batched SET off, want false")
	}
	msgs := parseWireMsgs(t, out.Bytes())
	var sawSet, sawSelect bool
	for _, m := range msgs {
		if m.typ == 'C' {
			if bytes.Contains(m.body, []byte("SET")) {
				sawSet = true
			}
			if bytes.Contains(m.body, []byte("SELECT")) {
				sawSelect = true
			}
		}
	}
	if !sawSet || !sawSelect {
		t.Fatalf("batch did not complete both statements (SET=%v SELECT=%v); msgs=%s", sawSet, sawSelect, describeMsgs(msgs))
	}
}

// TestS3CacheMixedBatchWorkerFailureAborts asserts a failed swap inside a
// batch aborts the remaining statements — they may depend on the requested
// cache state.
func TestS3CacheMixedBatchWorkerFailureAborts(t *testing.T) {
	exec := &s3CacheRecordingExecutor{err: errors.New("boom")}
	c, out := newBufferedConn(exec)

	if err := c.handleQuery([]byte("SET duckgres.s3_cache = off; SELECT 1\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if exec.queryCalls != 0 {
		t.Fatalf("SELECT ran after the failed SET: QueryContext called %d times, want 0", exec.queryCalls)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "XX000") {
		t.Fatalf("failed batched SET did not surface XX000; msgs=%s", describeMsgs(msgs))
	}
}

// TestS3CacheStartupOption asserts `-c duckgres.s3_cache=...` parsing and
// application: valid values apply through the executor capability, invalid
// values return the 22023 error the startup handler turns into a FATAL
// rejection (before session state changes).
func TestS3CacheStartupOption(t *testing.T) {
	opts := ParseStartupOptions("-c duckgres.s3_cache=off")
	if got := opts[s3CacheGUCName]; got != "off" {
		t.Fatalf("ParseStartupOptions[%q] = %q, want %q", s3CacheGUCName, got, "off")
	}

	valid := map[string]bool{ // raw -> want S3CacheEnabled
		"off":   false,
		"OFF":   false,
		"false": false,
		"0":     false,
		"on":    true,
		"true":  true,
		" on ":  true,
		"":      true, // empty = default
	}
	for raw, want := range valid {
		exec := &s3CacheRecordingExecutor{}
		c, _ := newBufferedConn(exec)
		if err := c.applyStartupS3Cache(raw); err != nil {
			t.Fatalf("applyStartupS3Cache(%q) error: %v", raw, err)
		}
		if got := c.S3CacheEnabled(); got != want {
			t.Fatalf("after startup option %q, S3CacheEnabled() = %v, want %v", raw, got, want)
		}
		if !want && (len(exec.calls) != 1 || exec.calls[0] != false) {
			t.Fatalf("startup option %q: executor calls = %v, want [false]", raw, exec.calls)
		}
		if want && len(exec.calls) != 0 {
			t.Fatalf("startup option %q: executor calls = %v, want none (already on)", raw, exec.calls)
		}
	}

	for _, raw := range []string{"garbage", "offf", strings.Repeat("x", 10*1024)} {
		exec := &s3CacheRecordingExecutor{}
		c, _ := newBufferedConn(exec)
		err := c.applyStartupS3Cache(raw)
		if err == nil {
			t.Fatalf("applyStartupS3Cache(%.40q) = nil error, want 22023 rejection", raw)
		}
		var coded interface{ SQLState() string }
		if !errors.As(err, &coded) || coded.SQLState() != "22023" {
			t.Fatalf("applyStartupS3Cache(%.40q) error = %v, want SQLSTATE 22023", raw, err)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("rejected startup option reached the executor: calls = %v", exec.calls)
		}
		if !c.S3CacheEnabled() {
			t.Fatalf("rejected startup option flipped session state")
		}
	}

	// ValidateS3CacheOption (the control plane's pre-acquire gate) agrees with
	// the apply path.
	if err := ValidateS3CacheOption("off"); err != nil {
		t.Fatalf("ValidateS3CacheOption(off) error: %v", err)
	}
	if err := ValidateS3CacheOption("junk"); err == nil {
		t.Fatalf("ValidateS3CacheOption(junk) = nil error, want rejection")
	}
}

// TestRedactSecretStatementError asserts RefreshS3Secret's error scrubber
// keeps only the engine error-class prefix: its errors reach worker and CP
// logs and, via the duckgres.s3_cache SET / session-create restore paths,
// client-facing error messages, the query log, and the admin recent-errors
// ring. Exact-value replacement is NOT enough — DuckDB ellipsizes long echoed
// SQL lines, so a truncated echo carries credential FRAGMENTS that no
// full-value match catches (reproduced against a live DuckDB: a 640-char
// secret left a 57-char fragment in the "LINE 1: ...xxx' ..." excerpt).
func TestRedactSecretStatementError(t *testing.T) {
	secret := strings.Repeat("S", 40) + strings.Repeat("T", 600)
	// The ellipsized echo shape DuckDB actually produces: full value absent,
	// contiguous fragment present.
	echo := "Parser Error: syntax error at or near \"BROKEN\"\n" +
		"LINE 1: ..." + secret[len(secret)-57:] + "' BROKEN"
	got := redactSecretStatementError(echo)
	if got != "Parser Error: [details redacted: engine errors may echo secret SQL]" {
		t.Fatalf("unexpected redaction: %q", got)
	}
	for i := 0; i+12 <= len(secret); i += 4 {
		if strings.Contains(got, secret[i:i+12]) {
			t.Fatalf("redacted message still contains a credential fragment: %q", got)
		}
	}

	// Ordinary runtime errors keep their class for triage, nothing more.
	if got := redactSecretStatementError("IO Error: Connection refused (s3.us-east-1.amazonaws.com:443)"); got !=
		"IO Error: [details redacted: engine errors may echo secret SQL]" {
		t.Fatalf("IO error class not preserved: %q", got)
	}

	// Messages that don't follow the class-colon shape — or whose "prefix" is
	// long or quote-bearing enough to carry echoed SQL — drop entirely.
	for _, msg := range []string{
		"no colon here at all",
		"", // empty
		"prefix with 'quoted " + secret[:24] + "' content: detail",
		strings.Repeat("x", 80) + ": detail",
	} {
		got := redactSecretStatementError(msg)
		if got != "[details redacted: engine errors may echo secret SQL]" {
			t.Fatalf("unsafe prefix survived for %.40q: %q", msg, got)
		}
	}
}

// TestS3CacheExtendedParse asserts the extended-protocol path: an invalid
// value is rejected at Parse time; a valid SET parses, applies at Execute
// time through the executor capability, and Describe returns NoData.
func TestS3CacheExtendedParse(t *testing.T) {
	exec := &s3CacheRecordingExecutor{}
	c, out := newBufferedConn(exec)
	c.stmts = make(map[string]*preparedStmt)
	c.portals = make(map[string]*portal)

	// Invalid value: rejected at Parse, nothing stored.
	body := append([]byte("s1\x00SET duckgres.s3_cache = 'garbage'\x00"), 0, 0)
	c.handleParse(body)
	_ = c.flushWriter()
	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "22023", `"on"`, `"off"`) {
		t.Fatalf("extended Parse of invalid SET not rejected with 22023; msgs=%s", describeMsgs(msgs))
	}
	if _, ok := c.stmts["s1"]; ok {
		t.Fatalf("rejected Parse still stored the prepared statement")
	}

	// Valid SET: parses with s3CacheSet populated.
	out.Reset()
	body = append([]byte("s2\x00SET duckgres.s3_cache = 'off'\x00"), 0, 0)
	c.handleParse(body)
	_ = c.flushWriter()
	st, ok := c.stmts["s2"]
	if !ok {
		t.Fatalf("valid SET did not parse: msgs=%s", describeMsgs(parseWireMsgs(t, out.Bytes())))
	}
	if st.s3CacheSet == nil || *st.s3CacheSet != "off" {
		t.Fatalf("prepared stmt s3CacheSet = %v, want off", st.s3CacheSet)
	}

	// Bind + Execute applies through the capability.
	out.Reset()
	// Bind message: portal name, statement name (NUL-terminated), int16 format
	// count (0), int16 param count (0), int16 result-format count (0).
	bindBody := append([]byte("\x00s2\x00"), 0, 0, 0, 0, 0, 0)
	c.handleBind(bindBody)
	c.handleExecute(append([]byte("\x00"), 0, 0, 0, 0))
	_ = c.flushWriter()
	if len(exec.calls) != 1 || exec.calls[0] != false {
		t.Fatalf("executor calls after extended Execute = %v, want [false]", exec.calls)
	}
	if c.S3CacheEnabled() {
		t.Fatalf("S3CacheEnabled() = true after extended SET off, want false")
	}
}
