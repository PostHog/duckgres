package server

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/transpiler/transform"
)

// recordingWorkerTTLControl is a WorkerTTLControl whose Apply records the
// requested TTLs, so a test can assert what the conn asked the control plane
// to stamp on its worker (and in what order relative to statements).
type recordingWorkerTTLControl struct {
	baseline time.Duration
	current  time.Duration
	hasCur   bool
	applied  []time.Duration
	err      error
	// clampTo, when positive, makes Apply return min(requested, clampTo),
	// mirroring the control plane's WorkerMaxTTL clamp.
	clampTo time.Duration
}

func (r *recordingWorkerTTLControl) control() *WorkerTTLControl {
	return &WorkerTTLControl{
		Baseline: r.baseline,
		Apply: func(_ context.Context, ttl time.Duration) (time.Duration, error) {
			if r.err != nil {
				return 0, r.err
			}
			r.applied = append(r.applied, ttl)
			if r.clampTo > 0 && ttl > r.clampTo {
				return r.clampTo, nil
			}
			return ttl, nil
		},
		Current: func() (time.Duration, bool) {
			return r.current, r.hasCur
		},
	}
}

func newWorkerTTLConn(exec QueryExecutor, rec *recordingWorkerTTLControl) (*clientConn, *bytes.Buffer) {
	c, out := newBufferedConn(exec)
	if rec != nil {
		c.workerTTLCtl = rec.control()
	}
	return c, out
}

// TestWorkerTTLSetAppliesThroughControl asserts the core contract of the SET
// path: `SET duckgres.worker_ttl = '20m'` invokes the control-plane hook with
// 20m BEFORE the session state flips, SHOW then reports the override, and a
// redundant SET to the same value does NOT re-invoke the hook. RESET restores
// the connect-time baseline on the worker.
func TestWorkerTTLSetAppliesThroughControl(t *testing.T) {
	rec := &recordingWorkerTTLControl{baseline: time.Minute}
	c, out := newWorkerTTLConn(&selectOneExecutor{}, rec)

	if got := c.workerTTLValue(); got != "1m0s" {
		t.Fatalf("fresh session workerTTLValue() = %q, want %q (baseline)", got, "1m0s")
	}

	if err := c.handleQuery([]byte("SET duckgres.worker_ttl = '20m'\x00")); err != nil {
		t.Fatalf("handleQuery(SET 20m): %v", err)
	}
	if len(rec.applied) != 1 || rec.applied[0] != 20*time.Minute {
		t.Fatalf("applied after SET 20m = %v, want [20m]", rec.applied)
	}
	if got := c.workerTTLValue(); got != "20m0s" {
		t.Fatalf("workerTTLValue() = %q after SET, want %q", got, "20m0s")
	}

	// SHOW reports the session state.
	out.Reset()
	if err := c.handleQuery([]byte("SHOW duckgres.worker_ttl\x00")); err != nil {
		t.Fatalf("handleQuery(SHOW): %v", err)
	}
	sawValue := false
	for _, m := range parseWireMsgs(t, out.Bytes()) {
		if m.typ == 'D' && bytes.Contains(m.body, []byte("20m0s")) {
			sawValue = true
		}
	}
	if !sawValue {
		t.Fatalf("SHOW duckgres.worker_ttl did not report '20m0s'")
	}

	// Redundant SET to the same value: no control-plane call.
	if err := c.handleQuery([]byte("SET duckgres.worker_ttl = '20m'\x00")); err != nil {
		t.Fatalf("handleQuery(redundant SET): %v", err)
	}
	if len(rec.applied) != 1 {
		t.Fatalf("redundant SET re-invoked the control plane: applied = %v", rec.applied)
	}

	// RESET restores the connect-time baseline on the worker.
	if err := c.handleQuery([]byte("RESET duckgres.worker_ttl\x00")); err != nil {
		t.Fatalf("handleQuery(RESET): %v", err)
	}
	if len(rec.applied) != 2 || rec.applied[1] != time.Minute {
		t.Fatalf("applied after RESET = %v, want [20m 1m]", rec.applied)
	}
	if got := c.workerTTLValue(); got != "1m0s" {
		t.Fatalf("workerTTLValue() = %q after RESET, want baseline %q", got, "1m0s")
	}
}

// TestWorkerTTLSetApplyFailureKeepsState asserts a failed control-plane apply
// fails the SET (ErrorResponse, no CommandComplete) and leaves the session
// state on its previous value — SHOW must never claim a TTL the worker will
// not park with.
func TestWorkerTTLSetApplyFailureKeepsState(t *testing.T) {
	rec := &recordingWorkerTTLControl{baseline: time.Minute, err: errors.New("pool: boom")}
	c, out := newWorkerTTLConn(&selectOneExecutor{}, rec)

	if err := c.handleQuery([]byte("SET duckgres.worker_ttl = '20m'\x00")); err != nil {
		t.Fatalf("handleQuery(SET 20m): %v", err)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "XX000", WorkerTTLGUCName) {
		t.Fatalf("failed apply did not surface as XX000 ErrorResponse; msgs=%s", describeMsgs(msgs))
	}
	for _, m := range msgs {
		if m.typ == 'C' && bytes.Contains(m.body, []byte("SET")) {
			t.Fatalf("failed SET still produced CommandComplete(SET); msgs=%s", describeMsgs(msgs))
		}
	}
	if got := c.workerTTLValue(); got != "1m0s" {
		t.Fatalf("failed apply flipped session state: workerTTLValue() = %q, want baseline %q", got, "1m0s")
	}
}

// TestWorkerTTLSetGateRejection asserts a control-plane rejection carrying a
// SQLSTATE (the AllowClientWorkerProfile gate's 22023) surfaces with THAT
// code, not the generic XX000 an apply failure gets.
func TestWorkerTTLSetGateRejection(t *testing.T) {
	rec := &recordingWorkerTTLControl{
		baseline: time.Minute,
		err:      &transform.CodedError{Code: "22023", Message: "duckgres.worker_ttl overrides are not enabled on this server"},
	}
	c, out := newWorkerTTLConn(&selectOneExecutor{}, rec)

	if err := c.handleQuery([]byte("SET duckgres.worker_ttl = '20m'\x00")); err != nil {
		t.Fatalf("handleQuery(SET 20m): %v", err)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "22023", "not enabled") {
		t.Fatalf("gate rejection did not surface as 22023; msgs=%s", describeMsgs(msgs))
	}
	if got := c.workerTTLValue(); got != "1m0s" {
		t.Fatalf("rejected SET flipped session state: workerTTLValue() = %q", got)
	}
}

// TestWorkerTTLSetClampReportsClamped asserts that when the control plane
// clamps the requested TTL (WorkerMaxTTL), the session state stores the
// CLAMPED value so SHOW never reports a TTL the worker will not park with.
func TestWorkerTTLSetClampReportsClamped(t *testing.T) {
	rec := &recordingWorkerTTLControl{baseline: time.Minute, clampTo: time.Hour}
	c, _ := newWorkerTTLConn(&selectOneExecutor{}, rec)

	if err := c.handleQuery([]byte("SET duckgres.worker_ttl = '24h'\x00")); err != nil {
		t.Fatalf("handleQuery(SET 24h): %v", err)
	}
	if len(rec.applied) != 1 || rec.applied[0] != 24*time.Hour {
		t.Fatalf("applied after SET 24h = %v, want [24h] (the hook sees the full request)", rec.applied)
	}
	if got := c.workerTTLValue(); got != "1h0m0s" {
		t.Fatalf("workerTTLValue() = %q after a clamped SET, want %q", got, "1h0m0s")
	}
}

// TestWorkerTTLSetWithoutControlIsStateOnly asserts the standalone/in-process
// case: a connection without the control-plane capability gets
// session-state-only SET/SHOW (no error) — those deployments have no hot-idle
// worker TTL to override.
func TestWorkerTTLSetWithoutControlIsStateOnly(t *testing.T) {
	c, out := newWorkerTTLConn(&selectOneExecutor{}, nil)

	if err := c.handleQuery([]byte("SET duckgres.worker_ttl = '20m'\x00")); err != nil {
		t.Fatalf("handleQuery(SET 20m): %v", err)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	sawSet := false
	for _, m := range msgs {
		if m.typ == 'C' && bytes.Contains(m.body, []byte("SET")) {
			sawSet = true
		}
	}
	if !sawSet {
		t.Fatalf("SET without control did not complete; msgs=%s", describeMsgs(msgs))
	}
	if got := c.workerTTLValue(); got != "20m0s" {
		t.Fatalf("workerTTLValue() = %q after SET, want %q (state-only)", got, "20m0s")
	}
}

// TestWorkerTTLShowDefaults pins the SHOW fallback order: the session
// override wins, then the bound worker's current TTL, then the connect-time
// baseline, then the built-in default.
func TestWorkerTTLShowDefaults(t *testing.T) {
	// No control at all (standalone): the documented built-in default.
	c, _ := newWorkerTTLConn(&selectOneExecutor{}, nil)
	if got := c.workerTTLValue(); got != "1m0s" {
		t.Fatalf("standalone workerTTLValue() = %q, want %q", got, "1m0s")
	}

	// A bound worker's CURRENT TTL beats the connect-time baseline (e.g. the
	// connection reused a hot-idle worker carrying a previous request's TTL).
	rec := &recordingWorkerTTLControl{baseline: 5 * time.Minute, current: 7 * time.Minute, hasCur: true}
	c, _ = newWorkerTTLConn(&selectOneExecutor{}, rec)
	if got := c.workerTTLValue(); got != "7m0s" {
		t.Fatalf("workerTTLValue() = %q, want current %q", got, "7m0s")
	}

	// The session override beats both.
	if err := c.applyWorkerTTLSetting("20m0s"); err != nil {
		t.Fatalf("applyWorkerTTLSetting: %v", err)
	}
	if got := c.workerTTLValue(); got != "20m0s" {
		t.Fatalf("workerTTLValue() = %q with override, want %q", got, "20m0s")
	}
}

// TestWorkerTTLInvalidSetRejected asserts the simple-protocol SET path
// rejects a non-duration value with 22023, does not echo the client input,
// and leaves the session state untouched.
func TestWorkerTTLInvalidSetRejected(t *testing.T) {
	rec := &recordingWorkerTTLControl{baseline: time.Minute}
	c, out := newWorkerTTLConn(&selectOneExecutor{}, rec)

	if err := c.handleQuery([]byte("SET duckgres.worker_ttl = 'garbage'\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "22023", "duration") {
		t.Fatalf("no 22023 ErrorResponse describing the expected shape; msgs=%s", describeMsgs(msgs))
	}
	if errorResponseWith(msgs, "garbage") {
		t.Fatalf("rejection echoes the offending value; msgs=%s", describeMsgs(msgs))
	}
	if len(rec.applied) != 0 {
		t.Fatalf("rejected SET reached the control plane: applied = %v", rec.applied)
	}
	if got := c.workerTTLValue(); got != "1m0s" {
		t.Fatalf("rejected SET flipped session state: workerTTLValue() = %q", got)
	}
}

// TestWorkerTTLMixedBatch asserts the split-batch path:
// `SET duckgres.worker_ttl = '20m'; SELECT 1` applies the GUC (control-plane
// call included) and still runs the trailing SELECT.
func TestWorkerTTLMixedBatch(t *testing.T) {
	exec := &selectOneExecutor{}
	rec := &recordingWorkerTTLControl{baseline: time.Minute}
	c, out := newWorkerTTLConn(exec, rec)

	if err := c.handleQuery([]byte("SET duckgres.worker_ttl = '20m'; SELECT 1\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if len(rec.applied) != 1 || rec.applied[0] != 20*time.Minute {
		t.Fatalf("applied = %v, want [20m]", rec.applied)
	}
	if exec.queryCalls != 1 {
		t.Fatalf("SELECT did not run: QueryContext called %d times, want 1", exec.queryCalls)
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

// TestWorkerTTLMixedBatchApplyFailureAborts asserts a failed apply inside a
// batch aborts the remaining statements — they may depend on the requested
// warm-retention state.
func TestWorkerTTLMixedBatchApplyFailureAborts(t *testing.T) {
	exec := &selectOneExecutor{}
	rec := &recordingWorkerTTLControl{baseline: time.Minute, err: errors.New("boom")}
	c, out := newWorkerTTLConn(exec, rec)

	if err := c.handleQuery([]byte("SET duckgres.worker_ttl = '20m'; SELECT 1\x00")); err != nil {
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

// TestWorkerTTLExtendedParse asserts the extended-protocol path: an invalid
// value is rejected at Parse time; a valid SET parses, applies at Execute
// time through the control-plane hook, and Describe returns NoData.
func TestWorkerTTLExtendedParse(t *testing.T) {
	rec := &recordingWorkerTTLControl{baseline: time.Minute}
	c, out := newWorkerTTLConn(&selectOneExecutor{}, rec)
	c.stmts = make(map[string]*preparedStmt)
	c.portals = make(map[string]*portal)

	// Invalid value: rejected at Parse, nothing stored.
	body := append([]byte("s1\x00SET duckgres.worker_ttl = 'garbage'\x00"), 0, 0)
	c.handleParse(body)
	_ = c.flushWriter()
	msgs := parseWireMsgs(t, out.Bytes())
	if !errorResponseWith(msgs, "22023", "duration") {
		t.Fatalf("extended Parse of invalid SET not rejected with 22023; msgs=%s", describeMsgs(msgs))
	}
	if _, ok := c.stmts["s1"]; ok {
		t.Fatalf("rejected Parse still stored the prepared statement")
	}

	// Valid SET: parses with workerTTLSet populated.
	out.Reset()
	body = append([]byte("s2\x00SET duckgres.worker_ttl = '20m'\x00"), 0, 0)
	c.handleParse(body)
	_ = c.flushWriter()
	st, ok := c.stmts["s2"]
	if !ok {
		t.Fatalf("valid SET did not parse: msgs=%s", describeMsgs(parseWireMsgs(t, out.Bytes())))
	}
	if st.workerTTLSet == nil || *st.workerTTLSet != "20m0s" {
		t.Fatalf("prepared stmt workerTTLSet = %v, want 20m0s", st.workerTTLSet)
	}

	// Bind + Execute applies through the hook.
	out.Reset()
	// Bind message: portal name, statement name (NUL-terminated), int16 format
	// count (0), int16 param count (0), int16 result-format count (0).
	bindBody := append([]byte("\x00s2\x00"), 0, 0, 0, 0, 0, 0)
	c.handleBind(bindBody)
	c.handleExecute(append([]byte("\x00"), 0, 0, 0, 0))
	_ = c.flushWriter()
	if len(rec.applied) != 1 || rec.applied[0] != 20*time.Minute {
		t.Fatalf("applied after extended Execute = %v, want [20m]", rec.applied)
	}
	if got := c.workerTTLValue(); got != "20m0s" {
		t.Fatalf("workerTTLValue() = %q after extended SET, want %q", got, "20m0s")
	}
}

// TestWorkerTTLSetActivatesLazyConnection asserts that a SET on a
// not-yet-acquired (exploratory-tier lazy) connection binds a worker FIRST —
// the TTL is worker-side pool state, so applying without a worker would leave
// SHOW reporting a TTL no worker parks with.
func TestWorkerTTLSetActivatesLazyConnection(t *testing.T) {
	rec := &recordingWorkerTTLControl{baseline: 48 * time.Hour}
	c, _ := newWorkerTTLConn(nil, rec)
	activations := 0
	c.sessionActivator = func(_ context.Context, pinned bool) (QueryExecutor, int, string, error) {
		activations++
		if pinned {
			t.Errorf("SET duckgres.worker_ttl activated pinned; it is not a pinning statement")
		}
		return &selectOneExecutor{}, 7, "worker-7", nil
	}

	if err := c.handleQuery([]byte("SET duckgres.worker_ttl = '20m'\x00")); err != nil {
		t.Fatalf("handleQuery(SET 20m): %v", err)
	}
	if activations != 1 {
		t.Fatalf("activations = %d, want 1 (SET must bind a worker before applying)", activations)
	}
	if len(rec.applied) != 1 || rec.applied[0] != 20*time.Minute {
		t.Fatalf("applied = %v, want [20m]", rec.applied)
	}
	if got := c.workerTTLValue(); got != "20m0s" {
		t.Fatalf("workerTTLValue() = %q, want %q", got, "20m0s")
	}
}

// TestWorkerTTLReappliedOnWorkerEscalation asserts that a session carrying a
// TTL override carries it onto the worker it escalates to: the new worker's
// profile carries the connect-time baseline, so without the re-apply the
// connection's warm retention would silently revert at escalation.
func TestWorkerTTLReappliedOnWorkerEscalation(t *testing.T) {
	rec := &recordingWorkerTTLControl{baseline: 48 * time.Hour}
	c, _ := newWorkerTTLConn(&selectOneExecutor{}, rec)
	if err := c.handleQuery([]byte("SET duckgres.worker_ttl = '20m'\x00")); err != nil {
		t.Fatalf("handleQuery(SET 20m): %v", err)
	}

	c.onExploratoryWorker = true
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return &selectOneExecutor{}, 8, "worker-8", nil
	}
	if err := c.escalateWorker(context.Background(), escalateReasonState); err != nil {
		t.Fatalf("escalateWorker: %v", err)
	}
	if len(rec.applied) != 2 || rec.applied[1] != 20*time.Minute {
		t.Fatalf("applied after escalation = %v, want the override re-applied to the new worker", rec.applied)
	}
	if got := c.workerTTLValue(); got != "20m0s" {
		t.Fatalf("workerTTLValue() = %q after escalation, want %q (override preserved)", got, "20m0s")
	}
}

// TestWorkerTTLEscalationReapplyFailureResetsState asserts that when the
// override cannot be re-applied on the new worker, the statement fails AND
// the session state is reset to match the TTL the worker will actually park
// with (the connect-time baseline) — SHOW must never lie. The ESCALATION
// itself succeeded, so the failure is statement-scoped, not connection-fatal.
func TestWorkerTTLEscalationReapplyFailureResetsState(t *testing.T) {
	rec := &recordingWorkerTTLControl{baseline: 48 * time.Hour}
	c, _ := newWorkerTTLConn(&selectOneExecutor{}, rec)
	if err := c.handleQuery([]byte("SET duckgres.worker_ttl = '20m'\x00")); err != nil {
		t.Fatalf("handleQuery(SET 20m): %v", err)
	}

	rec.err = errors.New("pool: worker gone")
	c.onExploratoryWorker = true
	c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return &selectOneExecutor{}, 8, "worker-8", nil
	}
	err := c.escalateWorker(context.Background(), escalateReasonState)
	if err == nil {
		t.Fatal("escalateWorker returned nil, want the re-apply failure")
	}
	if !errors.Is(err, errWorkerTTLReapplyFailed) {
		t.Fatalf("error %v is not tagged errWorkerTTLReapplyFailed; callers would terminate the connection", err)
	}
	if !strings.Contains(err.Error(), "duckgres.worker_ttl") {
		t.Fatalf("error %q does not name the GUC", err)
	}
	if got := c.workerTTLValue(); got != "48h0m0s" {
		t.Fatalf("workerTTLValue() = %q after a failed re-apply; state must match the worker's actual TTL %q", got, "48h0m0s")
	}
	if c.onExploratoryWorker {
		t.Fatal("onExploratoryWorker = true after a failed re-apply; the escalation itself SUCCEEDED and must not be rolled back")
	}
}
