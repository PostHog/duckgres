package server

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeTierExecutor is a minimal QueryExecutor for exercising escalateWorker's
// executor swap. escalateWorker only assigns it to c.executor and never calls
// any method, so embedding the (nil) interface to satisfy QueryExecutor is
// sufficient — no method bodies needed.
type fakeTierExecutor struct {
	QueryExecutor
}

func TestEscalateWorkerSwapsExecutorOnce(t *testing.T) {
	c := &clientConn{onExploratoryWorker: true}
	fake := &fakeTierExecutor{}
	calls := 0
	c.workerSwitcher = func(ctx context.Context, reason string) (QueryExecutor, int, string, error) {
		calls++
		if reason != escalateReasonState {
			t.Fatalf("reason=%q", reason)
		}
		return fake, 42, "pod-42", nil
	}
	if err := c.escalateWorker(context.Background(), escalateReasonState); err != nil {
		t.Fatal(err)
	}
	if c.executor != QueryExecutor(fake) || c.workerID != 42 || c.workerPod != "pod-42" {
		t.Fatalf("executor/worker not swapped: %+v", c)
	}
	if c.onExploratoryWorker {
		t.Fatal("must leave exploratory tier after escalation")
	}
	// Second call is a no-op (sticky pin).
	if err := c.escalateWorker(context.Background(), escalateReasonOOM); err != nil || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestEscalateWorkerFailureKeepsState(t *testing.T) {
	c := &clientConn{onExploratoryWorker: true}
	c.workerSwitcher = func(ctx context.Context, reason string) (QueryExecutor, int, string, error) {
		return nil, 0, "", errors.New("no capacity")
	}
	if err := c.escalateWorker(context.Background(), escalateReasonState); err == nil {
		t.Fatal("want error")
	}
	// Failure does NOT clear the flag: caller decides (it sends an error to
	// the client and the next statement may retry the escalation).
	if !c.onExploratoryWorker {
		t.Fatal("failed escalation must not mark the connection pinned")
	}
}

func TestCurrentWorkerTier(t *testing.T) {
	exploratory := &clientConn{onExploratoryWorker: true}
	if got := exploratory.currentWorkerTier(); got != "exploratory" {
		t.Fatalf("currentWorkerTier() on exploratory worker = %q, want %q", got, "exploratory")
	}

	standard := &clientConn{onExploratoryWorker: false}
	if got := standard.currentWorkerTier(); got != "standard" {
		t.Fatalf("currentWorkerTier() off exploratory worker = %q, want %q", got, "standard")
	}
}

// TestAcquisitionFailureOutcome pins the bounded label set the tier's two
// acquisition metrics share. Every label is derived from the CLASSIFIED
// SQLSTATE, so an unknown code must degrade to "error" rather than leak a new
// series.
func TestAcquisitionFailureOutcome(t *testing.T) {
	for code, want := range map[string]string{
		"57014": AcquisitionOutcomeCanceled,
		"53300": AcquisitionOutcomeCapacity,
		"57P03": AcquisitionOutcomeDraining,
		"28000": AcquisitionOutcomeDisabled,
		"53400": AcquisitionOutcomeError,
		"3D000": AcquisitionOutcomeError,
		"XX000": AcquisitionOutcomeError,
		"":      AcquisitionOutcomeError,
	} {
		if got := AcquisitionFailureOutcome(code); got != want {
			t.Fatalf("AcquisitionFailureOutcome(%q) = %q, want %q", code, got, want)
		}
	}
}

// TestEscalationMetricRecordsOutcome asserts FAILED escalations are counted,
// not just successful ones — without the outcome label a cluster that cannot
// escalate anything looks exactly like one nobody escalates on. "ok" is counted
// at the swap, so a later s3_cache re-apply failure (statement-scoped, the
// escalation itself happened) does not turn a successful escalation into a
// failed one.
func TestEscalationMetricRecordsOutcome(t *testing.T) {
	counter := func(reason, outcome string) float64 {
		return testutil.ToFloat64(exploratoryEscalationsTotal.WithLabelValues(reason, outcome))
	}

	before := counter(escalateReasonState, AcquisitionOutcomeOK)
	ok := &clientConn{onExploratoryWorker: true}
	ok.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
		return &fakeTierExecutor{}, 1, "pod-1", nil
	}
	if err := ok.escalateWorker(context.Background(), escalateReasonState); err != nil {
		t.Fatalf("escalateWorker: %v", err)
	}
	if got := counter(escalateReasonState, AcquisitionOutcomeOK); got != before+1 {
		t.Fatalf("outcome=ok counter = %v, want %v", got, before+1)
	}

	for _, tc := range []struct {
		name   string
		err    error
		reason string
		want   string
	}{
		{"disabled", errors.New("this account is disabled; contact your administrator"), escalateReasonState, AcquisitionOutcomeDisabled},
		{"capacity", errors.New("worker capacity exhausted for organization"), escalateReasonState, AcquisitionOutcomeCapacity},
		{"draining", &SessionAcquireError{Code: "57P03", Message: "the database system is shutting down"}, escalateReasonOOM, AcquisitionOutcomeDraining},
		{"canceled", &SessionAcquireError{Code: "57014", Message: "canceling statement due to user request"}, escalateReasonOOM, AcquisitionOutcomeCanceled},
		{"error", errors.New("dial worker: connection refused"), escalateReasonOOM, AcquisitionOutcomeError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := counter(tc.reason, tc.want)
			c := &clientConn{onExploratoryWorker: true}
			c.workerSwitcher = func(context.Context, string) (QueryExecutor, int, string, error) {
				return nil, 0, "", tc.err
			}
			if err := c.escalateWorker(context.Background(), tc.reason); err == nil {
				t.Fatal("escalateWorker returned nil, want the switcher failure")
			}
			if got := counter(tc.reason, tc.want); got != start+1 {
				t.Fatalf("counter{reason=%q,outcome=%q} = %v, want %v", tc.reason, tc.want, got, start+1)
			}
		})
	}
}
