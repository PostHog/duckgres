package server

import (
	"context"
	"errors"
	"testing"
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
