package server

import (
	"context"
	"testing"
	"time"
)

// Audit H4 regression test.
//
// In ProcessIsolation child-worker mode, SIGUSR1 signals query cancellation
// via the server's externalCancelCh (wired in runChildWorker). The channel
// used to be CLOSED on the first signal and never re-armed, while
// queryContextInner wires EVERY new query's context to it — so after one
// cancel request the permanently-readable channel instantly cancelled every
// subsequent query's context and the connection could never run another
// statement.
//
// The contract: a cancel signal kills the query in flight, not the
// connection. A query started after a cancel was consumed must get a live
// context, and a later cancel must still work (the mechanism re-arms).
func TestExternalCancelDoesNotPoisonSubsequentQueries(t *testing.T) {
	cancelCh := make(chan struct{}, 1)
	srv := &Server{
		activeQueries:    make(map[BackendKey]context.CancelFunc),
		externalCancelCh: cancelCh,
	}
	c := &clientConn{server: srv}

	waitDone := func(ctx context.Context, what string) {
		t.Helper()
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
			t.Fatalf("external cancel did not cancel %s", what)
		}
	}
	requireLive := func(ctx context.Context, what string) {
		t.Helper()
		select {
		case <-ctx.Done():
			t.Fatalf("%s was cancelled at start: a single SIGUSR1 must target only the query in flight, not poison the connection", what)
		case <-time.After(200 * time.Millisecond):
		}
	}

	// Query 1 is in flight when SIGUSR1 arrives.
	ctx1, cleanup1 := c.queryContextInner(false)
	notifyQueryCancel(cancelCh)
	waitDone(ctx1, "the in-flight query")
	cleanup1()

	// Query 2 starts after the cancel was consumed: must get a live context.
	ctx2, cleanup2 := c.queryContextInner(false)
	requireLive(ctx2, "the query after a consumed cancel")

	// The mechanism must re-arm: a new SIGUSR1 cancels the new in-flight query.
	notifyQueryCancel(cancelCh)
	waitDone(ctx2, "the second query after a fresh cancel signal")
	cleanup2()

	// A cancel delivered while NO query is in flight is stale; like Postgres,
	// it must not kill the next query.
	notifyQueryCancel(cancelCh)
	ctx3, cleanup3 := c.queryContextInner(false)
	defer cleanup3()
	requireLive(ctx3, "the query after a stale between-queries cancel")
}
