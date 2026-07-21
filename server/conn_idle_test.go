package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/server/wire"
)

type blockingDrainExecutor struct {
	QueryExecutor
	entered chan struct{}
	release chan struct{}
}

type blockAfterFirstRead struct {
	first             []byte
	firstDelivered    bool
	secondReadStarted chan struct{}
	releaseSecondRead chan struct{}
	secondReadOnce    sync.Once
}

func (r *blockAfterFirstRead) Read(p []byte) (int, error) {
	if !r.firstDelivered {
		r.firstDelivered = true
		return copy(p, r.first), nil
	}
	r.secondReadOnce.Do(func() { close(r.secondReadStarted) })
	<-r.releaseSecondRead
	return 0, io.EOF
}

func (e *blockingDrainExecutor) LastProfilingOutput() string { return "" }

func (e *blockingDrainExecutor) ExecContext(ctx context.Context, _ string, _ ...any) (ExecResult, error) {
	close(e.entered)
	select {
	case <-e.release:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TestMessageLoopIdleTimeoutClosesConnection proves the mechanism the
// control-plane idle default relies on: with IdleTimeout configured, a
// connection that sends nothing hits the read deadline and the message loop
// returns nil (a clean close), which in the CP triggers DestroySession →
// worker back to hot-idle.
func TestMessageLoopIdleTimeoutClosesConnection(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer func() { _ = clientSide.Close() }()
	defer func() { _ = serverSide.Close() }()

	s := &Server{}
	InitMinimalServer(s, Config{IdleTimeout: 50 * time.Millisecond}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cc := &clientConn{
		server: s,
		conn:   serverSide,
		reader: bufio.NewReader(serverSide),
		writer: bufio.NewWriter(serverSide),
		ctx:    ctx,
		cancel: cancel,
	}

	done := make(chan error, 1)
	go func() { done <- cc.messageLoop() }()

	// The client never sends a message → the server idle-times-out and the loop
	// returns nil (clean close), NOT an error.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil on idle close, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("messageLoop did not return on idle timeout")
	}
}

func TestLateCopyFramesPreserveReadyBoundary(t *testing.T) {
	for _, msgType := range []byte{wire.MsgCopyData, wire.MsgCopyDone, wire.MsgCopyFail} {
		t.Run(string(msgType), func(t *testing.T) {
			input := &blockAfterFirstRead{
				first:             pgFrame(msgType, nil),
				secondReadStarted: make(chan struct{}),
				releaseSecondRead: make(chan struct{}),
			}
			c := &clientConn{
				server: &Server{},
				reader: bufio.NewReader(input),
				writer: bufio.NewWriter(io.Discard),
				ctx:    context.Background(),
			}

			done := make(chan error, 1)
			go func() { done <- c.messageLoop() }()

			select {
			case <-input.secondReadStarted:
			case <-time.After(time.Second):
				t.Fatal("messageLoop did not consume the late COPY frame")
			}

			// A backend COPY error may race with already-sent frontend COPY
			// frames. PostgreSQL silently drops those frames after ErrorResponse /
			// ReadyForQuery; they must not make the connection look active again.
			atReadyBoundary := c.idleRead.Load()
			close(input.releaseSecondRead)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("messageLoop returned error: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("messageLoop did not exit after test reader EOF")
			}
			if !atReadyBoundary {
				t.Fatal("late COPY frame moved connection away from ReadyForQuery boundary")
			}
		})
	}
}

// TestDrainOrgConnectionsClosesBlockedIdleConnection proves a reshard drain
// does not have to wait for the normal idle timeout. A PostgreSQL connection
// blocked waiting for its next client message is already at a safe idle
// boundary, so requesting a drain for its org must wake the read and make the
// message loop return immediately.
func TestDrainOrgConnectionsClosesBlockedIdleConnection(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer func() { _ = clientSide.Close() }()
	defer func() { _ = serverSide.Close() }()

	s := &Server{}
	InitMinimalServer(s, Config{IdleTimeout: -1}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cc := &clientConn{
		server: s,
		conn:   serverSide,
		reader: bufio.NewReader(serverSide),
		writer: bufio.NewWriter(serverSide),
		pid:    42,
		orgID:  "org-a",
		ctx:    ctx,
		cancel: cancel,
	}
	s.registerConn(cc)
	defer s.unregisterConn(cc.pid)

	done := make(chan error, 1)
	go func() { done <- cc.messageLoop() }()

	if got := s.DrainOrgConnections("org-a"); got != 1 {
		t.Fatalf("DrainOrgConnections returned %d, want 1", got)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected clean close at idle drain boundary, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("messageLoop remained blocked after org drain was requested")
	}
}

// TestDrainOrgConnectionsWaitsForActiveQueryBoundary proves requesting a
// reshard drain never cancels work already executing. The connection closes
// only after that query has completed and its ReadyForQuery boundary is safe.
func TestDrainOrgConnectionsWaitsForActiveQueryBoundary(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer func() { _ = clientSide.Close() }()
	defer func() { _ = serverSide.Close() }()

	s := &Server{activeQueries: make(map[BackendKey]context.CancelFunc)}
	InitMinimalServer(s, Config{IdleTimeout: -1}, nil)
	executor := &blockingDrainExecutor{entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cc := &clientConn{
		server:   s,
		conn:     serverSide,
		reader:   bufio.NewReader(serverSide),
		writer:   bufio.NewWriter(serverSide),
		executor: executor,
		pid:      43,
		orgID:    "org-a",
		txStatus: txStatusIdle,
		ctx:      ctx,
		cancel:   cancel,
	}
	s.registerConn(cc)
	defer s.unregisterConn(cc.pid)

	done := make(chan error, 1)
	go func() { done <- cc.messageLoop() }()
	go func() { _, _ = io.Copy(io.Discard, clientSide) }()
	if err := wire.WriteMessage(clientSide, wire.MsgQuery, []byte("BEGIN\x00")); err != nil {
		t.Fatalf("write query: %v", err)
	}
	select {
	case <-executor.entered:
	case <-time.After(time.Second):
		t.Fatal("query did not start")
	}

	if got := s.DrainOrgConnections("org-a"); got != 1 {
		t.Fatalf("DrainOrgConnections returned %d, want 1", got)
	}
	select {
	case err := <-done:
		t.Fatalf("messageLoop returned during active query: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(executor.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected clean close after active query, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("messageLoop did not close at post-query idle boundary")
	}
}

// TestCopyStreamReArmsIdleDeadline proves the COPY-FROM-STDIN forwarding reader
// (copyDataWireReader) re-arms the connection idle read deadline on EVERY
// CopyData message, so a COPY that keeps putting bytes on the wire for LONGER
// than the idle timeout — while never stalling longer than it between messages
// — is NOT reaped mid-stream. This is the exact product guarantee the e2e
// "COPY survives idle timeout" assertion exercises end-to-end.
//
// The connection starts with a deadline already armed (as it is when a COPY is
// dispatched). If the reader failed to re-arm per message, that initial
// deadline would fire partway through the >timeout stream and the read would
// error — so deleting the per-message armIdleReadDeadline() makes this test
// fail, which is what gives it teeth.
func TestCopyStreamReArmsIdleDeadline(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer func() { _ = clientSide.Close() }()
	defer func() { _ = serverSide.Close() }()

	s := &Server{}
	InitMinimalServer(s, Config{IdleTimeout: 250 * time.Millisecond}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cc := &clientConn{
		server: s,
		conn:   serverSide,
		reader: bufio.NewReader(serverSide),
		writer: bufio.NewWriter(serverSide),
		ctx:    ctx,
		cancel: cancel,
	}
	// Simulate the deadline that is already armed when the COPY is dispatched.
	cc.armIdleReadDeadline()

	const msgs = 40
	const payload = "row-data\n"
	// Stream 40 CopyData messages 10ms apart (well under the 250ms timeout) then
	// CopyDone: ~400ms total, comfortably PAST the idle timeout, but every gap is
	// ~25x under it — so only correct per-message re-arming keeps the read alive.
	go func() {
		for i := 0; i < msgs; i++ {
			time.Sleep(10 * time.Millisecond)
			if err := wire.WriteCopyData(clientSide, []byte(payload)); err != nil {
				return
			}
		}
		_ = wire.WriteCopyDone(clientSide)
	}()

	r := &copyDataWireReader{c: cc}
	total := 0
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		total += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("COPY stream reader errored mid-stream (idle re-arm broken?): %v", err)
		}
	}
	if want := msgs * len(payload); total != want {
		t.Fatalf("streamed %d bytes, want %d", total, want)
	}
}
