package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/posthog/duckgres/server/wire"
)

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
