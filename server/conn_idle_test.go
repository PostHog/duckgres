package server

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

// TestMessageLoopIdleTimeoutClosesConnection proves the mechanism the
// control-plane idle default relies on: with IdleTimeout configured, a
// connection that sends nothing hits the read deadline and the message loop
// returns nil (a clean close), which in the CP triggers DestroySession →
// worker back to hot-idle.
func TestMessageLoopIdleTimeoutClosesConnection(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()

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
