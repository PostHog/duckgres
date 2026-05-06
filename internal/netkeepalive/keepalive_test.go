package netkeepalive

import (
	"net"
	"testing"
)

// TestTuneAcceptedConn_NonTCP confirms TuneAcceptedConn is a no-op (no panic,
// no error) on connections that are not *net.TCPConn. The accept loop should
// stay generic — keepalive tuning is best-effort.
func TestTuneAcceptedConn_NonTCP(t *testing.T) {
	t.Parallel()

	// io.Pipe doesn't give us a net.Conn, so use a local socketpair via
	// net.Pipe — both ends are *pipe (an in-memory net.Conn) which fails
	// the *net.TCPConn assertion inside TuneAcceptedConn.
	a, b := net.Pipe()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	TuneAcceptedConn(a)
}

// TestTuneAcceptedConn_TCP confirms TuneAcceptedConn returns cleanly for an
// accepted TCP connection. The platform-specific assertions (Linux getsockopt
// roundtrip) live in keepalive_linux_test.go.
func TestTuneAcceptedConn_TCP(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	dialErrCh := make(chan error, 1)
	go func() {
		c, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			dialErrCh <- err
			return
		}
		_ = c.Close()
		dialErrCh <- nil
	}()

	conn, err := listener.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	defer func() { _ = conn.Close() }()

	TuneAcceptedConn(conn)

	if err := <-dialErrCh; err != nil {
		t.Fatalf("dial: %v", err)
	}
}
