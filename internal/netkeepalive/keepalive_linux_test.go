//go:build linux

package netkeepalive

import (
	"net"
	"syscall"
	"testing"
	"time"
)

// Linux setsockopt names for the keepalive triplet. Declared locally to
// avoid a hard dep on golang.org/x/sys/unix; values match <netinet/tcp.h>.
const (
	tcpKeepIdle  = 4 // TCP_KEEPIDLE
	tcpKeepIntvl = 5 // TCP_KEEPINTVL
	tcpKeepCnt   = 6 // TCP_KEEPCNT
)

// TestTuneAcceptedConn_LinuxSocketOptions reads back the actual sockopts
// after TuneAcceptedConn so the kernel's view — not just Go's KeepAliveConfig
// API — is what we assert on. This is the canary for the prod fix: the bug
// was that the unset TCP_USER_TIMEOUT let dead writes hang ~15 minutes; the
// regression is "we shipped a build where TCP_USER_TIMEOUT was no longer
// applied," which only this round-trip catches.
func TestTuneAcceptedConn_LinuxSocketOptions(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	dialed := make(chan net.Conn, 1)
	go func() {
		c, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			dialed <- nil
			return
		}
		dialed <- c
	}()

	conn, err := listener.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	defer func() { _ = conn.Close() }()
	defer func() {
		if c := <-dialed; c != nil {
			_ = c.Close()
		}
	}()

	TuneAcceptedConn(conn)

	tcpConn := conn.(*net.TCPConn)
	raw, err := tcpConn.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}

	var (
		keepalive int
		idle      int
		interval  int
		count     int
		userTO    int
		getErr    error
	)
	ctrlErr := raw.Control(func(fd uintptr) {
		ifd := int(fd)
		if keepalive, getErr = syscall.GetsockoptInt(ifd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE); getErr != nil {
			return
		}
		if idle, getErr = syscall.GetsockoptInt(ifd, syscall.IPPROTO_TCP, tcpKeepIdle); getErr != nil {
			return
		}
		if interval, getErr = syscall.GetsockoptInt(ifd, syscall.IPPROTO_TCP, tcpKeepIntvl); getErr != nil {
			return
		}
		if count, getErr = syscall.GetsockoptInt(ifd, syscall.IPPROTO_TCP, tcpKeepCnt); getErr != nil {
			return
		}
		userTO, getErr = syscall.GetsockoptInt(ifd, syscall.IPPROTO_TCP, tcpUserTimeout)
	})
	if ctrlErr != nil {
		t.Fatalf("raw.Control: %v", ctrlErr)
	}
	if getErr != nil {
		t.Fatalf("GetsockoptInt: %v", getErr)
	}

	if keepalive == 0 {
		t.Errorf("SO_KEEPALIVE: got 0, want non-zero (enabled)")
	}
	if want := int(keepAliveIdle / time.Second); idle != want {
		t.Errorf("TCP_KEEPIDLE: got %d, want %d", idle, want)
	}
	if want := int(keepAliveInterval / time.Second); interval != want {
		t.Errorf("TCP_KEEPINTVL: got %d, want %d", interval, want)
	}
	if count != keepAliveCount {
		t.Errorf("TCP_KEEPCNT: got %d, want %d", count, keepAliveCount)
	}
	if want := int(userTimeout / time.Millisecond); userTO != want {
		t.Errorf("TCP_USER_TIMEOUT: got %d ms, want %d ms", userTO, want)
	}
}
