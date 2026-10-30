//go:build linux

package netlisten

import (
	"context"
	"net"
	"syscall"
	"testing"
)

const (
	// Not exported by the syscall package, which is frozen.
	ipProtoMPTCP = 0x106 // IPPROTO_MPTCP
	ipProtoTCP   = 6     // IPPROTO_TCP
)

// TestListenBindsPlainTCPSocket is the behavioural test for this package: on a
// kernel that really does hand out MPTCP sockets, Listen must still bind a
// plain TCP socket.
//
// The control listener comes first on purpose. Without it a passing subject
// would not prove anything: on a kernel built without CONFIG_MPTCP, or with
// net.mptcp.enabled=0, every listener is plain TCP whether we ask or not. The
// control shows this host is one where the distinction is observable, and the
// test skips where it is not.
func TestListenBindsPlainTCPSocket(t *testing.T) {
	controlConfig := &net.ListenConfig{}
	controlConfig.SetMultipathTCP(true)
	control, err := controlConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("control listener: %v", err)
	}
	defer func() { _ = control.Close() }()

	if got := socketProtocol(t, control); got != ipProtoMPTCP {
		t.Skipf("host does not bind MPTCP sockets (control listener protocol %d, want %d): MPTCP is not compiled in or net.mptcp.enabled=0, so this test cannot distinguish the two behaviours", got, ipProtoMPTCP)
	}

	ln, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	if got := socketProtocol(t, ln); got != ipProtoTCP {
		t.Fatalf("Listen bound protocol %d, want %d (IPPROTO_TCP); MPTCP listeners are what CVE-2026-98432's report asks us to avoid", got, ipProtoTCP)
	}
}

func socketProtocol(t *testing.T, ln net.Listener) int {
	t.Helper()

	sc, ok := ln.(syscall.Conn)
	if !ok {
		t.Fatalf("%T does not expose a syscall.Conn", ln)
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}

	var proto int
	var sockErr error
	if err := rc.Control(func(fd uintptr) {
		proto, sockErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_PROTOCOL)
	}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if sockErr != nil {
		t.Fatalf("getsockopt SO_PROTOCOL: %v", sockErr)
	}

	return proto
}
