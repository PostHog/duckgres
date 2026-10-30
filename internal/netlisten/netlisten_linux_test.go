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

// TestListenBindsPlainTCPSocket is the behavioural test for this package, and it
// covers both mechanisms at once: the listener built here, and the listener a
// plain stdlib call builds under the module default from go.mod.
//
// The control listener forces MPTCP on, which is the only way to find out whether
// this host can tell the two apart at all. A zero ListenConfig would not work as
// the control any more: go.mod's directive makes it bind plain TCP, so the
// control would always fail and the test would always skip.
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

	// The go.mod `godebug multipathtcp=0` directive, observed at the syscall
	// layer: a plain net.Listen in this module must not bind an MPTCP socket.
	stdlib, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("stdlib listener: %v", err)
	}
	defer func() { _ = stdlib.Close() }()

	if got := socketProtocol(t, stdlib); got != ipProtoTCP {
		t.Fatalf("net.Listen bound protocol %d, want %d (IPPROTO_TCP): the `godebug multipathtcp=0` directive in go.mod is not taking effect", got, ipProtoTCP)
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

// TestListenBindsPlainTCPSocketUnderGODEBUG proves the helper is load-bearing at
// the socket level too.
//
// Under GODEBUG=multipathtcp=1 a zero ListenConfig binds an MPTCP socket again, so
// the explicit SetMultipathTCP(false) inside Listen is the only thing keeping the
// subject on plain TCP. The control is asserted in the same process, so an
// override that quietly fails to apply cannot make this test vacuous.
func TestListenBindsPlainTCPSocketUnderGODEBUG(t *testing.T) {
	if !hostBindsMPTCP(t) {
		t.Skip("host cannot bind MPTCP sockets, so the override has nothing to observe")
	}

	t.Setenv("GODEBUG", "multipathtcp=1")

	control, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("control listener: %v", err)
	}
	defer func() { _ = control.Close() }()

	if got := socketProtocol(t, control); got != ipProtoMPTCP {
		t.Fatalf("control listener bound protocol %d under GODEBUG=multipathtcp=1, want %d: the override is not applied, so this test could not tell whether Listen works", got, ipProtoMPTCP)
	}

	ln, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	if got := socketProtocol(t, ln); got != ipProtoTCP {
		t.Fatalf("Listen bound protocol %d under GODEBUG=multipathtcp=1, want %d: an explicit SetMultipathTCP(false) must win over GODEBUG", got, ipProtoTCP)
	}
}

// hostBindsMPTCP reports whether this kernel hands out MPTCP sockets when asked.
func hostBindsMPTCP(t *testing.T) bool {
	t.Helper()

	lc := &net.ListenConfig{}
	lc.SetMultipathTCP(true)
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	return socketProtocol(t, ln) == ipProtoMPTCP
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
