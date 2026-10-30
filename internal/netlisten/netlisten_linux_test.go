//go:build linux

package netlisten

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
)

const (
	// Not exported by the syscall package, which is frozen.
	ipProtoMPTCP = 0x106 // IPPROTO_MPTCP
	ipProtoTCP   = 6     // IPPROTO_TCP
)

// TestListenBindsPlainTCPSocket is the behavioural test for this package, and it
// covers both mechanisms at once: the listener built by this package, and the
// listener a stdlib call builds under the module default from go.mod.
//
// The control listener comes first on purpose. Without it a passing subject would
// not prove anything: on a kernel built without CONFIG_MPTCP, or with
// net.mptcp.enabled=0, every listener is plain TCP whether we ask or not. The
// control shows this host is one where the distinction is observable, and the
// test skips where it is not.
func TestListenBindsPlainTCPSocket(t *testing.T) {
	control, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("control listener: %v", err)
	}
	defer func() { _ = control.Close() }()

	if got := socketProtocol(t, control); got != ipProtoMPTCP {
		t.Skipf("host does not bind MPTCP sockets by default (control listener protocol %d, want %d): MPTCP is not compiled in or net.mptcp.enabled=0, so this test cannot distinguish the two behaviours", got, ipProtoMPTCP)
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

// TestListenBindsPlainTCPSocketUnderGODEBUG proves the helper is load-bearing
// rather than decorative.
//
// The test above passes because two independent mechanisms both hold MPTCP off,
// so deleting SetMultipathTCP(false) from Config() would not turn it red: the
// go.mod default would still bind plain TCP. Here GODEBUG=multipathtcp=1 puts the
// net package default back on the MPTCP path, leaving the explicit call as the
// only thing keeping the socket plain. A zero ListenConfig is asserted to bind an
// MPTCP socket in the same process, so a silent failure to apply the override
// cannot make this test vacuous.
func TestListenBindsPlainTCPSocketUnderGODEBUG(t *testing.T) {
	if os.Getenv("NETLISTEN_SOCKET_PROBE") == "1" {
		probeSocketProtocols(t)

		return
	}

	if !hostBindsMPTCP(t) {
		t.Skip("host cannot bind MPTCP sockets, so the GODEBUG override has nothing to observe; covered by TestListenBindsPlainTCPSocket's skip on the same hosts")
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestListenBindsPlainTCPSocketUnderGODEBUG$", "-test.v")
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "GODEBUG=") {
			continue
		}
		cmd.Env = append(cmd.Env, kv)
	}
	cmd.Env = append(cmd.Env, "NETLISTEN_SOCKET_PROBE=1", "GODEBUG=multipathtcp=1")

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child process with GODEBUG=multipathtcp=1 failed: %v\n%s", err, out)
	}
}

// probeSocketProtocols runs in the child process, where GODEBUG=multipathtcp=1 is
// in force.
func probeSocketProtocols(t *testing.T) {
	t.Helper()

	controlConfig := &net.ListenConfig{}
	controlConfig.SetMultipathTCP(true)
	control, err := controlConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("control listener: %v", err)
	}
	defer func() { _ = control.Close() }()

	if got := socketProtocol(t, control); got != ipProtoMPTCP {
		t.Fatalf("control listener bound protocol %d, want %d: GODEBUG=multipathtcp=1 did not take effect in this process", got, ipProtoMPTCP)
	}

	defaultConfig, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("default listener: %v", err)
	}
	defer func() { _ = defaultConfig.Close() }()

	if got := socketProtocol(t, defaultConfig); got != ipProtoMPTCP {
		t.Fatalf("a zero ListenConfig bound protocol %d under GODEBUG=multipathtcp=1, want %d: the override is not applied, so this test could not tell whether netlisten works", got, ipProtoMPTCP)
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
