// Package netlisten creates listeners with Multipath TCP turned off.
//
// Go 1.24 changed the default for servers: net.Listen and http.ListenAndServe
// now open the listening socket with IPPROTO_MPTCP, not IPPROTO_TCP, whenever
// the kernel supports MPTCP (net/dial.go: defaultMPTCPEnabledListen is true,
// defaultMPTCPEnabledDial is false). Clients still fall back to plain TCP, so
// nothing breaks visibly, and until recently nothing in this repository cared.
//
// CVE-2026-98432 made it matter. An MPTCP listener hands SYNs to the kernel's
// MPTCP subflow code, which reaches the same tcp_v4_syn_recv_sock() that the
// CVE describes: it transfers ownership of ireq->ireq_opt to the child socket,
// leaving the request socket's pointer readable by a retransmitted-SYN SYNACK
// path on another CPU. The unpatched destructors free that ip_options_rcu with
// a plain kfree(), so a reader still inside its rcu_read_lock() can dereference
// freed memory in ip_options_build().
//
// The MPTCP path is worse than the plain TCP one for a reason worth spelling
// out: subflow_syn_recv_sock() can create the child and then dispose of it in
// the same call (see its dispose_child label, reachable by a SYN carrying a
// forged MP_JOIN option). That puts the child's destruction — and therefore the
// kfree() of the ip_options — microseconds after the ownership transfer, fully
// under a remote caller's control. Plain TCP needs a harder race to hit the
// same window: the child has to be torn down while a SYNACK is being built.
//
// So a listener we own should not be an MPTCP socket. Two things enforce that:
//
//   - `godebug multipathtcp=0` in go.mod pins the pre-1.24 default for every
//     binary and test built from this module. That covers listeners created
//     through helper code we do not own, such as http.ListenAndServe.
//   - Config and Listen below set MultipathTCP(false) explicitly, which also
//     survives a stray GODEBUG=multipathtcp=1 in a deployment environment.
//     GODEBUG only sets the default; an explicit SetMultipathTCP call wins.
//
// Neither is a fix for the kernel bug. The fix is the kernel: the upstream
// change frees both options structures with kfree_rcu() and is backported to
// 6.1.190, 6.12.113, 7.2.12 and later. These are the mitigations we can apply
// from userspace in the meantime, and they shrink the window rather than close
// it: the same race exists for plain TCP listeners, and for every other process
// on the host.
package netlisten

import (
	"context"
	"net"
)

// Config returns a listen config with Multipath TCP disabled, suitable for
// net.ListenConfig.Listen and for tableflip.Options.ListenConfig.
func Config() *net.ListenConfig {
	lc := &net.ListenConfig{}
	lc.SetMultipathTCP(false)

	return lc
}

// Listen is net.Listen with Multipath TCP disabled.
func Listen(network, addr string) (net.Listener, error) {
	return Config().Listen(context.Background(), network, addr)
}
