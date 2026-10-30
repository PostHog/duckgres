// Package netlisten creates listeners with Multipath TCP turned off.
//
// Go 1.24 changed the default for servers: net.Listen and http.ListenAndServe
// now open the listening socket with IPPROTO_MPTCP, not IPPROTO_TCP, whenever
// the kernel supports MPTCP (net/dial.go: defaultMPTCPEnabledListen is true,
// defaultMPTCPEnabledDial is false). Clients still fall back to plain TCP, so
// nothing breaks visibly, and until recently nothing in this repository cared.
//
// CVE-2026-98432 made it matter. On an MPTCP listener the handshake's final ACK
// reaches the kernel's MPTCP subflow code, which calls the same
// tcp_v4_syn_recv_sock() the CVE describes: it transfers ownership of
// ireq->ireq_opt to the child socket, leaving the request socket's pointer
// readable by a retransmitted-SYN SYNACK path on another CPU. The unpatched
// destructors free that ip_options_rcu with a plain kfree(), so a reader still
// inside its rcu_read_lock() can dereference freed memory in ip_options_build().
//
// The MPTCP path is worse than the plain TCP one for a reason worth spelling
// out. A SYN carrying an MP_JOIN option whose token is valid builds a request
// socket with its ireq_opt (subflow_v4_conn_request); the completing ACK then
// runs tcp_check_req -> subflow_syn_recv_sock, which creates the child through
// tcp_v4_syn_recv_sock and jumps to its dispose_child label when the join fails
// validation (bad HMAC, missing MPJ_ACK, port mismatch, subflow limit), destroying
// the child inside that same call. That puts the child's destruction — and
// therefore the kfree() of the ip_options — microseconds after the ownership
// transfer. Two details narrow this without closing it: an MP_JOIN with an
// unknown token is rejected in subflow_check_req (-EPERM) before any request
// socket exists, so the attacker must first learn a token from an MP_CAPABLE
// connection of its own; and the attacker controls when the child is freed, not
// when the stale reader runs, which still requires a retransmitted SYN handled on
// another CPU. Plain TCP needs a rarer teardown for the same window: the child is
// not freed by a RST, only after accept and close, on listener teardown, or via
// sock_diag, all of which land long after the SYNACK.

// So a listener we own should not be an MPTCP socket. Two things enforce that:
//
//   - `godebug multipathtcp=0` in go.mod pins the pre-1.24 default for every
//     binary and test built from this module, including listeners created inside
//     a dependency or by code that does not use this package.
//   - Config and Listen below set MultipathTCP(false) explicitly, which also
//     survives a stray GODEBUG=multipathtcp=1 in a deployment environment.
//     GODEBUG only sets the default; an explicit SetMultipathTCP call wins.
//
// Every first-party listener goes through this package (listener_hygiene_test.go
// enforces it), so the directive is the second line of defence for our own code
// and the only one for anything a dependency binds.
//
// These settings are applied when a socket is bound. A listener that arrives over
// SCM_RIGHTS keeps the protocol it was created with, which is why tableflip's
// inherited FD across an in-place upgrade (SIGUSR1, process mode) does not
// re-run Config; see the runbook.
//
// Neither is a fix for the kernel bug. The fix is the kernel: the upstream
// change frees both options structures with kfree_rcu() and is backported to
// 6.1.190, 6.6.159, 6.12.113, 6.18.56 and 7.2.10, then mainline 7.3. These are
// the mitigations we can apply
// from userspace in the meantime, and they shrink the window rather than close
// it: the same race exists for plain TCP listeners, and for every other process
// on the host.
package netlisten

import (
	"context"
	"net"
)

// Config returns a listen config with Multipath TCP disabled, suitable for
// net.ListenConfig.Listen and for tableflip.Options.ListenConfig. Networks that
// are not TCP (unix, udp) ignore the setting.
func Config() *net.ListenConfig {
	lc := &net.ListenConfig{}
	lc.SetMultipathTCP(false)

	return lc
}

// Listen is net.Listen with Multipath TCP disabled.
func Listen(network, addr string) (net.Listener, error) {
	return Config().Listen(context.Background(), network, addr)
}
