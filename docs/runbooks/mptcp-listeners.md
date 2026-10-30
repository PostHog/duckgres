# Runbook: MPTCP listeners and CVE-2026-98432

## Goal

Keep the Duckgres client-facing listener off Multipath TCP, so the kernel bug in
CVE-2026-98432 has one fewer path to reach us, and know what is still exposed
until the node kernels are updated.

## The bug

`ipv4: free inet_opt and ireq_opt after an RCU grace period`
([upstream commit e3f33b0a1d89](https://git.kernel.org/linus/e3f33b0a1d89d738e2b913dab519226eaf76e15b),
backported to 6.1.190, 6.6.159, 6.12.113, 6.18.56 and 7.2.10, then to
mainline 7.3).

`tcp_v4_syn_recv_sock()` hands `ireq->ireq_opt` to the child socket without
copying it. A retransmitted SYN processed on another CPU can still be building
a SYNACK from that same `ireq_opt` (it holds only `rcu_read_lock()`), while the
child socket's destructor frees it with a plain `kfree()`. The reader then uses
freed memory in `ip_options_build()`. The fix replaces both `kfree()` calls with
`kfree_rcu()`.

Preconditions: the crafted SYN carries IP options (that is where `ireq_opt`
comes from in the first place), and the child socket is destroyed while the
SYNACK is being built.

## Why a Go listener is on the interesting side of this

Since Go 1.24, `net.Listen` opens the listening socket with `IPPROTO_MPTCP`
whenever the kernel supports MPTCP (`net/dial.go`: `defaultMPTCPEnabledListen`
is true, `defaultMPTCPEnabledDial` is false). Nothing in Duckgres asks for MPTCP;
it arrives as the net package default, which is why this needs a guard rather
than an invocation-level fix.

With an MPTCP listener the handshake's final ACK is handled by
`subflow_syn_recv_sock()`. That function calls the same `tcp_v4_syn_recv_sock()`
and then, on several paths, jumps to its own `dispose_child` label and tears the
child down in the same call — for example when an `MP_JOIN` subflow's third ACK
fails validation (bad HMAC, missing `MPJ_ACK`, port mismatch, subflow limit).
That puts "child destroyed while a SYNACK is in flight" microseconds after the
ownership transfer instead of leaving it to chance.

Two things narrow this, and neither closes it: an `MP_JOIN` carrying an unknown
token is rejected in `subflow_check_req()` with `-EPERM` before any request
socket is created, so the attacker has to learn a token from an `MP_CAPABLE`
connection of its own first; and the attacker controls when the child is freed,
not when the stale SYNACK reader runs, which still requires a retransmitted SYN
processed on another CPU.

Plain TCP has the same race but needs a rarer teardown: a RST does not free an
accept-queue child. It is freed after `accept()` and `close()` in userspace, on
listener teardown (`inet_child_forget`), through `sock_diag` destroy, or via
`tcp_abort` — all far later than the SYNACK window. The Go security team
explicitly does not rule out plain TCP exploitation, so treat this as reach
reduction rather than immunity.

## What this repository does

1. `godebug multipathtcp=0` in `go.mod`. This pins the pre-1.24 default for every
   binary and test built from the module, including listeners created by
   `http.ListenAndServe` and other helpers we do not construct ourselves.
2. `internal/netlisten` sets `SetMultipathTCP(false)` explicitly on every
   first-party listener: the control plane's PG listener (through
   `tableflip.Options.ListenConfig`) and its API and metrics servers, the
   standalone server, the DuckDB service and its local cache-proxy router, the
   cache proxy's three HTTP servers, the ACME HTTP-01 listener, the boot metrics
   server, and the admin devserver. An explicit `SetMultipathTCP(false)` overrides
   `GODEBUG`, so a deployment environment that sets `GODEBUG=multipathtcp=1`
   cannot undo it for any of them.
3. Tests, so the invariant does not rot:
   - `internal/netlisten` asserts the module default, the helper, that the helper
     survives `GODEBUG=multipathtcp=1`, and (on MPTCP-capable hosts) that the
     bound socket is `IPPROTO_TCP` where a forced-on control listener gets
     `IPPROTO_MPTCP`.
   - `listener_hygiene_test.go` fails if a new TCP listener bypasses
     `internal/netlisten` — including `tls.Listen` and `http.ListenAndServe` — or
     if the tableflip upgrader is handed a listen config that is not exactly
     `netlisten.Config()`.

## Rollout

This is a property of the built binary, so it takes effect when the binary is
rebuilt and deployed. The control-plane, worker and standalone images rebuild on
every push to `main`; the cache-proxy image has its own path-filtered CD workflow,
which now also triggers on `go.mod`/`go.sum` for this reason.

The socket options are fixed when the listener is bound, not when the process
starts. A pod replacement binds a fresh listener and gets plain TCP. An in-place
tableflip upgrade is different: `SIGUSR1` makes the parent hand its listener file
descriptor to the child and `tableflip`'s `listenerLocked` wraps it again with
`net.FileListener`, so the socket keeps whatever options it was created with.
That path is process-mode only (the `SIGUSR1` handler is gated on `!isK8s`), so a
cluster rollout is unaffected. In process mode, restart the process rather than
upgrading it in place if you need an existing listener switched over.

## What this does not do

The kernel is the fix. The same race exists for plain TCP listeners, and for
every other process sharing the node — CNPG, pgbouncer, the edge proxy, the
kubelet. Treat this as attack-surface reduction, not remediation. For the rollout
procedure itself see [control-plane-rollout.md](control-plane-rollout.md).

Remaining follow-ups, in priority order:

- Update the node kernels to a build carrying the backport. Bottlerocket
  re-resolves the AMI per release, so a node-group or AMI version bump rolls the
  fleet. Do not decide that from `uname -r`: distributions backport the fix into
  their own versioning, so a kernel that looks older can be patched and one that
  looks newer can be missing it. Use the vendor's advisory or the Bottlerocket
  release notes for the variant in use, or check the source the vendor ships.
- If a node cannot be updated, `net.mptcp.enabled=0` turns MPTCP off in a
  network namespace. Note what that means here: the sysctl is per-namespace, so
  setting it on the node's root namespace covers host-network processes but
  does nothing for a pod with its own netns, which is where this process runs.
  For the pod it has to be set inside the pod's network namespace, which takes a
  privileged init container or an equivalent per-pod mechanism; Duckgres is
  deployed from its own chart, so this is a change to the pod template rather
  than a values key. Go falls back to plain TCP when socket creation fails with
  `ENOPROTOOPT`, so the setting is safe for this process. Either way it is a
  deployment change, not a repository change.
- Where the network path allows it, drop IPv4 TCP packets that carry IP options
  (`ihl > 5`) before they reach a listener. `ireq_opt` only exists for those
  packets, so dropping them removes the precondition for every process on the
  node, not just this one. nftables can express it by header length, which is the
  simplest form because it needs no option-type knowledge:

  ```sh
  nft add rule inet filter input tcp flags syn ip hdrlength != 5 counter drop
  ```

  `ip option` matches (`lsrr`, `ssrr`, `rr`, `ra`) work too, but nothing matches
  the timestamp option by name. In iptables the mainline match is `u32`
  (`-p tcp --syn -m u32 --u32 "0>>24&0xF=6:15" -j DROP`); `-m ipv4options` comes
  from xtables-addons, not the mainline kernel, and is not available on
  Bottlerocket. Worth checking whether the load balancer or the VPC security
  layer can drop these more cheaply.
- The request-socket migration half of the CVE needs
  `net.ipv4.tcp_migrate_req=1` or a `BPF_SK_REUSEPORT_SELECT_OR_MIGRATE` program.
  Linux defaults it to 0 and the Duckgres charts do not set it, but that is a
  per-pod question rather than a per-cluster one: check the value in each netns
  that serves traffic, because a pod that inherits the setting from a template or
  a host-network process can have it on without the Duckgres manifests saying so.

## Verifying a running deployment

The build-time guarantee is the reliable one: `go test ./internal/netlisten/`
asserts the module default and the helper, and `listener_hygiene_test.go` fails if
a TCP listener stops going through `internal/netlisten`. Those checks do not care
what kernel the pod runs on.

At runtime, first establish that this pod's network namespace has MPTCP at all:

```sh
# inside the control-plane container
ls /proc/sys/net/mptcp/          # absent => kernel built without CONFIG_MPTCP
grep -E '^(MPTCP|MPTCPv6)' /proc/net/protocols   # absent => kernel built without MPTCP
```

`net.mptcp.enabled` is per network namespace, not a node-wide switch: it is
registered through `register_net_sysctl_sz` on each namespace and a fresh pod
netns starts at `mptcp_enabled = 1` (`mptcp_pernet_set_defaults`). A node-level
setting does not reach this process, so do not read the node's value as evidence
about the pod's.

Then look for the listeners themselves. There is no `/proc` file that lists MPTCP
sockets — an early draft of this runbook said `/proc/net/mptcp_net/mptcp`, which
does not exist in mainline Linux; MPTCP stats live in `/proc/net/netstat`
(`MPTcpExt:`) and the sockets are only reachable through `sock_diag`.

```sh
ss -Mltnp   # MPTCP listeners; want: nothing on the PG port
ss -ltnp    # plain TCP listeners; the PG port must be here
```

Four traps in reading that:

- `-l` is required. Without it `ss -M` dumps only non-listening sockets, so an
  empty result proves nothing about a listener.
- `ss -M` needs `CONFIG_INET_MPTCP_DIAG` and a recent iproute2, and **it returns
  success with no output when the diag support is missing or the netlink request
  fails** (`mptcp_show` in iproute2 discards the error). An empty `ss -M` on its
  own is not a pass. Prove the diagnostic path works with a positive control
  first: bind an MPTCP listener yourself and confirm `ss -Mlnp` shows it.

  ```sh
  python3 - <<'EOF'
  import socket
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0x106)  # IPPROTO_MPTCP
  s.bind(("127.0.0.1", 0)); s.listen(1)
  print("mptcp listener on", s.getsockname())  # now check ss -Mlnp in another shell
  input()
  EOF
  ```

  If `socket()` raises `EPROTONOSUPPORT`, the kernel has no MPTCP and the whole
  question is moot for that pod.
- `/proc/net/tcp` and `/proc/net/tcp6` do **not** distinguish the two.
  `mptcp_listen()` calls `__inet_listen_sk()` on the subflow socket, so an MPTCP
  listener has an ordinary TCP listening entry either way. A listener bound to
  `:port` is usually `AF_INET6`, so it appears in `/proc/net/tcp6` regardless.
- On 6.6 and 6.12 kernels the third column of the `MPTCP`/`MPTCPv6` rows in
  `/proc/net/protocols` counts live **listener** sockets (`mptcp_listen()` and
  `mptcp_stop_listen()` are the only callers of `sock_prot_inuse_add`), so it is
  a usable listener count there; it is not a census of connected MPTCP sockets.
  The 6.1 branch has no such accounting at all, so on 6.1 ignore that column and
  rely on the `ss` positive control.

## Rollback

Reverting the `go.mod` directive and the `netlisten` call sites restores Go's
MPTCP-listener default. Nothing else depends on either, so a revert has no data
or protocol consequence: MPTCP listeners fall back to plain TCP for clients that
do not request MPTCP, and the clients we know of (psql, pgx, JDBC, psycopg) do
not. A client could opt in deliberately (`mptcpize run psql`), which is the one
case where a revert would change the transport.
