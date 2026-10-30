# Runbook: MPTCP listeners and CVE-2026-98432

## Goal

Keep the Duckgres client-facing listener off Multipath TCP, so the kernel bug in
CVE-2026-98432 has one fewer path to reach us, and know what is still exposed
until the node kernels are updated.

## The bug

`ipv4: free inet_opt and ireq_opt after an RCU grace period`
(upstream `e3f33b0a1d89d738e2b913dab519226eaf76e15b`, backported to 6.1.190,
6.12.113, 7.2.12 and later).

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

With an MPTCP listener, SYNs are handled by `subflow_syn_recv_sock()`. That
function calls the same `tcp_v4_syn_recv_sock()` and then, on a few paths, jumps
to its own `dispose_child` label and tears the child down in the same call — for
example when a SYN presents a forged `MP_JOIN` option. The window the CVE needs
("child destroyed while a SYNACK is in flight") stops being a lucky race and
becomes something a remote caller schedules. Plain TCP needs the child to be
reset or closed at the wrong moment instead.

## What this repository does

1. `godebug multipathtcp=0` in `go.mod`. This pins the pre-1.24 default for every
   binary and test built from the module, including listeners created by
   `http.ListenAndServe` and other helpers we do not construct ourselves.
2. `internal/netlisten` sets `SetMultipathTCP(false)` explicitly on the listeners
   we do construct: the control plane's PG listener (through
   `tableflip.Options.ListenConfig`), the standalone server, the DuckDB service
   and its local cache-proxy router, the ACME HTTP-01 listener, and the admin
   devserver. An explicit `SetMultipathTCP(false)` overrides `GODEBUG`, so a
   deployment environment that sets `GODEBUG=multipathtcp=1` cannot undo it.
3. Tests, so the invariant does not rot:
   - `internal/netlisten` asserts the module default, the helper, that the helper
     survives `GODEBUG=multipathtcp=1`, and (on MPTCP-capable hosts) that the
     bound socket is `IPPROTO_TCP` where a forced-on control listener gets
     `IPPROTO_MPTCP`.
   - `listener_hygiene_test.go` fails if a new TCP listener bypasses
     `internal/netlisten`, or if the tableflip upgrader is handed a listen config
     that does not come from it.

## What this does not do

The kernel is the fix. The same race exists for plain TCP listeners, and for
every other process sharing the node — the Envoy edge, the CNPG instances, the
kubelet. Treat this as attack-surface reduction, not remediation.

Remaining follow-ups, in priority order:

- Update the node kernels to a build carrying the backport. Bottlerocket
  re-resolves the AMI per release, so a node-group or AMI version bump rolls the
  fleet. Confirm with `uname -r` on the nodes against 6.12.113 / 7.2.12+ (or
  6.1.190 for 6.1).
- If a node cannot be updated, `net.mptcp.enabled=0` disables MPTCP node-wide and
  covers non-Go listeners as well. Go falls back to plain TCP when the socket
  creation fails with `ENOPROTOOPT`, so this is safe for us. It is a node-level
  change, not a repository change.
- The request-socket migration half of the CVE needs `net.ipv4.tcp_migrate_req=1`
  or a `BPF_SK_REUSEPORT_SELECT_OR_MIGRATE` program. Linux defaults to 0; nothing
  in these charts sets it. Confirm before spending time on that half.

## Verifying a running deployment

MPTCP sockets are listed separately from TCP ones. An empty file means no MPTCP
sockets in that network namespace:

```sh
# inside the control-plane container
cat /proc/net/mptcp_net/mptcp          # want: no rows
ls /proc/sys/net/mptcp/                # absent => kernel has no CONFIG_MPTCP
cat /proc/sys/net/mptcp/enabled        # 0 => MPTCP off; 1 => MPTCP available
```

A listener that appears in `/proc/net/tcp` and in `/proc/net/mptcp_net/mptcp` is
an MPTCP listener. After a rollout with this change, the PG port must appear only
in `/proc/net/tcp`.

## Rollback

Reverting the `go.mod` directive and the `netlisten` call sites restores Go's
MPTCP-listener default. Nothing else depends on either, so a revert has no data
or protocol consequence: MPTCP listeners negotiate plain TCP with clients that do
not speak MPTCP, which is all of them.
