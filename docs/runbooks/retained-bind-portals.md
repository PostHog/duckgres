# Runbook: Retained Bind Portal Budgets

## Purpose

Protect control-plane processes from client connections that retain large Bind
payloads in named portals. The limits are enforced independently for each
client connection:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `max_retained_bind_bytes` | `67108864` (64 MiB) | Aggregate portal-owned storage retained by open portals (Bind storage, cached RowDescriptions, and retained protocol names) |
| `max_open_portals` | `1024` | Installed portal shells, including lightweight terminal shells until they are cleaned up |

Configure these with YAML (`max_retained_bind_bytes`, `max_open_portals`),
environment (`DUCKGRES_MAX_RETAINED_BIND_BYTES`, `DUCKGRES_MAX_OPEN_PORTALS`),
or CLI (`--max-retained-bind-bytes`, `--max-open-portals`). Values must be
positive. They are byte and portal-count limits, not parameter-count limits:
a valid 27,000-parameter Bind remains allowed when it fits the byte budget.

## Monitor

| Metric | What it shows |
| --- | --- |
| `duckgres_retained_bind_bytes` | Aggregate portal-owned bytes across the process (Bind storage, cached RowDescriptions, and retained protocol names) |
| `duckgres_open_portals` | Aggregate installed portal shells across the process |
| `duckgres_portal_payload_releases_total{reason}` | Releases by a fixed lifecycle reason: `terminal_success`, `terminal_failure`, `close_portal`, `close_statement`, `unnamed_rebind`, `idle_sync`, `tx_end`, `simple_query`, or `connection_close` |
| `duckgres_portal_budget_rejections_total{reason}` | Rejected Bind admissions: `reason=retained_bytes` or `reason=open_portals` |

Alert on a nonzero rejection rate, for example:

```promql
sum by (reason) (rate(duckgres_portal_budget_rejections_total[5m])) > 0
```

The gauges are process-wide aggregates. Do not add user, organization, or
portal labels when creating dashboards or alerts.

## Diagnose and recover

1. Confirm the rejection reason and inspect the retained-byte and open-portal
   gauges alongside the payload-release rate. A Bind rejected with SQLSTATE
   `54000` did not install a portal and did not change accounting. If the
   rejection was for an oversized framed Bind before its body was read, the
   connection is closed to avoid desynchronizing the protocol; reconnect
   before retrying with a smaller Bind.
2. For post-read admission rejections, finish executing portals, explicitly
   close portals or their statements, or send `Sync` while idle outside an
   explicit transaction. These protocol boundaries release payloads and remove
   portal shells as appropriate.
3. If a client has abandoned a connection, terminate that connection. Restart
   a control-plane process only when connection-level remediation is not
   possible and immediate capacity recovery is required.
4. Increase a limit only after confirming the workload's legitimate retained
   payload size. Do not use a larger limit as a substitute for closing portals
   or completing extended-query protocol cycles.

## Validation after a change

1. Check that `duckgres_portal_budget_rejections_total` stops increasing for
   the expected reason.
2. Verify terminal execution causes `duckgres_portal_payload_releases_total`
   to increase and the retained-byte gauge to fall. Closing a terminal shell
   can still lower retained metadata/protocol-name bytes without another
   payload release counter increment.
3. Keep the limits explicit in deployment configuration so later rollouts do
   not silently change the protection level.
