# Metrics

Duckgres exposes Prometheus metrics on `:9090/metrics`. The port is currently
fixed. Some control-plane and per-org metrics are available only in the
Kubernetes build.

This document is the reference for request-path metrics. The short catalog in
the README links here rather than duplicating boundary and aggregation details.

## Naming and labels

Metric names follow `duckgres_<domain>_<operation>_<measurement>`. Durations
end in `_seconds`, monotonically increasing event counts end in `_total`, and
current state uses a gauge without either suffix.

The request path uses these label terms consistently:

- `org`: managed warehouse organization ID. It is empty in standalone mode.
- `protocol`: protocol boundary; session-start metrics currently emit `postgres`.
- `outcome`: terminal result at the metric's documented boundary.
- `decision`: what happened to the polling request in one admission poll.
- `reason`: a bounded, metric-specific cause. It is not a poll count.
- `source`: how a worker was obtained.
- `phase`: one internal worker-acquisition phase.

Histograms expose the usual `_bucket`, `_count`, and `_sum` series.

## Request path boundaries

| Stage | Metrics | Boundary |
|---|---|---|
| Admission evaluation | `duckgres_session_admission_evaluation_*` | One request-owned DB admission poll, including its serialized transaction and lock wait. |
| Admission queue | `duckgres_session_admission_wait_seconds`, `duckgres_session_admission_requests_total` | After a request is successfully enqueued until grant, hard rejection, timeout, cancellation, or evaluation error. Enqueue failures are excluded. |
| Admission state | `duckgres_session_admission_queue_depth`, `duckgres_session_admission_active_vcpus`, `duckgres_session_admission_limit_vcpus` | Local waiting callers, live local lease handles, and the effective org cap reconciled for active org stacks from each control-plane process's current config snapshot. |
| Worker acquisition | `duckgres_worker_acquire_*` | After admission grants until an existing, hot-idle, or newly spawned worker is allocated. |
| Session start | `duckgres_session_start_duration_seconds` | After successful PostgreSQL authentication until `ReadyForQuery` is flushed or session bootstrap terminates. |
| Query | `duckgres_query_total`, `duckgres_query_duration_seconds` | One non-empty query attempt and its execution duration. |

Session start includes profile resolution, admission, worker
allocation, worker session creation, catalog probing, metadata initialization,
session defaults, and the final ready flush. It excludes failed authentication.

## Admission metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `duckgres_session_admission_evaluation_duration_seconds` | Histogram | `decision`, `reason` | Latency of one DB-backed admission poll for the polling request. It intentionally has no `org` label. |
| `duckgres_session_admission_evaluations_total` | Counter | `decision`, `reason` | Admission poll volume. This can be much larger than request volume because a queued request is polled repeatedly. |
| `duckgres_session_admission_wait_seconds` | Histogram | `org`, `outcome`, `reason` | End-to-end queue wait for each successfully enqueued request. |
| `duckgres_session_admission_requests_total` | Counter | `org`, `outcome`, `reason` | Exactly one terminal event per successfully enqueued request. |
| `duckgres_session_admission_queue_depth` | Gauge | `org` | In-process callers still waiting after successful durable enqueue. It is not a count of durable queue rows. |
| `duckgres_session_admission_active_vcpus` | Gauge | `org` | Requested vCPUs held by live local lease handles. It is admitted capacity, not measured CPU usage or the exact durable lease-row total. |
| `duckgres_session_admission_limit_vcpus` | Gauge | `org` | Effective org cap for an active org stack, reconciled from this process's current config snapshot. `0` means unlimited. |
| `duckgres_session_start_duration_seconds` | Histogram | `org`, `protocol`, `outcome` | Authenticated PostgreSQL create-to-ready latency. |

Admission request outcomes are `granted`, `rejected`, `timeout`, `canceled`,
and `error`. `rejected` means the requested worker shape can never fit its hard
organization or user vCPU ceiling.
Session-start outcomes are `success`, `timeout`, `canceled`, `capacity`,
`draining`, and `error`.

Evaluation decisions are `granted_current`, `already_granted`, `rejected`,
`blocked`, `waiting`, `inactive`, `missing`, `canceled`, `timeout`, and `error`.
Each evaluation describes only the polling request. Evaluation reasons are
`none`, `org_vcpu`, `user_vcpu`, `org_user_vcpu`, `user_ineligible`,
`resharding`, `fifo`, and `store_error`.

A terminal request retains vCPU-cap attribution across every admission poll.
No blocking poll produces `reason="none"`. If the request encountered an org
cap, a user cap, or both, its terminal reason is `org_vcpu`, `user_vcpu`, or
`org_user_vcpu`, respectively, even when it also encountered another reason.
Without a vCPU-cap reason, one distinct reason is kept as-is and multiple
distinct reasons become `mixed`. An admission store failure contributes
`store_error`; interruption by the caller is classified as cancellation or
timeout instead.

Queue depth and active vCPUs are process-local logical contributions. Use
`sum by (org)` across control-plane replicas. Active vCPUs drop when a live
lease handle transfers cleanup ownership to the reclaimer; durable rows still
awaiting cleanup are excluded, so database-enforced usage can temporarily be
higher. Each replica reconciles the limit when it creates, updates, or removes
an active org stack, so use `max by (org)` to collapse the duplicated replica
values. Replicas can briefly differ while a config update propagates. Process
exit removes its local gauge series automatically.

### Admission cleanup metrics

Cleanup ownership is reserved before durable enqueue and retained through the
request and lease lifetime. These process-level metrics intentionally omit org
and request labels:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `duckgres_session_admission_reclaim_pending` | Gauge | None | Activated cleanup work retained or executing. |
| `duckgres_session_admission_reclaim_attempts_total` | Counter | `outcome` | Exact cleanup attempts; outcomes are `success` and `error`. |
| `duckgres_session_admission_reclaim_reservations_in_use` | Gauge | None | Reservations held before enqueue, while queued, by live leases, or by cleanup-pending work. |
| `duckgres_session_admission_reclaim_reservation_capacity` | Gauge | None | Process-local cleanup reservation ceiling. |
| `duckgres_session_admission_reclaim_reservation_rejections_total` | Counter | `reason` | Reservation failures: `full`, `closed`, or `duplicate`. |

Sum the gauges across control-plane replicas for fleet totals. Pair cleanup
backlog and error rate with the logical admission gauges when diagnosing a gap
between live sessions and database-enforced vCPU usage.

## Worker and query metrics

The Kubernetes worker allocator keeps its existing names:

| Metric | Labels | Meaning |
|---|---|---|
| `duckgres_worker_acquire_total_seconds` | `org`, `source`, `outcome` | End-to-end worker allocation latency after admission. Sources are `idle_reuse`, `hot_idle_claim`, `spawn`, or `none`. |
| `duckgres_worker_acquire_gate_wait_seconds` | `org`, `outcome` | Time waiting for the per-org FIFO worker-acquire gate. |
| `duckgres_worker_acquire_phase_seconds` | `org`, `phase`, `outcome` | Individual `hot_idle_claim`, `spawn`, or `activate` phase latency. |
| `duckgres_query_total` | `org`, `outcome` | One event per non-empty query attempt; outcomes are `success`, `error`, or `canceled`. |
| `duckgres_query_duration_seconds` | `org` | Query execution latency. Use `duckgres_query_total` for terminal result counts. |

The older fleet-level `duckgres_control_plane_worker_*` metrics remain useful
for process-wide worker counts, spawn time, and the approximate post-admission
worker queue. They have no org label and are not admission saturation metrics.

## PromQL recipes

Admission wait p95 by org and terminal result:

```promql
histogram_quantile(
  0.95,
  sum by (org, outcome, le) (
    rate(duckgres_session_admission_wait_seconds_bucket[5m])
  )
)
```

Terminal requests affected by an org or user vCPU cap:

```promql
sum by (org, reason) (
  rate(duckgres_session_admission_requests_total{reason=~"org_vcpu|user_vcpu|org_user_vcpu"}[5m])
)
```

Current queue depth and admitted vCPUs:

```promql
sum by (org) (duckgres_session_admission_queue_depth)
sum by (org) (duckgres_session_admission_active_vcpus)
```

Live admitted-session utilization for capped orgs (`limit=0` is deliberately
filtered out):

```promql
sum by (org) (duckgres_session_admission_active_vcpus)
  / on (org)
(max by (org) (duckgres_session_admission_limit_vcpus) > 0)
```

Authenticated session-start p95:

```promql
histogram_quantile(
  0.95,
  sum by (org, protocol, outcome, le) (
    rate(duckgres_session_start_duration_seconds_bucket[5m])
  )
)
```

## Admission metric migration

The pre-canonical admission family is retired. Existing TSDB history remains,
but old series stop receiving samples.

| Retired metric | Replacement | Compatibility note |
|---|---|---|
| `duckgres_org_connection_admission_duration_seconds{outcome}` | `duckgres_session_admission_evaluation_duration_seconds{decision,reason}` | Same admission-evaluation layer, with the overloaded outcome split into decision and reason. |
| `duckgres_org_connection_admission_attempts_total{outcome}` | `duckgres_session_admission_evaluations_total{decision,reason}` | Counts admission polls, not logical requests. |
| `duckgres_org_connection_admission_queue_depth` histogram | `duckgres_session_admission_queue_depth{org}` gauge | A current per-process count of waiting callers replaces samples of durable queue shape; the values are not numerically equivalent. |
| `duckgres_org_connection_admission_user_queues` | None | Per-user queue-head shape is no longer exported. |
| `duckgres_org_connection_admission_user_limit_skips_total` | `duckgres_session_admission_requests_total{reason=~"user_vcpu|org_user_vcpu"}` | Measures affected logical requests rather than repeated skipped admission polls; the values are not numerically equivalent. |
| `duckgres_org_connection_admission_ineligible_user_skips_total` | `duckgres_session_admission_requests_total{reason="user_ineligible"}` | Measures affected logical requests rather than repeated admission polls; use `duckgres_session_admission_evaluations_total{decision="blocked",reason="user_ineligible"}` for poll-level diagnostics. |
| `duckgres_org_connection_reclaim_pending` | `duckgres_session_admission_reclaim_pending` | Semantics-preserving prefix rename. |
| `duckgres_org_connection_reclaim_attempts_total{outcome}` | `duckgres_session_admission_reclaim_attempts_total{outcome}` | Semantics-preserving prefix rename. |
| `duckgres_org_connection_reclaim_reservations_in_use` | `duckgres_session_admission_reclaim_reservations_in_use` | Semantics-preserving prefix rename. |
| `duckgres_org_connection_reclaim_reservation_capacity` | `duckgres_session_admission_reclaim_reservation_capacity` | Semantics-preserving prefix rename. |
| `duckgres_org_connection_reclaim_reservation_rejections_total{reason}` | `duckgres_session_admission_reclaim_reservation_rejections_total{reason}` | Semantics-preserving prefix rename. |
