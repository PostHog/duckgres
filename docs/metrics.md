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
- `direction`: a bounded byte-flow direction; the metadata proxy emits
  `client_to_upstream` and `upstream_to_client`.

Histograms expose the usual `_bucket`, `_count`, and `_sum` series.

## Managed warehouse state

`duckgres_managed_warehouse_state{org,duckling,state}` is a Kubernetes-only
gauge with value `1` for each warehouse that has not finished deletion. The
state label is one of `pending`, `provisioning`, `ready`, `failed`, `deleting`,
`resharding`, or `unknown`. Unexpected stored values map to `unknown` to keep
metric cardinality bounded.

Each control-plane replica emits the same snapshot-backed series. Use `max by`
instead of `sum by` when evaluating warehouse state across replicas. A deleted
warehouse disappears from the metric on the next snapshot refresh.

## Request path boundaries

| Stage | Metrics | Boundary |
|---|---|---|
| Admission evaluation | `duckgres_session_admission_evaluation_*` | One request-owned DB admission poll, including its serialized transaction and lock wait. |
| Admission queue | `duckgres_session_admission_wait_seconds`, `duckgres_session_admission_requests_total` | After a request is successfully enqueued until grant, hard rejection, timeout, cancellation, or evaluation error. Enqueue failures are excluded. |
| Admission state | `duckgres_session_admission_queue_depth`, `duckgres_session_admission_active_vcpus`, `duckgres_session_admission_limit_vcpus` | Local waiting callers, live local lease handles, and the effective org cap reconciled for active org stacks from each control-plane process's current config snapshot. |
| Worker acquisition | `duckgres_worker_acquire_*` | After admission grants until an existing, hot-idle, or newly spawned worker is allocated. |
| Session start | `duckgres_session_start_duration_seconds`, `duckgres_postgres_session_start_total` | After successful PostgreSQL authentication until `ReadyForQuery` is flushed or session bootstrap terminates. The counter records exactly one terminal result after server-side retries. |
| Query | `duckgres_query_total`, `duckgres_query_duration_seconds` | One non-empty query attempt and its execution duration. |
| Native metadata proxy | `duckgres_metadata_proxy_*` | The separate metadata SNI branch, from its fail-closed auth/gate through internal Postgres connect and opaque pgwire relay. |

Session start includes profile resolution, admission, worker
allocation, worker session creation, catalog probing, metadata initialization,
session defaults, and the final ready flush. It excludes failed authentication.

## Native metadata Postgres proxy metrics

The native metadata proxy branches before worker allocation. It has dedicated
metric families rather than adding a protocol label to established metrics:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `duckgres_metadata_proxy_connections_open` | Gauge | `org` | Admitted proxy connections on this control-plane process, including upstream bootstrap. |
| `duckgres_metadata_proxy_connection_attempts_total` | Counter | `org`, `outcome` | Exactly one terminal event per connection that matched a configured metadata SNI suffix. `success` is recorded when frontend `ReadyForQuery` is flushed. |
| `duckgres_metadata_proxy_connection_duration_seconds` | Histogram | `org` | Lifetime from admission against the per-org proxy cap until teardown, including failed upstream bootstrap. |
| `duckgres_metadata_proxy_upstream_connect_duration_seconds` | Histogram | `org`, `outcome` | Time spent connecting and authenticating to the internally resolved metadata Postgres target. Outcomes are `success` and `error`. |
| `duckgres_metadata_proxy_bytes_total` | Counter | `org`, `direction` | Post-authentication pgwire bytes relayed in the fixed `client_to_upstream` or `upstream_to_client` direction. |
| `duckgres_metadata_proxy_cancel_requests_total` | Counter | `outcome` | Raw proxy CancelRequests handled as `session_terminated` on the owning replica or `not_local` when the synthetic key is not registered on this replica. |

Connection-attempt outcomes are the closed set `success`, `unavailable`,
`invalid_database`, `auth_failed`, `draining`, `capacity`,
`target_resolution_error`, `upstream_connect_error`, `upstream_sync_error`,
`upstream_hijack_error`, `cancel_key_error`, and `handshake_error`. Unknown or
disabled SNI prefixes use an empty `org` label so denial metrics do not invent
tenant values.

The open gauge is process-local because the configured safety cap is enforced
per org on each replica. Use `sum by (org)` for fleet totals and retain the
scrape target's `pod` label when diagnosing one replica at its cap. Counters
and histograms can be summed across replicas normally.

`duckgres_connections_open` is incremented before the SNI branch and therefore
includes metadata proxy sockets. Existing worker-path metrics intentionally do
not: `duckgres_connection_duration_seconds`,
`duckgres_org_sessions_active`, `duckgres_org_pg_sessions_accepted_total`,
admission/worker metrics, query metrics, durable query logs, and query traces
remain DuckDB-worker-only. The relay is byte-opaque and cannot safely produce
statement-level observations. Use the proxy connection/byte metrics together
with CNPG and PgBouncer metrics for this path.

Two process-wide security metrics also span the branch boundary.
`duckgres_auth_failures_total` includes wrong-password metadata-proxy attempts;
use
`duckgres_metadata_proxy_connection_attempts_total{outcome="auth_failed"}` for
the proxy-specific subset. `duckgres_rate_limit_rejects_total` is incremented
for pre-TLS rate-limit rejections, before SNI is available, so those rejected
sockets cannot be attributed to either the metadata endpoint or the DuckDB
worker endpoint.

The internal upstream session starts with the fixed
`application_name=duckgres-metadata-proxy`, rather than forwarding the
client-controlled application name, so it can be distinguished from DuckDB
`postgres_scanner` sessions in `pg_stat_activity`. A client with full metadata
database access can change its application name later, so this is operational
attribution, not an authorization control. Upstream availability does not feed
the control-plane health endpoint: one org's shard must not remove healthy
control-plane pods from service. Target resolution and upstream
connect/auth/synchronization share a 10-second bootstrap deadline; established
relay traffic has no inherited bootstrap deadline. An admin warehouse update
that includes `metadata_proxy_enabled` reloads the local snapshot and fans a
reload out to peer replicas. Established sessions observe a disabled gate on
their next five-second authorization recheck after that snapshot reaches the
replica.

Metadata-proxy cancellation intentionally closes the whole session, not only
the in-flight statement. On a matching synthetic backend key, the owning
control-plane replica closes the exact established frontend and upstream
connections. It does not redial the PgBouncer Service because cancellation
keys are local to the PgBouncer instance that accepted the session. A raw
PostgreSQL `CancelRequest` uses a new TCP connection and the NLB can route it to
a different control-plane replica, matching the existing control-plane
locality constraint. Such a high-range synthetic-key miss is absorbed and
counted with `outcome="not_local"`; the owning session remains active.

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
| `duckgres_postgres_session_start_total` | Counter | `org`, `outcome`, `reason` | Exactly one terminal authenticated PostgreSQL session-start result after server-side retries. |

Admission request outcomes are `granted`, `rejected`, `timeout`, `canceled`,
and `error`. `rejected` means the requested worker shape can never fit its hard
organization or user vCPU ceiling.
Session-start outcomes are `success`, `timeout`, `canceled`, `capacity`,
`draining`, and `error`.

The PostgreSQL terminal counter collapses those outcomes to `success` or
`failure`. Success always has `reason="none"`. Failure reasons are
`capacity`, `worker`, `metadata_store`, `control_plane`, `client`, `lifecycle`,
`canceled`, `transport`, and `unknown`. The first four represent failures an
operator can usually alleviate. The remaining reasons let alerts exclude bad
client input, planned lifecycle transitions, client disconnects, wire errors,
and newly added paths that have not yet been classified. `capacity` covers
runtime worker exhaustion and admission
timeouts; requests that exceed a configured hard org or user vCPU limit are
reported with reason `client`.

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
| `duckgres_query_total` | `org`, `status`, `reason` | One event per non-empty query attempt. Valid pairs: `success/none`; `failure/user`, `failure/canceled`, `failure/conflict`; `error/metadata_connection_lost`, `error/system`. |
| `duckgres_query_duration_seconds` | `org` | Query execution latency. Use `duckgres_query_total` for terminal result counts. |

The older fleet-level `duckgres_control_plane_worker_*` metrics remain useful
for process-wide worker counts, spawn time, and the approximate post-admission
worker queue. They have no org label and are not admission saturation metrics.

### Node-local cache proxy metrics

These worker-local metrics have no tenant labels. Exactly one
`duckgres_worker_cache_proxy_mode{mode}` series is `1`.

| Metric | Labels | Meaning |
|---|---|---|
| `duckgres_worker_cache_proxy_mode` | `mode` | Current cache mode: `cached`, `bypassed`, or `disabled`; the active mode is `1`. |
| `duckgres_worker_cache_proxy_bypass_transitions_total` | `reason` | Entries into bypass mode: `startup_unavailable`, `runtime_unavailable`, or `upstream_unavailable`. |
| `duckgres_worker_cache_proxy_bypassed_operations_total` | `reason` | Operations routed around the node-local cache. |
| `duckgres_worker_cache_proxy_reconnect_attempts_total` | None | Health checks made by the recovery supervisor. |
| `duckgres_worker_cache_proxy_recoveries_total` | None | Successful cache re-enablement events. |

### Cache proxy request-path metrics

These are emitted by the standalone `cache-proxy` binary itself (`cmd/cache-proxy`), on its own `HEALTH_ADDR` `/metrics` endpoint — a separate process and port from the control plane's `:9090`. The `duckgres_worker_cache_proxy_*` family above is the worker-side client wrapper's view; these are the proxy's own view of the requests it served.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `cache_proxy_request_duration_seconds` | Histogram | `path`, `source` | End-to-end duration of a served request. `path` is `block` (block-aligned cache path) or `forward` (uncached forward-proxy path); `source` is `local`, `peer`, or `s3` for `block`, and always `origin` for `forward`. |
| `cache_proxy_forward_requests_total` | Counter | `method` | Requests handled by the uncached forward-proxy path, by HTTP method. |
| `cache_proxy_inflight_requests` | Gauge | None | Requests currently being handled by the proxy's request entry point; the queue-depth signal. |
| `cache_proxy_hits_total` | Counter | None | Worker-facing cacheable requests served entirely from data already present on local NVMe. Peer API reads and requests that first fetch any block from a peer are excluded. |
| `cache_proxy_misses_total` | Counter | None | Worker-facing cacheable requests that require a peer or origin fill before they can be served. |
| `cache_proxy_bytes_served_total` | Counter | `source` | Directional byte mix by `local`, `peer`, or `s3`. Block mode counts assembled response bytes under the slowest source used; the legacy exact-range path counts the local read or deduplicated fill once, so this is not an exact client-egress counter. |
| `cache_proxy_peer_fetches_total` | Counter | None | Logical peer lookups in either mode. |
| `cache_proxy_peer_hits_total` | Counter | None | Successful peer body transfers. A block-mode request can record up to two transfers for one logical lookup. |
| `cache_proxy_peer_probes_total` | Counter | `outcome` | Physical `/cache/has` attempts only. In summary mode these are bounded confirmations of Bloom-positive or uncovered peers, never fleet-wide fanout. The per-request maximum is `CACHE_PEER_MAX_PROBES`. |
| `cache_proxy_peer_probes_skipped_total` | Counter | None | Summary-mode confirmations skipped because the pod-wide active `/cache/has` HTTP-request/socket budget was exhausted; those requests fall back to origin without queuing. This budget does not count total process goroutines. |
| `cache_proxy_summary_pulls_total` | Counter | `outcome` | Receiver-driven `GET /cache/summary` attempts by outcome, including success, not-modified, timeout, rejection, and size/read failures. |
| `cache_proxy_summary_serves_total` | Counter | `outcome` | Local summary endpoint and snapshot-build outcomes. |
| `cache_proxy_summary_resident_count` | Gauge | None | Current retained peer summaries. |
| `cache_proxy_summary_resident_bytes` | Gauge | None | Conservative total Bloom-state accounting: the fixed local counting filter, maximum snapshot/pull transient reserve, and retained remote summary bits. This is reserved/accounted memory, not measured process RSS. It is `0` outside summary mode. |
| `cache_proxy_summary_memory_limit_bytes` | Gauge | None | Effective `CACHE_SUMMARY_MEMORY_LIMIT_BYTES` ceiling used for the total Bloom-state accounting above. It is `0` outside summary mode. Alert when resident bytes approach this value. |
| `cache_proxy_summary_age_seconds` | Histogram | None | Age of summaries used in local Bloom lookups. |
| `cache_proxy_summary_lookups_total` | Counter | `outcome` | `no_valid_summary`, `no_positive`, or `positive_candidate`. |
| `cache_proxy_summary_confirmed_gets_total` | Counter | `outcome` | Peer body GET outcomes in summary mode. A GET is attempted only after an exact bounded `/cache/has` confirmation. |
| `cache_proxy_summary_bloom_items` | Gauge | None | Local live cache keys represented by the incrementally maintained counting Bloom filter. |
| `cache_proxy_summary_bloom_bits` / `cache_proxy_summary_bloom_hashes` | Gauge | None | Fixed Bloom-filter layout, sized for 1m keys at a 1% target false-positive rate. |
| `cache_proxy_summary_bloom_false_positive_ratio` | Gauge | None | Predicted per-peer false-positive ratio from live item count and filter layout. This rises smoothly after 1m keys; use fleet size to interpret aggregate request cost. |
| `cache_proxy_summary_bloom_saturated` | Gauge | None | `1` when the local live key count exceeds the 1m target capacity; snapshot refresh and serving continue. |
| `cache_proxy_summary_bloom_bit_occupancy_ratio` | Gauge | None | Fraction of local Bloom bits set; a direct saturation signal independent of the FPR estimate. |
| `cache_proxy_summary_bloom_additions_total` / `cache_proxy_summary_bloom_removals_total` | Counter | None | Cache commits and evictions applied incrementally to the local Bloom index. |
| `cache_proxy_summary_bloom_counter_saturations_total` | Counter | None | Counting-Bloom cells that reached `uint16` saturation and were made sticky to avoid false negatives. |
| `cache_proxy_summary_bloom_snapshots_total` / `cache_proxy_summary_bloom_snapshot_bytes` | Counter / Gauge | None | Immutable local Bloom snapshots prepared for the pull endpoint and their bounded raw-bit size. |

Cache-proxy metrics deliberately have no org label. Use the existing per-org
Duckgres query and worker-acquisition metrics for customer-facing rollout
guardrails, and use these proxy metrics for cache locality and amplification.

Local cache hit ratio:

```promql
sum(rate(cache_proxy_hits_total[5m]))
/
clamp_min(
  sum(rate(cache_proxy_hits_total[5m]))
    + sum(rate(cache_proxy_misses_total[5m])),
  1e-9
)
```

Average physical peer fanout per logical lookup:

```promql
sum(rate(cache_proxy_peer_probes_total[5m]))
/
clamp_min(sum(rate(cache_proxy_peer_fetches_total[5m])), 1e-9)
```

Successful peer body transfers per logical lookup (this can exceed `1` for a
multi-block request):

```promql
sum(rate(cache_proxy_peer_hits_total[5m]))
/
clamp_min(sum(rate(cache_proxy_peer_fetches_total[5m])), 1e-9)
```

For an org-affinity canary, pair those signals with the existing per-org
customer failure and worker-acquisition guardrails:

```promql
sum by (org) (
  rate(duckgres_query_total{status="error"}[5m])
)
/
clamp_min(
  sum by (org) (rate(duckgres_query_total[5m])),
  1e-9
)

sum by (org, outcome) (
  rate(duckgres_worker_acquire_total_seconds_count{outcome=~"capacity|error|canceled"}[5m])
)
/
on (org) group_left
clamp_min(
  sum by (org) (rate(duckgres_worker_acquire_total_seconds_count[5m])),
  1e-9
)
```

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

Current metadata proxy connections by org and control-plane pod:

```promql
sum by (org, pod) (duckgres_metadata_proxy_connections_open)
```

Metadata proxy target-phase failure ratio:

```promql
sum by (org) (
  rate(duckgres_metadata_proxy_connection_attempts_total{
    outcome=~"target_resolution_error|upstream_(connect|sync|hijack)_error|cancel_key_error"
  }[5m])
)
/
clamp_min(
  sum by (org) (
    rate(duckgres_metadata_proxy_connection_attempts_total{
      outcome=~"success|target_resolution_error|upstream_(connect|sync|hijack)_error|cancel_key_error"
    }[5m])
  ),
  0.001
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
