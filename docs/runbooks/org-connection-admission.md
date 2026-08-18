# Org Connection Admission

The multitenant control plane admits each connection by its resolved worker
vCPUs and memory. `duckgres_orgs.max_vcpus` and `duckgres_orgs.max_memory` are
the org ceilings, while `duckgres_org_users.max_vcpus` is the user vCPU
ceiling. CPU and memory are independent: neither limit derives the other.
`max_memory` is a Kubernetes quantity such as `240Gi`; empty or `0` means
unlimited. Newly provisioned orgs default to unlimited memory.

Admission charges the worker shape resolved from client options, org defaults,
deployment defaults, or built-in defaults. Only live session leases count.
Hot-idle workers do not count, just as they do not count toward `max_vcpus`.
For example, two simultaneous `15` vCPU / `120Gi` sessions require ceilings of
at least `30` vCPUs and `240Gi`; setting either ceiling lower blocks the second
session.

## Admission invariants

- A PostgreSQL advisory transaction lock gives each org one admission writer at
  a time across all control-plane replicas.
- Limits and the user `disabled` flag are read from PostgreSQL in that
  transaction. Admission-relevant config updates take the same lock.
- Pending requests are ordered by `(enqueued_at, request_id)`. A user at its
  limit may be skipped without allowing a later request from that same user to
  pass it. An org-capacity block is not bypassed by a smaller later request.
- Admission selection can create only the caller's lease and reject only the
  caller's request. Serialized housekeeping may prune expired or inactive
  foreign rows, but never grants or reserves capacity for them.
- A request larger than its hard org memory/vCPU or user vCPU ceiling is rejected. Temporary
  saturation remains queued until capacity becomes available or the request
  times out.
- Lowering a limit below current usage does not terminate existing sessions.
  Existing leases remain active and new requests wait until usage falls within
  both ceilings.
- Resharding takes the same org lock. No lease can be granted after the
  ready-to-resharding transition commits.

If Alice and Bob contend, either goroutine may acquire the org lock first. The
transaction determines the eligible queue head, but creates a lease only when
that head is the polling request. Otherwise the caller remains queued and the
head's owner admits itself on its next poll.

During a rolling deployment from the previous admission implementation, old
replicas may still grant a foreign queue head, evaluate limits from their local
config snapshot, and grant leases without recording requested memory. Capacity
remains protected by the shared org lock for limits every replica understands,
but the strict request-owned, authoritative-limit, and memory-limit invariants
begin only after every old replica has exited. Keep `max_memory` unlimited until
all control-plane replicas run the new version and all legacy active leases
whose `requested_memory_bytes` is zero have drained. A new replica deliberately
fails closed on such an unknown active lease when a non-zero memory limit is
enabled. Avoid changing vCPU limits during the overlap when an exact change
boundary matters.

After any org has a non-zero `max_memory`, do not roll an old control-plane
binary back into the fleet: it does not enforce the cap and writes leases with
unknown memory. Prefer roll-forward. If rollback is unavoidable, first clear
every `max_memory` cap, complete the rollback, and leave the caps unlimited.
After rolling forward again, wait until every replica is upgraded and every
zero-memory active lease has drained before re-enabling the caps.

The connection queue timeout is configured by
`DUCKGRES_WORKER_QUEUE_TIMEOUT` (default `60s`). Owners poll every `100ms` while
waiting. Client disconnect, PostgreSQL cancellation, and control-plane drain
cancel the owning admission context and submit its exact
`(request, org, control-plane instance)` identity to the control-plane-wide
admission reclaimer.

## Failure recovery

- Each live control-plane instance has one admission reclaimer shared by all
  orgs. Before enqueue, it reserves one cleanup-ownership slot, from a default
  capacity of 4096 configured by
  `DUCKGRES_ADMISSION_RECLAIMER_MAX_RESERVATIONS` (or
  `admission_reclaimer_max_reservations` in YAML); the
  same slot stays attached to the request and then its live lease. If all slots
  are occupied, a new connection is rejected before PostgreSQL is mutated.
  This bounds retained memory without ever dropping an older cleanup. It
  retains activated cleanup intents before attempting PostgreSQL, retries
  transient or ambiguous failures with bounded-duration attempts and jittered
  backoff, and removes an intent only after the idempotent database transaction
  succeeds. Removing one org stack does not stop this control-plane-wide
  reclaimer.
- The reclaimer is the normal cleanup path for canceled requests and released
  leases while their owner is alive. It atomically removes the exact queue and
  lease rows under the org admission lock; it cannot mutate a row belonging to
  another org or control-plane instance.
- A crashed control plane loses its in-memory reclaimer. The liveness janitor
  first marks that control-plane instance expired; a later serialized admission
  for each affected org then removes rows owned by the expired instance. This
  expired-owner path is crash recovery, not the routine release path.
- Expired requests and requests owned by an inactive control plane are removed
  during admission and drain checks, so an abandoned head cannot block the
  queue indefinitely.
- A committed lease is authoritative. Its queue row is only a lifecycle mirror
  and is removed with the lease when the session ends.
- If admission is blocked, inspect active leases and unexpired queue rows for
  the org, including `requested_vcpus` and `requested_memory_bytes`. Confirm the
  owning control-plane instance is active before removing any row manually.
- Do not delete a lease for a live session. If an owner is gone, expire its
  control-plane runtime record and let the serialized cleanup path reclaim its
  admission rows.

Monitor `duckgres_session_admission_reclaim_pending` for activated cleanup work,
`duckgres_session_admission_reclaim_attempts_total{outcome}` for cleanup-attempt
outcomes,
and the ratio of `duckgres_session_admission_reclaim_reservations_in_use` to
`duckgres_session_admission_reclaim_reservation_capacity` for ownership
headroom.
`duckgres_session_admission_reclaim_reservation_rejections_total{reason}` records
requests rejected before enqueue. Diagnose sustained backlog growth or high
reservation utilization together with reclaim error rate; a continuously
non-zero pending count can be healthy during steady connection churn. Reclaim
logs include the request, org, retry count, and age; the metrics deliberately
omit request and org labels.

The org-labeled admission queue and active-resource gauges are logical local
contributions, not exact durable row counts. Active resources drop when cleanup is
transferred to the reclaimer, before the durable lease row is necessarily
deleted. Use the reclaim backlog and attempt metrics above when that distinction
matters; [the metrics reference](../metrics.md) documents aggregation rules.

Monitor `duckgres_session_admission_active_memory_bytes` with `sum by (org)`
across replicas and `duckgres_session_admission_limit_memory_bytes` with
`max by (org)`. The corresponding vCPU gauges use the same aggregation rules.

For local verification, run `just test-configstore-integration`; it exercises
cross-replica ordering, cancellation races, eventual live-owner reclamation,
hard-limit rejection, resharding, and stale-owner cleanup against PostgreSQL.
