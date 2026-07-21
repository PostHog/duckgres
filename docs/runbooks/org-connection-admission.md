# Org Connection Admission

The multitenant control plane admits each connection by its requested worker
vCPUs. `duckgres_orgs.max_vcpus` is the org ceiling and
`duckgres_org_users.max_vcpus` is the user ceiling; `0` means unlimited.

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
- A request larger than its hard org or user ceiling is rejected. Temporary
  saturation remains queued until capacity becomes available or the request
  times out.
- Resharding takes the same org lock. No lease can be granted after the
  ready-to-resharding transition commits.

If Alice and Bob contend, either goroutine may acquire the org lock first. The
transaction determines the eligible queue head, but creates a lease only when
that head is the polling request. Otherwise the caller remains queued and the
head's owner admits itself on its next poll.

During a rolling deployment from the previous scheduler, old replicas may
still grant a foreign queue head and evaluate limits from their local config
snapshot. Capacity remains protected by the shared org lock, but the strict
request-owned and authoritative-limit invariants begin only after every old
replica has exited. Avoid changing vCPU limits during that overlap when an
exact change boundary matters.

The connection queue timeout is configured by
`DUCKGRES_WORKER_QUEUE_TIMEOUT` (default `60s`). Owners poll every `100ms` while
waiting. Client disconnect, PostgreSQL cancellation, and control-plane drain
cancel the owning admission context and remove its durable queue row.

## Failure recovery

- Expired requests and requests owned by an inactive control plane are removed
  during admission and drain checks, so an abandoned head cannot block the
  queue indefinitely.
- A committed lease is authoritative. Its queue row is only a lifecycle mirror
  and is removed with the lease when the session ends.
- If admission is blocked, inspect active leases and unexpired queue rows for
  the org. Confirm the owning control-plane instance is active before removing
  any row manually.
- Do not delete a lease for a live session. If an owner is gone, expire its
  control-plane runtime record and let the serialized cleanup path reclaim its
  admission rows.

For local verification, run `just test-configstore-integration`; it exercises
cross-replica ordering, cancellation races, hard-limit rejection, resharding,
and stale-owner cleanup against PostgreSQL.
