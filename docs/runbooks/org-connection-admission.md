# Org Connection Admission

The multitenant control plane admits each connection by its requested worker
vCPUs. `duckgres_orgs.max_vcpus` is the org ceiling and
`duckgres_org_users.max_vcpus` is the user ceiling; `0` means unlimited.

## Scheduler invariants

- A PostgreSQL advisory transaction lock gives each org one admission writer at
  a time across all control-plane replicas.
- Limits and the user `disabled` flag are read from PostgreSQL in that
  transaction. Admission-relevant config updates take the same lock.
- Pending requests are ordered by `(enqueued_at, request_id)`. A user at its
  limit may be skipped without allowing a later request from that same user to
  pass it. An org-capacity block is not bypassed by a smaller later request.
- One scheduler pass can create at most 64 durable offers. An offer reserves
  capacity for 5 seconds, but only the request's owning control plane can turn
  it into an active lease.
- Active leases and unexpired offers both consume the org and user vCPU
  budgets. Release, cancellation, owner expiry, and offer expiry return
  capacity.
- Resharding takes the same org lock. No offer can be claimed after the
  ready-to-resharding transition commits.

If Alice and Bob contend, either goroutine may acquire the org lock first. The
winner performs a bounded FIFO offer pass for all eligible committed requests
visible to that transaction, which may include the other contender, then
claims only its own offer. It never creates the other request's active lease.
Each other owner claims its durable offer on its next poll.

The connection queue timeout is configured by
`DUCKGRES_WORKER_QUEUE_TIMEOUT` (default `60s`). Owners poll every `100ms` while
waiting. The offer TTL and batch size above are internal fixed defaults.

## Two-phase rollout and activation

Durable offers raise the minimum compatible control-plane version. Activation
is therefore explicit and is never performed by ordinary connection traffic.

1. Roll the offer-aware binary to every control-plane replica while offers are
   still disabled. During this phase it uses the legacy request-owned grant
   path.
2. Verify the deployment inventory is entirely on the offer-aware image. Also
   verify every live or draining row in the runtime `cp_instances` table has
   `supports_admission_offers = true`.
3. Let old sessions and stale admission rows drain. The activation endpoint
   returns `409 Conflict` while a live incompatible control plane, or a queue or
   lease row with an incompatible/missing owner, remains.
4. As an admin, activate once:

   ```bash
   curl -X POST \
     -H "X-Duckgres-Internal-Secret: ${DUCKGRES_INTERNAL_SECRET}" \
     https://<admin-host>/api/v1/admission/offers/activate
   ```

   A successful or repeated activation returns `200` with
   `{"offers_enabled":true}`.

The activation transaction locks the protocol singleton, fences concurrent
control-plane registration, rechecks compatibility, and then enables offers.
Database triggers reject queue or lease inserts from an inactive or
offer-unaware owner after the cutover.

## Rollback

Before activation, a normal rolling rollback to the prior binary is safe.
After activation, roll back only to an offer-aware revision. A pre-offer binary
does not understand durable reservations and its runtime heartbeat is rejected
by the database fence.

An emergency rollback below that compatibility floor requires a maintenance
window:

1. Stop ingress and scale every control-plane replica to zero.
2. Drain or terminate every live session and verify no control plane remains
   running.
3. Record queue and lease counts for diagnosis.
4. In one transaction, delete the runtime org-connection queue and lease rows,
   then set the singleton's `offers_enabled` to `false` and clear
   `enabled_at`. Use the runtime schema reported by the control plane; do not
   drop the whole runtime schema because it also contains worker and Flight
   coordination state.
5. Deploy the old binary with fresh control-plane instance IDs and verify
   heartbeats and admission before reopening ingress.

Never reset the protocol while sessions are live. Queue and lease state is
rebuildable only when connection disruption is acceptable.

## Failure recovery

- An offered owner that is still healthy retries its claim on the next poll.
- An expired offer returns to pending and can be offered again.
- Pending/offered requests belonging to a dead owner are deleted during a
  scheduler or drain pass.
- A committed active lease is authoritative; its queue row is only a lifecycle
  mirror and must not be reset to pending while the lease exists.
- If activation reports incompatible queue rows after all old replicas are
  gone, wait for their queue timeout and run an admission/drain pass for the
  affected org. Remove rows manually only after confirming their owners and
  sessions are dead.
