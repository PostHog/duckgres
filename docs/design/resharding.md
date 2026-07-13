# Resharding: metadata-store migrations (design)

Operator-driven migration of an org's DuckLake metadata catalog between
metadata stores, driven from the admin console with a verbose live log.
Supported directions:

- **cnpg shard → cnpg shard** (rebalancing tenants between shards)
- **external (RDS) → cnpg shard** (onboarding onto shared shards)
- **cnpg shard → external (RDS)** — the emergency **escape hatch**: if cnpg
  itself turns out bad, orgs must not be trapped on it.

Only the Postgres DuckLake catalog (`ducklake_*` tables, one database per
org) moves. **S3 parquet data never moves** — the catalog holds pointers, and
those stay valid because the bucket is untouched.

## Pieces

| Piece | Where |
|---|---|
| Operation row + verbose log (config store) | `controlplane/configstore/reshard.go`, migration `000018` |
| Runner (claims + executes ops) | `controlplane/provisioner/reshard_runner.go` |
| Catalog copier (schema + rows + verify) | `controlplane/provisioner/catalog_copy.go` |
| Duckling CR patches (flip, compaction pause) | `controlplane/provisioner/k8s_client.go` |
| Admin REST API (start/read/cancel + `GET /reshards/targets` destination discovery: all cnpg shards incl. empty ones via the cluster-topology pod read, RBAC-degrading to occupied shards; known external stores from live warehouse rows) | `controlplane/admin/reshard.go` |
| Console UI (form + operation page with live log) | `admin/ui/src/pages/ReshardForm.tsx`, `ReshardOperation.tsx` |
| Connection gates | `controlplane/control.go` (57P03), `flight_ingress.go`, grant-path check in `configstore/org_connections.go` |

The cutover primitive underneath is `spec.metadataStore.cnpgShard` on the
Duckling CR (charts #12918): changing it re-points the provider-sql
Role/Database at the target shard's ProviderConfig **in place** — same
role/database name (pinned `pgIdentPrefix`), same pinned password, old shard's
role/DB **orphaned, not dropped**, no data copied. A **type** flip
(cnpg↔external) is different: the composition stops rendering the cnpg MRs and
Crossplane **deletes** the cnpg role/DB (`managementPolicies: ["*"]`).

## Step machine (→cnpg targets)

```
blocking → draining → pausing_compaction → cutover → copying → verifying
        → cleaning_up → finalizing
```

1. **blocking** — CAS warehouse `ready→resharding` inside a transaction
   holding the org's connection advisory lock. LOAD-BEARING: the lease-GRANT
   transaction checks warehouse state under the same lock
   (`org_connections.go`), so after the CAS commits no new connection lease
   can ever be granted. The connect-time 57P03 gates (PG wire + Flight) are
   UX only — a request that passed them can be granted a lease up to a
   queue-timeout later, which is why the grant-path check exists. The runner
   then waits 2× the config poll interval so every CP replica's snapshot
   shows the state.
2. **draining** — wait until, in one transaction, the org has zero active
   connection leases AND zero queued requests; then until zero live worker
   records. Never kills a running query. Hot-idle workers that linger past a
   grace are retired via the standard CAS retire path (what the janitor's TTL
   reap uses) — never raw pod deletes. Workers must be GONE because each one
   runs a `DuckLakeCheckpointer` that writes the catalog independent of
   sessions. Parked reconnectable Flight sessions would hold leases up to the
   token TTL (1h); each CP destroys its own parked (no txn/stream/query)
   Flight sessions for resharding orgs on the session reap tick.
3. **pausing_compaction** — always patch
   `spec.ducklake.maintenance.compaction.enabled=false`, recording key-presence
   + prior value (restored exactly at the end; key-absent differs from false
   because the chart's name-list can enable compaction when the key is
   absent). Residual window: an already-running millpond Job (≤30m, source
   endpoint baked in) can outlive the pause — covered by the verify step and
   the cleanup fence.
4. **cutover** — record the FULL source connection info from CR status first,
   then patch `cnpgShard` (and `type` for ext→cnpg; the external block is
   left in spec, ignored). Wait: CR status endpoint on the target pooler,
   Ready condition, `SELECT 1` with tenant creds. Timeout → rollback. The
   wait is bounded per-op by `cutover_timeout_seconds` (0 = default 15m /
   `DUCKGRES_RESHARD_FLIP_TIMEOUT`); real cnpg cutovers need minutes —
   provider-sql role/DB creation plus cnpg SASL credential propagation.
5. **copying** — source read via its recorded pre-flip endpoint (the orphaned
   role/DB survives a shard flip; an ext source is reached direct-to-RDS with
   TLS because the flip deletes its ESO sync + pgbouncer). One
   REPEATABLE READ read-only transaction = consistent snapshot; faithful DDL
   from `pg_catalog` introspection; rows via raw binary COPY passthrough;
   constraints then non-constraint indexes (PK-backed indexes excluded — ADD
   CONSTRAINT already made them). No sequences exist in the DuckLake catalog
   (next-ids are rows). A `pg_advisory_lock` in the TARGET database fences a
   zombie ex-runner for copy+verify.
6. **verifying** — per-table counts (snapshot vs target), then re-read the
   source OUTSIDE the transaction: any change means a concurrent writer
   (straggler compactor, stray client) → rollback.
7. **cleaning_up** — **cnpg sources only**: `DROP DATABASE … WITH (FORCE)`
   via the source pooler (`dbname=postgres`; the tenant role owns the DB).
   Also the fence that makes any straggler writer fail loudly. Failure is
   ERROR-logged but doesn't fail the op. The role stays (cannot drop itself).
   **External sources are never modified.**
8. **finalizing** — restore compaction exactly, reconcile the warehouse
   config-store row (kind/endpoint/secret fields), CAS `resharding→ready`,
   write the **report** (maintenance duration = blocked→unblocked, per-op
   counters: tables/rows/bytes) into the log. New sessions cold-spawn workers
   that read the new CR status; no cache invalidation needed
   (`migrationChecked` is orgID-keyed and endpoint-independent;
   `ducklake.migrations` is DSN-keyed → new endpoint = new entry).

## cnpg→external (escape hatch): copy-before-flip

The type flip DELETES the cnpg source role/DB, so this direction inverts the
order: `blocking → draining → pausing_compaction → copying → verifying →
cutover → finalizing`. The flip IS the source cleanup and only runs after
verify passed.

The target password is **ephemeral**: sent once in the start request,
validated by connecting, handed in-process to the local runner
(claim-on-create), never persisted to the op row/log/audit — the row stores
the AWS SM secret NAME only. Consequence: a crash-takeover mid-copy cannot
resume (password lost); the new runner fails the op with a clear re-run
message. After the flip, the runner checks the ESO-synced status password
matches the provided one (catches a wrong/missing SM secret).

**The SM secret name and value are constrained by the ESO wiring.** After the
flip, the composition renders an ExternalSecret
(`duckling-<name>-password` in the duckling namespace, store
`aws-cluster-secret-store`) whose `remoteRef.key` is the typed secret NAME
with no `property` — ESO copies the **whole secret value verbatim** into the
k8s Secret key `password`, so the value must be the **raw password string**
(JSON like `{"password": …}` breaks the equality check). The external-secrets
IAM role can only `GetSecretValue` on a per-env name-prefix allowlist —
`posthog-*` and `duckling-*` in every managed-warehouse env (see the
`external-secrets-pod-identity` terraform module in posthog-cloud-infra) —
so an RDS-managed master secret (`rds!db-…` / `rds/…/master`) is NEVER
readable and would hang the cutover on an ESO AccessDenied. Defenses, outer
to inner: the reshard form documents the convention
(`duckling-<name>-<env>-<region>-rds-password`) and warns live on suspicious
names (`classifySecretName`); the start handler 400s names matching
`^rds[!/]` (`rdsManagedSecretNamePattern`); and during the cutover wait the
runner reads the tenant's ExternalSecret (best-effort, via the duckling
client's dynamic client — `ExternalSecretSyncError`) and, while the status
password is still empty, surfaces a failing sync condition (reason+message,
e.g. the AccessDenied) into the op log at warn, deduped by content. A
diagnostic read error (RBAC Forbidden — the CP has no `external-secrets.io`
grant today, CRD absent, object not rendered yet) degrades quietly to the
generic waiting line and never fails the op.

**Recovery if the flip goes bad** (ESO secret missing/mismatched): flip back
to the source shard — provider-sql re-creates the role/DB **empty** — then
copy back from the external target, whose data passed verify. The one path
where the source is rebuilt rather than left untouched.

## Rollback, cancel, crash-safety

- Rollback never removes `spec.metadataStore.cnpgShard` — it patches the
  source VALUE back (key-absent would fall through to the freshly-stamped
  bogus status pin). ext→cnpg rollback patches `{type: external, cnpgShard:
  null}` (the XRD's CEL forbids the key on external).
- Cancel (`POST /reshards/:id/cancel`): flag checked between steps and inside
  every wait loop; rolls back like a failure at that step → `cancelled`.
  Pending (unclaimed) ops finish immediately. On cnpg→ext, cancel is refused
  once the flip started (forward-or-recovery only).
- Runner fencing: claim bumps `runner_epoch`; every runner write is CAS-fenced
  on (runner, epoch); heartbeat ~30s; an op with a >5m-stale heartbeat is
  claimable (takeover). The target-DB advisory lock fences the copy itself.
- An org is never stranded: every failure path restores `resharding→ready`
  (or logs at ERROR if it can't — that's the page-someone signal).

## Testing

Unit: `configstore` (tests/configstore/reshard_postgres_test.go — claim CAS,
takeover fencing, cancel, log pagination, the grant-path gate),
`provisioner/reshard_runner_test.go` (all three directions, rollbacks, cancel,
ephemeral-password loss), `admin/reshard_test.go` (validation, secrets never
persisted). e2e (`tests/mw-dev/e2e/harness.sh`): validation 400s,
cancel-during-drain (drain-not-kill + 57P03 visible), bogus-shard rollback
(real flip → Synced=False → rollback, data intact), and the **ext→cnpg
positive path** (real catalog move off the harness RDS onto shard-001).
cnpg→cnpg positive path needs a second mw-dev shard (follow-up infra);
cnpg→ext positive path is unit-test-only (the harness has no RDS password —
the start API requires it ephemerally).
