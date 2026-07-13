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
| Operation row + verbose log (config store) | `controlplane/configstore/reshard.go`, migrations `000018` + `000021` (backup URI) |
| Runner (claims + executes ops) | `controlplane/provisioner/reshard_runner.go` |
| Catalog copier (schema + rows + verify) | `controlplane/provisioner/catalog_copy.go` |
| Pre-flip catalog backuper (pg_dump → S3) | `controlplane/provisioner/catalog_backup.go` |
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
blocking → draining → pausing_compaction → backup_catalog → cutover → copying
        → verifying → cleaning_up → finalizing
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
4. **backup_catalog** — the safety net (§ Pre-flip catalog backup below). Dump
   the SOURCE catalog to the org's own S3 data bucket BEFORE any flip. Runs for
   every direction; the source is quiescent here (post-drain, compaction paused)
   and `recordSource` (which the runner runs just before this step) has resolved
   the source endpoint + password.
5. **cutover** — record the FULL source connection info from CR status first,
   then patch `cnpgShard` (and `type` for ext→cnpg; the external block is
   left in spec, ignored). Wait: CR status endpoint on the target pooler,
   Ready condition, `SELECT 1` with tenant creds. Timeout → rollback. The
   wait is bounded per-op by `cutover_timeout_seconds` (0 = default 15m /
   `DUCKGRES_RESHARD_FLIP_TIMEOUT`); real cnpg cutovers need minutes —
   provider-sql role/DB creation plus cnpg SASL credential propagation.
6. **copying** — source read via its recorded pre-flip endpoint (the orphaned
   role/DB survives a shard flip; an ext source is reached direct-to-RDS with
   TLS because the flip deletes its ESO sync + pgbouncer). One
   REPEATABLE READ read-only transaction = consistent snapshot; faithful DDL
   from `pg_catalog` introspection; rows via raw binary COPY passthrough;
   constraints then non-constraint indexes (PK-backed indexes excluded — ADD
   CONSTRAINT already made them). No sequences exist in the DuckLake catalog
   (next-ids are rows). A `pg_advisory_lock` in the TARGET database fences a
   zombie ex-runner for copy+verify.
7. **verifying** — per-table counts (snapshot vs target), then re-read the
   source OUTSIDE the transaction: any change means a concurrent writer
   (straggler compactor, stray client) → rollback.
8. **cleaning_up** — **cnpg sources only**: `DROP DATABASE … WITH (FORCE)`
   via the source pooler (`dbname=postgres`; the tenant role owns the DB).
   Also the fence that makes any straggler writer fail loudly. Failure is
   ERROR-logged but doesn't fail the op. The role stays (cannot drop itself).
   **External sources are never modified.**
9. **finalizing** — restore compaction exactly, reconcile the warehouse
   config-store row (kind/endpoint/secret fields), CAS `resharding→ready`,
   write the **report** (maintenance duration = blocked→unblocked, per-op
   counters: tables/rows/bytes) into the log. New sessions cold-spawn workers
   that read the new CR status; no cache invalidation needed
   (`migrationChecked` is orgID-keyed and endpoint-independent;
   `ducklake.migrations` is DSN-keyed → new endpoint = new entry).

## Pre-flip catalog backup (safety net)

Before a reshard mutates the catalog, the runner dumps the **source** catalog to
the org's **own** S3 data bucket, so a catalog damaged or lost at the flip is a
one-command restore away rather than an RDS-PITR archaeology dig. Only the
Postgres catalog metadata is ever at risk in a reshard (the S3 parquet data is
never touched), and this backs up exactly that. Layer C of defense-in-depth:
independent of the live copy/verify and of orphan-adopt — the artifact survives
even if both live copies are damaged, and covers all directions.

- **What**: `pg_dump --format=custom --no-owner --schema=public` of the source
  (the `ducklake_*` tables). Custom format → restore with stock `pg_restore`.
  The DB password is passed via `PGPASSWORD` env, **never argv**. Source DSN is
  the same the copy step uses (`o.source`, recorded pre-flip: external →
  `sslmode=require` direct to RDS, cnpg → `sslmode=disable` via the pooler).
  `catalog_backup.go::PGCatalogBackuper`.
- **Where**: `s3://<org-data-bucket>/_reshard_catalog_backups/op-<opID>-<UTC
  timestamp>.dump`. Bucket + region come from the duckling CR status
  (`status.dataStore.bucketName` / `s3Region`); the reserved
  `_reshard_catalog_backups/` prefix keeps backups clear of DuckLake's parquet.
- **Creds**: the upload assumes the org's **own** IAM role
  (`status.iamRoleARN`) via the same STS broker the worker activator uses for
  DuckLake S3 access — the CP writes to the org bucket with short-lived,
  org-scoped credentials, no worker required. The broker is injected into the
  runner from `multitenant.go` (the STS broker lives in the `controlplane`
  package; the runner takes an `AssumeRoleFunc` to avoid the import cycle).
- **Gate strength by direction**:
  - **cnpg→external (destructive)**: a dump- or upload-failure **fails the op
    before the flip** — never destroy the source without a durable snapshot.
  - **ext→cnpg, cnpg→cnpg (non-destructive)**: the source is left
    intact/orphaned, so the backup is belt-and-suspenders — a failure logs a
    warning and the op continues.
- **Recorded + logged**: the artifact's `s3://` URI lands on the op row
  (`backup_s3_uri`, migration `000021`) and in the op log, alongside the exact
  `pg_restore` command an operator can run.

### Restore procedure

The op log carries the ready-to-run command (host/user/db name the recorded
source; the password is a `<PASSWORD>` placeholder — supply the target catalog
DB's password). To recover a catalog into a target DB:

```
aws s3 cp s3://<org-data-bucket>/_reshard_catalog_backups/op-<id>-<ts>.dump ./catalog.dump
PGPASSWORD=<password> pg_restore --no-owner --clean --if-exists --format=custom \
  --host=<catalog-host> --port=5432 --username=<user> --dbname=<db> ./catalog.dump
```

Point `--dbname` at whichever catalog DB needs the data — usually the org's
CURRENT catalog after a bad flip. `--clean --if-exists` makes the restore
idempotent (drops each `ducklake_*` object before recreating it).

### Retention

Backups are **kept**, not deleted on success — a subtly corrupted catalog can be
noticed days later, and the escape-hatch direction is exactly when you want the
net to persist. Every object carries the tag `duckgres-reshard-catalog-backup=1`
and lives under the `_reshard_catalog_backups/` prefix, so a single S3 lifecycle
rule expires them without touching DuckLake data. **Suggested rule** (on the
per-org data bucket, or a bucket-wide default): expire objects under prefix
`_reshard_catalog_backups/` after **30 days**. A catalog is tens of MB to
~100 MB, so even frequent reshards stay negligible against the parquet data —
but the lifecycle rule guarantees they never accumulate unbounded. No in-app GC
is built; the lifecycle rule is the retention mechanism.

## cnpg→external (escape hatch): copy-before-flip

The type flip DELETES the cnpg source role/DB, so this direction inverts the
order: `blocking → draining → pausing_compaction → backup_catalog → copying →
verifying → cutover → finalizing`. The flip IS the source cleanup and only runs
after verify passed. `backup_catalog` still runs first, and for this
destructive direction it is a **hard prerequisite** (see below).

The target password is **ephemeral**: sent once in the start request,
validated by connecting, handed in-process to the local runner, never
persisted to the op row/log/audit — the row stores the AWS SM secret NAME
only. Because the password lives only in the creating replica's memory, the op
must be run by THAT replica. This is **claim-on-create** (real, not just a
comment): the admin start handler creates the op via
`CreateReshardOperationClaimed(op, cpID)`, a single insert that lands the row
already `running` and owned by the local CP (`runner_epoch=1`,
`heartbeat_at=now`), then hands it straight to the local runner via
`AdoptClaimedOperation(op, password)` — which stashes the password and launches
execution on the runner's LIFECYCLE context (not the HTTP request context,
which dies with the 202 response). A create-claimed op has a fresh-heartbeat
`running` row owned by the local CP, so no sibling replica's scanOnce loop can
list or claim it. (The prior code created the op `pending` and only stashed the
password locally; on a multi-replica CP a *different* replica's scanOnce would
win the op ~2/3 of the time and fail the copy — "external target password is
not available to this runner". Fixed.) cnpg targets are create-claimed too when
a local runner exists (lower latency, uniform path); with no local runner they
fall back to a `pending` op any replica may claim (no password needed).
Consequence for external targets: a genuine crash-takeover mid-copy still
cannot resume (password lost with the dead replica's memory); the new runner
fails the op with a clear re-run message, and — because cnpg→ext copies BEFORE
the flip — a pre-flip failure leaves the cnpg source untouched. After the flip,
the runner checks the ESO-synced status password matches the provided one
(catches a wrong/missing SM secret).

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
readable and would hang the cutover on an ESO AccessDenied. Defenses: the reshard form documents the convention
(`duckling-<name>-<env>-<region>-rds-password`) and warns live on suspicious
names (`classifySecretName`), and the start handler enforces a **positive
allowlist** — the SM secret name MUST start with `posthog-` or `duckling-`
(`esoReadableSecretPrefixes`), else 400. RDS-managed names (`^rds[!/]`,
`rdsManagedSecretNamePattern`) are detected first and get a more specific
message; any other non-allowlisted name gets the general allowlist message.
The two prefixes are identical across every env, so they're hardcoded (not
resolved per-env). If a name that slips through is
still unreadable by ESO, the cutover simply waits for the status password
until its per-op timeout and then recovers (below) — the op log shows the
generic "waiting for external target" line. (Surfacing the ExternalSecret's
own AccessDenied into the op log would need an `external-secrets.io` read
grant on the CP SA, which it does not have; not worth the extra RBAC.)

**Pre-flight connection check (fail before the destructive flip).** The
cnpg→ext flip is destructive — Crossplane DELETEs the cnpg source role/DB on
the TYPE change, so a doomed external target that only fails *after* the flip
forces the flip-back + copy-back recovery, which can wedge a Crossplane Role
MR for a long time. Two submit-time gates make a doomed op fail fast with a
400 (nothing created): Fix 1 above (the ESO-readable name allowlist), and a
bounded `SELECT 1` against the target Postgres (`ExternalTargetProber`, an
adapter over `provisioner.PGCatalogCopier.Probe` wired in `multitenant.go`)
that proves endpoint reachability + the user/database/password all work,
before any maintenance window or flip. External RDS uses `sslmode=require`
(matches the runner's copy). The whole check is bounded (~8s) so a black-holed
endpoint can't hang the handler; a timeout counts as a connect failure. The
400 message is redacted (never echoes the password). The prober nil-degrades
in tests / non-k8s builds — the check is then skipped and the runner's copy
step still catches a bad credential later (fail-fast optimization, not the
only line of defense). When the prober IS present and the connect fails, the
op is refused.

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
  Claim-on-create (see the cnpg→external section) creates the op already at
  `runner_epoch=1`, owned by the creating CP — a fresh-heartbeat running row no
  sibling replica can steal, which is what keeps the ephemeral external
  password with the one runner that has it.
- An org is never stranded: every failure path restores `resharding→ready`
  (or logs at ERROR if it can't — that's the page-someone signal).

## Testing

Unit: `configstore` (tests/configstore/reshard_postgres_test.go — claim CAS,
takeover fencing, cancel, log pagination, the grant-path gate),
`provisioner/reshard_runner_test.go` (all three directions, rollbacks, cancel,
ephemeral-password loss, the pre-flip backup: recorded before the flip, the
destructive-direction hard-fail-before-flip, the non-destructive
warning-and-continue), `admin/reshard_test.go` (validation incl. the
ESO-name allowlist + the pre-flight connection-check pass/fail/skip, secrets
never persisted). The `backup_s3_uri` column is asserted in
`tests/configstore/migrations_postgres_test.go` (migration `000021`). e2e
(`tests/mw-dev/e2e/harness.sh`): validation 400s (incl. a
non-allowlisted external secret name),
cancel-during-drain (drain-not-kill + 57P03 visible), bogus-shard rollback
(real flip → Synced=False → rollback, data intact), and the **ext→cnpg
positive path** (real catalog move off the harness RDS onto shard-001), which
also asserts the pre-flip backup completed and recorded a `backup_s3_uri` under
`_reshard_catalog_backups/` (the Job has no aws CLI, so it asserts the recorded
URI + restore command in the op log, not an S3 list — same limitation as the
tenant-isolation object-store half).
cnpg→cnpg positive path needs a second mw-dev shard (follow-up infra);
cnpg→ext positive path is unit-test-only (the harness has no RDS password —
the start API requires it ephemerally).
