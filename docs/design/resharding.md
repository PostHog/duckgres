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
| Operation row + verbose log (config store) | `controlplane/configstore/reshard.go`, migrations `000018` + `000021` (backup URI) + `000022` (password pull URL) |
| Runner (the step machine, executed inside the dedicated per-op pod) | `controlplane/provisioner/reshard_runner.go` |
| Runner pod entrypoint (`--mode reshard-runner`) | `controlplane/reshard_runner_mode.go` |
| Runner pod spawner (CP-side) | `controlplane/reshard_pod.go` |
| Runner pod reconciler (leader-only respawn/reap) | `controlplane/reshard_reconciler.go` |
| Catalog copier (schema + rows + verify) | `controlplane/provisioner/catalog_copy.go` |
| Pre-flip catalog backuper (pg_dump → streamed S3 multipart upload) | `controlplane/provisioner/catalog_backup.go` |
| Duckling CR patches (flip, compaction pause, cnpg-orphan/adopt: `SetMetadataStoreRetainCnpgOnFlip`, `CnpgSourceMRsOrphaned`, `SetMetadataStoreCnpgAdopt`) | `controlplane/provisioner/k8s_client.go` |
| Admin REST API (start/read/cancel + `GET /reshards/targets` destination discovery: all cnpg shards incl. empty ones via the cluster-topology pod read, RBAC-degrading to occupied shards; known external stores from live warehouse rows) | `controlplane/admin/reshard.go` |
| Console UI (form + operation page with live log) | `admin/ui/src/pages/ReshardForm.tsx`, `ReshardOperation.tsx` |
| Connection gates | `controlplane/control.go` (57P03), `flight_ingress.go`, grant-path check in `configstore/org_connections.go` |

The cutover primitive underneath is `spec.metadataStore.cnpgShard` on the
Duckling CR (charts #12918): changing it re-points the provider-sql
Role/Database at the target shard's ProviderConfig **in place** — same
role/database name (pinned `pgIdentPrefix`), same pinned password, old shard's
role/DB **orphaned, not dropped**, no data copied. A **type** flip
(cnpg↔external) is different: the composition stops rendering the cnpg MRs, and
what Crossplane does with the underlying role/DB then depends on those MRs'
`managementPolicies` — which the composition renders conditionally on
`spec.metadataStore.retainCnpgOnFlip` (charts):

- `retainCnpgOnFlip=false` (default, every normal tenant): the cnpg
  Role/Database render with **full lifecycle** `["*"]` (Delete included), so the
  un-render **deletes** the role/DB. This is also what makes a normal
  **deprovision** (delete the whole Duckling → finalizer cascade) cleanly DROP
  them — the clean-slate-on-recreate guarantee.
- `retainCnpgOnFlip=true` (set ONLY by the reshard runner, ONLY for the
  cnpg→external escape hatch): they render **without Delete**
  `["Observe","Create","Update"]` (the v2 Orphan equivalent), so the un-render
  **orphans** the role/DB (they survive on the shard) — the orphan-adopt safety
  net below.

**Deprovision-unaffected invariant (non-negotiable):** `retainCnpgOnFlip` is a
per-Duckling spec field defaulting to false, flipped true only transiently
during a cnpg→ext reshard (and cleared again on success/recovery/rollback). A
normal never-resharded tenant always has it false, so provisioning and
deprovisioning drop/create the cnpg role/DB exactly as before. The orphan
behavior is bound to the reshard type-flip, never to Duckling deletion. The e2e
`lifecycle_teardown_cnpg` asserts the finalizer cascade still fully deletes the
CR (and its cnpg role/DB) on a normal deprovision.

## Source identity: the duckling STATUS is authoritative

The start handler (`admin/reshard.go`) derives the op's **source side**
(`source_kind`, `from_shard`, the external source block) with the live
Duckling **status** as the source of truth: the composition writes the status
after convergence, so it records where the catalog *actually* lives, while
the config-store warehouse row and the duckling *spec* are operator-editable
inputs that can drift. The op is **refused (400, nothing created)** when the
identity is incomplete or contradicted:

The status contains only a non-sensitive `credentialSecretRef`; Duckgres reads
the referenced key from the Secret in the `ducklings` namespace before any
catalog copy, verification, or rollback. During the ordered rollout it still
accepts the legacy plaintext status password as a fallback, but the reference
is authoritative whenever it can be resolved.

- **kind drift** — the row's `metadata_store_kind` (when set) disagrees with
  the status type → 400 naming both values ("refusing to reshard until
  reconciled").
- **cnpg source with an unresolvable current shard** (no lister / no CR
  status / empty endpoint) → 400: an op recorded with an empty `from_shard`
  cannot roll back — the XRD rejects an empty `cnpgShard` patch. The resolved
  shard is also shape-checked against the XRD pattern.
- **external source with an incomplete row block** (no endpoint or SM
  password secret) → 400: an ext→cnpg rollback re-renders the external spec
  from those fields.
- **empty row kind AND no status** → 400 (nothing to derive from).

The runner re-checks at `recordSource` (pre-flip, defense in depth for ops
created by older CPs or hand-inserted rows): a status type that contradicts
the op's recorded `source_kind` fails the op before anything is flipped.

This guards against a real prod incident: an org whose config-store metadata
block was EMPTY (previously silently defaulted to cnpg-shard) and whose
duckling *spec* type had drifted from its *status* (the org actually lived on
an external RDS) got a cnpg→cnpg op with `from_shard=""`. cnpg→cnpg flips
BEFORE copying, so the org was re-pointed at a freshly created EMPTY catalog
with no copy performed; the copy then failed (the external source was probed
with the cnpg branch's `sslmode=disable`), and the rollback patch
(`cnpgShard: ""`) was rejected by the XRD — leaving the duckling on the empty
target while the warehouse was unblocked to ready.

## Execution model: one dedicated pod per operation

A reshard never executes inside a control-plane process. When an operator
starts one, the admin handler creates the op row **pending** and spawns a
dedicated pod `duckgres-reshard-op-<id>` (labels `app=duckgres-reshard`,
`duckgres-op-id=<id>`; restartPolicy Never; terminationGracePeriodSeconds 600
so a deleted pod can roll back; requests=limits from the env-only
`DUCKGRES_RESHARD_POD_CPU` / `DUCKGRES_RESHARD_POD_MEMORY`, defaults 2 CPU /
8Gi). Motivation: a real incident — the CP pod (512Mi limit) OOM-crashed while
running `pg_dump` on a ~20k-table catalog during `backup_catalog`, orphaning
the op. Resharding must never compete with live traffic for CP resources, and
catalogs can be much larger than that one.

The pod runs the CP's **own image** (it carries `pg_dump` and the duckgres
binary) in `--mode reshard-runner`, under the CP's **own ServiceAccount** (the
runner patches Duckling CRs — RBAC the CP already holds; the ONE extra grant
is the cnpg→ext orphan wait's read of the provider-sql MRs, see
`orphaning_source` below), and
inherits an **allowlist** of the CP container's env spec verbatim
(`DUCKGRES_CONFIG_STORE`, `DUCKGRES_INTERNAL_SECRET`, `DUCKGRES_AWS_REGION`, …
— a secretKeyRef is copied as a ref, so nothing secret ever lands in a pod
spec). The pod pins `kubernetes.io/arch` to the CP's own arch (the image is
arch-specific and worker nodepools can be mixed-arch) and inherits the CP's
nodeSelector/tolerations.

The pod's whole job (`RunSingleOperation`): claim the ONE op named by
`DUCKGRES_RESHARD_OP_ID` via the standard claim CAS (pending, or running with
a stale heartbeat — the respawn/takeover path, which bumps the fencing epoch
and runs `reconstructProgress`), execute the step machine to a terminal state,
exit. **The op row is the source of truth for the outcome**: a failed /
rolled-back / cancelled op still exits 0; only infrastructure errors (config
store unreachable, claim not winnable, fenced away mid-run) exit nonzero.
SIGTERM (pod deletion) cancels the run context → rollback within the
termination grace.

A **leader-only reconciler** (attached under the janitor lease, 30s tick)
keeps the fleet honest:

- an op that should be executing but has no live pod — pending older than a
  grace (2m), or running with a stale heartbeat (>5m) — gets its pod
  (re)spawned. Bounded: after 3 failed interventions the op is force-failed
  with a clear operator-facing error (the warehouse may still be blocked —
  that is the page-someone signal; there is no runner to roll back).
- pods of terminal ops are reaped (exited pods immediately; a still-running
  pod only after the op has been terminal for a grace).

Network policy: reshard pods need egress to Postgres (5432 — config store,
cnpg shard poolers, external RDS), S3/STS (443), the CP admin API (8080, the
password pull below) and DNS, and no ingress. The e2e manifest policy is
`duckgres-reshard-runner-boundaries` in `k8s/networkpolicy.yaml`; the
production chart needs the equivalent (charts repo).

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
   **XRD-defaulting guard (prod incident)**: a legacy CR can have NO
   `spec.ducklake` object at all — its DuckLake enablement then comes from the
   legacy metadata-store type coupling (`status.metadataStore.type !=
   cnpg-shard`, the same derivation the worker activator uses). The compaction
   merge patch MATERIALIZES `spec.ducklake`, and the XRD's
   `ducklake.enabled: {default: false}` stamps `enabled: false` onto the fresh
   object — silently disabling DuckLake for the org (activation then builds an
   S3-only payload and the worker fails with "tenant activation requires a
   ducklake metadata_store"). `SetCompactionEnabled` therefore reads the CR
   first and, when the object is absent, pins the org's EFFECTIVE enablement
   into the same patch; an existing `spec.ducklake` is patched exactly as
   before (compaction key only). The pinned explicit value is deliberately
   never removed afterwards: it equals the effective value (a no-op), whereas
   removing it after a successful flip would hand enablement back to the type
   coupling, whose answer may have CHANGED with the new store type (ext→cnpg
   would re-derive false — the same outage through another door).
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
   REPEATABLE READ transaction = consistent snapshot. The transaction takes
   `SHARE` locks on every discovered `ducklake_*` table, re-discovers the table
   set after locking, and remains open through verification and source cleanup;
   this blocks DML and conflicting DDL without killing an already-running
   writer. Faithful DDL from `pg_catalog` introspection; rows via raw binary
   COPY passthrough. The source `COPY TO` and target `COPY FROM` command-tag row
   counts must match for every table; no additional full-table count or
   fingerprint scans are performed. Constraints then non-constraint indexes
   (PK-backed indexes excluded — ADD
   CONSTRAINT already made them). No sequences exist in the DuckLake catalog
   (next-ids are rows). A `pg_advisory_lock` in the TARGET database fences a
   zombie ex-runner for copy+verify. The target database may be REUSED
   (failed/rolled-back attempts and prior residences leave their catalog in
   place — external stores are never cleaned up — and a dev target can even
   host another tenant's live catalog): each table is `DROP TABLE IF EXISTS`d
   before CREATE, and because index names are schema-global the index replay
   is idempotent — `DROP INDEX IF EXISTS` by name before every `CREATE INDEX`
   and every index-backed `ADD CONSTRAINT` (never `IF NOT EXISTS`, which would
   silently keep a wrong stale index), with a bounded re-drop+retry when the
   create still hits 42P07 because a live worker's
   `ensureDuckLakeMetadataIndexes` (`CREATE INDEX IF NOT EXISTS` on attach)
   raced the replay on a shared target.
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
- **How**: STREAMED. pg_dump writes the archive to stdout and the bytes flow
  straight into an S3 **multipart upload** (`feature/s3/manager`, 16MiB parts,
  concurrency 2) — no temp file, no whole-dump buffering, memory bounded by
  partsize × concurrency regardless of catalog size (the previous
  temp-file+PutObject implementation buffered the dump and OOM-killed a 512Mi
  CP pod on a ~20k-table catalog). A nonzero pg_dump exit or an empty dump
  deletes the just-uploaded partial object so a broken artifact never
  masquerades as a valid backup; a failed upload aborts the multipart and
  kills pg_dump.
- **Where**: `s3://<org-data-bucket>/_reshard_catalog_backups/op-<opID>-<UTC
  timestamp>.dump`. Bucket + region come from the duckling CR status
  (`status.dataStore.bucketName` / `s3Region`); the reserved
  `_reshard_catalog_backups/` prefix keeps backups clear of DuckLake's parquet.
- **Creds**: the upload assumes the org's **own** IAM role
  (`status.iamRoleARN`) via the same STS AssumeRole the worker activator uses
  for DuckLake S3 access — the runner pod writes to the org bucket with
  short-lived, org-scoped credentials, no worker required. The broker is
  injected into the runner by the reshard-runner mode
  (`controlplane/reshard_runner_mode.go`; the STS broker lives in the
  `controlplane` package and the provisioner runner takes an `AssumeRoleFunc`
  to avoid the import cycle).
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
net to persist. Every object lives under the reserved
`_reshard_catalog_backups/` prefix, so a single prefix-scoped S3 lifecycle rule
expires them without touching DuckLake data. (The objects are deliberately NOT
object-tagged: `PutObject` with an `x-amz-tagging` header requires
`s3:PutObjectTagging`, which the per-org duckling IAM roles do not grant — a
tagged upload 403s on the real cluster. The prefix is the sole, sufficient
lifecycle key.) **Suggested rule** (on the
per-org data bucket, or a bucket-wide default): expire objects under prefix
`_reshard_catalog_backups/` after **30 days**. A catalog is tens of MB to
~100 MB, so even frequent reshards stay negligible against the parquet data —
but the lifecycle rule guarantees they never accumulate unbounded. No in-app GC
is built; the lifecycle rule is the retention mechanism.

## cnpg→external (escape hatch): orphan-adopt then verified-delete

This direction never destroys the cnpg copy coupled to the flip. Instead it
retains the cnpg role/DB across the flip (so a bad flip is instantly
recoverable), verifies the external target actually caught the full catalog, and
only THEN drops the retained source. The pre-flip `backup_catalog` (above) still
runs first, and for this destructive direction it is a **hard prerequisite** (a
backup failure fails the op before any flip):

```
blocking → draining → pausing_compaction → backup_catalog → copying → verifying
        → orphaning_source → cutover → verifying_external → cleaning_up
        → finalizing
```

1. **copying / verifying** — as for →cnpg, but the source is read via its
   recorded pre-flip cnpg pooler endpoint (unchanged by the flip since the
   role/DB are retained).
2. **orphaning_source** — patch `spec.metadataStore.retainCnpgOnFlip=true`
   *while type is still cnpg-shard*, then WAIT until the cnpg Role AND Database
   MRs actually reflect the no-Delete policy (poll their
   `spec.managementPolicies` via `CnpgSourceMRsOrphaned`) before proceeding.
   This two-step-flip closes the race where the type flip un-renders the MRs
   before the retain policy propagated. **XRD-compat:** after the patch the
   runner reads the flag back; if it did not stick (the cluster's Duckling XRD
   predates the field → the API server pruned the patch) the runner **REFUSES**
   the reshard with a clear "deploy the charts first" error rather than risk the
   old destructive delete-on-flip. (Refuse is safer than silently falling back
   to data-loss risk.)
   **RBAC:** this is the ONE reshard step that reads beyond the Duckling CR —
   `CnpgSourceMRsOrphaned` gets the provider-sql `Role`/`Database` MRs
   (`postgresql.sql.m.crossplane.io`, ducklings namespace), which the
   duckling-reader grant does NOT cover; the runner's ServiceAccount needs an
   explicit get grant on those resources (duckgres chart RBAC). A **Forbidden**
   read fails the wait IMMEDIATELY with an error naming the missing grant — an
   RBAC denial is permanent, and polling to the timeout would only produce a
   misleading "composition skew?" error (a real mw-dev incident: the first live
   cnpg→ext burned its full 15m orphan wait on Forbidden reads). Transient read
   errors keep polling; every ~15s the op log records what the poll observed
   (per-MR found/not-found + current `managementPolicies`), and the timeout
   error carries the last observation.
3. **cutover** — flip `type` to external (`cnpgShard: null`, external block set;
   `retainCnpgOnFlip` left true). The un-render now ORPHANS the cnpg role/DB.
   Wait for external Ready + ESO password synced + matching the typed password.
4. **verifying_external** — record that the copy completed with matching source
   `COPY TO` and target `COPY FROM` command-tag row counts for every table while
   the source SHARE fence remained held.
5. **cleaning_up** — only after external verify passes: `DROP DATABASE … WITH
   (FORCE)` the retained cnpg source database via the source pooler (best-effort;
   a leftover DB is cruft, not data loss — the external target is verified live),
   then clear `retainCnpgOnFlip` (harmless on the now-external tenant; keeps a
   later ext→cnpg reshard from inheriting a no-Delete cnpg MR). The role is left
   (orphaned, harmless — roles cannot drop themselves).

The target password is **ephemeral**: sent once in the start request,
validated by connecting, never persisted to the op row/log/audit/pod spec —
the row stores the AWS SM secret NAME only. With execution in a dedicated pod,
the handoff is a **one-shot pull**: the creating CP replica keeps the password
in an in-memory stash (per op id, pruned once the op is terminal) and the
runner pod fetches it at startup from
`GET /api/v1/reshards/<id>/password` on the creating replica's **pod IP**
(the stash is process-local, so the URL must bypass the service VIP). The
endpoint accepts the INTERNAL-SECRET identity only — an SSO admin, even with
the admin role, is 403'd; operators must never read tenant credentials — and
404s for unknown/terminal ops or a replica without the stash. The pull URL
(never the password) is persisted on the op row (`password_url`, migration
`000022`) and injected into the pod env as `DUCKGRES_RESHARD_PASSWORD_URL`, so
a reconciler respawn re-wires the same handoff while the stashing replica
lives. (This replaces claim-on-create —
`CreateReshardOperationClaimed`/`AdoptClaimedOperation` — which pinned
execution to the creating replica's own process; with no in-CP execution left,
that pinning is neither possible nor needed.)
If the pull 404s (the creating replica is gone), the runner proceeds without
the stash and the step machine fails the op with the clear "external target
password is not available … cancel and re-run" message, then rolls back —
and because cnpg→ext copies BEFORE the flip, a pre-flip failure leaves the
cnpg source untouched. After the flip, the runner checks the ESO-synced status
password matches the provided one (catches a wrong/missing SM secret).

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
cnpg→ext flip is destructive — Crossplane un-renders the cnpg source role/DB on
the TYPE change, so a doomed external target that only fails *after* the flip
forces the flip-back adopt recovery. Two submit-time gates make a doomed op
fail fast with a 400 (nothing created): Fix 1 above (the ESO-readable name
allowlist), and a bounded `SELECT 1` against the target Postgres
(`ExternalTargetProber`, an adapter over `provisioner.PGCatalogCopier.Probe`
wired in `multitenant.go`) that proves endpoint reachability + the
user/database/password all work, before any maintenance window or flip.
External RDS uses `sslmode=require` (matches the runner's copy). The whole
check is bounded (~8s) so a black-holed endpoint can't hang the handler; a
timeout counts as a connect failure. The 400 message is redacted (never echoes
the password). The prober nil-degrades in tests / non-k8s builds — the check
is then skipped and the runner's copy step still catches a bad credential later
(fail-fast optimization, not the only line of defense). When the prober IS
present and the connect fails, the op is refused.

**Recovery if the flip goes bad** (external never Ready, ESO secret
missing/mismatched, or external-catalog verify fails): flip `type` back to
cnpg-shard AND clear `retainCnpgOnFlip` in one patch (`SetMetadataStoreCnpgAdopt`)
— the composition re-renders the cnpg Role/Database MRs at full lifecycle and
provider-sql **ADOPTS** the still-present orphaned role/DB by external-name (same
`pgIdent`, same pinned password). **No empty-recreate, no copy-back** — the
catalog never left the source. This replaces the old recreate-then-copy-back
recovery (which stranded the tenant with an empty catalog when the copy-back
also failed, the prod-shaped data-loss incident this change fixes) and avoids
the `2BP01` "role can't drop while its DB exists" wedge the coupled delete hit.

Every wait loop in the runner (drain, flip converge, orphan-policy wait, this
recovery) announces its deadline on entry and logs what it observes every ~15s
(duckling endpoint/Ready state, classified probe errors), and its timeout
error/log line carries the LAST observation — a stuck reshard is diagnosable
from the op log alone. One recovery failure mode deserves naming: the
re-adopted role's ACTUAL Postgres password can differ from the freshly
regenerated status password (a **stranded password** — the probe fails with
`FATAL: SASL authentication failed` / SQLSTATE 28P01 until the composition
converges the password). The runner classifies auth probe failures
(`isAuthProbeError`) and the observation names the condition and the manual
remedy — `ALTER ROLE <role> WITH PASSWORD '<status password>'` on the source
shard primary — but it still polls to the deadline rather than bailing early,
since a charts/composition fix can converge the password mid-wait. Passwords
never appear in the op log.

## Rollback, cancel, crash-safety

- Rollback never removes `spec.metadataStore.cnpgShard` — it patches the
  source VALUE back (key-absent would fall through to the freshly-stamped
  bogus status pin). ext→cnpg rollback patches `{type: external, cnpgShard:
  null}` (the XRD's CEL forbids the key on external). cnpg→ext rollback:
  post-flip → adopt recovery (above); pre-flip → if `retainCnpgOnFlip` was set,
  reset it to false so the still-cnpg source keeps full-lifecycle (Delete) MRs
  and deprovision stays clean.
- **Rollback never emits a patch the XRD would reject.** If the recorded
  source identity is unusable — `from_shard` empty or outside the XRD's
  shard-name pattern (`isValidCnpgShardName` mirrors it), or an external
  source block missing endpoint/password secret — the rollback flip-back (and
  the cnpg→ext adopt recovery) is SKIPPED with an ERROR carrying explicit
  operator instructions, and the warehouse is **intentionally left blocked in
  `resharding`** (see the never-stranded caveat below). Emitting the invalid
  patch would just be refused by the API server and mislead the log — the
  prod incident's rollback failed exactly that way. The submit-time
  source-identity validation makes such ops unrepresentable going forward;
  this guard covers pre-guard rows and drift after creation.
- Takeover-resume reconstructs `o.source`/`o.target` from the durable op row +
  live duckling status with each side's **sslmode derived from its
  metadata-store kind** (`sslModeFor`: cnpg pooler → `disable`, external RDS →
  `require`) — never hardcoded; a hardcoded `disable` fails RDS pg_hba with
  "no encryption".
- Cancel (`POST /reshards/:id/cancel`): flag checked between steps and inside
  every wait loop; rolls back like a failure at that step → `cancelled`.
  Pending (unclaimed) ops finish immediately. On cnpg→ext, cancel is refused
  once the flip started (forward-or-recovery only).
- Runner fencing: claim bumps `runner_epoch`; every runner write is CAS-fenced
  on (runner, epoch); heartbeat ~30s; an op with a >5m-stale heartbeat is
  claimable (takeover — in the pod model that is the leader reconciler
  respawning `duckgres-reshard-op-<id>`, whose fresh claim fences the old
  pod's zombie writes and reconstructs rollback progress from the row). The
  target-DB advisory lock fences the copy itself. A pod crash therefore never
  strands an op: the respawned pod re-claims, and for ext targets re-pulls the
  password from the persisted `password_url` (or fails with the clear re-run
  message if the stashing replica died too).
- Takeover rollback reconstructs progress from the persisted row. A runner that
  claims an op a prior epoch advanced starts with all in-process rollback flags
  false, so it calls `reconstructProgress()` before running: `blocked` from
  `blocked_at`/step-past-`blocking`, `compactionPaused` from step-past-
  `pausing_compaction`, `flipped`/`retainRequested` from step reaching
  `cutover`/`orphaning_source`. Without this, a cancel or failure that
  short-circuits before the steps re-execute (the first `cancelRequested()`
  check) would mark the op terminal while leaving the warehouse stuck in
  `resharding` and compaction paused — the exact mw-dev incident where the
  blocking runner OOM-crashed mid-backup and a sibling replica finalized the
  cancel without unblocking. Reconstruction is conservative (only signals that
  prove a step completed flip a flag) and every rollback action is idempotent, so
  a takeover rolls back identically to the original runner.
- An org is never stranded: every failure path restores `resharding→ready`
  (or logs at ERROR if it can't — that's the page-someone signal) — with ONE
  deliberate carve-out: when the duckling is known to point at the WRONG
  store and rollback cannot repair it (an invalid/incomplete recorded source
  identity, or a flip-back that failed), the warehouse stays BLOCKED in
  `resharding`. Unblocking would let clients activate against the wrong
  (likely empty) catalog — worse than a blocked org: the prod incident
  unblocked after a failed rollback and clients landed on an empty catalog.
  The ERROR log carries the manual repair steps (verify where the catalog
  actually lives, patch `spec.metadataStore`, verify activation, then set the
  warehouse ready).

## Testing

Unit: `configstore` (tests/configstore/reshard_postgres_test.go — claim CAS,
takeover fencing, cancel, log pagination, the grant-path gate, the password
pull URL + active-op listing), `controlplane` (reshard_pod_test.go — pod spec:
labels/SA/image/env-allowlist/secretKeyRef preservation/resource knobs;
reshard_reconciler_test.go — respawn, retry bound + force-fail, terminal-pod
reap; reshard_runner_mode_test.go — the password pull incl. the 404-is-terminal
contract; catalog_backup_test.go — the streamed dump→S3 path, stderr
diagnostics, partial-object cleanup, empty-dump refusal),
`provisioner/reshard_runner_test.go` (all three directions, rollbacks, cancel,
ephemeral-password loss, the pre-flip backup: recorded before the flip, the
destructive-direction hard-fail-before-flip, the non-destructive
warning-and-continue; for cnpg→ext specifically the two-step orphan flip, the
external-catalog verify gate, the flip-back adopt recovery with NO copy-back,
and the XRD-without-`retainCnpgOnFlip` refusal; plus the source-identity
guards: the recordSource status-drift refusal, the invalid-from_shard /
incomplete-external-source rollback refusals with the warehouse left blocked,
and the kind-derived takeover sslmode reconstruction), `admin/reshard_test.go`
(validation incl. the ESO-name allowlist + the pre-flight connection-check
pass/fail/skip, the submit-time source-identity validation
(`TestReshardStartSourceIdentityValidation`), secrets never persisted).
`provisioner/k8s_client_test.go` pins the compaction patch's XRD-defaulting
guard (`TestSetCompactionEnabledPinsDuckLakeEnablement`). The `backup_s3_uri` column is asserted
in `tests/configstore/migrations_postgres_test.go` (migration `000021`). The
charts side (composition + `retainCnpgOnFlip` XRD field) is tested by
`charts/charts/crossplane-config/tests/composition_retain_cnpg_test.sh` (both
managementPolicies variants render; the XRD field defaults false). e2e
(`tests/mw-dev/e2e/harness.sh`): validation 400s (incl. a non-allowlisted
external secret name), cancel-during-drain (drain-not-kill + 57P03 visible),
bogus-shard rollback (real flip → Synced=False → rollback, data intact), the
**ext→cnpg positive path** (real catalog move off the harness RDS onto
shard-001) — which, since reshards execute in dedicated pods, also asserts the
op is created `pending`, the `duckgres-reshard-op-<id>` pod appears with the
right labels while the op runs, and the leader reconciler reaps it after the
op finishes — and which also asserts the pre-flip backup completed and recorded a
`backup_s3_uri` under `_reshard_catalog_backups/` (the Job has no aws CLI, so it
asserts the recorded URI + restore command in the op log, not an S3 list — same
limitation as the tenant-isolation object-store half), and the LOAD-BEARING
`lifecycle_teardown_cnpg` (a normal cnpg tenant deprovision still fully deletes
the Duckling CR + its cnpg role/DB — the deprovision-unaffected regression net
for `retainCnpgOnFlip`). cnpg→cnpg positive path needs a second mw-dev shard
(follow-up infra); the cnpg→ext positive path is unit-test-only (the harness has
no RDS password — the start API requires it ephemerally).
