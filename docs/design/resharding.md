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

## cnpg→external (escape hatch): orphan-adopt then verified-delete

This direction never destroys the cnpg copy coupled to the flip. Instead it
retains the cnpg role/DB across the flip (so a bad flip is instantly
recoverable), verifies the external target actually caught the full catalog, and
only THEN drops the retained source:

```
blocking → draining → pausing_compaction → copying → verifying
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
3. **cutover** — flip `type` to external (`cnpgShard: null`, external block set;
   `retainCnpgOnFlip` left true). The un-render now ORPHANS the cnpg role/DB.
   Wait for external Ready + ESO password synced + matching the typed password.
4. **verifying_external** — connect to the now-live external target and require
   its per-table `ducklake_*` row counts to match the copy snapshot
   (`CatalogCopyResult.PerTableRows`) EXACTLY. A missing table / count mismatch
   fails here — the cnpg source is still present (orphaned), so recovery adopts
   it back with no data loss.
5. **cleaning_up** — only after external verify passes: `DROP DATABASE … WITH
   (FORCE)` the retained cnpg source database via the source pooler (best-effort;
   a leftover DB is cruft, not data loss — the external target is verified live),
   then clear `retainCnpgOnFlip` (harmless on the now-external tenant; keeps a
   later ext→cnpg reshard from inheriting a no-Delete cnpg MR). The role is left
   (orphaned, harmless — roles cannot drop themselves).

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

## Rollback, cancel, crash-safety

- Rollback never removes `spec.metadataStore.cnpgShard` — it patches the
  source VALUE back (key-absent would fall through to the freshly-stamped
  bogus status pin). ext→cnpg rollback patches `{type: external, cnpgShard:
  null}` (the XRD's CEL forbids the key on external). cnpg→ext rollback:
  post-flip → adopt recovery (above); pre-flip → if `retainCnpgOnFlip` was set,
  reset it to false so the still-cnpg source keeps full-lifecycle (Delete) MRs
  and deprovision stays clean.
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
ephemeral-password loss; for cnpg→ext specifically the two-step orphan flip, the
external-catalog verify gate, the flip-back adopt recovery with NO copy-back,
and the XRD-without-`retainCnpgOnFlip` refusal), `admin/reshard_test.go`
(validation incl. the ESO-name allowlist + the pre-flight connection-check
pass/fail/skip, secrets never persisted). The charts side (composition +
`retainCnpgOnFlip` XRD field) is tested by
`charts/charts/crossplane-config/tests/composition_retain_cnpg_test.sh` (both
managementPolicies variants render; the XRD field defaults false). e2e
(`tests/mw-dev/e2e/harness.sh`): validation 400s (incl. a non-allowlisted
external secret name), cancel-during-drain (drain-not-kill + 57P03 visible),
bogus-shard rollback (real flip → Synced=False → rollback, data intact), the
**ext→cnpg positive path** (real catalog move off the harness RDS onto
shard-001), and the LOAD-BEARING `lifecycle_teardown_cnpg` (a normal cnpg
tenant deprovision still fully deletes the Duckling CR + its cnpg role/DB — the
deprovision-unaffected regression net for `retainCnpgOnFlip`). cnpg→cnpg
positive path needs a second mw-dev shard (follow-up infra); the cnpg→ext
positive path is unit-test-only (the harness has no RDS password — the start
API requires it ephemerally).
