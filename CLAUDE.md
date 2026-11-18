# Claude Code Context for Duckgres

Duckgres is a PostgreSQL wire-protocol server backed by DuckDB: any PG client (psql, lib/pq, psycopg, JDBC, …) can query DuckDB.

## Architecture

Run modes (`--mode`, CLI-only): `standalone` (default), `control-plane`, `duckdb-service`, and `reshard-runner` (`-tags kubernetes`; per-op reshard pod entrypoint, see Resharding). Three topologies:

1. **Standalone** — one process does TLS, auth, PG wire, transpilation, and in-process DuckDB per user.
2. **Control plane + local process workers** (`--worker-backend process`, default) — CP owns client connections and spawns `duckdb-service` children, talking Arrow Flight SQL over Unix sockets.
3. **Control plane + remote K8s workers** (`--worker-backend remote`, needs `-tags kubernetes` + Postgres config store) — CP pod routes per-org traffic to worker pods over TCP+TLS; adds config store, org router, runtime tracker, janitor/leader election, provisioning/admin HTTP API.

Clients only ever see PG wire; Flight SQL is internal.

### Key Components

- **main.go / config_resolution.go** — flags; precedence CLI > env > YAML > defaults. Some K8s knobs are env-only.
- **server/** — wire protocol (`server.go`, `conn*.go`, `protocol.go`), execution (`executor.go`, `flight_executor.go`, `transient.go`), catalog/types, auth/TLS/rate limit (`auth_policy.go`, `ratelimit.go`, `certs.go`, `acme.go`), DuckLake (`ducklake_migration.go`, `checkpoint.go`), observability (`querylog.go`, `tracing.go`), process-isolation workers (`parent.go`, `worker*.go`).
- **controlplane/** — `control.go`, `session_mgr.go`, `worker_mgr.go`, `worker_pool.go`, janitor/leader/rebalancer/runtime tracker; K8s files under tag `kubernetes` (`multitenant.go`, `k8s_pool*.go`, `org_router.go`, `org_reserved_pool.go`, `sts_broker.go`, `shared_worker_activator.go`, …). Subpackages: `admin/` (console + API; see `admin/README.md`), `provisioner/`, `provisioning/`, `configstore/`.
- **duckdbservice/** — Flight SQL DuckDB service (`service.go`, `flight_handler.go`, `activation.go`, `cache_proxy.go`, …).
- **transpiler/** — AST-based PG→DuckDB; pipeline registered in `transpiler.go` `New()`, transforms in `transform/`.

### Native metadata Postgres proxy (K8s CP)

SNI suffixes in `DUCKGRES_METADATA_HOSTNAME_SUFFIXES` (e.g. `.md.us.postwh.com`) expose opted-in CNPG warehouse metadata DBs. Early connection branch, not a DuckDB executor: org `root` password authenticates, startup database MUST be exactly `metadata`, the real endpoint/db/role/password are resolved internally via `SharedWorkerActivator.MetadataPostgresURL`, then bytes are relayed opaquely (upstream `application_name=duckgres-metadata-proxy`).

- Fail-closed gate: warehouse `metadata_proxy_enabled` AND `state=ready` AND `metadata_store_kind=cnpg-shard`. Never infer from shard placement; never pass client-supplied upstream db/user/host/password.
- Dedicated single-customer shards only — not shared shards until upstream `CONNECT` ACLs/role hardening exist. After connect, `root` has full access of the metadata role.
- `DUCKGRES_METADATA_PROXY_MAX_CONNECTIONS_PER_ORG` (default 20/replica). Participates in per-user kill/disable; sessions re-check the gate every 5s and close if ineligible. Admin warehouse PUT including `metadata_proxy_enabled` reloads snapshot + notifies peers.
- 10s bootstrap deadline (resolve + upstream connect/auth), cancelled after hijack.
- Metrics: `duckgres_metadata_proxy_{connections_open,connection_attempts_total,connection_duration_seconds,upstream_connect_duration_seconds,bytes_total,cancel_requests_total}`. `duckgres_connections_open` includes these sockets; query metrics/logs/traces do not see relayed SQL. Never put an org's metadata target in the CP health check.
- `CancelRequest` is session-terminating: synthetic keys map to the exact frontend/upstream pair on the owning replica, which closes both (never redial PgBouncer — instance-local keys could hit the wrong pod). Misses on other replicas count as `outcome="not_local"`.

## Control-plane flags (selected)

- `--worker-backend process|remote`, `--process-{min,max}-workers`, `--process-retire-on-session-end`, `--worker-queue-timeout`, `--worker-idle-timeout`, `--memory-budget` (default 75% RAM), `--memory-rebalance`, `--socket-dir`, `--ducklake-delta-catalog-{enabled,path}`.
- `--handover-drain-timeout` — default `24h` process; **remote default `0` = unbounded** (pod `terminationGracePeriodSeconds` is the wall). tableflip FD passing is process/standalone only.
- `--idle-timeout` — closes idle connections with FATAL 57P05 naming the timeout. **CP default `5m`** (`server.DefaultControlPlaneIdleTimeout`), standalone `24h`, negative disables. CP sets it explicitly before `InitMinimalServer` (which skips defaulting).
- `--statement-timeout` — default `0` = unbounded. Blast-radius containment only (nothing else bounds a wedged statement). Surfaces as `57014` `canceling statement due to statement timeout` (exact PG wording). Deadline rides the STATEMENT context, never the connection.
  - **Every execution path must get its context via `queryContext`/`queryContextInner`** — the only `RegisterQuery` site, so skipping it means no timeout AND no pgwire cancel. Use the `*Context` executor methods (context-less ones run on `Background()`). Regression: `TestStatementTimeoutReachesExtendedProtocolExecute`.
  - Classification is error-driven and gated on the knob (`statementTimedOut(err)`), never stored per-conn state (that went sticky). Off ⇒ byte-identical behavior.
  - Cursors (`queryContextForCursor`) and suspended portals are bounded over their whole lifetime, not per FETCH — deliberate.
  - An engine ignoring cancellation (duckdb#24961) leaves the worker wedged; retiring it is a follow-up that must respect destroy-before-reuse.
- Remote: `--config-store`, `--config-poll-interval`, `--internal-secret`, `--k8s-worker-{image,namespace,port,secret,configmap,image-pull-policy,service-account}`, `--k8s-control-plane-id`, `--aws-region`. No global worker cap; `Org.MaxWorkers` (0=unbounded) is the only cap. Pod scheduling knobs are env-only.
- `DUCKGRES_PROVISIONER_ENABLED=false` stops the provisioning controller (Duckling CR lifecycle, warehouse state, Trino projection) for hand-over to hogtower — never two writers. A `component='provisioning'` row in `duckgres_control_handover` (hogtower-owned; missing table = no hand-over) is read once at startup by `applyControlHandover` and forces off the controller, `DUCKGRES_TRINO_POOL_{OPERATOR,CATALOG_WRITER,NODE_DISRUPTION}_ENABLED` and drops `DUCKGRES_TRINO_DEFAULT_CELL`; read error fails startup. The same marker makes `/api/v1` read-only (`management_read_only.go`, re-read live, 10s cache): non-GETs return 409 `managed by <owner>` except an allow-list (snapshot reload, billing ack, session/query cancel+kill, impersonate query, persistent-secret delete, reshards, `?scope=local` user disable/enable legs). New write routes are read-only by default. `DUCKGRES_MANAGEMENT_READ_ONLY=true|false` overrides. The `trino` e2e lane asserts the flip.

duckdb-service flags: `--duckdb-listen`, `--duckdb-listen-fd` (internal), `--duckdb-token`, `--duckdb-max-sessions` (0=unlimited).

## Keep docs in sync

A behavior/default/flag/invariant change must update every doc that states it **in the same PR**: this file, `README.md`, `docs/`, flag help (`main.go`/`cliflags.go`), design docs. Grep for the old value. Stale docs are a bug.

## Development & Testing

Use `just` (run `just` for recipes). Before pushing: `golangci-lint run ./...`, `go build ./...`.

**Every runtime-affecting change (feature, fix, knob, refactor) ships with a deterministic assertion in `tests/mw-dev/e2e/harness.sh`** — real cluster, both metadata backends (cnpg + ext) where metadata is touched, cold-pool conditions handled. Bugfixes get a regression assertion. Refactors must keep coverage of moved paths. If something cannot be asserted in-Job, say so in the harness/README with the reason.

Lanes: unit (`go test ./...`, incl. `tests/manifests/`), `tests/integration/` (`just test-integration`, standalone vs MinIO+Postgres), `tests/mw-dev/e2e/` (per-PR `e2e-mw-dev.yml` on real EKS; `run.sh` orchestrates; replaces retired kind suite `tests/k8s/`; see `tests/mw-dev/README.md`).

Changes to `shared_worker_activator.go`, `sts_broker.go`, activation payloads (`TenantActivationPayload`, `server.DuckLakeConfig`), `server.AttachDeltaCatalog`/`attachDuckLake*`/`refresh*Secret`, `configstore/models.go`, `duckdbservice/activation.go`, `worker_activation.go`, or AWS-credential→SECRET wiring obligate harness updates. Keep the DuckLake round-trip/durability/concurrent-writer checks honest.

## Known Limitations

No replication; some pg_catalog tables are stubs; MAP/STRUCT/UNION/ENUM/BIT fall back to OidText; DML RETURNING unsupported via extended protocol.

**DML RETURNING detection** (`isDMLReturning`, `server/conn.go`): rejected at Describe with `0A000` because Describe executes the query. Heuristic lexer with any-depth writable-CTE scanning. False negatives cause silent mutations — err toward false positives. LIMIT 0 does not stop CTE side effects. Add `MERGE` if DuckDB gains MERGE RETURNING.

## Worker Session Model (remote backend) — LOAD-BEARING

A worker pod serves **exactly one client session at a time**. `workerDuckDBLimits` (`control.go`) gives that session the pod's RAM minus headroom and 2.5 threads per CPU (rounded up) — not divided by sessions.

- Headroom = `max(25% of pod, min(6GiB, 40% of pod))` (`workerMemoryHeadroomBytes`): 40% <15Gi, 6GiB in [15,24)Gi, 25% ≥24Gi. It covers memory outside `memory_limit` and is cumulative (DuckDB doesn't release buffer pages, so hot-idle RSS ratchets). Sub-crossover budgets are formatted in MB, not truncated GB.
- **Enforced**: workers spawn with `DUCKGRES_DUCKDB_MAX_SESSIONS=1`. Internal work uses `controlDB`/`warmupDB` side connections (uncounted). Never raise >1; never route internal work through `CreateSession`.
- **`OrgReservedPool` never co-assigns**: reuse only idle (`activeSessions==0`, Hot, org-owned) via `findIdleAssignedWorkerLocked`, else claim/spawn. No least-loaded sharing path (that's only in flat `K8sWorkerPool.AcquireWorker`, unused in remote mode).
- At org max + all busy → fail fast (`WorkerClaimMissReasonOrgCap`, `capacity_policy.go`); never busy-wait. Under cap → spawn on demand (`spawnReservedWorkerForSlot`), cap rechecked cross-CP in `CreateSpawningWorkerSlot`. Spawn+activate runs detached (`context.WithoutCancel` + `workerSpawnActivateTimeout`); an abandoned success parks hot-idle, a failure retires. Nothing leaks in Reserved/Activating.
- **FIFO anti-snatch**: `acquireDecision` is serialized per org by `orgAcquireGate`; spawn+activate runs outside the gate (waiters 1:1 bound to their slot, session pre-claimed before Hot). Gate must be cancel-safe.
- **Destroy-before-reuse**: `SessionManager.DestroySession` awaits the worker `DestroySession` RPC before `ReleaseWorker`.
- **Hot-idle claim refuses doomed pods**: `adoptClaimedWorker` → `claimedPodDoomReason` (`k8s_pool_doomed_claim.go`): deletionTimestamp, non-Running, or node with `karpenter.sh/disrupted` taint ⇒ retire (`pod_doomed`, origin `reserve_doomed_pod`), count `duckgres_control_plane_hot_idle_claim_skipped_total{reason}`, re-run the decision. Node lookup failure = unknown, never doomed.
- **Cap drift is recovered**: worker "max sessions reached" (`isWorkerSessionCapError`) → ERROR log, `duckgres_control_plane_worker_session_cap_drift_total`, retire, re-acquire (bounded by `maxWorkerSessionCapDriftRetries`). Nonzero = broken invariant; fix root cause.
- **Invalidated DuckDB instance retires the worker, never reused.** Detection `isInstanceFatalError` (`duckdbservice/instance_fatal.go`): typed `*duckdb.Error` `ErrorTypeInternal`/`ErrorTypeFatal` is authoritative. **Never substring-match `"INTERNAL Error"`/`"FATAL Error"`** — DuckDB echoes user SQL (`TestInstanceFatalIgnoresEchoedQueryText`). Only string fallback: the `database has been invalidated…` marker. OOM and DuckLake conflicts must not classify. Flag is sticky; set from statement paths, `CreateSession`, and an async single-flight `SELECT 1` probe kicked by health checks — **off the 3s health-check critical path**; a stuck probe raises `duckgres_worker_instance_probe_stuck`. Stored reason MUST be redacted (`usersecrets.RedactErrorForLog` / `noteInstanceErrorOpaque`). Worker reports `instance_invalidated`+`healthy:false`; CP retires on FIRST report (bypassing `maxConsecutiveHealthFailures`) and rejects in `validateReservedWorkerHealth`. Metrics: `duckgres_control_plane_worker_instance_invalidated_total`, `duckgres_worker_instance_invalidated_{total,state}`. Containment only; engine fix ships via `DUCKLAKE_EXTENSION_TAG` in `Dockerfile`/`Dockerfile.worker`.

Touching `org_reserved_pool.go`, `org_acquire_gate.go`, `k8s_pool.go::spawnWorker/AcquireWorker`, `workerDuckDBLimits`, `instance_fatal.go`, or session counting → update `org_reserved_pool_test.go`, `org_acquire_gate_test.go`, `k8s_pool_doomed_claim_test.go`, `duckdbservice/{service,instance_fatal}_test.go`, `controlplane/instance_invalidated_test.go`, and harness `one_session_per_worker` + `cold_burst_parallel_spawns`.

## Exploratory Worker Tier (small-first routing) — LOAD-BEARING

Design: `docs/superpowers/specs/2026-08-04-exploratory-worker-tier-design.md`. Remote only. Unsized connections start on a small pod and escalate one-way to standard. Env: `DUCKGRES_EXPLORATORY_TIER_{ENABLED,WORKER_CPU,WORKER_MEMORY,WORKER_TTL}` (TTL default 48h), resolved by `exploratoryWorkerProfile`. Code: `server/conn_tier.go`, `tier_classify.go`, `controlplane/session_activation.go`, closures in `control.go::handleConnection`.

- **Eligibility decided at connect** (`useExploratoryTier`): remote + valid profile + not passthrough + no client `duckgres.worker_*` option (`clientSuppliedWorkerGUCs`; GUC-sized connections acquire eagerly). Half-configured tier → nil → eager behavior, never BestEffort.
- **Lazy acquisition**: `c.executor == nil` + `SessionActivator`; `activateForStatement` acquires on first need. CP-answered statements (`SET/SHOW duckgres.query_source`, ignored SETs, no-ops, `pg_stat_activity`, empty query) MUST NOT acquire. Exceptions (all protocol paths): `SET duckgres.s3_cache`/`worker_ttl` always acquire; `SHOW duckgres.s3_cache` acquires only if `c.hasPendingS3Cache`; `SHOW duckgres.worker_ttl` never.
- A pinning FIRST statement acquires standard directly (`pinned=true` → `MarkConnectionPinned`).
- **Pin set** (all load-bearing): DML, DDL, COPY (both directions), SET, BEGIN, DECLARE (simple/batched/extended — FETCH/CLOSE rely on it), secret DDL **before** user-secrets interception, and extended Describe of a pinning statement. Parse failure pins.
- **Escalation is one-way and sticky**; `escalateWorker` destroys the small session first. Metric `duckgres_exploratory_escalations_total{reason=state|oom|heuristic, outcome=ok|canceled|capacity|draining|disabled|error}` counts every attempt; outcome via `server.AcquisitionFailureOutcome` (shared with `duckgres_session_activation_total`).
- **Failed activation/escalation is connection-fatal** (`failWorkerAcquisition`: FATAL, no ReadyForQuery). SQLSTATE from `escalationErrorSQLState`: `*server.SessionAcquireError` (classified by `sessionCreationErrorResponse`) is authoritative — 28000 disabled / 53300 capacity / 57P03 draining / 57014 cancel / 3D000 catalog / XX000 s3_cache / 53400 other. Client sees the classified message, not the internal chain. Extended handlers park it on `c.fatalErr` (one-shot; never set-then-continue).
- **OOM re-execute** only if READ, zero DataRows sent, `txStatus` idle; no RowDescription resend; retry only on the escalated worker. `isWorkerOutOfMemoryError` = DuckDB engine OOM only; pod OOMKill (`ErrWorkerDead`) never re-executes.
- **s3_cache interplay**: connect-time option is parked (`SetPendingS3CacheOption`) and applied in `ensureSessionActive` after installing the executor (never inside the activator). Failure → fatal XX000. On escalation `reapplyS3CacheAfterWorkerSwitch`; its failure (`errS3CacheReapplyFailed`, routed by `failEscalation`) is the ONE non-fatal outcome: statement ERROR XX000, flag reset to proxied, connection lives, pin kept. SHOW never lies.
- **Billing**: largest size wins over the connection; stamped at activation, restamped at escalation.
- **Accepted gaps**: admission/vCPU lease happen at first statement; a `kill` in the switcher's destroy→create window, or against an authed-but-unactivated connection, is missed (`disable` covers both via 28000 re-check); client FIN mid-activation doesn't abort the spawn.
- Tests: `server/{tier_classify,conn_tier,conn_tier_exec,conn_lazy_activation,s3_cache}_test.go`, `controlplane/{session_activation,worker_profile,compute_size}_test.go`, harness `exploratory_tier`/`_lazy_activation`/`_state_pin`/`_oom_escalation`, `org_default_profile`, `sized_worker`, `assert_worker_pod`. Keep the audit header above `exploratory_tier` current.

## Hot-Idle Reporting + Per-Org Caps (remote)

Source is durable `worker_records`. `configstore.ListHotIdleByOrg` + `GET /api/v1/workers/hot-idle` (`admin/live.go`) back the Workers page card. **Shape resolution chain everywhere**: worker profile → org default profile → CP default (`cfg.K8s.Worker{CPU,Memory}Request`, else 8/16Gi); never report zero for an unsized worker (also used by `ListWorkerLifecycleStats`, `orgHotIdleLimitsFromSnapshot`).

Caps `max_hot_idle_{workers,cpu,memory}` on `duckgres_orgs` (migration 000037; 0/"" = unlimited):
- Enforced by convergent janitor sweep `reapHotIdleCaps` retiring OLDEST first via fenced `RetireFromSnapshot`, origin `janitor_hot_idle_cap`; on error stop that org for the tick. Runs even if TTL reaper disabled.
- Cap beats the `DefaultWorkerMinHotIdle` floor.
- Write path is fail-closed: unparseable/zero/negative quantity → 400 (sweep would read it as unlimited).
- Tests: `tests/configstore/hot_idle_reporting_postgres_test.go` (+ migration asserts), `janitor_test.go` (incl. `TestOrgHotIdleLimitsFromSnapshot`), `admin/api_test.go` `TestUpdateOrgHotIdleCaps*`, `live_test.go` `TestHotIdleRoute`, `ui/src/pages/{Workers,OrgDetail}.test.tsx`, harness `hot_idle_reporting_and_cap`.

## Worker Drain Protocol (#690)

On SIGTERM workers reject new work, finish in-flight work, exit; CP marks `Draining` and retires cleanly. Drain refcount `activeWork` (`duckdbservice/service.go`): exactly one token taken/released per unit of work on every path; `reapIdle` releases tokens from `GetFlightInfo` without `DoGet`. `terminationGracePeriodSeconds=3600` must exceed `workerShutdownDrainTime` (55m).

**Adopted workers get no informer event** (informer is scoped to `duckgres/control-plane=<POD_NAME>`). So `HealthCheckLoop`'s draining branch, after `drainingPodVerifyThreshold` failed probes, `Get`s the pod and on NotFound/terminal takes `handleTerminatedWorkerLease` (origin `health_check_drained_pod_gone`). Leader backstop `reapOrphanedDrainingWorkers` (origin `janitor_draining_orphan`, `defaultDrainingOrphanGrace`) retires pod-gone draining rows via fenced CAS. Both are pod-verified. Steady `…{operation="drain",origin="worker_drain",outcome="fence_miss_lease"}` is expected (non-owner CPs). Tests: `k8s_pool_draining_orphan_test.go`, `janitor_draining_orphan_test.go` (unit-only; see `tests/mw-dev/README.md`).

## PostHog Logs (OTLP) — LOAD-BEARING

slog → PostHog Logs via OTLP (`internal/cliboot.InitLogging`); not a replacement for `ducklake.system.query_log`.
- `service.name` = role: `duckgres-control-plane` / `duckgres-worker` / `duckgres-reshard` / `duckgres`; `DUCKGRES_IDENTIFIER` → `duckgres.deployment`. Logs+traces share `otelResource(bi)`.
- PostHog level default WARN (`DUCKGRES_POSTHOG_LOG_LEVEL`); stderr uses `DUCKGRES_LOG_LEVEL`. Query text default `redacted` (`RedactForLog`, 4096). Connection key is `pid`. `user`/`pid` live on the session logger, never `SetDefault`; workers stamp org/worker via `stampWorkerLogIdentity`.
- Worker `POSTHOG_API_KEY` is a named `env:` `secretKeyRef` copied from the CP pod spec (`pod_env.go`, `k8s_pool_spawn.go`) — never `envFrom`, never plaintext `value:`; on miss WARN and omit, never fail spawn. Don't forward `ADDITIONAL_POSTHOG_API_KEYS`/`POSTHOG_ANALYTICS_API_KEY`.
- `FlushLogging()` only at drain exits (worker success/timeout, CP SIGTERM after drain, `drainAfterUpgrade`).
- `duckgres_otlp_log_export_failures_total{source,reason}` on CP; workers report `otlp_export_{enabled,failures}` in health JSON, CP adds deltas. Workers don't `InitMetrics`.
- Tests: `k8s_pool_spawn_test.go`, `otlp_export_test.go`, `internal/cliboot/*_test.go`, harness `assert_worker_pod`.

## `duckgres.s3_cache` (per-session cache bypass, remote)

`SET duckgres.s3_cache = on|off|passthrough` (also `-c`). CP intercepts (never forwarded) and calls worker `SetSessionS3Cache` (`FlightExecutor.SetS3CacheMode` → `SessionPool.SetS3CacheMode`), rebuilding the `ducklake_s3` secret: `off` = native HTTPS (CONNECT-tunnelled, no cache), `passthrough` = proxy transport + marker → `forwardUncached`, `on` = caching. Never touch global `http_proxy`.
- State follows the worker: `clientConn.s3CacheMode` flips only after a successful swap; failure → XX000 (SET/batch/connect).
- No leak to next session: `CreateSession` restores proxy (failure fails create); `DestroySession` best-effort.
- Credential refresh (`reuseExistingActivation`) honors the flag; all rebuilds serialize on `secretSwapMu`.
- Exploratory tier: applied at activation (see above).
- Closed enum via `transform.NormalizeS3Cache`; else `22023` without echoing the value, on every set path (startup option validated before acquiring).
- Non-cache deployments: no-op, session state only. Query-log sink stays proxied.
- Tests: `transpiler/s3_cache_test.go`, `server/s3_cache_test.go`, `duckdbservice/s3_cache_test.go`, harness `s3_cache_guc` (swap itself unit-only).

## `duckgres.worker_ttl` (mid-session, remote)

`SET/SHOW/RESET duckgres.worker_ttl` (`transform/setshow.go`, `server/conn_worker_ttl.go`, `controlplane/worker_ttl.go::workerTTLControlFor` → `SetWorkerTTLForPID`). Design: `docs/design/worker-ttl-pool.md`.
- Gated by `DUCKGRES_K8S_ALLOW_CLIENT_WORKER_PROFILE` (else 22023), clamped to `DUCKGRES_K8S_WORKER_MAX_TTL` (stored clamped).
- State follows the worker; SHOW falls back to the worker's current pool TTL.
- Whole minutes: persisted `ttl_minutes` (0 = deployment default), so `NormalizeWorkerTTL` rejects zero/sub-minute with 22023.
- Both reapers (leader janitor + per-CP fallback) read the persisted override.
- Exploratory: SET acquires, SHOW doesn't; escalation re-applies (failure RESETs, statement error); s3_cache and worker_ttl re-applies both always run.
- Describe must cover both 'S' and 'P'. Standalone/process: session state only.
- Tests: `transpiler/worker_ttl_test.go`, `server/worker_ttl_test.go`, `controlplane/{worker_ttl,k8s_pool_worker_ttl}_test.go`, harness `worker_ttl_guc`.

## User Persistent Secrets (remote)

`CREATE PERSISTENT SECRET` is intercepted (`server/conn_user_secrets.go`, `server/usersecrets/`), executed on the session, stored AES-GCM in `duckgres_org_user_secrets` (org/user/name), replayed in future `CreateSession` payloads (`duckdbservice/user_secrets.go`). Needs `DUCKGRES_USER_SECRET_KEY` (base64 32 bytes); unset → 0A000. Plain/TEMPORARY secrets are passthrough.
- **Isolation is the wipe at session create**: `wipeUserSecrets` drops ALL user-created secrets (persistent and not), keeping only `usersecrets.IsReservedName` (`ducklake_s3`, `__default_*`, `duckgres_*`). Runs before replay on every shared-warm create; failure fails the session.
- **Client-ATTACHed catalogs likewise**: `detachUserCatalogs` (`duckdbservice/user_catalogs.go`) right after the wipe (failure fails session; best-effort at destroy). Attached catalogs freeze credentials and `ATTACH IF NOT EXISTS` would silently reuse the prior session's target. Preserved: DuckDB internals, primary DB (lowest oid), `isSystemCatalog` allowlist (`ducklake`, `delta`, `__ducklake_metadata_*`). **New worker-managed ATTACHes must be added to that allowlist.**
- Execute-then-persist; store failure after exec → ERROR saying it won't persist. Replay failures are warnings.
- No silent non-persistence: reject persistent-secret DDL in multi-statement batches or parameterized statements.
- DROP store-fallback only on `isSecretNotFoundError`; any other failure surfaces and leaves the store alone.
- Never log/store secret text: `usersecrets.RedactForLog` for query sinks; `RedactErrorForLog(query, errMsg)` for error sinks (engine errors echo SQL). Pass the original query so it can classify.
- Tests: `server/conn_user_secrets_test.go`, `duckdbservice/{user_secrets,user_catalogs}_test.go`, harness `persistent_user_secret` (+`_isolation`, incl. attached-catalog leak).

## Admin Console (`controlplane/admin/`, `kubernetes` tag)

React/Vite SPA + REST on `:8080`; design `docs/design/admin-ui.md`, details `controlplane/admin/README.md`. Exposed via internal ALB + Cognito behind Tailscale (charts `ingress-admin.yaml`).
- **Embedded SPA**: `ui/dist` is gitignored except `.gitkeep` (keeps `go build` working without node). `just ui-build`; both Dockerfiles run `npm run build` before `go build`. Never delete `.gitkeep` or commit `ui/dist`.
- **Authz** (`authz.go`): internal secret → admin; else ALB `X-Amzn-Oidc-Data` JWT (`@posthog.com`, `email_verified != false`, else 401) mapped per-request via `duckgres_operators` (migration 000006) — `admin` row → admin, else viewer. First SSO login auto-creates a viewer; first admin is set via break-glass token. Last-admin guard → 409. `RoleGate` requires admin for mutations + audit GET; `AuditMiddleware` records mutations. Never bypass either. Operators page (`/operators`, console access) ≠ Org Users page (`/users`, DB logins) — keep labelled/cross-linked.
- **Impersonation** (`impersonate.go`, `admin_providers.go`): real session via `CreateSessionWithProtocol`, always `DestroySession` in defer, admin-only, audited with redacted SQL, writes require `allow_write=true` (CTEs count as writes). Consumes a worker and counts against org limits.
- **Metrics proxy** (`metrics_proxy.go`): client sends a panel key; PromQL built server-side from `rangePanels` → `DUCKGRES_PROMETHEUS_URL`. Never an open PromQL relay.
- **Product monitoring API** (`monitoring.go`, `GET /api/v1/orgs/:id/monitoring/{snapshot,series}`): internal-secret-only, org from path, org-scoped reads, strips usernames/PIDs/pods/images/SQL/secrets; closed enums for metric/window.
- `duckgres_admin_audit` is AutoMigrated; `duckgres_operators` is goose-migrated. `ManagedSession.Username` must be set on every create path.
- **Errors page**: in-memory ring (`server/recent_errors.go`, cap 500) captured at `logQueryError`, stores only redacted Query/Message, `GET /api/v1/errors` merged across replicas via `PeerFetcher.FetchPeers`. Not durable history.
- **Kill switch**: `POST …/users/:username/kill` = one-shot teardown (misses authed-but-unactivated tier connections); `…/disable` sets `duckgres_org_users.disabled` (migration 000011), kills sessions, refuses new connections with 28000 "account is disabled" only after password check; `enable` reverses. Disable/enable call `ReloadSnapshot()`. Cluster-wide via `PeerFetcher.PostPeers` (`?scope=local` guard), summing `killed`. Kill must be scoped to the target user.
- **Live Nodes view** (`ui/src/pages/Nodes.tsx`, `pages/nodes/peepernetes.*`): imperative DOM, polls read-only `GET /cluster/{nodes,pods,events,nodepools}` (`cluster.go`, minimal projections; nodepools v1→v1beta1, empty if no karpenter). Topbar polls `GET /cluster/summary` (workers = `app=duckgres-worker`). Backed by `Extras.ClusterClient` (nil → routes unregistered). RBAC is the separate `duckgres-control-plane-cluster-topology` ClusterRole in charts; Forbidden degrades to `{items:[]}` 200 (e2e hits this).
- **Trino views** (`trino.go`, `trino_client.go`, `pages/Trino*.tsx`, `OrgTrinoCard.tsx`, `lib/trino.ts`): live queries + audited admin kill, cell health, per-org provisioning state. Read as OBSERVER, never provisioner admin. Redacted SQL, never `RootPasswordHash`; ready orgs include non-secret `status.connection`. Reads cached/timeout-bounded, degrade to `available:false`. Shared-pool observer (`trino_pool_observer.go`) lists live instances per call — never a fixed coordinator. No pool registry → routes unregistered.
- Tests: `controlplane/admin/*_test.go` (esp. `authz`, `kill_switch`, `operators_api`, `trino`, `trino_client`, `cluster`), `session_mgr_test.go` `TestDestroySessionsForUser`, `configstore/store_test.go` `TestDisabledUserEnforcement`, `ui/src/pages/Operators.test.tsx`, harness `admin_*`, `admin_operators`, `impersonation_*`, `user_kill_switch`, `user_disable_block`, `admin_console_api` `/cluster/*` checks.

## Project-Scoped Logins (`project_reader` / `project_user`) — LOAD-BEARING

Generated per team (`duckgres_org_users.access_mode` + `team_id`, migrations 000026/000031): `posthog_team_<id>` (read-only) and `posthog_team_<id>_rw`. Minted via `PUT /api/v1/orgs/:id/teams/:team_id/{project-reader,project-user}`. Enforced by the query gateway, not GRANT.
- Both modes derive the SAME namespaces (`ConfigStore.OrgUserQueryAccess`: `<schema>`, `<schema>_data_imports`, `shadow_<team>_models`, plus non-NULL legacy `posthog.<events|persons>` overrides); only `ReadOnly` differs. Writes never widen reach.
- Missing/disabled team → empty `ReadOnly` policy (writer downgraded).
- `QueryAccessPolicy.Authorize` (`server/query_access.go`) is the single tap (simple + Parse), default-deny; unparseable → reject. `CREATE SCHEMA` / `ALTER … SET SCHEMA` denied.
- **Write targets go through `authorizeWriteTarget`** (always requires schema qualification), never the read-position `authorizeRangeVar` (which accepts visible CTE names / pg_catalog compat names — unsound for targets, e.g. `WITH shared AS (…) INSERT INTO shared`). New `authorizeWriteStatement` cases must enumerate target fields by name; the `walkMessage` RangeVar branch is just a lenient net. Tripwire: `TestQueryAccessPolicyRejectsUnqualifiedWriteTargets`.
- No passthrough for either mode (DB constraint + API guard).
- Policy changes (credential, team, disable, **mode**) tear down sessions (`changedProjectScopedUsers`, `org_router.go`). Deleting a team deletes both logins. Separate partial unique indexes on `(org_id, team_id)` per mode.
- Service credentials are not project-scoped and never touch `duckgres_org_users`.
- Tests: `server/query_access_test.go`, `configstore/query_access_test.go`, `admin/api_test.go`, `org_router_test.go`, `tests/configstore/{org_teams,migrations}_postgres_test.go`, `docs/postgres-compatibility.md`, harness `project_reader_isolation` / `project_user_isolation`.

## Service Credentials — LOAD-BEARING

Short-lived credentials for PostHog backend jobs, each its own `duckgres_service_grants` row (never `duckgres_org_users`), root-shaped at org level.

- `POST /api/v1/orgs/:id/service-credentials` `{principal, ttl_seconds?}` → `{credential_id, credential_secret, expires_at, connect}`. `principal` is audit metadata only; **every mint creates a new grant**. TTL clamped [1m, 1h], default 15m.
- `POST …/service-credentials/refresh` `{credential_id, ttl_seconds?, rotate_secret?}`: rotates by default (`secret_rotated:true`); `rotate_secret:false` renews a live grant without changing the secret (`secret_rotated:false`, secret omitted) — clients must see the explicit false before keeping their secret. Unknown → 404, revoked → 410 (terminal); expired-unrevoked may be refreshed. Never creates.
- `trino_connect` included when `DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE` (JSON map of cell ID → token rotation set) covers the org's ready cell; the validation endpoint derives the cell from the bearer token and checks the org's current assignment each request.
- `connect` = `{host: <org-id><first ManagedHostnameSuffixes entry>, port: 5432, database: "ducklake", sslmode: "require"}` — always the canonical ingress name the TLS SNI pins, never a pod/ClusterIP, never per-caller (fallback `provisioning.DefaultManagedIngressSuffix`; wired in `multitenant.go`).
- `credential_id` = `svc_<24 hex>`, server-generated; it's the pgwire username. Auth for `svc_` names uses only `Snapshot.OrgServiceGrant`; unknown/revoked/expired/blanked all fail identically with equalized bcrypt time. Live = `revoked_at IS NULL AND expires_at > now()`.
- Management by `credential_id`; plaintext unrecoverable. Removed `force_rotate` is ignored — deploy always-create Duckgres fleet-wide BEFORE PostHog drops its fallback.
- **Established sessions are never torn down** by expiry/rotation/refresh; freshness is handshake-only.
- Revoke (`DELETE /api/v1/orgs/:id/service-grants/:credential_id`) stamps `revoked_at` and blanks the hash, keeping the row. `GET …/service-grants` lists all (no hashes). No expiry deletion. Org deletion deletes the org's grants; org creation clears orphans only when the locked lookup confirms no org row.
- Mint/refresh/revoke take `LockOrgConnectionAdmissionTx`. Admission treats `svc_` as root-shaped (consumes vCPU budget, no per-user cap, no re-check of expiry).
- After write: local `ReloadSnapshot` + best-effort `/api/v1/internal/reload-snapshot` fan-out (`PeerFanout`).
- Tests: `provisioning/service_credential{,_test}.go`, `configstore/service_credential{,_postgres_test}.go`, `admin/api.go`, PostHog `products/managed_warehouse/backend/service_credentials.py`.

## Compute-Usage Billing (remote only)

Design: `docs/design/billing-pull-api.md` (metering side of `billing-compute-seconds-plan.md` still applies). Pipeline: conn end → in-proc counter keyed (org, team, query_source, worker size) → ~15s UPSERT-increment flush into `duckgres_org_compute_usage`; leader storage sampler → `duckgres_org_storage_usage`; billing pulls `GET /api/v1/billing/usage` and acks `POST /api/v1/billing/ack {watermark_high}`; leader GC deletes >30d.

- Metrics: `cpu_seconds = vCPU × ceil(conn_secs)`, `memory_seconds = GiB × ceil(conn_secs)` over the connection lifetime using provisioned size; internally millicore-/MiB-seconds (`compute_meter.go`); size stored as exact NUMERIC.
- `team_id` is informational (integer): connecting user's team if any, else org's oldest team (`OrgUsageTeamID`); storage uses `OrgOldestTeamID`. Never re-attributed.
- `query_source` GUC is a closed enum (`transform.NormalizeQuerySource`: `standard`|`endpoints`, else 22023 on every set path incl. `-c`); `server.ConnectionBilling` clamps to `standard`. Final value bills the whole connection.
- Best-effort, off the hot path; never fail a query/teardown. `cp.computeMeter` nil outside remote (nil-safe). No enable knob.
- Size plumbed via `server.SetConnectionWorkerSize` from `workerBillingSize(workerProfile)`; 0 → skip. Recorded at the same teardown as `CloseConnectionMetrics` (#841 defer).
- Bucket = connection-end floored to 60s; remainders carried across flushes.
- Serve only closed buckets: `watermark_high` ≤ now − 60s − 30s grace; GET aggregates `(cursor, watermark_high]` per key per UTC day.
- Ack (`AckComputeUsage`) advances one global cursor monotonically and deletes ≤ watermark in one txn; idempotent; beyond latest closed bucket → 400. Both routes `RequireAdmin` inside the audited `/api/v1` group.
- GC (`runComputeUsageGC`) leader-only, WARN with count (nonzero = billing stopped pulling). Graceful shutdown does a final flush.
- **Org teams** (`duckgres_org_teams`): `GET/POST /api/v1/orgs/:id/teams`, `DELETE …/teams/:team_id` (internal secret). POST is a grandfather upsert (may overwrite `schema_name` + legacy overrides; NULL = derived). Unique `(org_id, schema_name)` → 409. New-org provisioning REQUIRES `team_id` (`ErrProvisionTeamRequired`; `default_team_id` alias). DELETE removes config only; last team undeletable (409). Admin console: `GET/POST /teams`, `PUT /orgs/:id/teams/:team_id` (`schema_name` immutable). Shared logic `configstore.UpsertOrgTeamTx`/`DeleteOrgTeamTx`; harness `org_teams_crud`.
- **Storage** (`storage_meter.go`, `managed_warehouse_storage_gib_seconds`): leader-only (UPSERT is additive), every 30m (`DUCKGRES_STORAGE_SAMPLE_INTERVAL`; e2e 60s), credits `tracked_bytes × interval`. SUM over `ducklake_data_file` + `ducklake_delete_file` with no snapshot filter (never `ducklake_table_info()`/`table_stats`). NUMERIC byte-seconds; exact GiB via `byteSecondsToGiBSeconds`. Connection via `MetadataPostgresURL`. Gauges `duckgres_org_storage_{pending_delete_files,tracked_bytes}`.
- **Console usage views** (`admin/usage_api.go`): `GET /api/v1/usage/monthly`, `GET /api/v1/orgs/:id/usage/daily`, pure reads over the buffer (`Aggregate{Compute,Storage}Usage{Monthly,Daily}`), `RequireAdmin`, org-scoped by path (e2e asserts `.org_id`), return `watermark_low` + retention caveat. Pricing calculator (`UsagePricing.tsx`, `lib/pricing.ts`) is browser-only.
- Tests: `compute_meter_test.go`, `compute_billing_api_test.go`, `compute_size_test.go`, `storage_meter_test.go`, `configstore/storage_usage_test.go`, migration asserts, `admin/usage_api_test.go`, `tests/configstore/usage_{monthly,daily}_postgres_test.go`, `ui/src/pages/{Usage,OrgUsage}.test.tsx`, harness `compute_usage_pull_api` (+ `usage-monthly`/`usage-daily`).

## Discovery Endpoints

`GET /api/v1/warehouses` + `GET /api/v1/warehouse-team-ids` (`provisioning/discovery.go`) for external writers (viaduck, millpond).
- Set = ready + resharding (`writable=false`). New states MUST be classified in `discoveryStates`/`discoveryExcludedStates` (`TestDiscoveryStateClassification`) — an unclassified state reads as tenant removal.
- Teams from `duckgres_org_teams` with resolved locations via `resolveTeamTables`; overrides are bare identifiers (`configstore.ValidateOrgTeamTableName`).
- `enabled` is passthrough info, NOT an ingestion signal; only row absence stops ingestion.
- Store errors fail the whole request; zero-team warehouses degrade to empty arrays (`duckgres_discovery_broken_team_rows_total{reason}`).
- `config_generation` = max `updated_at` over warehouse+org+org-team rows, read before data; equality-compare only. Relies on `DeleteOrgTeamTx` touching the org row (`TestLatestConfigChangeCoversTeamsPostgres`).
- No plaintext credentials (SecretRefs only). cnpg details mirrored from Duckling CR status by `reconcileMetadataStoreRow` (drift-only writes).
- Separate auth surface (`RegisterDiscoveryAPI`, `admin.AnyTokenAuthMiddleware`): read-only secret (`--read-only-secret`/`DUCKGRES_READ_ONLY_SECRET`) valid ONLY here; admin secret also accepted. Never register discovery in the admin group or accept `readOnlyTokens` elsewhere. Equal to internal secret → startup fails (`validateDistinctReadOnlySecret`). Tripwires: `TestAnyTokenAuthMiddlewareScoping`, `TestReadOnlyGroupTopology`.
- Tests: `provisioning/discovery_test.go`, `tests/configstore/org_teams_postgres_test.go`, harness `discovery_endpoints`.

## Resharding — LOAD-BEARING

Operator-driven moves of an org's DuckLake catalog between metadata stores (cnpg↔cnpg, ext→cnpg, cnpg→ext). Design: `docs/design/resharding.md`. Code: `configstore/reshard.go` (migrations 000018/000021/000022), `provisioner/{reshard_runner,catalog_copy,catalog_backup}.go`, `controlplane/{reshard_runner_mode,reshard_pod,reshard_reconciler}.go`, `admin/reshard.go`, UI `ReshardForm.tsx`/`ReshardOperation.tsx`.

- **Runs in a dedicated per-op pod, never a CP**: `duckgres-reshard-op-<id>` (labels `app=duckgres-reshard`, `duckgres-op-id`; restartPolicy Never; TGPS 600; `DUCKGRES_RESHARD_POD_{CPU,MEMORY}` default 2/8Gi), CP image + SA, env from `reshardPodEnvAllowlist` (secretKeyRefs stay refs). Start creates op PENDING + spawns; pod claims via CAS, runs to terminal, exits 0 (nonzero only on infra errors). Leader reconciler respawns pending/stale ops (3 attempts then force-fail) and reaps terminal pods. Netpol `duckgres-reshard-runner-boundaries` in `k8s/networkpolicy.yaml`; prod chart needs the equivalent.
- **Barrier is the lease-GRANT check** (refuses `resharding` orgs under the same per-org advisory lock as `SetWarehouseResharding`), not the connect-time 57P03 gate.
- **Drain, never kill**: leases==0 AND queue==0 (one tx) AND zero live org workers; hot-idle retired via CAS only, never raw pod deletes.
- Flip semantics: cnpgShard change re-points in place (source orphaned, cleaned after verify); type flip to external un-renders MRs (delete vs orphan per charts `retainCnpgOnFlip`). **External stores are never modified/deleted.**
- **cnpg→ext = orphan-adopt then verified-delete**: copy → verify → set `retainCnpgOnFlip=true` and poll MRs until no-Delete policy (needs explicit get on `postgresql.sql.m.crossplane.io` roles+databases; Forbidden fails fast) → flip → verify external counts EXACTLY → only then `DROP DATABASE` source + clear flag. Any earlier failure → flip back + clear flag in one patch (`SetMetadataStoreCnpgAdopt`), provider-sql re-adopts; no copy-back. If the XRD lacks `retainCnpgOnFlip` (pruned), REFUSE.
- **Deprovision-unaffected**: `retainCnpgOnFlip` defaults false, true only mid-reshard; normal deprovision still drops role/DB. Net: harness `lifecycle_teardown_cnpg`.
- **Pre-flip backup** (`backup_catalog` step): streamed `pg_dump` (custom format, `PGPASSWORD`) → S3 multipart (16MiB × 2) to `s3://<org bucket>/_reshard_catalog_backups/op-<id>-<ts>.dump` using STS AssumeRole of the org role (`AssumeRoleFunc` injected from runner mode). Failed/empty dump deletes the partial. cnpg→ext HARD-FAILS on backup failure; others warn. URI on `backup_s3_uri` + op log with restore command. Retention via S3 lifecycle. **No object tags** (needs `s3:PutObjectTagging`, not granted). pg tools via Wolfi `postgresql-18-client` in both Dockerfiles.
- **Source identity from duckling STATUS**, validated at submit (400 on incomplete/contradictory), rechecked at recordSource. Never reintroduce "empty kind defaults to cnpg-shard".
- **Metadata credentials come from `status.metadataStore.credentialSecretRef`** (name/namespace/key all required, `ducklings` namespace only; `DucklingClient.Get` resolves); missing/unreadable is a hard error. Never read credentials from CR status.
- **Rollback patches the source shard VALUE back, never removes the key**; ext→cnpg rollback nulls `cnpgShard`. Never emit an XRD-invalid patch: invalid from_shard (`isValidCnpgShardName`) or incomplete external block → skip, log ERROR instructions, leave warehouse blocked in `resharding`. sslmode from kind via `sslModeFor` (cnpg disable, external require).
- **Compaction pause** (`SetCompactionEnabled`) pins effective `spec.ducklake.enabled` when the CR lacks `spec.ducklake`, never removed (`TestSetCompactionEnabledPinsDuckLakeEnablement`).
- **Ext target password is ephemeral**: creating replica's in-memory stash → runner pulls once from `GET /api/v1/reshards/:id/password` on that replica's pod IP (URL persisted on `password_url`; internal-secret only, SSO admin 403); pruned when terminal. 404 → proceed stashless, op fails with "cancel and re-run" and rolls back.
- **Ext SM secret** must be ESO-readable raw string: positive allowlist `esoReadableSecretPrefixes` (`posthog-*`/`duckling-*`), RDS-managed names (`rdsManagedSecretNamePattern`) rejected with a specific message. UI mirrors in `ui/src/lib/reshard.ts::classifySecretName`.
- **cnpg→ext fails fast at submit**: name allowlist + bounded `SELECT 1` probe (`admin.ExternalTargetProber` / `PGCatalogCopier.Probe`, `sslmode=require`); nil prober skips. 400 never echoes the password.
- **Fencing**: claim bumps `runner_epoch`; writes CAS on (runner, epoch); >5m stale heartbeat is takeover-able; copy holds a target-DB advisory lock.
- **Takeover must call `reconstructProgress()` before `run()`** to rebuild `blocked`/`compactionPaused`/`flipped`/`retainRequested` from the row (conservative; over-marking is safe). Otherwise a cancel leaves the warehouse blocked.
- Tests: `tests/configstore/reshard_postgres_test.go`, `provisioner/reshard_runner_test.go` (incl. `TestReshardBackup*`), `provisioner/k8s_client_test.go`, `admin/reshard_test.go`, `controlplane/{reshard_pod,reshard_reconciler,reshard_runner_mode}_test.go`, migration asserts, `tests/manifests/manifests_test.go`, harness `reshard_*` + `lifecycle_teardown_cnpg`; charts `composition_retain_cnpg_test.sh`. cnpg→ext positive path is unit-only; cnpg→cnpg positive needs a second shard (follow-up).

## Trino Cells (`kubernetes` tag)

Trino-enabled orgs get a catalog, login, authz and resource limits on a shared cell. The CP is the only writer: `provisioner/trino_provisioner.go` projects every tick from `duckgres_managed_warehouse_trino` + warehouse row + Duckling CR. Config: `DUCKGRES_TRINO_CELLS_FILE`; only `mode: "shared-pool"` accepted; invalid config fails startup. See `docs/trino-cells.md`.

- **Catalog is DuckLake with NO secret**: `connector.name=ducklake`, JDBC `ducklake.metadata.connection-url` (sslmode by store kind), `connection-user`, **`connection-password-file`** (never `connection-password` — Trino logs/renders catalog properties), `ducklake.data-path`, `fs.s3.enabled` (NOT `fs.native-s3.enabled`), `s3.region`, `s3.auth-type=IAM_ROLE`, `s3.iam-role` (per-org duckling role), small `s3.max-connections`.
- **Tenant passwords**: one Secret keyed by org id (`TrinoTenantSecretName`, `TenantSecretMountPath`), read via the same `credentialSecretRef` resolver as worker activation. Authoritative (disabled org's key removed).
- **Client coordinates**: `status.connection` only when catalog ready + coordinator reachable; registry `client_url`, no direct-coordinator fallback; never a password. A `{database_name}` leading host label (`ResolveTrinoClientURL`) advertises `<database_name>.<domain>` + user `root` (fork's host-qualified users) — set only once routed.
- **Reconcile order**: cluster secrets → auth files → resource groups → OPA bundle → tenant passwords → catalogs; `globalErr` skips catalogs if any projection failed.
- **`ensureClusterSecrets` write-once with sentinel** (`duckgres_trino_cluster_bootstrap`): missing + not bootstrapped ⇒ generate; missing + bootstrapped ⇒ fail loud (would split-brain the shared secret). Admin/observer hash pairs regenerate together if missing/incomplete; single-snapshot read, resourceVersion update, conflict → reload/adopt (5 attempts). Complete-but-invalid pairs or transient errors fail without writing. Never overwrite a complete pair.
- Catalog publication uses the pool writer fence (no coordinator CREATE/DROP CATALOG). Admin may read `catalog_name`,`state` from `system.metadata.catalogs` + node inventory; `FAILING` placeholders aren't usable.
- **Catalog naming is a three-way contract**: `TrinoCatalogName` (`org_` + sanitized id), `opa.ManagedCatalogPattern`, regex in `policy.rego` (`TestTrinoCatalogNameMatchesManagedNamePattern`, `TestPolicyRegoContainsManagedNamePattern`).
- **Principal → org by exact lookup** (`configstore.NewTrinoPrincipalOwners`: `<database_name>` → root, `<database_name>.<username>` → login), used by metering (`trino_usage_collector.go`) and console.
- **Every duckgres login authenticates as `<database_name>.<username>`**: `BuildTrinoAuthFiles` copies the bcrypt hash unchanged. Bare `<database_name>` follows root exactly (no/disabled/blank root drops only it, everywhere; `svc_` attribution keeps a root-independent db→org index). Load-bearing: usernames pass `trinoUsernamePattern` allowlist (prevents password.db injection); `rejectPrincipalCollisions` holds back colliding orgs; `orgCaptureRegex` captures only up to the first `.`.
- **Project-scoped logins join `scope_<org>_team_<id>`** with `group_scopes` narrowing (scopes only subtract), derived from `OrgUserQueryAccess`; unresolvable scope → dropped. No write authority on Trino.
- **`provisioner/opa/policy.rego` is the tenant-isolation boundary — security review.** Query visibility same-org only: `ViewQueryOwnedBy`, `FilterViewQueryOwnedBy`, `KillQueryOwnedBy` allowed only when requester and owner (`input.action.resource.user.{user,groups}`; `ImpersonateUser` uses a groups-less shape) share a bundle-known `org_<sanitized>` group. Admin gets no cross-tenant visibility. Every filter op needs a `batch` rule too (`opa.policy.batched-uri` on; `TestBatchedQueryFilteringMatchesNonBatched`).
- **Observer (`__duckgres_observer`)** = console's read-only identity: cluster-wide query view/filter/kill + `ReadSystemInformation` (MANAGEMENT_READ resources enumerated in policy.rego), NO tenant catalog, excluded from `tenant_owns_catalog`/same-org match. One data grant: `AccessCatalog` on `system` + `SelectFromColumns` only on `system.runtime.nodes` (`TestObserverSystemGrantIsPinnedToTheNodesTable`). `is_observer` = username AND group. Credential via `ensureCredentialPair`, read per call via `TrinoProvisioner.ObserverCredential`.
- **Resource groups keep selectors for `root.admin.__admin_provisioner` and `root.admin.__duckgres_observer`** (else they become phantom JMX tenants).
- **Ownership explicit**: `trino_cell_id = registered:<cell-id>`; assigned at selection/default, never claimed or changed by reconcile. Bundles pool-scoped at `/bundles/trino/<cell-id>` with that pool's token; no fallback; mounted after bootstrap.
- Tests: `provisioner/{trino_provisioner,trino_cluster_secrets}_test.go`, `provisioner/opa/*_test.go`, `provisioning/api_test.go`, `tests/configstore/{trino,trino_cluster_secrets}_postgres_test.go` + migrations, `admin/trino{,_client}_test.go`, `ui/src/lib/trino.test.ts`, `tests/mw-dev/e2e/trino.sh`. Cell moves: `tests/configstore/trino_selection_postgres_test.go`, `admin/trino_fleet_test.go`, harness `trino_cell_move`.

## Shared Trino Compute Pool (`mode: "shared-pool"`) — LOAD-BEARING

Operator-managed pool of immutable Trino instances. Code: `controlplane/trinopool/` (blueprint, phases, planner), `trinocatalog/` (fenced publisher), `trinogateway/` (Gateway v1 client), `controlplane/trino_pool_*.go`; migrations from 000041 (upstream owns 000040).

**Env** (default-off): `DUCKGRES_TRINO_POOL_ENABLED` (pooled cell with it off FAILS startup), `_OPERATOR_ENABLED`, `_GATEWAY_URL`, `_CATALOG_WRITER_{ENABLED,BOOTSTRAP,DSN_FILE,WRITER_IDENTITY}`, `_CONFIG_{CONFIGMAP,NAMESPACE,REGISTRY_KEY}` (REQUIRED), `_CATALOG_SCHEMA` and `_CATALOG_CELL_ID` (both REQUIRED with the writer), `_PUBLISHER_IMAGE` (own digest-pinned image), `_NODE_DISRUPTION_ENABLED`.

Identity & config:
- Pool keeps the cell's routing group and namespace.
- **Catalog store partition is its own identity** (`trinoPoolCatalogCellID` = every coordinator's `catalog-store.cell-id`), never derived from the `registered:` pool id; required, no fallback. `publication_revision` addresses the pool. Regressions: `TestCatalogWriterPublishesUnderTheConfiguredStorePartition`, `TestCatalogWriterRequiresAStorePartition`.
- Missing/invalid desired config or a vanished pool **freezes** at last-good — never desired=0, never startup abort.
- **Desired state is published from ONE read of the ConfigMap API object** (registry + blueprint from the same read; blueprint key = last element of `blueprint_file`), not mounted files (boot source only). Pooled cell without the ConfigMap name refuses to start.
- **Fences, not leadership**: janitor lease picks the executor; `authority_epoch` fences every durable write, K8s object (annotation checked before adopt) and Gateway call (`controllerEpoch`). Seeding the row is the only unfenced write (INSERT only). A refused fenced write ENDS the term. Fence owner is per process.
- **Ordered**: blueprint `generation` must not go backwards; a backwards generation freezes while KEEPING the lease. Backstop only.
- Catalog writer fence = pool authority, claimed on lease win (not startup); takeover needs strictly higher epoch.

Catalog/publication:
- **Published revision is read from the catalog STORE**; `publication_revision` is a cache. Failed checkpoints are returned, the writer's claim checkpoints what takeover read, and each tenant-admission tick reconciles against store writer state and fails CLOSED. `reconcileOnce` isolates admission from compute lifecycle.
- Managed-Hoglake connector adoption answered from `trino_catalogs` rows (`CatalogConnectors`) — the published definition, not proof of operation.
- Catalog writer: one fenced txn per mutation (writer-state row lock, epoch+identity, journal replay, `catalog_count` recomputed); schema via `trinocatalog.EnsureSchema` (`tests/trinocatalog/`). No implicit epoch claims; lost COMMIT resolved from the journal.

Member lifecycle:
- **Failure is not drain**: SUSPECT excludes from new work; LOST needs positive evidence the admitted process ended. Repairs name the replaced instance (repair budget). No local path back from SUSPECT — it leaves via loss claim or planned drain after `trinoPoolSuspectDrainAfter` (not reducing serving count again). Planned ACTIVE drains keep the serving-floor guard.
- **Capacity repair counts SERVING only**; one PREPARING candidate at a time; a repair holds its slot for its lifetime; `repair_for` reserves one target; departing members are repair targets only while short. Never exceed desired+surge+repair, lower the floor, skip validation, or clear the repair flag locally. Deploy Gateway support first. Auto-normalization after targets retire is a TODO.
- **Process-termination evidence** (`processTerminationEvidence`): all recorded objects verifiably absent, OR the exact recorded container instance (migration 000041) is no longer running in its pod. Anything ambiguous fails closed (unreadable cluster, partial pod list, >1 live coordinator pod, no recorded container id, mismatched termination). Label-filtered omission ≠ deletion; timeouts never evidence; never delete/restart to manufacture proof. Boundary: what the K8s API reports. Regression: `TestALiveAdmittedProcessIsNotDeclaredLostByAnUncorrelatedTermination`.
- **Serving member's process identity is re-observed** (≤1/`trinoPoolIdentityObserveEvery`); a different identity suspects; no answer changes nothing; stored identity never updated.
- **FAILED_PREPARING is cleaned up**: Gateway grants never-admitted retirement → delete UID-bound objects → verified absence → FAILURE_RETIRED. PREPARING candidates whose release/blueprint digest (snapshot's, not SpecDigest) differs from desired take this path; VALIDATING does not.
- **Admission is a durable step**: intent recorded before the call; OK/FAILED/UNKNOWN distinct; step identity = business intent (not envelope); decided outcomes never re-decided; failures record `attempts`/`next_attempt_at` (full jitter 0.5s→30s, durable); completion closes the op (`terminal_at`).
- **Identity persisted before any K8s object**; deterministic names; identities/endpoints never reused. New names `cell-<8 random hex>` (crypto random).
- **Blueprint is not a template engine**: validated JSON with real `PodTemplateSpec`s; duckgres injects only names, labels, selectors, replicas and the three `identity_binding` env vars. Digest-pinned images (sidecars too). Per-instance immutable snapshot. Worker anti-affinity narrowed to the instance.
- **Serving instances roll on blueprint changes** (parsed comparison, not stored `SpecDigest`; encoder/hex-case changes don't roll). Malformed digest + readable snapshot → guarded replacement of that instance; unreadable snapshot blocks only that instance's replacement (never infer its namespace). Registry-only changes don't roll.
- **Deletion requires an irreversible Gateway retirement claim** for that incarnation, only from RETIRING (or FAILED_PREPARING). UID verified per object; done only on verified absence incl. terminating pods. Pool-shared blueprint objects refused by name.
- **Drain has no deadline** (Gateway obligations drive sealing); serving-floor refusals surface, never overridden.
- **Administrative recovery** of an absent admitted pod: UID verified against every page of an unfiltered namespace inventory (instance's pinned namespace). With explicit destructive authorization, Gateway records `DESTRUCTIVE_OVERRIDE`/`FAILED`. Floor, exact identity, fencing and UID-scoped teardown still apply. Inventories cached 30s per namespace only if authorization preceded listing; errors throttled, never evidence; proofs last one leadership term. After the irreversible claim, retries finish cleanup without rechecking the floor.
- **Voluntary node replacement** (`_NODE_DISRUPTION_ENABLED`, requires operator): charts set `karpenter.sh/do-not-disrupt=true`; operator protects existing pods after verifying pod/ReplicaSet/Deployment UID ownership; Drifted/deleting evidence needs complete NodeClaim inventory + exact identity; migration 000043 records the first request per instance under the fence; operator cordons the exact node (UID/resourceVersion), never evicts/deletes. Surge admits a replacement before draining. Node lookup errors block only voluntary rollout. See `docs/runbooks/trino-pool-node-replacement.md`.

Validation & auth projection:
- **Candidate validation (no canary)** via its own Service with the observer credential: `/v1/catalog/sync` enabled+ready, zero failed catalogs, applied revision ≥ durable `publication_revision`, stable process identity, worker count = running pods, running images = pinned release, and `auth-revision` only when every security component reports exactly what this CP serves: OPA `opa.PolicyRevision` (`data.trino.revision`, digests policy bytes + data) and `sha256:` of `password.db`/`group.db`; every instance of a required kind must match. **`opa.policy.revision-uri` is REQUIRED** on pooled coordinators (absent ⇒ fail closed).
- **Auth projection is fenced**: config store records accepted digest+revision under pool authority, built from rows read inside a `REPEATABLE READ` txn (project scopes from the same read). Only a process whose startup `_PUBLISHER_IMAGE` equals the ConfigMap `publisher-image` (both `@sha256:<64 hex>`; equality, never ordering) may advance. Auth Secret written under that revision with resourceVersion CAS, never replacing a newer stamp. Bundle handler gates the captured bundle before 200 AND 304, reads the durable record EVERY request (uncached — OPA activates inline), unreadable = refusal. Non-advancer replicas still build/serve, write the Secret only on digest match, and never report it as failure. Admission compares against the accepted projection.

Gateway protocol:
- **A retried step repeats the IDENTICAL request** (Gateway hashes the whole body minus envelope): no re-read generations, no re-stamped `observedAt` (omitted from loss claims), no re-probed boot identity. Non-reproducible requests are READ BACK and the recorded value adopted; a failed read-back stays an error. Accepted read-back states are explicit sets; retirement counts as loss only if kind FAILED. Regressions: `TestALostResponseDoesNotChangeTheRetriedRequest`, `…RetriedMemberRequest`.
- **One instance never stops the pool**: `progressInstances` records and continues; only a lost fence ends the sweep.
- **Tenant admission = committed publication**: publish principals (PUT `…/tenants/{t}/principals`), then barrier open → per-member receipt (member's `/v1/catalog/sync` must show the catalog revision + all three projection revisions) → commit. Departed tenants REVOKED once. With the gate on, warehouses stay Provisioning until commit; readiness follows the COMMITTED target revision (not state). Durable record (migration 000041), never leader memory.
- **A refusal settles an occurrence only on its FIRST request** (`failTenantStep(firstAttempt)`: true from `publishChangedBindings`/`revokeDepartedTenant`, false from `reissuePublication`/`reissueRevoke`) — a reissue's earlier copy may still land. Pinned: `TestACheckpointedBindingNeverDivergesFromTheGateway`. Known limitation: UNKNOWN-then-persistently-refused tenants stay held; don't "fix" by closing on reissue.
- **One boot identity**: `processId` probed before registration, sent as `bootId` on register and admit; restart before admission → `FAILED_PREPARING`.
- **Transport**: plain internal HTTP to each instance's ClusterIP; TLS at the Gateway; probes set `X-Forwarded-Proto`; never `allow-insecure-over-http`; no pool cert/CA. Unencrypted in-cluster hop accepted (NetworkPolicies deferred) — don't call admission non-bypassable in-cluster.
- **Client matches the Java source** (`GET members` bare array; one `admit` with nested receipt; `desiredMembers`/`maxRepair`; Gateway computes guard hash). Fixtures from real records via `tools/gatewaywire`. Pooled registration needs an INACTIVE Gateway backend record, never activated via the legacy route.
- **Gateway auth**: with `DUCKGRES_TRINO_MANAGED_GATEWAY_USERNAME` set, Basic (username + rollout token) plus `X-Gateway-Transaction-Admin-Token`; unset → token-only Bearer.
- **Principal binding** is derived from the same projection as `password.db` (asserted identical in test).

Tests: `controlplane/trinopool/*_test.go`, `trinogateway/*_test.go` (see `tools/gatewaywire/README.md`), `controlplane/trino_pool_*_test.go` (incl. `_member_retries`, `_publication_order`), `tests/configstore/trino_pool_postgres_test.go`, `tests/trinocatalog/*_postgres_test.go`, harness `trino_shared_pool_disabled`. New migrations: update version asserts in `tests/configstore/migrations_postgres_test.go` incl. `TestConfigStoreMigration40PinsExistingTrinoBackends`.

## Logical Catalog Alias (`org_<database_name>`)

Startup `database` may be `""`, `ducklake`, or the SNI-resolved org's `configstore.TrinoCatalogName` (else 3D000), so SQLMesh sees one catalog name across Duckgres and Trino.
- **Identity stays SNI-only**: the alias is compared to the already-resolved org's name, never used as a lookup key (that would reintroduce what PR #651 removed). See `ResolvePostgresConnection`.
- `EffectiveCatalog` stays `ducklake` (execution); `LogicalCatalog`/`visibleCatalog` is reported by `current_database()`, `pg_database`, `information_schema`, logs; chosen in `visibleCatalogName`.
- `LogicalCatalogTransform` rewrites `<alias>.public.t` → `ducklake.main.t`; `rewriteDirectQuery` expands `USE <alias>`.
- Opt-in; `ducklake`/empty connections unaffected.
- Tests: `configstore/store_test.go`, `session_search_path_test.go`, `server/{direct_query_rewrite,logical_catalog_alias,session_database_metadata}_test.go`, harness `logical_catalog_alias`.

## TODO Reference

`TODO.md` is a lightweight backlog. Compatibility status lives in `docs/postgres-compatibility.md`.
