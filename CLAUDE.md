# Claude Code Context for Duckgres

This file provides context for Claude Code sessions working on this codebase.

## Project Overview

Duckgres is a PostgreSQL wire protocol server backed by DuckDB. It allows any PostgreSQL client (psql, pgAdmin, lib/pq, psycopg2, JDBC, etc.) to connect and execute queries against DuckDB databases.

## Architecture

Duckgres has three deployment topologies, built from three run modes (`standalone`, `control-plane`, `duckdb-service`):

**1. Standalone** — single process. One binary running in `standalone` mode handles the PG wire protocol, auth, TLS, transpilation, and DuckDB execution itself. Each user gets their own DuckDB database in-process.
```
PG Client → TLS → Server (standalone) → DuckDB
```

**2. Control plane + local process workers** — single host, multiple processes. A `control-plane` parent process owns client connections (TLS, auth, PG wire, transpilation) and spawns child `duckdb-service` worker processes, communicating via Arrow Flight SQL over Unix sockets. Used for stronger isolation between sessions on a single host. Selected with `--worker-backend process` (the default).
```
PG Client → TLS/Auth/PG Protocol → Control Plane (process)
                                 → Flight SQL (UDS) → local Worker process (DuckDB)
```

**3. Control plane + remote workers on Kubernetes** — multitenant cluster deployment. The `control-plane` runs as its own pod and routes per-org traffic to dedicated `duckdb-service` worker pods over TCP+TLS. Worker pods are scheduled by the control plane via the K8s API; org config and worker state are persisted in a Postgres-backed config store. Selected with `--worker-backend remote`; requires a binary built with `-tags kubernetes`.
```
PG Client → TLS/Auth/PG Protocol → Control Plane pod
                                 → Flight SQL (TCP+TLS) → per-org Worker pod (DuckDB)
```

In topologies 2 and 3, the control plane also exposes an Arrow Flight SQL ingress (`--flight-port`) for clients that prefer Flight over the PG wire protocol.

### Key Components

- **main.go / config_resolution.go**: CLI flags; effective config resolution (CLI > env > YAML > defaults), including env-only K8s knobs.
- **server/** — PG wire protocol server and DuckDB execution
  - Wire protocol & connections: `server.go`, `conn.go`, `conn_errors.go`, `conn_query_exec.go`, `conn_results.go`, `conn_copy.go`, `conn_extended_query.go`, `conn_pg_stat_activity.go`, `conn_cursor.go`, `protocol.go`, `exports.go`
  - Execution: `executor.go`, `flight_executor.go`, `chsql.go`, `transient.go`
  - Catalog & types: `catalog.go`, `types.go`, `session_database_metadata.go`
  - Auth, TLS, rate limiting: `auth_policy.go`, `ratelimit.go`, `certs.go`, `acme.go`
  - DuckLake: `ducklake_migration.go`, `checkpoint.go`
  - Observability: `querylog.go`, `tracing.go`
  - ProcessIsolation child workers: `parent.go`, `worker.go`, `worker_activation.go`, `worker_control.go`
  - Flight SQL ingress (shared with control plane): `flightsqlingress/`
- **controlplane/** — Multi-process / multi-tenant control plane
  - Core: `control.go`, `session_mgr.go`, `worker_mgr.go`, `worker_pool.go` (process/k8s abstraction), `validation.go`, `sdnotify.go`
  - Flight SQL ingress adapter: `flight_ingress.go`
  - Runtime loops: `janitor.go`, `leader_loop.go`, `memory_rebalancer.go`, `runtime_tracker.go`
  - K8s / multitenant under build tag `kubernetes` (including: `multitenant.go`, `k8s_pool.go`, `k8s_pool_acquire.go`, `k8s_pool_spawn.go`, `k8s_pool_lifecycle.go`, `k8s_pool_reconcile.go`, `k8s_pool_helpers.go`, `k8s_factory.go`, `org_router.go`, `org_reserved_pool.go`, `sts_broker.go`, `shared_worker_activator.go`, `worker_rpc_security.go`, `janitor_leader_k8s.go`)
  - Subpackages: `admin/` (HTTP admin API + dashboard, `kubernetes` tag; includes the models explorer UI `static/models.html` + `models_api.go`, and `devserver/` for local UI dev against a port-forwarded CP — see `admin/README.md`), `provisioner/` (k8s controller, `kubernetes` tag), `provisioning/` (HTTP API), `configstore/` (Postgres-backed config)
- **duckdbservice/** — DuckDB Arrow Flight SQL service
  - Core: `service.go`, `flight_handler.go`, `arrow_helpers.go`, `auth.go`, `config.go`
  - Lifecycle, caching, profiling, metrics: `activation.go`, `transient.go`, `cache_proxy.go`, `profiling.go`, `progress.go`, `metrics.go`
- **transpiler/** — AST-based PostgreSQL → DuckDB SQL transpiler
  - Top-level: `transpiler.go`, `config.go`, `boolpredicates.go`, `show_create.go`
  - `transform/`: individual transforms; see registered pipeline in `transpiler.go` `New()`

## Run Modes

- **standalone** (default): Single process, handles everything including TLS, auth, PG protocol, and DuckDB execution.
- **control-plane**: Multi-process. Owns client connections end-to-end (TLS, auth, PG wire protocol, SQL transpilation, optional Flight SQL ingress) and routes queries to a worker pool.
  - **Process backend** (default, `--worker-backend process`): local Flight SQL workers over Unix sockets.
  - **Remote backend** (`--worker-backend remote`): per-org Kubernetes worker pods over TCP+TLS. Multitenant; requires `-tags kubernetes` and a Postgres-backed config store. Adds config store, org router, runtime tracker, janitor/leader election, and a provisioning/admin HTTP API.
- **duckdb-service**: Thin DuckDB execution engine exposed via Arrow Flight SQL. Spawned automatically by the control plane as worker processes, or run standalone for testing.

Key CLI flags for control-plane mode:
- `--mode control-plane|duckdb-service|standalone`
- `--worker-backend process|remote`
- `--process-min-workers N` / `--process-max-workers N`
- `--process-retire-on-session-end`
- `--worker-queue-timeout DURATION` / `--worker-idle-timeout DURATION`
- `--idle-timeout DURATION` — connection idle timeout: a client connection with no traffic for this long is closed and its worker released to hot-idle (in control-plane mode an idle connection otherwise pins a worker forever). **Control-plane default is `60s`** (`server.DefaultControlPlaneIdleTimeout`; standalone defaults to `24h`); a negative value disables it. `server.New` applies the standalone default, so the control plane sets it explicitly before `InitMinimalServer` (which skips that defaulting).
- `--memory-budget SIZE` (default 75% RAM) / `--memory-rebalance`
- `--socket-dir /path` (process backend)
- `--handover-drain-timeout DURATION` (default `24h` process; **remote default is `0` = unbounded** — the CP waits for active sessions for as long as it takes and the pod's k8s `terminationGracePeriodSeconds` is the only hard wall. cloudflare/tableflip FD passing applies to process/standalone single-host upgrades, not k8s pod replacement.)
- `--flight-port N` (Arrow Flight SQL ingress) plus `--flight-session-idle-ttl`, `--flight-session-reap-interval`, `--flight-handle-idle-ttl`, `--flight-session-token-ttl`
- `--ducklake-delta-catalog-enabled` / `--ducklake-delta-catalog-path`
- Remote backend (requires `--config-store`; `-tags kubernetes` for K8s pool):
  - Config store: `--config-store`, `--config-poll-interval`, `--internal-secret`
  - K8s pool: `--k8s-worker-image`, `--k8s-worker-namespace`, `--k8s-control-plane-id`, `--k8s-worker-port`, `--k8s-worker-secret`, `--k8s-worker-configmap`, `--k8s-worker-image-pull-policy`, `--k8s-worker-service-account` (no global worker cap — per-org `Org.MaxWorkers`, 0=unbounded, is the only cap)
  - AWS / STS: `--aws-region`
  - Compute-usage billing (emit side): `--billing-ingest-url` / `--billing-ingest-token` (env `DUCKGRES_BILLING_INGEST_URL` / `DUCKGRES_BILLING_INGEST_TOKEN`). Both must be set to enable per-org compute-usage metering+emit; either unset disables it (usage ships nowhere, queries never fail). See `docs/design/billing-compute-seconds-plan.md` and "Compute-Usage Billing" below.
  - Pod scheduling knobs (CPU/memory requests, node selector, tolerations) are env-only — see `config_resolution.go`.

Key CLI flags for duckdb-service mode:
- `--duckdb-listen` (e.g., `unix:///...` or `:8816`)
- `--duckdb-listen-fd` (internal; set by control plane)
- `--duckdb-token` (bearer auth)
- `--duckdb-max-sessions` (0=unlimited)

## Configuration

Configuration is resolved in `config_resolution.go` with the following precedence (highest to lowest):
1. CLI flags (`--port`, `--config`, etc.)
2. Environment variables (`DUCKGRES_PORT`, etc.)
3. YAML config file
4. Built-in defaults

Note: `--mode` is CLI-only (not loadable from YAML/env). A handful of K8s pod-scheduling knobs are env-only (no CLI flag).

## Keep docs in sync with behavior

When you change a behavior, default, flag, or invariant that is documented
anywhere in the repo, **update that documentation in the same PR.** Stale docs
are worse than no docs — they actively mislead the next reader (human or agent).
This applies to, at least: this `CLAUDE.md`, `README.md`, `docs/`, CLI flag help
text (`main.go` / `cliflags.go`), and any design/plan docs that pin the changed
behavior. Concretely: if you change a default value, a flag's meaning, a drain or
shutdown semantic, an activation/routing/teardown order, or any of the
LOAD-BEARING CONTRACT sections below, grep for the old value/term across `*.md`
and help strings and fix every mention. A behavior change that leaves a doc
asserting the old behavior is incomplete, the same way a behavior change without
a test is incomplete.

## Development

The project uses [just](https://github.com/casey/just) as a command runner. Run `just` to see all available recipes for building, testing, running, metrics, and scripts.

## Testing

**Every feature, behavior change, bugfix, AND refactor that affects runtime or
cluster behavior MUST ship with a solid end-to-end test case in
`tests/e2e-mw-dev/` (`harness.sh`).** This is not just for new features — any
change to how the system behaves at runtime (new capability, changed semantics,
a fixed bug, a new config knob, an activation/routing/teardown tweak) extends or
adds a harness assertion in the same PR. Refactors count too: when you move or
rewrite a code path the harness covers, confirm the relevant assertion still
exercises it (and update it if the path moved) — a refactor that quietly drops
e2e coverage is a regression in the test suite even if behavior is unchanged. Unit/package tests are necessary but not sufficient: a
change is only "done" once it is exercised against the real mw-dev cluster —
real worker pods, real Crossplane ducklings, real cnpg/RDS metadata, real
Lakekeeper, real S3/Iceberg/STS. "Solid" means a deterministic pass/fail
assertion of the actual user-visible behavior (not just "it didn't error"), with
transient/cold-pool conditions handled, on both metadata backends (cnpg + ext)
where it touches metadata. A bugfix gets a regression assertion that would have
caught the bug. If a change genuinely cannot be asserted in-Job (e.g. it needs
cnpg-shards exec, or warm-pool-only state), say so explicitly in the
harness/README with the reason — don't silently skip. The harness is the gate
that catches what unit tests fake.

Three test lanes worth knowing about, in increasing order of blast radius:

- **Unit / package tests** (`go test ./...`): in-process, no external deps. Where most coverage lives. Includes `tests/manifests/` (static-manifest artifact asserts for `k8s/rbac.yaml` + `k8s/networkpolicy.yaml`).
- **`tests/integration/`** (`just test-integration`): spins up the standalone server binary against a real MinIO + Postgres metadata store via docker compose. Covers wire protocol, DuckLake on real S3-compatible storage, transpilation against a live server.
- **`tests/e2e-mw-dev/`** (per-PR GitHub workflow `e2e-mw-dev.yml`): the full multi-tenant activation pipeline against the **real posthog-mw-dev EKS cluster** — real Cilium, real Crossplane ducklings, real cnpg-shard + external-RDS metadata, real per-org Lakekeeper, real AWS S3/Iceberg. A shell harness (`harness.sh`) runs as an in-cluster Job per PR; `run.sh` orchestrates deploy/test/teardown/e2e-cleanup. **Replaces the retired kind suite** (`tests/k8s/`) — that suite's `k8s-integration-tests` CI job and its Go tests are gone; the supporting `k8s/` scripts/manifests + Dockerfiles are kept for now. See `tests/e2e-mw-dev/README.md`.

### When code changes obligate test changes

`tests/e2e-mw-dev/` is the only place we exercise the full activation pipeline (control plane → STS broker → worker pod → DuckDB → ATTACH against real cloud storage). If your change touches any of the following, treat updating the harness as part of the change, not a follow-up:

- `controlplane/shared_worker_activator.go`, `controlplane/sts_broker.go`, anything in the activation payload shape (`TenantActivationPayload`, `server.DuckLakeConfig`, `server.IcebergConfig`)
- `server/server.go::AttachDeltaCatalog`, `server.AttachIcebergCatalog`, `server.attachDuckLake*`, `server.refresh*Secret`
- `server/iceberg/` (config, dispatcher, backend implementations) — the harness provisions iceberg-enabled ducklings on both cnpg + ext backends and asserts the catalog attaches + reads/writes
- `controlplane/configstore/models.go` — new columns flow through the provisioning API the harness calls; exercise them via a provision body field
- `duckdbservice/activation.go`, `worker_activation.go` — worker-side activation order
- Any code path that wires AWS credentials through to DuckDB SECRETs

The contract: if the harness no longer exercises a path you changed, **update `harness.sh`**; if your change removes a path it asserts against, **delete the assertion**. The DuckLake round-trip / durability / concurrent-writers / iceberg activation checks in `harness.sh` are the load-bearing ones for catalog wiring — keep them honest.

## Dependencies

- `github.com/duckdb/duckdb-go/v2` - DuckDB Go driver
- `github.com/pganalyze/pg_query_go/v6` - PostgreSQL SQL parser (CGO, uses libpg_query)
- `gopkg.in/yaml.v3` - YAML config parsing

## Known Limitations

- No replication
- Some pg_catalog tables are stubs (return empty)
- Unmapped DuckDB types (MAP, STRUCT, UNION, ENUM, BIT) fall back to OidText
- DML RETURNING is not supported via extended query protocol (see below)

## DML RETURNING Detection

DML with RETURNING is rejected at extended-query Describe time with SQLSTATE `0A000` — the Describe path probes schema by executing the query, which would cause an unintended mutation. Detection lives in `isDMLReturning` and friends in `server/conn.go` (heuristic SQL-aware lexer, with any-depth scanning for WITH-prefixed writable CTEs). Invariants for anyone editing this code:

- **False negatives are dangerous** — they cause silent mutations during Describe. False positives are safe (just an error to the client). Err toward false positives.
- All detection is heuristic string scanning. If precision becomes critical, switch to `pg_query_go` AST parsing.
- LIMIT 0 does NOT prevent CTE side effects — Postgres CTEs are optimization fences, so writable CTEs execute even with LIMIT 0.
- DuckDB does not currently support MERGE. If it adds MERGE RETURNING, add `MERGE` to the prefix check in `isDMLReturning`.

## Worker Session Model (k8s / remote backend) — LOAD-BEARING CONTRACT

In the **control-plane remote/k8s backend** a worker pod serves **exactly one
client query session at a time**. This is deliberate: `workerDuckDBLimits`
(`controlplane/control.go`) gives the single session ~75% of the *whole pod's*
RAM + all CPU cores — it does NOT divide by session count. Two sessions on one
pod would each believe they own 75% → ~150% overcommit → nondeterministic OOM /
a heavy query killed by a co-resident one. Do not break the following:

- **One session per worker is enforced, not emergent.** The CP spawns remote
  worker pods with `DUCKGRES_DUCKDB_MAX_SESSIONS=1` (`k8s_pool.go::spawnWorker`).
  A 2nd concurrent `CreateSession` on a worker is rejected, not silently
  overcommitted. Internal control/maintenance work uses the worker's side
  connections (`controlDB`/`warmupDB`), which are NOT counted sessions — so
  cap=1 does not starve them. Do not raise this to >1 for k8s workers, and do not
  route internal work through `CreateSession`.
- **`OrgReservedPool` (remote/multitenant) must never co-assign.** It reuses only
  idle (`activeSessions==0`, Hot, org-owned) workers via
  `findIdleAssignedWorkerLocked`, or claims/spawns a fresh one. There is NO
  least-loaded "share onto a busy worker" path (that exists only in the
  single-tenant flat `K8sWorkerPool.AcquireWorker`, which is not used in remote
  mode). Do NOT add one, and do not resurrect a `leastLoaded*` helper here.
- **At org max workers + all busy → fail fast with the clear org-cap message**
  (`WorkerClaimMissReasonOrgCap`, see `capacity_policy.go`). Never busy-wait at cap.
- **Under cap → spawn a worker on demand** (`spawnReservedWorkerForSlot`). There
  is no warm pool to wait on; the cap is re-checked authoritatively cross-CP in
  `CreateSpawningWorkerSlot`. The spawn+activate runs DETACHED from the request
  ctx (`context.WithoutCancel` + `workerSpawnActivateTimeout`): the requester
  waits for the result or its own ctx, but a requester that gives up must NOT
  kill the in-flight pod (doomed-spawn thrash). An abandoned spawn that succeeds
  is parked hot-idle (`ReleaseWorker`/`TransitionToHotIdleIfNoSessions`, record
  persisted) for the org's next connection; one that fails is retired. Nothing
  may leak in Reserved/Activating.
- **FIFO anti-snatch:** the slow acquisition path's DECISION section (idle-reuse
  re-check → hot-idle claim → spawning-slot creation; `acquireDecision` in
  `org_reserved_pool.go`) is serialized per org by `orgAcquireGate`
  (`org_acquire_gate.go`) so a worker the CP scaled up for an earlier waiter
  cannot be snatched by a later connection. The multi-minute spawn+activate runs
  OUTSIDE the gate — each waiter is 1:1 bound to the claim/slot it owns and the
  session is pre-claimed before the worker becomes Hot, so a cold burst ramps N
  spawns in parallel without breaking anti-snatch. Keep the gate cancel-safe (a
  queued waiter whose ctx is cancelled must be skipped, not deadlock the gate).
- **Destroy-before-reuse ordering:** `SessionManager.DestroySession`
  (`session_mgr.go`) MUST await the worker-side `DestroySession` RPC *before*
  `ReleaseWorker`, so a reused (hot-idle) worker's prior session is gone before
  the next one is assigned (otherwise cap=1 spuriously rejects the reuse).
- **Cap-drift is recovered, not fatal:** if a worker still rejects a CP-scheduled
  session at its cap (CP↔worker accounting drift — should never happen),
  `SessionManager.CreateSessionWithProtocol` does NOT fail the client: it logs
  loudly (ERROR), bumps `duckgres_control_plane_worker_session_cap_drift_total`,
  retires (recycles) the inconsistent worker, and re-acquires a fresh one
  (bounded by `maxWorkerSessionCapDriftRetries`). Detection is
  `isWorkerSessionCapError` (matches the worker's "max sessions reached"
  message). A nonzero drift metric means the scheduling invariant is broken —
  fix the root cause, don't just lean on the retry.

Touching any of: `controlplane/org_reserved_pool.go`, `org_acquire_gate.go`,
`k8s_pool.go::spawnWorker`/`AcquireWorker`, `control.go::workerDuckDBLimits`, or
`duckdbservice` session counting → update the unit tests
(`org_reserved_pool_test.go`, `org_acquire_gate_test.go`,
`duckdbservice/service_test.go`) AND the `one_session_per_worker` +
`cold_burst_parallel_spawns` assertions in `tests/e2e-mw-dev/harness.sh`.

## Worker Drain Protocol (graceful shutdown, #690)

Remote worker pods drain on SIGTERM (pod deletion): they reject new work, keep
in-flight work alive, then exit; the CP marks them `Draining` (not crashed) and
retires them cleanly. Drain readiness is tracked by a refcount (`activeWork` in
`duckdbservice/service.go`) of "drain tokens" — one taken per unit of in-flight
work (query, txn, metadata stream, COPY, activation), released when it finishes.
Invariants: take exactly one token when work starts and release exactly one when
it ends on **every** path (a leak hangs drain to the shutdown timeout, an early
release lets shutdown kill live work); `reapIdle` releases tokens stranded by a
`GetFlightInfo` whose `DoGet` never arrived. `terminationGracePeriodSeconds=3600`
(`k8s_pool.go`) must stay above `workerShutdownDrainTime` (55m).

## User Persistent Secrets (multitenant remote backend)

`CREATE PERSISTENT SECRET` from a client survives across sessions and worker
pods: the CP intercepts it (`server/conn_user_secrets.go`, classification in
`server/usersecrets/`), executes it on the live session first (DuckDB
validates), then stores the statement AES-GCM-encrypted in the config store
(`duckgres_org_user_secrets`, keyed org/user/name) and replays it in the
`CreateSession` payload on the user's future sessions
(`duckdbservice/user_secrets.go`). Enabled by the env-only
`DUCKGRES_USER_SECRET_KEY` (base64 32-byte AES key); unset → clear 0A000
error. Plain/TEMPORARY `CREATE SECRET` stays session-scoped passthrough.
Invariants for anyone touching this path:

- **Cross-user isolation is the wipe at session create, not the destroy-time
  cleanup.** DuckDB secrets are instance-global, and a hot-idle worker is
  reused across users of an org: `wipeUserSecrets` drops ALL user-created
  secrets — persistent ones AND non-persistent (plain/TEMPORARY `CREATE
  SECRET`) ones, which pass through to the worker and would otherwise leak to
  the next user. It preserves only the system-managed allowlist
  (`usersecrets.IsReservedName`: `ducklake_s3`/`iceberg_sigv4`/`iceberg_oauth`
  + the `__default_*`/`duckgres_*` prefixes, which activation re-creates). It
  MUST run before replay on every CreateSession in shared-warm mode, and a
  wipe failure MUST fail the session.
- **Execute-then-persist ordering.** Persist only statements DuckDB accepted;
  a store failure after a successful exec is an ERROR telling the user the
  secret will NOT survive the session. Replay failures at session create are
  warnings, never connection refusals.
- **No silent non-persistence.** Any path where persistent-secret DDL would
  execute but not persist must REJECT instead: multi-statement batches and
  parameterized statements (CP interception), and the Flight SQL ingress
  (`flightsqlingress.Config.RejectPersistentSecretDDL`). Otherwise the secret
  works for one session and is silently deleted by the next session's wipe.
- **DROP's store-fallback is gated on DuckDB's not-found error only**
  (`isSecretNotFoundError`). Any other exec failure (cancel, RPC error,
  ambiguity, aborted txn) must surface and leave the store untouched — a
  false "DROP succeeded" is fatal for a credential revocation.
- **Never log/store secret statement text.** `usersecrets.RedactForLog` guards
  logQueryStarted/Finished/Error, the query log, spans, and pg_stat_activity
  (`currentQuery`); keep new logging of query text behind it. Engine **error
  messages echo the offending SQL** (DuckDB emits `LINE 1: ... SECRET '...'`),
  so a failed CREATE SECRET leaks the credential via the `error` attribute /
  query-log `Exception` even when the query attribute is redacted —
  `usersecrets.RedactErrorForLog(query, errMsg)` guards those error sinks
  (logQueryError/logQueryFinished, `logQuery`); keep new error logging behind it
  too, and pass the original (un-redacted) query so it can classify.
- Touching the interception, wipe/replay, or payload shape → update
  `server/conn_user_secrets_test.go`, `duckdbservice/user_secrets_test.go`,
  and the `persistent_user_secret`(+`_isolation`) assertions in
  `tests/e2e-mw-dev/harness.sh`.

## Admin Console (VPC-private web UI, `kubernetes` tag)

`controlplane/admin/` serves a React admin console + REST API on `:8080` — the
operate-everything surface (metrics, live queries/sessions/connections, recent
errors, worker fleet, live cluster node/pod topology, full config store, user
impersonation, audit log; sliceable by org + user). Design + decisions:
`docs/design/admin-ui.md`; package details:
`controlplane/admin/README.md`. Exposed VPC-privately via an internal-scheme ALB
+ Cognito (Google SSO) behind Tailscale (charts: `ingress-admin.yaml`). Invariants:

- **Frontend is an embedded React/Vite SPA** (`ui/`, built to `ui/dist/`,
  `//go:embed all:ui/dist` in `embed_ui.go`, SPA-fallback served by Gin; the SPA
  owns `/`). `ui/dist` is a **gitignored build artifact** — only `ui/dist/.gitkeep`
  is tracked, so the embed has a target and `go build` compiles without node
  (the server then serves a "UI not built" notice). `just ui-build` builds it
  locally; both `Dockerfile` and `Dockerfile.controlplane` run `npm run build`
  **before** `go build`. Do not delete `.gitkeep` and do not commit `ui/dist`.
- **Two-tier authz** (`authz.go`): `AuthMiddleware` resolves every `/api/v1`
  request to admin (valid `TokenSet` internal secret — service/break-glass) or to
  an SSO identity from the ALB `X-Amzn-Oidc-Data` JWT. The SSO email
  (`@posthog.com` + `email_verified != false`, else 401) is mapped to a role
  **per-request** by a `RoleResolver` backed by the `duckgres_operators` config-schema
  table (goose migration `000006_create_operators.sql`) — `admin` row → admin, else
  viewer. Admins manage operators
  under **Admin → Operators** (`/api/v1/operators`); the first SSO login
  auto-provisions a create-only **viewer** row, and the first admin is minted by
  logging in over the break-glass internal token and patching that row to `admin`
  under **Admin → Operators**. `RoleGate` requires admin for
  all mutating verbs + the audit GET. `AuditMiddleware` records every mutation.
  Keep new mutating routes under this gate; never add a write path that bypasses
  RoleGate/audit.
- **Impersonation is a real session** (`impersonate.go` + `admin_providers.go`):
  it reuses `SessionManager.CreateSessionWithProtocol` (workers trust the CP — no
  password) and **always** `DestroySession` in a defer. Admin-only, every
  statement audited with the admin actor + `usersecrets.RedactForLog` SQL; writes
  require `allow_write=true` (conservative classifier — WITH/CTEs count as
  writes). It consumes a worker under one-session-per-worker and counts against
  the org's connection limits — do not silently exempt it.
- **Metrics proxy is allow-listed** (`metrics_proxy.go`): the client passes a
  panel KEY, PromQL is built server-side from `rangePanels` (never an open PromQL
  relay) and forwarded to `DUCKGRES_PROMETHEUS_URL`. Org-labelled panels keep
  slicing enforced.
- **Env-only knobs**: `DUCKGRES_PROMETHEUS_URL` (read in
  `multitenant.go`; set by the chart). The audit table `duckgres_admin_audit` is
  AutoMigrated at startup (operational state, not goose-migrated tenant config).
  The `duckgres_operators` table is authoritative access-control data, so it lives
  in the config schema via goose migration `000006_create_operators.sql`, not
  AutoMigrate.
- `ManagedSession.Username` is populated at session create so the console can
  slice live sessions/queries by user; keep it set on every create path.
- **Errors page is a redacted, in-memory live-triage buffer** — NOT durable
  history. Every failed query is captured into a bounded per-server ring
  (`server/recent_errors.go`, `DefaultRecentErrorCap=500`) at the single
  `logQueryError` tap (`server/conn.go`), surfaced at `GET /api/v1/errors` and
  merged across CP replicas by `PeerFetcher.FetchPeers` (each error belongs to
  exactly one CP — disjoint union, no worker-id dedup; sorted newest-first, then
  capped). The ring stores ONLY the redacted forms: `Query` via
  `RedactForLog`, `Message` via `RedactErrorForLog` — a failed CREATE SECRET
  must never leak its credential into the ring. Keep the capture behind those
  redactors; long-term error history lives in the external query-log pipeline
  (Kafka sink), not here.
- **Per-user kill switch** (`live.go` routes + `admin_providers.go` +
  `session_mgr.go::DestroySessionsForUser` + `configstore` `disabled` column):
  - `POST …/users/:username/kill` is a **one-shot** terminate — it tears down all
    of a user's sessions + in-flight queries but does NOT block reconnects.
  - `POST …/users/:username/disable` is the **persistent block**: it sets the
    `duckgres_org_users.disabled` column (goose migration
    `000011_add_org_user_disabled.sql`), kills the user's live sessions, AND
    refuses the user's NEW connections at auth time on BOTH front-ends — PG wire
    (`control.go`, distinct `28000` "account is disabled" error, emitted only
    after the password checks out so it never leaks account existence) and Flight
    (`ConfigStore.ValidateOrgUser` / `ValidateOrgUserAndGetPassthrough` return
    false). `enable` reverses it. The disabled state is read from the in-memory
    snapshot, so disable/enable call `ConfigStore.ReloadSnapshot()` to make the
    flip effective immediately instead of one config-poll later.
  - These are **cluster-wide**: a user's sessions live on whichever CP replica
    owns each connection, so the handlers fan out the kill/disable/enable to peers
    via `PeerFetcher.PostPeers` (POST sibling of the read fan-out, same
    `?scope=local` recursion guard) and sum the per-CP `killed` counts. The
    snapshot reload is fanned out too so every replica enforces the block at once.
  - Kill must be **scoped to the target user** — never tear down another user's
    sessions on the shared org stack (the regression the e2e asserts with a
    concurrent root query that must survive).
- **Live Nodes view** (`ui/src/pages/Nodes.tsx` + `pages/nodes/peepernetes.{ts,css}`,
  a port of the standalone peepernetes visualizer): a full-bleed, animated
  cluster node/pod TV — nodes grouped by karpenter nodepool (or by namespace /
  deployment), CPU/MEM request bars, pod chips colored per deployment,
  placeholder/system-pod classification, Karpenter empty-node reclaim countdown,
  draining-duration on nodes (deletion-timestamp or client-tracked first-seen)
  and terminating-duration on pods, each pod's running image, unscheduled tray,
  and a synthesized event ticker. Its header carries only the filters + live
  indicator; the nodes/workers/placeholders/pending counters are lifted into the
  shared admin **Topbar** via a small external store (`ui/src/lib/clusterCounts.ts`
  → `Topbar.tsx` `useSyncExternalStore`; the view pushes counts each repaint and
  pushes `null` on unmount so they only show on this page). It's imperative DOM (mounted
  by the React page into a `.peeper` root, scoped CSS + `pn-`-prefixed keyframes)
  and does NOT use native K8s watch — the browser can't reach the API, so it
  POLLS four **read-only** projected endpoints (`server/`-free; `cluster.go`):
  `GET /cluster/{nodes,pods,events}` project the in-cluster objects down to the
  minimal K8s-shaped subset the view reads (annotations trimmed to
  `kubernetes.io/config.mirror`; no raw objects), and `GET /cluster/nodepools`
  proxies the karpenter NodePool CRD (v1→v1beta1, degrading to an empty list when
  karpenter is absent). Backed by the shared K8s pool's clientset
  (`Extras.ClusterClient`, nil on non-k8s backends → routes unregistered). All
  four are GETs so RoleGate admits viewers; there is no mutation path. **RBAC:**
  these reads are cluster-scoped / cross-namespace, which the CP's in-namespace
  Role doesn't cover — the grant lives on its own `duckgres-control-plane-cluster-topology`
  ClusterRole in the `charts` repo (`charts/duckgres/templates/rbac.yaml`), bound
  to the CP SA. It's a *separate* role (not folded into `duckgres-duckling-reader`)
  so binding duckling-reader elsewhere doesn't drag these broader reads along and
  trip RBAC escalation-prevention. When the ClusterRole is absent the handlers
  **degrade a Forbidden to an empty `{items:[]}` (200)** and log a warning, so the
  view shows nothing rather than 500ing — the e2e CP hits exactly this path (its
  SA can't be granted cluster-scoped RBAC from CI), so `admin_console_api` only
  asserts the `{items:[...]}` envelope; projection shape is covered by
  `cluster_test.go`. Touching
  the projection/endpoints or the view → update `controlplane/admin/cluster_test.go`
  and the `/cluster/{nodes,pods,events,nodepools}` checks in `admin_console_api`
  (`tests/e2e-mw-dev/harness.sh`).
- Touching any of the above → update `controlplane/admin/*_test.go` (esp
  `authz_test.go`, `kill_switch_test.go`), `controlplane/session_mgr_test.go`
  (`TestDestroySessionsForUser`), `controlplane/configstore/store_test.go`
  (`TestDisabledUserEnforcement`) AND the `admin_*` / `impersonation_*` /
  `user_kill_switch` / `user_disable_block` assertions in
  `tests/e2e-mw-dev/harness.sh`.

## Compute-Usage Billing (managed-warehouse, remote backend only)

duckgres meters per-org compute usage of worker pods and ships it to PostHog's
public ingestion as `"managed warehouse compute usage"` capture events (billing
sums the two raw metrics externally). Full design + decisions:
`docs/design/billing-compute-seconds-plan.md`. Scope is the **emit side**, and
**only** the remote/k8s backend (per-org worker pod with a known
`WorkerProfile` size). Pipeline:

```
conn end → in-proc per-org counter (best-effort; never fails teardown)
        │  flusher (~15s) UPSERT-increment → config-store buffer (cross-CP sum)
        ▼  duckgres_org_compute_usage / duckgres_org_compute_drain_state
   leader drain (~60s) → POST {DUCKGRES_BILLING_INGEST_URL}/capture (ship-then-delete)
```

Two raw metrics per connection over its full lifetime, using the **provisioned**
worker size: `cpu_seconds = vCPU × ceil(conn_secs)`, `memory_seconds = GiB ×
ceil(conn_secs)`. Counted internally in integer **millicore-seconds** /
**MiB-seconds** (`compute_meter.go`) to avoid truncating a fractional-core /
sub-GiB worker. Invariants for anyone touching this path:

- **Metering is strictly best-effort and off the hot path.** A metering error
  (counter, flush, drain) must NEVER block or fail a query or connection
  teardown. The connection-end record is added to an in-process per-org counter
  (map+mutex, microseconds, no I/O); everything downstream is async/retried.
  `cp.computeMeter` is nil unless the remote backend is configured with both
  ingest URL+token — every call site is nil-safe.
- **Enable gate = both `DUCKGRES_BILLING_INGEST_URL` and
  `DUCKGRES_BILLING_INGEST_TOKEN` set** (`ControlPlaneConfig.BillingMeteringEnabled`).
  Either unset → metering disabled (logged once at startup, ships nowhere). The
  env names are fixed (infra wires them); keep them exact.
- **Worker size is plumbed onto the connection** (`server.SetConnectionWorkerSize`
  → `clientConn.workerMillicores/workerMiB`, set in `control.go::handleConnection`
  from `workerBillingSize(workerProfile)`, remote-only). `workerMillicores==0`
  (non-remote / unknown) → metering skipped. The metric is computed once at the
  SAME teardown point as `CloseConnectionMetrics` (the `#841` lifetime defer).
- **Bucket = connection-end time floored to 60s.** Flush carries the sub-unit
  remainder forward so rounding never loses counts across flushes. Buffer flush
  is UPSERT-increment so all CP pods sum into one row.
- **Drain is leader-only** (co-located under the janitor lease via
  `JanitorLeaderManager.AttachLeaderLoop`), **ship-then-delete / at-least-once**:
  only buckets `bucket_start ≤ now-60s-30s grace AND > last_drained_bucket` ship;
  on HTTP 2xx → TXN advance high-water + DELETE row; on failure → keep + retry.
  `event_uuid = hash(org_id, bucket_start)` + `timestamp = bucket_start` are
  deterministic so a re-ship collapses onto the same ClickHouse row.
- **Graceful shutdown does a final flush** after connections drain to their
  natural end (`shutdown`/`drainAndShutdown`), so a departing CP pod lands its
  last interval before exit.
- Touching the meter/flush/drain/capture, the worker-size plumbing, or the
  config knobs → update `controlplane/compute_meter_test.go`,
  `compute_drain_test.go`, `compute_size_test.go`, the migration assertion in
  `tests/configstore/migrations_postgres_test.go`, and the
  `compute_usage_metering_wired` assertion in `tests/e2e-mw-dev/harness.sh`.

## TODO Reference

`TODO.md` is a lightweight backlog for ideas that do not yet have a better
home. It is not the PostgreSQL compatibility source of truth; use
`docs/postgres-compatibility.md` for compatibility status, test citations, and
known gaps.
