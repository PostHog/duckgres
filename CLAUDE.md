# Claude Code Context for Duckgres

This file provides context for Claude Code sessions working on this codebase.

## Project Overview

Duckgres is a PostgreSQL wire protocol server backed by DuckDB. It allows any PostgreSQL client (psql, pgAdmin, lib/pq, psycopg2, JDBC, etc.) to connect and execute queries against DuckDB databases.

## Architecture

Duckgres has three deployment topologies, built from three primary run modes (`standalone`, `control-plane`, `duckdb-service`; a fourth utility mode, `reshard-runner`, is the entrypoint of the dedicated per-operation reshard pods the control plane spawns — see the Resharding section):

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

### Native metadata Postgres proxy

The Kubernetes control plane can expose explicitly opted-in CNPG-backed
warehouse metadata databases on a separate SNI suffix
(`DUCKGRES_METADATA_HOSTNAME_SUFFIXES`; managed-warehouse deployments use
`.md.dev.postwh.com`, `.md.us.postwh.com`, or `.md.eu.postwh.com`). This is an
early connection branch, not a DuckDB
executor: the existing org `root` password authenticates at Duckgres, the
startup database MUST be exactly `metadata`, and the control plane resolves
the real endpoint/database/tenant role/password internally through
`SharedWorkerActivator.MetadataPostgresURL`. After authenticating upstream
with that internal credential, protocol traffic is relayed byte-for-byte. The
endpoint and password remain internal; the upstream role and database can be
visible to the fully privileged client through normal PostgreSQL introspection
such as `current_user` and `current_database()`.

Access is fail-closed on the warehouse row's `metadata_proxy_enabled` flag,
`state=ready`, and `metadata_store_kind=cnpg-shard`. Never infer publication
from shard placement and never pass a client-supplied upstream database,
username, host, or password. `DUCKGRES_METADATA_PROXY_MAX_CONNECTIONS_PER_ORG`
(default 20 per control-plane replica) bounds public sessions so they cannot
exhaust the internal PgBouncer pool. These sessions participate in the
existing per-user kill / disable fan-out, and an established session is closed
if the warehouse gate stops being eligible. An admin warehouse PUT that
explicitly includes `metadata_proxy_enabled` reloads the local config snapshot
and notifies peer replicas; established sessions observe the new gate on their
next five-second recheck after snapshot propagation.

The initial scope is dedicated, single-customer CNPG shards only. Do not enable
an org on a shared shard until upstream `CONNECT` ACLs and role hardening are in
place. The exact virtual database check prevents selecting a different startup
database, but after connection the customer `root` credential has the full
access of the internally resolved metadata role.

Observability stays explicit at the branch boundary:
`duckgres_connections_open` includes these client sockets because it is the
process-wide accepted-connection gauge, while
`duckgres_metadata_proxy_connections_open`,
`duckgres_metadata_proxy_connection_attempts_total`,
`duckgres_metadata_proxy_connection_duration_seconds`,
`duckgres_metadata_proxy_upstream_connect_duration_seconds`, and
`duckgres_metadata_proxy_bytes_total` isolate proxy load and failures. The
relay is intentionally opaque, so DuckDB query metrics, query logs, and query
traces do not include SQL executed through it. Use the CNPG/PgBouncer metrics
and the fixed upstream `application_name=duckgres-metadata-proxy` for
database-side attribution; never make one org's metadata target part of the
control-plane health check. Target resolution plus upstream
connect/auth/synchronization has a fixed 10-second bootstrap deadline; the
deadline is canceled after hijack and never applies to established relay
traffic. `duckgres_auth_failures_total` includes wrong-password proxy attempts;
the dedicated attempt counter with `outcome="auth_failed"` is the proxy split.
Pre-TLS `duckgres_rate_limit_rejects_total` events cannot be assigned to either
endpoint because SNI has not yet been observed.

Metadata-proxy `CancelRequest` handling is session-terminating. Synthetic
backend keys map to the exact established frontend/upstream connection pair on
the owning control-plane replica, which closes both rather than redialing a
PgBouncer Service where instance-local cancel keys could reach the wrong pod.
Raw cancel connections remain control-plane-local behind the NLB, matching the
existing cancellation locality: a synthetic-key miss on another replica is
absorbed and counted by
`duckgres_metadata_proxy_cancel_requests_total{outcome="not_local"}`.

In topologies 2 and 3, the control plane exposes only PostgreSQL wire protocol to clients. Arrow Flight SQL is internal transport between the control plane and workers.

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
- **controlplane/** — Multi-process / multi-tenant control plane
  - Core: `control.go`, `session_mgr.go`, `worker_mgr.go`, `worker_pool.go` (process/k8s abstraction), `validation.go`, `sdnotify.go`
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
- **control-plane**: Multi-process. Owns pgwire client connections end-to-end (TLS, auth, protocol handling, SQL transpilation) and routes queries to a worker pool.
  - **Process backend** (default, `--worker-backend process`): local Flight SQL workers over Unix sockets.
  - **Remote backend** (`--worker-backend remote`): per-org Kubernetes worker pods over TCP+TLS. Multitenant; requires `-tags kubernetes` and a Postgres-backed config store. Adds config store, org router, runtime tracker, janitor/leader election, and a provisioning/admin HTTP API.
- **duckdb-service**: Thin DuckDB execution engine exposed via Arrow Flight SQL. Spawned automatically by the control plane as worker processes, or run standalone for testing.
- **reshard-runner** (`-tags kubernetes` only): entrypoint of the dedicated per-operation reshard pod (`duckgres-reshard-op-<id>`), spawned by the control plane when an operator starts a reshard. Claims ONE op (`DUCKGRES_RESHARD_OP_ID`), executes the reshard step machine to a terminal state, exits (0 unless an infrastructure error). See "Resharding" below.

Key CLI flags for control-plane mode:
- `--mode control-plane|duckdb-service|standalone|reshard-runner`
- `--worker-backend process|remote`
- `--process-min-workers N` / `--process-max-workers N`
- `--process-retire-on-session-end`
- `--worker-queue-timeout DURATION` / `--worker-idle-timeout DURATION`
- `--idle-timeout DURATION` — connection idle timeout: a client connection with no traffic for this long is closed and its worker released to hot-idle (in control-plane mode an idle connection otherwise pins a worker forever). **Control-plane default is `5m`** (`server.DefaultControlPlaneIdleTimeout`; standalone defaults to `24h`); a negative value disables it. `server.New` applies the standalone default, so the control plane sets it explicitly before `InitMinimalServer` (which skips that defaulting).
- `--memory-budget SIZE` (default 75% RAM) / `--memory-rebalance`
- `--socket-dir /path` (process backend)
- `--handover-drain-timeout DURATION` (default `24h` process; **remote default is `0` = unbounded** — the CP waits for active sessions for as long as it takes and the pod's k8s `terminationGracePeriodSeconds` is the only hard wall. cloudflare/tableflip FD passing applies to process/standalone single-host upgrades, not k8s pod replacement.)
- `--ducklake-delta-catalog-enabled` / `--ducklake-delta-catalog-path`
- Remote backend (requires `--config-store`; `-tags kubernetes` for K8s pool):
  - Config store: `--config-store`, `--config-poll-interval`, `--internal-secret`
  - K8s pool: `--k8s-worker-image`, `--k8s-worker-namespace`, `--k8s-control-plane-id`, `--k8s-worker-port`, `--k8s-worker-secret`, `--k8s-worker-configmap`, `--k8s-worker-image-pull-policy`, `--k8s-worker-service-account` (no global worker cap — per-org `Org.MaxWorkers`, 0=unbounded, is the only cap)
  - AWS / STS: `--aws-region`
  - Compute-usage billing needs no config: metering is always on for the remote backend and billing PULLS usage over the internal-secret-authed HTTP API (`GET /api/v1/billing/usage` + `POST /api/v1/billing/ack`). See `docs/design/billing-pull-api.md` and "Compute-Usage Billing" below.
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
S3/STS. "Solid" means a deterministic pass/fail
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
- **`tests/e2e-mw-dev/`** (per-PR GitHub workflow `e2e-mw-dev.yml`): the full multi-tenant activation pipeline against the **real posthog-mw-dev EKS cluster** — real Cilium, real Crossplane ducklings, real cnpg-shard + external-RDS metadata, real AWS S3. A shell harness (`harness.sh`) runs as an in-cluster Job per PR; `run.sh` orchestrates deploy/test/teardown/e2e-cleanup. **Replaces the retired kind suite** (`tests/k8s/`) — that suite's `k8s-integration-tests` CI job and its Go tests are gone; the supporting `k8s/` scripts/manifests + Dockerfiles are kept for now. See `tests/e2e-mw-dev/README.md`.

### When code changes obligate test changes

`tests/e2e-mw-dev/` is the only place we exercise the full activation pipeline (control plane → STS broker → worker pod → DuckDB → ATTACH against real cloud storage). If your change touches any of the following, treat updating the harness as part of the change, not a follow-up:

- `controlplane/shared_worker_activator.go`, `controlplane/sts_broker.go`, anything in the activation payload shape (`TenantActivationPayload`, `server.DuckLakeConfig`)
- `server/server.go::AttachDeltaCatalog`, `server.attachDuckLake*`, `server.refresh*Secret`
- `controlplane/configstore/models.go` — new columns flow through the provisioning API the harness calls; exercise them via a provision body field
- `duckdbservice/activation.go`, `worker_activation.go` — worker-side activation order
- Any code path that wires AWS credentials through to DuckDB SECRETs

The contract: if the harness no longer exercises a path you changed, **update `harness.sh`**; if your change removes a path it asserts against, **delete the assertion**. The DuckLake round-trip / durability / concurrent-writers checks in `harness.sh` are the load-bearing ones for catalog wiring — keep them honest.

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
RAM plus 2.5 DuckDB threads per requested CPU, rounded up. It does NOT divide
by session count. Two sessions on one pod would each believe they own 75% →
~150% overcommit → nondeterministic OOM /
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

- **An invalidated DuckDB instance retires the worker; it is NEVER reused.**
  A DuckDB Internal- or Fatal-class exception does not just fail one statement,
  it poisons the whole instance: every later statement on ANY connection to it —
  including a brand new session — fails with "database has been invalidated
  because of a previous fatal error" until the process restarts. DuckLake's
  commit path is a known source (an InternalException inside the commit retry
  loop is rethrown by `ErrorData::Throw` with its original type). Detection is
  `isInstanceFatalError` (`duckdbservice/instance_fatal.go`). The **error TYPE
  is authoritative**: a typed `*duckdb.Error` of
  `ErrorTypeInternal`/`ErrorTypeFatal`. **Never add substring matches for
  `"INTERNAL Error"` / `"FATAL Error"`** — DuckDB echoes the offending SQL back
  in its error text (`LINE 1: <query>`), so those matched the USER'S OWN QUERY
  and handed every tenant a one-statement worker kill via
  `SELECT 'INTERNAL Error' + 1` (regression:
  `TestInstanceFatalIgnoresEchoedQueryText`). The one string fallback is the
  `database has been invalidated…` marker, for an error that arrives already
  flattened. **OOM and DuckLake transaction conflicts are distinct error types
  and must never classify here**; they retry in place. The flag is sticky
  (invalidation is permanent) and is set from the statement paths, the
  `CreateSession` path, and an async `SELECT 1` liveness probe kicked by each
  health check — the probe is what catches a fatal thrown on a session that has
  since been destroyed. **The stored reason MUST be redacted before it is kept**
  (`usersecrets.RedactErrorForLog`, or `noteInstanceErrorOpaque` where no
  statement is available to classify, as on the secret-replaying `CreateSession`
  path): it is logged on the worker, shipped to the CP as
  `instance_invalid_reason`, and logged again on retire, so an un-redacted
  engine error leaks a failed `CREATE SECRET`'s credential into three sinks.
  **The probe must stay OFF the health check's critical path**: the CP's
  health-check budget is 3s and already shared with progress polling, so a
  blocking probe would get healthy workers killed for unresponsiveness. It is
  single-flight and a wedged CGO call can outlive its context, so a probe stuck
  past the threshold raises `duckgres_worker_instance_probe_stuck` rather than
  silently disabling detection. The worker reports
  `instance_invalidated` + `healthy:false`; the CP retires it on the FIRST
  report, bypassing `maxConsecutiveHealthFailures` (the process answers RPCs
  fine, so the failure counter would never fire) and rejects it for reuse in
  `validateReservedWorkerHealth` — the hot-idle reuse path is what previously
  turned one bad statement into "the warehouse is down until someone restarts
  it". `duckgres_control_plane_worker_instance_invalidated_total` and
  `duckgres_worker_instance_invalidated_total` /
  `duckgres_worker_instance_invalidated_state` are the signals; nonzero means
  a tenant hit an engine bug and lost a worker — contained, but chase the root
  cause. This is blast-radius containment ONLY: it does not fix the engine bug,
  which is fixed by shipping a DuckLake extension build that guards the read
  (see `DUCKLAKE_EXTENSION_TAG` in `Dockerfile`/`Dockerfile.worker`).

Touching any of: `controlplane/org_reserved_pool.go`, `org_acquire_gate.go`,
`k8s_pool.go::spawnWorker`/`AcquireWorker`, `control.go::workerDuckDBLimits`,
`duckdbservice/instance_fatal.go`, or
`duckdbservice` session counting → update the unit tests
(`org_reserved_pool_test.go`, `org_acquire_gate_test.go`,
`duckdbservice/service_test.go`, `duckdbservice/instance_fatal_test.go`,
`controlplane/instance_invalidated_test.go`) AND the
`one_session_per_worker` + `cold_burst_parallel_spawns` assertions in
`tests/mw-dev/e2e/harness.sh`.

## Exploratory Worker Tier (small-first routing) — LOAD-BEARING CONTRACT

Design: `docs/superpowers/specs/2026-08-04-exploratory-worker-tier-design.md`.
On the **remote/k8s backend only**, a connection that does not ask for a worker
shape starts on a small "exploratory" pod and grows into a normal one only when
it proves it needs to. Two mechanisms: **lazy acquisition** (no worker at
connect) and **escalation** (small → standard, one-way). Env-only knobs:
`DUCKGRES_EXPLORATORY_TIER_ENABLED` / `_WORKER_CPU` / `_WORKER_MEMORY` /
`_WORKER_TTL` (default 48h), resolved by `exploratoryWorkerProfile`
(`controlplane/worker_profile.go`). Server side lives in `server/conn_tier.go`
+ `tier_classify.go`; control-plane side in `controlplane/session_activation.go`
+ the activator/switcher closures in `control.go::handleConnection`.

- **Eligibility is decided once, at connect** (`useExploratoryTier`): remote
  backend AND a usable exploratory profile AND not a passthrough user AND no
  client `duckgres.worker_*` startup option (`clientSuppliedWorkerGUCs`). A
  half-configured tier (missing/invalid size) resolves to nil and degrades to
  today's eager behavior — never to a BestEffort pod. **Passthrough users are
  excluded** (they bypass the compat layer the classification is built on), and
  a **GUC-sized connection bypasses the tier entirely** — it acquires the
  requested shape eagerly at connect, as before.
- **Nothing is acquired until a statement needs an engine.** The connection
  reaches the message loop with `c.executor == nil` and a `SessionActivator`;
  `activateForStatement` acquires on first need. Statements the control plane
  answers itself — `SET`/`SHOW duckgres.query_source`, ignored SETs, no-ops,
  `pg_stat_activity`, the empty query — MUST NOT acquire. That is the point of
  the whole feature; adding an acquire to an engine-free path silently deletes
  the benefit. The `duckgres.s3_cache` and `duckgres.worker_ttl` GUCs are the
  exception, on all three protocol paths, because unlike the other duckgres
  GUCs they are WORKER state (the s3_cache secret swap; the worker_ttl
  pool-side hot-idle TTL): **`SET` always acquires** — the apply needs a
  worker to land on — and **`SHOW duckgres.s3_cache` acquires only when
  `c.hasPendingS3Cache`**, i.e. when a connect-time option has not been applied
  yet and answering first would report a transport the session is about to
  leave (see the s3_cache section below). `SHOW duckgres.worker_ttl` never
  acquires: until a worker exists, the connect-time baseline is the truthful
  answer (there is no pending worker-side TTL state).
- **A pinning FIRST statement acquires the standard profile directly** (one
  acquire, `pinned=true` → `MarkConnectionPinned`), never small-then-escalate.
- **The pin set is the state boundary, and every member is load-bearing:** DML,
  DDL, `COPY` (BOTH directions — COPY TO can reference session state and is
  routed above the transpile-time hook), `SET`, `BEGIN` (so an open transaction
  can never exist on a worker that is about to be replaced), `DECLARE` (simple,
  batched AND extended — `FETCH`/`CLOSE` are unhooked and rely entirely on the
  DECLARE pin, because a cursor's worker-side RowSet must not open on a session
  about to be destroyed), secret DDL **before** the user-secrets interception
  (that interception owns its own execution and sits ABOVE the general hook, so
  a plain/TEMPORARY `CREATE SECRET` would otherwise land on the small worker and
  be silently dropped), and the extended-protocol **Describe probe** of a
  pinning statement (Describe really executes it). A parse failure pins by
  default — false pins are free, a missed pin loses state.
- **Escalation is one-way and sticky.** `escalateWorker` destroys the small
  session BEFORE acquiring the standard one, so once it is entered there is no
  session to fall back to. Both reason and outcome are closed sets:
  `duckgres_exploratory_escalations_total{reason="state"|"oom"|"heuristic",
  outcome="ok"|"canceled"|"capacity"|"draining"|"disabled"|"error"}` — every
  ATTEMPT is counted, so a cluster that cannot escalate anything is
  distinguishable from one nobody escalates on (v1 ships no heuristic tier; the
  constant marks the hook point). The failure classes come from the CLASSIFIED
  SQLSTATE via `server.AcquisitionFailureOutcome`, the same helper
  `duckgres_session_activation_total{org,outcome}` uses — one helper, so the two
  acquisition metrics can never drift into different or unbounded label sets.
- **A failed acquisition — activation OR escalation — is CONNECTION-FATAL**
  (the one exception is the post-escalation s3_cache re-apply, below).
  `failWorkerAcquisition` sends a FATAL ErrorResponse, suppresses
  ReadyForQuery, and unwinds the message loop; there is no session left to
  resynchronize to. SQLSTATE comes from `escalationErrorSQLState`: a
  `*server.SessionAcquireError` (classified by the CP with the SAME logic the
  eager connect path uses, `sessionCreationErrorResponse`) is authoritative —
  28000 disabled / 53300 capacity / 57P03 draining / 57014 cancel / 3D000
  catalog / XX000 s3_cache apply / 53400 other; the substring fallback covers
  only the paths that still return a plain error. Keep the client message the
  classified one, never the wrapped internal chain. Extended-protocol handlers
  are void, so the error is ALSO parked on `c.fatalErr`, which
  `runExtendedQueryMessage` hands to the message loop. **`fatalErr` is one-shot
  by construction** — it is set on the way out and never cleared, because the
  connection is terminating; do not add a path that sets it and then continues.
- **OOM re-execute is transparent only under all three conditions:** the
  statement is a READ, ZERO DataRows have been sent, and `txStatus` is idle. A
  wire stream cannot be restarted, so a partial result must surface the error
  instead. RowDescription is NOT resent on the retry (same query, same engine
  version → identical schema), and the retry runs on the escalated worker only.
  Detection is `isWorkerOutOfMemoryError` (`server/conn_errors.go`) — DuckDB's
  engine OOM only; a pod-level OOMKill is `ErrWorkerDead` and is deliberately
  never re-executed.
- **`duckgres.s3_cache` interplay (both directions).** A connect-time
  `-c duckgres.s3_cache=...` cannot be applied at connect on this path (no
  worker), so the CP parks it (`SetPendingS3CacheOption`) and
  `ensureSessionActive` applies it AFTER installing the executor — applying it
  inside the activator would find a nil executor, no-op the worker swap, and
  still flip the session flag, the exact divergence `applyS3CacheSetting`
  exists to prevent. A failed apply fails the activation (XX000, fatal). On
  escalation the bypass is RE-APPLIED to the new worker
  (`reapplyS3CacheAfterWorkerSwitch`); that failure is the ONE
  non-connection-fatal escalation outcome, because the swap already succeeded:
  the statement fails with a normal `ERROR` (XX000, naming the re-apply), the
  session flag is reset to the worker's REAL transport (proxied — a fresh
  session always starts on the cache proxy), the connection stays alive (a
  ReadyForQuery on the simple protocol; Sync's on the extended one), and the
  pin is deliberately NOT rolled back. `escalateWorker` tags it
  `errS3CacheReapplyFailed` and `failEscalation` routes it — every call site
  goes through that dispatcher so the two severities can never be confused at
  one of them. **`SHOW` must never lie**: it activates iff an option is still
  pending.
- **Billing is largest-size-wins over the whole connection** (v1). The size is
  stamped at activation and re-stamped at escalation with the target profile;
  escalation only ever goes small→standard, so that stamp IS the maximum. The
  pre-activation idle prefix bills at the first acquired size.
- **Accepted gaps (do not "fix" silently — they are decisions):** per-org
  connection admission and the vCPU lease now happen at the FIRST STATEMENT,
  not at connect (the connect-time reshard/migration/draining gates are
  unchanged); a one-shot per-user `kill` landing inside the switcher's
  destroy→create window is missed, and so is a `kill` against a connection that
  authenticated but has never activated (no session for
  `DestroySessionsForUser` to iterate and no registered conn-closer until the
  first statement) — `disable` covers BOTH, because the activation/escalation
  re-check refuses the session outright (28000); extending `kill` to
  authed-but-unactivated connections is a named follow-up, not implemented; and
  a client that sends TCP FIN mid-activation does not abort the in-flight spawn
  (the message loop is blocked in the acquire) — the completed worker parks
  hot-idle for the org's next connection.
- Touching classification, the pin hooks, activation, escalation, the OOM
  retry, or the profile resolution → update `server/tier_classify_test.go`,
  `server/conn_tier_test.go`, `server/conn_tier_exec_test.go`,
  `server/conn_lazy_activation_test.go`, `server/s3_cache_test.go`,
  `controlplane/session_activation_test.go`,
  `controlplane/worker_profile_test.go`, `controlplane/compute_size_test.go`,
  AND the `exploratory_tier` / `exploratory_lazy_activation` /
  `exploratory_state_pin` / `exploratory_oom_escalation` /
  `org_default_profile` / `sized_worker` (GUC bypass) / `assert_worker_pod`
  assertions in `tests/mw-dev/e2e/harness.sh`. The harness header above
  `exploratory_tier` records which existing assertions the tier's connect
  semantics changed and why — keep that audit current.

## Hot-Idle Pool Reporting + Per-Org Caps (remote backend)

Two operator surfaces for the hot-idle pool, both over the durable runtime
store (`worker_records`, the only source that sees parked workers — they hold
no session):

- **Reporting**: `configstore.ListHotIdleByOrg` aggregates `hot_idle` rows
  per org (count, summed vCPU/memory, oldest park) and
  `GET /api/v1/workers/hot-idle` (`controlplane/admin/live.go`) joins each
  org's configured caps. Backs the Workers page "Hot idle by org" card
  (sortable, default memory-pinned desc). **Worker shape resolution is a
  three-step chain, everywhere**: the worker's explicit profile wins, else
  the org's default worker profile, else the CP-global default worker shape
  (`cfg.K8s.WorkerCPURequest`/`MemoryRequest`, else the 8/16Gi constants) —
  an unsized worker on a default-less org must never report zero cpu/memory
  (it pins real pod requests). The same chain backs the fleet rollup
  (`ListWorkerLifecycleStats`) and the cap sweep's shape math
  (`orgHotIdleLimitsFromSnapshot`); unparseable quantities still contribute 0.
- **Caps**: `max_hot_idle_workers` (count), `max_hot_idle_cpu` and
  `max_hot_idle_memory` (k8s quantity strings, e.g. `"16"` / `"64Gi"`) on
  `duckgres_orgs` (migration 000037; 0/"" = unlimited). Editable via the
  admin org PUT + the org detail form. Invariants:
  - **Enforcement is a convergent janitor sweep** (`reapHotIdleCaps`), NOT a
    park-time check: on each tick it retires the OLDEST parked workers
    (oldest-first listing) until the org is within ALL configured limits.
    This uniformly covers every park path AND cap decreases — lowering a cap
    drains the excess on the next ticks (config-poll reload, then the 5s
    janitor tick). Retires go through the fenced `RetireFromSnapshot` CAS
    with origin `janitor_hot_idle_cap` (a distinct metric origin — a nonzero
    rate means an operator's cost ceiling is biting, not stale capacity).
    On a retire error the sweep STOPS that org for the tick (never marches
    on and over-reaps on a transient failure).
  - **Cap wins over the floor** (`DefaultWorkerMinHotIdle`): the floor only
    guards the TTL reaper; the cap is the explicit operator intent. An org
    with floor > cap is a contradictory config that resolves to the cap.
  - **Validation is fail-closed on the write path**: a cap quantity that
    doesn't parse (or is zero/negative) is 400'd by the admin org PUT,
    because the sweep reads those as UNLIMITED — accepting them would
    silently mean "no cap".
  - The sweep runs even when the TTL reaper is disabled (`hotIdleTTL == 0`)
    — a cap is a hard ceiling, not a freshness rule.
- Touching any of this → update `tests/configstore/hot_idle_reporting_postgres_test.go`
  (+ the migration asserts in `migrations_postgres_test.go`),
  `controlplane/janitor_test.go` (the cap sweep cases +
  `TestOrgHotIdleLimitsFromSnapshot` glue test),
  `controlplane/admin/api_test.go` (`TestUpdateOrgHotIdleCaps*`) +
  `live_test.go` (`TestHotIdleRoute`), `ui/src/pages/Workers.test.tsx` +
  `OrgDetail.test.tsx`, AND the `/workers/hot-idle` envelope +
  `hot_idle_reporting_and_cap` assertions in `tests/mw-dev/e2e/harness.sh`.

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

## PostHog Logs (OTLP) — LOAD-BEARING CONTRACT

Process-level slog → PostHog Logs via OTLP (`internal/cliboot.InitLogging`).
This is **not** a replacement for `ducklake.system.query_log`. Product-analytics
events (`POSTHOG_ANALYTICS_API_KEY`) stay on the capture API.

- **`service.name` is the process role**, not `duckgres-<identifier>`:
  `duckgres-control-plane` / `duckgres-worker` / `duckgres-reshard` /
  `duckgres` (standalone). `DUCKGRES_IDENTIFIER` is resource attr
  `duckgres.deployment`. Logs and traces share `otelResource(bi)`.
- **Default PostHog level is WARN** (`DUCKGRES_POSTHOG_LOG_LEVEL`); stderr
  stays at `DUCKGRES_LOG_LEVEL`. User-class `Query execution failed.` stays
  Info and does not export at the default.
- **Query text default is `redacted`** (`RedactForLog`+4096). Secret DDL is a
  placeholder; ordinary SELECT text still leaves. Never redact on `LINE 1:`.
- **Public connection key is `pid`**, not `connection_id`. Org/worker are
  process-scoped on workers (`stampWorkerLogIdentity`); `user`/`pid` live on
  the session logger and must never be `SetDefault`'d.
- **Worker `POSTHOG_API_KEY` is a named `env:` `secretKeyRef`** copied from
  the CP pod spec (`controlplane/pod_env.go`, `k8s_pool_spawn.go`). Never
  `envFrom`. Never `os.Getenv` → `value:`. If Get/`POD_NAME`/named env misses:
  one WARN and omit — **never fail spawn**. Do not forward
  `ADDITIONAL_POSTHOG_API_KEYS` or `POSTHOG_ANALYTICS_API_KEY`.
- **`FlushLogging()`** only on the listed drain `os.Exit` sites (worker
  success/timeout; CP SIGTERM after drain; `drainAfterUpgrade`). Do not wrap
  every startup `os.Exit(1)`.
- **Exporter health** is scraped on the CP:
  `duckgres_otlp_log_export_failures_total{source,reason}` (no `{org}`).
  Workers report `otlp_export_enabled` / `otlp_export_failures` on the
  health-check JSON; the CP `Add`s last-seen **deltas** only. Workers do not
  call `InitMetrics`.
- Touching spawn env, health JSON, or the handler stack → update
  `controlplane/k8s_pool_spawn_test.go`, `controlplane/otlp_export_test.go`,
  `internal/cliboot/*_test.go`, AND `assert_worker_pod` in
  `tests/mw-dev/e2e/harness.sh` (plaintext-key assert; secretKeyRef copy when
  the CP named env has one).

## Per-Session S3 Cache Bypass (`duckgres.s3_cache`, remote backend)

`SET duckgres.s3_cache = on|off|passthrough` (default `on`; also a `-c`
startup option) controls the node-local S3 cache-proxy DaemonSet. `off`
bypasses its cache transport for cold-read benchmarking; `passthrough` keeps
requests flowing through cache-proxy while skipping its cache, so cache-off
workloads retain per-request proxy instrumentation.
Mechanism: the CP intercepts the duckgres-namespaced GUC (never forwarded to
DuckDB) and, on every state flip, calls the worker's `SetSessionS3Cache`
action (`flightclient.FlightExecutor.SetS3CacheMode` →
`SessionPool.SetS3CacheMode`), which rebuilds the `ducklake_s3` secret:
`off` = the org's native HTTPS transport, so httpfs CONNECT-tunnels through
the proxy as opaque TLS (no cache reads/fills — the deliberate, reversible
form of the mw-prod-us 2026-07-17 incident); `passthrough` = the same
`overrideS3EndpointForCacheProxy` transport as `on`, with a worker-local
marker that makes cache-proxy use `forwardUncached` and strips that marker
before it reaches S3; `on` = the normal caching transport. Global `http_proxy`
is never touched (post-attach propagation to DuckLake subcatalogs is
unreliable; secrets are consulted per request). Instance-global secret + one
session per worker = session-scoped in effect. Invariants:

- **State follows the worker, never leads it.** The session flag
  (`clientConn.s3CacheMode`) flips only after the worker swap succeeds; a
  failed swap fails the SET (`XX000`) / the batch / the connect (startup
  option), so `SHOW` can never report a transport the worker isn't using.
- **A bypass must never leak into the org's next session.** `CreateSession`
  restores the proxy transport before the session starts and a restore
  failure fails the create (CP retries); `DestroySession` restores
  best-effort. Both no-op unless a bypass is actually in effect.
- **Credential rotation respects the flag.** The hot-idle/mid-session refresh
  (`reuseExistingActivation`) rebuilds the secret with or without the proxy
  transport according to `s3CacheMode`; all secret rebuilds serialize on
  `secretSwapMu` so the last write always matches the flag. (Without this, an
  hourly STS rotation silently re-enables the cache mid-benchmark — the
  inverse of the 2026-07-17 incident.)
- **On an exploratory-tier connection the startup option is applied at
  ACTIVATION, not at connect** (there is no worker at connect). The CP parks it
  (`SetPendingS3CacheOption`) and `ensureSessionActive` applies it right after
  installing the executor — never inside the activator, where the executor is
  still nil and the swap would silently no-op while the session flag flipped. A
  failed apply is a fatal `XX000` activation failure, and a tier escalation
  re-applies the bypass on the new worker. `SHOW` activates iff an option is
  still pending, so it can neither lie nor spend a pod needlessly. See the
  Exploratory Worker Tier section.
- **Closed enum, validated on every set path** (`transform.NormalizeS3Cache`):
  PostgreSQL boolean spellings, normalized to on/off, plus `passthrough`; anything else is `22023`
  (simple/batched SET, extended Parse, and the startup option — which the CP
  validates BEFORE acquiring a worker). The rejection never echoes the value.
- **Scope: remote/k8s shared-warm workers with a cache proxy.** Elsewhere
  (standalone, process backend, no `DUCKGRES_CACHE_ENABLED`) the worker swap
  is a no-op and the GUC is session-state only — there is no cache to bypass.
  The query-log sink's activation stays proxied unconditionally.
- Touching the interception, swap, restore, or refresh interplay → update
  `transpiler/s3_cache_test.go`, `server/s3_cache_test.go`,
  `duckdbservice/s3_cache_test.go`, AND the `s3_cache_guc` assertion in
  `tests/mw-dev/e2e/harness.sh` (mw-dev has no cache proxy, so the e2e covers
  the client-visible plumbing incl. the worker action round-trip; the swap
  itself is unit-only — see the harness header note).

## Mid-Session Worker TTL (`duckgres.worker_ttl`, remote backend)

`SET duckgres.worker_ttl = '20m'` / `SHOW` / `RESET` let a client that cannot
set startup options change its bound worker's pool-side hot-idle TTL
mid-session (transpiler interception in `transform/setshow.go`, connection
apply in `server/conn_worker_ttl.go`, control-plane hook
`controlplane/worker_ttl.go::workerTTLControlFor` → pool
`SetWorkerTTLForPID`). Full design: `docs/design/worker-ttl-pool.md`.
Invariants:

- **Same trust boundary as the `duckgres.worker_*` startup options**: gated
  on `DUCKGRES_K8S_ALLOW_CLIENT_WORKER_PROFILE` (SET rejected 22023 when off)
  and clamped to `DUCKGRES_K8S_WORKER_MAX_TTL`; a clamped value is stored
  clamped.
- **State follows the worker, never leads it** (same rule as s3_cache): the
  session override flips only after the apply hook succeeds, and SHOW falls
  back to the bound worker's CURRENT pool TTL (`Current`), so SHOW never
  reports a TTL the worker won't park with.
- **Whole-minute granularity, enforced at validation.** The parked TTL is
  persisted as `ttl_minutes` (integer; 0 = "reaper applies the deployment
  default"), so `NormalizeWorkerTTL` REJECTS zero and sub-minute values with
  22023 — accepting them would park the worker for the deployment default
  while SHOW reported the shorter value. (Sub-minute STARTUP options still
  truncate at park — pre-existing.)
- **Both reapers read the persisted override** (`ttl_minutes` stamped at the
  hot→hot_idle park): the leader janitor's expiry query and the per-CP
  fallback.
- **Exploratory tier**: SET on a lazily-activated connection acquires a
  worker first (the TTL is pool-side per-worker state — there is nothing to
  apply to otherwise); SHOW never acquires (the connect-time baseline is the
  truthful answer pre-worker). Escalation re-applies the override on the new
  worker; on failure the override is RESET (statement error, not
  connection-fatal) — and BOTH session-worker-state re-applies (s3_cache +
  worker_ttl) always run (an s3 failure must not skip the TTL re-apply).
- **Describe must cover BOTH 'S' and 'P'** for the intercepted SET/SHOW — a
  portal-Describe miss returns NoData and probes DuckDB (acquiring a worker
  on lazy connections) for a GUC it does not know.
- **Standalone/process backends** accept SET/SHOW as session state only (no
  per-worker hot-idle TTL exists there).
- Touching the interception, apply hook, park persistence, or the reapers →
  update `transpiler/worker_ttl_test.go`, `server/worker_ttl_test.go`,
  `controlplane/worker_ttl_test.go` + `k8s_pool_worker_ttl_test.go`, AND the
  `worker_ttl_guc` assertion in `tests/mw-dev/e2e/harness.sh`.

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
  (`usersecrets.IsReservedName`: `ducklake_s3`
  + the `__default_*`/`duckgres_*` prefixes, which activation re-creates). It
  MUST run before replay on every CreateSession in shared-warm mode, and a
  wipe failure MUST fail the session.
- **Execute-then-persist ordering.** Persist only statements DuckDB accepted;
  a store failure after a successful exec is an ERROR telling the user the
  secret will NOT survive the session. Replay failures at session create are
  warnings, never connection refusals.
- **No silent non-persistence.** Any path where persistent-secret DDL would
  execute but not persist must REJECT instead, including multi-statement batches
  and parameterized statements in the control-plane interception path. Otherwise
  the secret works for one session and is silently deleted by the next session's wipe.
- **DROP's store-fallback is gated on DuckDB's not-found error only**
  (`isSecretNotFoundError`). Any other exec failure (cancel, RPC error,
  ambiguity, aborted txn) must surface and leave the store untouched — a
  false "DROP succeeded" is fatal for a credential revocation.
- **Never log/store secret statement text.** `usersecrets.RedactForLog` guards
  client-query and worker-statement logs, `logQueryError`, the query log, spans,
  and pg_stat_activity
  (`currentQuery`); keep new logging of query text behind it. Engine **error
  messages echo the offending SQL** (DuckDB emits `LINE 1: ... SECRET '...'`),
  so a failed CREATE SECRET leaks the credential via the `error` attribute /
  query-log `Exception` even when the query attribute is redacted —
  `usersecrets.RedactErrorForLog(query, errMsg)` guards those error sinks
  (`logQueryError`, `logQuery`); keep new error logging behind it too, and pass
  the original (un-redacted) query so it can classify.
- Touching the interception, wipe/replay, or payload shape → update
  `server/conn_user_secrets_test.go`, `duckdbservice/user_secrets_test.go`,
  and the `persistent_user_secret`(+`_isolation`) assertions in
  `tests/mw-dev/e2e/harness.sh`.

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
  viewer. Admins manage operators in the admin-only **Operators** page
  (`ui/src/pages/Operators.tsx` → `/api/v1/operators`, `operators_api.go`) — a
  deliberately distinct surface from the **Org Users** page (`ui/src/pages/Users.tsx`,
  `/users`, per-org *database* logins). The two are labelled + cross-linked in the
  UI so "users" is never ambiguous: Operators = who can sign in to this console;
  Org Users = customer DB accounts. The operators GET is `RequireAdmin`, so
  `useOperators` only fires for admins (viewers see an "admin only" notice). The
  first SSO login auto-provisions a create-only **viewer** row, and the first
  admin is minted by logging in over the break-glass internal token and patching
  that row to `admin`. The **last-admin guard** (`operators_api.go`) refuses to
  demote/delete the final admin (409) so the console can't be locked out.
  `RoleGate` requires admin for all mutating verbs + the audit GET.
  `AuditMiddleware` records every mutation. Keep new mutating routes under this
  gate; never add a write path that bypasses RoleGate/audit.
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
- **Product monitoring API is tenant-safe** (`monitoring.go`):
  `GET /api/v1/orgs/:id/monitoring/{snapshot,series}` is internal-secret-only
  for the PostHog backend. It fixes tenant identity from the path, uses only
  org-scoped runtime-store reads and Prometheus selectors, returns CP coverage
  for partial live-state fan-out, and strips usernames, PIDs, pod/image names,
  CP ownership, SQL, client/trace data, and secrets. The series metric and
  window are closed enums; never turn it into arbitrary PromQL or reuse the
  operator payloads wholesale.
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
    of a user's sessions + in-flight queries but does NOT block reconnects. It
    reaches only connections that HAVE a session: on the exploratory worker tier
    a connection that authenticated but has not yet run its first statement has
    no session (and no registered conn-closer) for `DestroySessionsForUser` to
    iterate, so `kill` misses it. Only `disable` covers that connection — its
    first statement's activation re-check refuses the session with 28000.
    Extending `kill` to authed-but-unactivated connections is a follow-up.
  - `POST …/users/:username/disable` is the **persistent block**: it sets the
    `duckgres_org_users.disabled` column (goose migration
    `000011_add_org_user_disabled.sql`), kills the user's live sessions, AND
    refuses the user's NEW pgwire connections at auth time (`control.go`,
    distinct `28000` "account is disabled" error, emitted only after the
    password checks out so it never leaks account existence). `enable` reverses
    it. The disabled state is read from the in-memory
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
  and a synthesized event ticker. Its header carries only the filters; the
  cluster counters live in the shared admin **Topbar** and show on EVERY page —
  the Topbar polls `GET /cluster/summary` (`useClusterSummary`), a server-side
  aggregate (`cluster.go`) of nodes (duckgres nodepools) · CP replicas · running
  **workers** (label `app=duckgres-worker`, NOT every app pod — so it matches the
  worker chips + the CP's own worker accounting) with their vCPU/GiB request
  totals as a sub-line · **placeholders** with their vCPU/GiB and the cpu%/mem%
  those headroom pods are OF the worker totals · pending. Computing it server-side
  (not from the Nodes view's pushed counts) is what lets the totals appear on
  every page, not just while the view is mounted. The view has no separate live
  indicator — the Topbar's "Connected" dot (admin-API reachability) is the single
  green pulsing live signal. It's imperative DOM (mounted
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
  (`tests/mw-dev/e2e/harness.sh`).
- **Trino cell views** (`trino.go` + `trino_client.go`, UI
  `pages/Trino{Cluster,Queries}.tsx` + `pages/OrgTrinoCard.tsx`, derivations
  in `lib/trino.ts`): live queries with an admin-only, audited kill; the
  cell's coordinator/node health; per-org Trino provisioning state (the
  `duckgres_managed_warehouse_trino` state/status_message/ready_at/failed_at
  columns, which were surfaced nowhere before, so a failed Trino provision
  was silent). Read as the OBSERVER principal, never the provisioner's
  admin — see the Trino Cells section and `admin/README.md`. Payloads are
  redacted (SQL) and projected (never `TrinoEnabledOrg.RootPasswordHash`).
  Every read is cached + timeout-bounded and degrades to `available:false`
  plus a reason rather than erroring the page: the console must render
  during exactly the incident it exists for. Unset
  `DUCKGRES_TRINO_COORDINATOR_URL` leaves the routes unregistered.
- Touching any of the above → update `controlplane/admin/*_test.go` (esp
  `authz_test.go`, `kill_switch_test.go`, `operators_api_test.go`,
  `trino_test.go`, `trino_client_test.go`),
  `controlplane/session_mgr_test.go`
  (`TestDestroySessionsForUser`), `controlplane/configstore/store_test.go`
  (`TestDisabledUserEnforcement`), the `Operators`/`Org Users` UI pages +
  `ui/src/pages/Operators.test.tsx`, AND the `admin_*` / `admin_operators` /
  `impersonation_*` / `user_kill_switch` / `user_disable_block` assertions in
  `tests/mw-dev/e2e/harness.sh`.

## Project-Scoped Logins (`project_reader` / `project_user`) — LOAD-BEARING CONTRACT

A team can hold two generated logins, both bound to one `duckgres_org_teams`
row via `duckgres_org_users.access_mode` + `team_id` (migrations `000026`,
`000031`): `project_reader` (`posthog_team_<id>`, read-only — the PostHog SQL
editor's login) and `project_user` (`posthog_team_<id>_rw`, read/write). Minted
by `PUT /api/v1/orgs/:id/teams/:team_id/{project-reader,project-user}` (admin
API only). Both are enforced by the **query gateway**, not PostgreSQL `GRANT`.
Invariants:

- **The two modes derive the SAME namespaces.** `ConfigStore.OrgUserQueryAccess`
  computes `AllowedSchemas` (`<schema>`, `<schema>_data_imports`,
  `shadow_<team>_models`) + `AllowedRelations` (the legacy `posthog.<events|
  persons>` overrides — a non-NULL override grants it even when it spells the
  derived default) identically for both; **`ReadOnly` is the only difference**.
  Write authorization must NEVER widen the reachable relation set — a project
  user is a project reader that may also mutate what it can already see. The
  e2e asserts cross-project denial in read AND write shapes for this reason.
- **Fail closed on an unresolvable team.** A scoped user whose team row is
  missing or disabled gets an empty, `ReadOnly` policy — a project user is
  DOWNGRADED, never left writing into a scope nothing can confirm.
- **`QueryAccessPolicy.Authorize` (`server/query_access.go`) is the single tap**,
  called on simple query and extended Parse. It is
  default-deny: the parser is the authorization boundary, so anything
  `pg_query` cannot describe is rejected (this is why DuckDB-only spellings
  fail). `CREATE SCHEMA` / `ALTER … SET SCHEMA` stay denied: the schema set IS
  the project boundary.
- **A WRITE TARGET is checked by `authorizeWriteTarget`, never by the walk's
  `authorizeRangeVar`.** The walk's check is the READ-position one: it accepts a
  bare name that is a visible CTE (sound, because a defined CTE provably shadows
  a base relation of the same name) or an unqualified pg_catalog compat name.
  **Neither is sound for a target** — a write target does not bind to the CTE, so
  it resolves against the session `search_path` (`sessionmeta` leaves it at
  `main,memory.main`) and reaches a real relation outside the grant;
  `WITH shared AS (…) INSERT INTO shared VALUES (1)` was a working escape into
  `ducklake.main.shared` before this split. `authorizeWriteTarget` therefore
  requires schema qualification unconditionally. Do NOT infer target-ness from
  proto position: `walk()` descends into a Node's own oneof, so a bare
  `*RangeVar` message reaches `walkMessage` for reads too. **Adding a case to
  `authorizeWriteStatement` REQUIRES enumerating that statement's target
  field(s) there by name** (`DropStmt` scopes its dotted-name lists itself); the
  `walkMessage` `RangeVar` branch is only the lenient defense-in-depth net for
  positions that enumeration does not cover (e.g. `Constraint.pktable`).
  `TestQueryAccessPolicyRejectsUnqualifiedWriteTargets` is the tripwire.
- **Neither mode may carry passthrough** (it bypasses the compat layer that
  enforces the scope) — DB check constraint + admin API guard.
- **A policy change tears down live sessions.** `changedProjectScopedUsers`
  (`org_router.go`) fires on credential, team, disable AND **mode** flips; the
  demotion direction is load-bearing (a demoted user must not keep a
  write-authorized session). Deleting a team deletes BOTH of its logins.
- **One of each per team**, via two SEPARATE partial unique indexes on
  `(org_id, team_id)` — deliberately not one index across both modes, so a
  reader and a writer coexist.
- **Service credentials are NOT project-scoped and never touch
  `duckgres_org_users`.** `POST /api/v1/orgs/:id/service-credentials` is how a
  PostHog backend job (dagster) fetches a short-lived credential: a
  per-credential grant row with its own `svc_` identity, root-shaped
  (unrestricted) at the org level. See "Service Credentials" below for the
  full contract.
- Touching any of this → update `server/query_access_test.go`,
  `controlplane/configstore/query_access_test.go`,
  `controlplane/admin/api_test.go`, `controlplane/org_router_test.go`,
  `tests/configstore/org_teams_postgres_test.go` +
  `migrations_postgres_test.go`, `docs/postgres-compatibility.md`, AND the
  `project_reader_isolation` / `project_user_isolation` assertions in
  `tests/mw-dev/e2e/harness.sh`.

## Service Credentials (`POST /orgs/:id/service-credentials`) — LOAD-BEARING CONTRACT

How PostHog backend jobs (dagster today) authenticate to duckgres WITHOUT a
long-lived password living in Django. Replaces the org-root credential read
from a `DuckgresServer` row with a per-credential grant minted on demand by
the CP — AWS AccessKey/Secret style: each minted credential is its own
`duckgres_service_grants` row with its OWN identity (`credential_id`), its
own TTL, and its own rotation clock. Storage NEVER touches
`duckgres_org_users` — there is no shared login row for an operator rotation
or password update to clobber mid-run, and service credentials are not
project-scoped (root-shaped: unrestricted at the org level).

**Caller contract:**
`POST /api/v1/orgs/:id/service-credentials` with
`{principal, ttl_seconds?}`. `principal` is audit attribution
(`"dagster:events-backfill"`) only: every request creates an independent grant
with a new `credential_id` and secret, even when principals are identical.
`ttl_seconds` is clamped to [1 min, 1 h]
(default 15 min, the RDS-IAM precedent). Response is
`{credential_id, credential_secret, expires_at, connect}`; all fields are
always present. `POST /api/v1/orgs/:id/service-credentials/refresh` with
`{credential_id, ttl_seconds?}` **ALWAYS rotates** the named grant's secret
and returns `{credential_id, credential_secret, expires_at, connect}`.
The caller is the internal-secret-authed PostHog backend — the routes sit
next to the other provisioning routes for exactly that trust class, NOT on
the admin/console side.

The `connect` block tells the caller WHERE to use the credential from the same
authoritative CP response that issued it, so nothing downstream re-derives its
own idea of the warehouse endpoint out of band (a Django `DuckgresServer` row
is exactly the drift this kills). It is
`{host, port: 5432, database: "ducklake", sslmode: "require"}`.
**`connect.host` is ALWAYS the org's canonical ingress name
`<org-id>` + the CP's configured managed-ingress suffix** (e.g.
`<org-id>.dw.us.postwh.com`) — the very value the pgwire TLS `server_name`
pins (the wildcard cert is `*<suffix>` and the SNI router resolves the
single-label prefix as the org; see "Native metadata Postgres proxy" /
`controlplane/sni_kubernetes.go`). It is one logical name handed back verbatim
for every caller: NEVER a pod IP, NEVER a ClusterIP, NEVER resolved per caller
network. How that name resolves for a given caller — public ingress vs an AWS
PrivateLink endpoint for dagster workers — is the caller network's business,
not the CP's. The CP wires the suffix from its first configured
`ManagedHostnameSuffixes` entry (`DUCKGRES_MANAGED_HOSTNAME_SUFFIXES`) at the
`RegisterAPI` site (`controlplane/multitenant.go`), falling back to
`provisioning.DefaultManagedIngressSuffix` when unwired.

**The load-bearing invariants:**
- **The credential IS its own grant row.** `credential_id` is
  `svc_<24 random hex>`, generated server-side, never caller-supplied; the
  client presents it as the pgwire username and the plaintext secret as the
  password. The bcrypt hash lives on the `duckgres_service_grants` row —
  `duckgres_org_users` holds NO service-credential material, so operator
  writes to the org's users table can never invalidate (or be depended on by)
  a minted credential.
- **Auth resolves `svc_`-prefixed usernames against the grants snapshot map
  ONLY** (`Snapshot.OrgServiceGrant`, loaded alongside `OrgUserPassword`).
  Expiry and revocation are enforced at that lookup: unknown, revoked,
  expired, and hash-blanked credentials all fail identically (with equalized
  bcrypt time — which state a credential_id is in is not probeable). A live
  grant is `revoked_at IS NULL AND expires_at > now()`.
- **Mint always creates.** `principal` is non-unique audit metadata, never an
  identity or reuse key. Every mint inserts a new row, returns its new ID and
  plaintext once, and leaves every same-principal credential untouched. This
  lets concurrent jobs keep a stable principal without sharing secrets.
- **Management is by `credential_id`.** A caller keeps the ID returned by its
  mint and supplies it to refresh/revoke. Losing the plaintext means minting a
  new credential; plaintext cannot be recovered from the stored bcrypt hash.
  New Duckgres servers ignore the removed `force_rotate` JSON field so an old
  caller can still reach the always-create endpoint, but that compatibility
  is one-way: deploy the always-create Duckgres version to the whole fleet
  BEFORE removing PostHog's `force_rotate`/reuse fallback. A new caller talking
  to an old server could otherwise receive a reused ID without plaintext.
- **Refresh always rotates and never creates.** Unknown `credential_id` →
  404; a REVOKED grant → 410 (revocation is terminal: refresh never
  resurrects). An EXPIRED (but unrevoked)
  grant MAY be refreshed — expiry only refuses NEW handshakes, so refresh is
  how a caller that missed the window recovers without minting a second
  identity for the same principal.
- **Established sessions are NEVER torn down** — not on expiry, not on
  rotation, not on refresh (the mint plane is separate from connection
  scheduling). Freshness is enforced only at the pgwire handshake — the
  RDS-IAM / Cloud-SQL-IAM contract. A long job's existing connection rides
  to completion; each NEW connection mints or refreshes afresh.
- **Revoke keeps the row** (`DELETE /api/v1/orgs/:id/service-grants/:credential_id`):
  `revoked_at` is stamped and the hash is BLANKED server-side, so a leaked
  credential can never authenticate again and the provenance (principal,
  mint/rotation times) survives for investigation. `GET
  /api/v1/orgs/:id/service-grants` is the flat, all-statuses list — no
  plaintext, no hashes (`PasswordHash` is `json:"-"`). Durable history is
  intentional because expired, unrevoked IDs remain refreshable and audit
  provenance must survive; do not add expiry deletion. If grant volume makes
  the operator list expensive, add pagination/archival as a separate API
  change rather than weakening lifecycle semantics.
- **Org deletion deletes all of that org's grant rows.** This is the explicit
  lifecycle boundary to the normal durable-history rule: leaving a grant
  behind would let its old secret authenticate if the org name were reused.
  Org creation takes the same advisory lock and clears pre-existing orphan
  grants only when the locked lookup confirms no current org row; duplicate
  creation and re-provisioning an existing org must preserve live grants.
- **Concurrency**: each mint owns a new row. Mint/refresh/revoke still take
  `LockOrgConnectionAdmissionTx` so org lifecycle changes cannot leave orphan
  grants and management of a named ID remains serialized.
- **Admission recognizes authenticated `svc_` identities as root-shaped.**
  They consume the org vCPU budget but do not require an org-user row or carry
  a normal per-user cap. Admission and post-acquisition worker switching do
  not reapply expiry/revocation: those are handshake-only checks, so an
  established session rides to completion.
- **After the write, THIS replica's snapshot is reloaded immediately**
  (`ReloadSnapshot`), then a best-effort `/api/v1/internal/reload-snapshot`
  fan-out to peer replicas (`PeerFanout`, wired from the same
  `clusterPeerFetcher` the admin kill-switch uses). Without both, a freshly
  minted credential routinely fails its first auth on whichever replica the
  pgwire connection lands on.
- Touching any of this → update `controlplane/provisioning/service_credential.go` +
  `_test.go`, `controlplane/configstore/service_credential.go` +
  `_postgres_test.go`, `controlplane/admin/api.go` (list/revoke), and the
  caller-side minter in the PostHog repo
  (`products/managed_warehouse/backend/service_credentials.py`).

## Compute-Usage Billing (managed-warehouse, remote backend only)

duckgres meters per-org compute usage of worker pods into 60s buckets in the
config store; the billing service **pulls** the accumulated usage over an HTTP
API and acks a watermark, at which point duckgres deletes the acked buckets.
Full design + decisions: `docs/design/billing-pull-api.md` (supersedes the
push/capture reporting hop of `billing-compute-seconds-plan.md`; the metering
side of that doc still applies). Scope is **only** the remote/k8s backend
(per-org worker pod with a known `WorkerProfile` size). Pipeline:

```
compute: conn end → in-proc counter keyed (org, informational team, query_source, worker size)
              │  flusher (~15s) UPSERT-increment → config-store buffer (cross-CP sum)
              ▼  duckgres_org_compute_usage (+ duckgres_compute_billing_cursor)
storage: leader sampler (~30m) → org's DuckLake metadata Postgres
              SUM(data+delete file sizes) × interval → duckgres_org_storage_usage
billing: GET /api/v1/billing/usage (usage + storage arrays, per key per UTC day, watermarks)
       → POST /api/v1/billing/ack {watermark_high} → cursor advance + delete ≤ it (BOTH tables)
safety:  leader-only GC hard-deletes buckets older than 30 days (WARN, alertable)
```

Two raw metrics per connection over its full lifetime, using the **provisioned**
worker size: `cpu_seconds = vCPU × ceil(conn_secs)`, `memory_seconds = GiB ×
ceil(conn_secs)`. Counted internally in integer **millicore-seconds** /
**MiB-seconds** (`compute_meter.go`) to avoid truncating a fractional-core /
sub-GiB worker; worker size is stored in the bucket key as exact NUMERIC
decimals (vCPU / GiB). `team_id` is **informational only** (an integer —
PostHog's `Team.id`; a JSON NUMBER on every API surface): duckgres does NOT
own team-level billing attribution — the external billing service maps
org → team(s) itself. The stamp is resolved from the config snapshot at
record time: compute buckets get the CONNECTING USER's team
(`duckgres_org_users.team_id`, e.g. a project-reader login) when it has one,
else the org's OLDEST team (min `created_at`, ties broken by the smaller
`team_id` — in practice the provision-time first team; `ConfigStore.OrgUsageTeamID`);
storage buckets always get the oldest team (`OrgOldestTeamID`). 0 appears
only defensively (unknown org / stale snapshot — a committed org always has
at least one team). Team changes/deletions NEVER re-attribute existing
buckets; `query_source` is the
`duckgres.query_source` session GUC (`standard` unless set; a mid-connection
change bills the whole connection under the final value). The GUC is a **closed
enum validated at SET time** (`transform.NormalizeQuerySource`): only
`standard` | `endpoints` (case-insensitive, normalized to lowercase; empty =
reset to default) — anything else is rejected with `22023` on every set path
(simple/batched SET, extended Parse, and the `-c` startup option, which rejects
the connection like invalid `duckgres.worker_*` options), and
`server.ConnectionBilling` clamps a non-canonical value to `standard` as
defense in depth so client junk can never become a billing bucket key.
Invariants for anyone
touching this path:

- **Metering is strictly best-effort and off the hot path.** A metering error
  (counter, flush) must NEVER block or fail a query or connection teardown. The
  connection-end record is added to an in-process counter (map+mutex,
  microseconds, no I/O); the flush is async. `cp.computeMeter` is nil outside
  the remote backend — every call site is nil-safe. There is no enable knob:
  the remote backend always meters.
- **Worker size is plumbed onto the connection** (`server.SetConnectionWorkerSize`
  → `clientConn.workerMillicores/workerMiB`, set in `control.go::handleConnection`
  from `workerBillingSize(workerProfile)`, remote-only). `workerMillicores==0`
  (non-remote / unknown) → metering skipped. The metric is computed once at the
  SAME teardown point as `CloseConnectionMetrics` (the `#841` lifetime defer),
  via `server.ConnectionBilling` (which also carries the query source).
- **Bucket = connection-end time floored to 60s.** Flush carries the sub-unit
  remainder forward so rounding never loses counts across flushes. Buffer flush
  is UPSERT-increment so all CP pods sum into one row per key.
- **Serve only closed buckets.** `watermark_high` = the newest bucket with
  `bucket_start ≤ now − 60s − 30s grace` (grace > flush interval, so every
  CP's contribution has landed before a minute is served). The GET aggregates
  the window `(cursor, watermark_high]` into one row per
  `(org, team, query_source, cpu, mem_gib)` per **UTC day** — response size is
  bounded by active keys × days, so billing downtime can't make it explode.
- **Ack is the only deletion path (plus the 30d GC).** `POST /billing/ack`
  advances the single global cursor monotonically and deletes buckets
  `≤ watermark_high` in one TXN (`AckComputeUsage`). Idempotent — re-acks and
  stale acks are no-ops. An ack beyond the latest closed bucket is rejected
  (400) so it can never delete buckets that were never served. Auth is the
  admin internal secret (`RequireAdmin` on both routes, registered inside the
  audited `/api/v1` group in `multitenant.go`).
- **Safety GC is leader-only** (`runComputeUsageGC`, attached under the janitor
  lease): hard-deletes buckets older than 30 days regardless of ack and logs a
  WARN with the dropped count — nonzero means billing stopped pulling (alert).
- **Graceful shutdown does a final flush** after connections drain to their
  natural end (`shutdown`/`drainAndShutdown`), so a departing CP pod lands its
  last interval before exit.
- **Org team CRUD (`duckgres_org_teams`)**: the PostHog backend manages an
  org's team rows via `GET/POST /api/v1/orgs/:id/teams` +
  `DELETE /api/v1/orgs/:id/teams/:team_id` (internal secret,
  `controlplane/provisioning`). The POST is the **grandfather upsert**: it MAY
  overwrite an existing row's `schema_name` and the legacy
  `events_table_name`/`persons_table_name`/`schema_data_imports_name`
  overrides (NULL = derive from `schema_name`: `<schema>.events`,
  `<schema>.persons`, `<schema>_data_imports`), because the PostHog backfill
  replaces migration 000024's `team_<id>` placeholder through it. Two teams in
  one org can never share a schema (unique `(org_id, schema_name)`, migration
  000025 → 409). Provisioning a warehouse for a NEW org REQUIRES `team_id`
  (`ErrProvisionTeamRequired` → 400; `default_team_id` is accepted as a
  transitional alias) and creates the org's first plain team row — a
  warehouse cannot exist without a team. DELETE removes CONFIG only (never
  warehouse data) and never touches usage buckets; the org's LAST team is
  undeletable (409 — an org must always have at least one team; delete the
  org instead). The admin console mirrors this on a user-facing surface
  (`GET /teams`, `POST /teams`, `PUT /orgs/:id/teams/:team_id`) where
  `schema_name` is immutable. Shared rules live in
  `configstore.UpsertOrgTeamTx` / `DeleteOrgTeamTx`; tests:
  `tests/configstore/org_teams_postgres_test.go`, the provisioning/admin API
  tests, and `org_teams_crud` in the e2e harness.
- **Storage metric** (`managed_warehouse_storage_gib_seconds`,
  `storage_meter.go`): a LEADER-ONLY sampler (double writers would
  double-bill — the UPSERT is additive) visits each Ready warehouse's DuckLake
  metadata Postgres every 30m (env-only `DUCKGRES_STORAGE_SAMPLE_INTERVAL`;
  e2e uses 60s) and credits exactly `tracked_bytes × interval` byte-seconds —
  no elapsed-time tracking, a missed sample under-bills one interval. The SUM
  is over `ducklake_data_file` + `ducklake_delete_file` with NO snapshot
  filter (never `ducklake_table_info()`/`ducklake_table_stats` — current-
  snapshot-only / approximate). byte-seconds are NUMERIC (BIGINT overflows);
  served as exact-decimal GiB-seconds (÷2³⁰ terminates;
  `byteSecondsToGiBSeconds` big-int math). Connection resolution reuses the
  cross-org activator (`MetadataPostgresURL`: duckling pgbouncer → sslmode
  disable, direct RDS → require). Drift gauges:
  `duckgres_org_storage_pending_delete_files` (alert on sustained nonzero) +
  `duckgres_org_storage_tracked_bytes`.
- **The admin console usage views read the SAME buffer** —
  `GET /api/v1/usage/monthly` (the **Usage** page) and
  `GET /api/v1/orgs/:id/usage/daily` (the org detail page's **Usage** charts)
  in `controlplane/admin/usage_api.go`, backed by
  `configstore.Aggregate{Compute,Storage}Usage{Monthly,Daily}`, sum retained
  buckets per UTC month / per UTC day per (org, team), merging the compute and
  storage families and joining the team schema name for display. Both
  self-gate with `RequireAdmin` (per-team cost data across all orgs is as
  sensitive as the raw billing families — viewers get 403, and the UI hides
  the nav item / fires no query for them). The daily endpoint's org scope is
  the `:id` path segment flowing into the queries' WHERE clause — one org's
  usage must never leak into another org's page (the e2e asserts
  `.org_id == $o` on the response). These are operations views, NOT invoices:
  acked buckets are already deleted and >30d buckets are
  GC'd, so responses carry the ack cursor as `watermark_low` and the UI
  shows the retention caveat instead of implying all-time totals. They add NO
  second accounting pipeline — keep them pure reads over the buffer.
  The Usage page also carries a **client-side pricing-sensitivity calculator**
  (`ui/src/pages/UsagePricing.tsx` + `lib/pricing.ts`): named unit-price
  scenarios ($/CPU-min, $/GiB·min, $/GiB·h) priced against each org's month
  totals. It is pure browser math over the monthly rows — no endpoint, no
  persistence beyond the operator's own localStorage — so it inherits the
  page's admin-only gate and needs no server-side access control of its own
  (a PM gets it by holding the console admin role; a lighter pricing-viewer
  role is a named follow-up, not implemented).
- Touching the meter/flush/API/GC, the worker-size or query-source plumbing,
  the storage sampler, or the bucket keys → update
  `controlplane/compute_meter_test.go`, `compute_billing_api_test.go`,
  `compute_size_test.go`, `storage_meter_test.go`,
  `configstore/storage_usage_test.go`, the migration assertion in
  `tests/configstore/migrations_postgres_test.go`, and the
  `compute_usage_pull_api` assertion (compute + storage, incl. the
  `usage-monthly` checks) in
  `tests/mw-dev/e2e/harness.sh`. Touching the monthly/daily aggregation or
  the usage views → update `controlplane/admin/usage_api_test.go`,
  `tests/configstore/usage_monthly_postgres_test.go` +
  `usage_daily_postgres_test.go`,
  `ui/src/pages/Usage.test.tsx` + `OrgUsage.test.tsx`, and the
  `usage-monthly` / `usage-daily` harness checks.

## Discovery Endpoints (external-writer tenant listing)

`GET /api/v1/warehouses` + `GET /api/v1/warehouse-team-ids` on the internal
provisioning API (`controlplane/provisioning/discovery.go`) are the read-only
"which tenants exist and where do I write" surface for EXTERNAL writers
(viaduck destination discovery; millpond's include-values poller). Semantics
are load-bearing for those consumers:

- **Warehouse set = ready + resharding** (`discoveryStates`). Resharding is
  listed with `writable=false` — vanishing would read as tenant REMOVAL
  downstream, not a pause. The state enum is open: a new state MUST be
  classified into `discoveryStates`/`discoveryExcludedStates`
  (`TestDiscoveryStateClassification` is the tripwire; an unclassified state
  reads as fleet-wide removal to every consumer).
- **Teams come from `duckgres_org_teams`**, with RESOLVED table locations:
  `events_table`/`persons_table` = `<schema_name>.<override-or-derived>`,
  `data_imports_schema` = override-or-`<schema>_data_imports`. The
  derivation lives ONCE, in `resolveTeamTables`; the legacy overrides are
  BARE identifiers (never schema-qualified), enforced at every write surface
  by `configstore.ValidateOrgTeamTableName`.
- **`enabled` is passed through as information only** — it is the per-team
  query-serving switch (migration 000024, not yet enforced), NOT an
  ingestion signal. Disabled teams stay in BOTH endpoints; deriving
  "stop ingesting" from it would turn a serving hold into permanent event
  loss. The only ingestion-stop signal is row absence.
- **Error contract:** transient store failures fail the WHOLE request (a
  polling consumer keeps last-known-good — the safe direction); only a
  warehouse with zero team rows degrades, per-warehouse, to an empty teams
  array (`duckgres_discovery_broken_team_rows_total{reason}` counts it — a
  sustained nonzero means a live tenant is silently unroutable).
- **`config_generation` is an opaque change token**: max `updated_at` over
  ALL warehouse+org+org-team rows regardless of state, read BEFORE the data
  queries. Its delete-visibility depends on `DeleteOrgTeamTx` touching the
  parent org row (a bare team DELETE leaves no timestamp behind) —
  `TestLatestConfigChangeCoversTeamsPostgres` pins that pair; keep them in
  sync. Compare for equality only.
- **No plaintext credentials in the payload** — metadata-store passwords are
  k8s SecretRefs. cnpg connection details are MIRRORED from the Duckling CR
  status into the row by the provisioner's ready-reconcile
  (`reconcileMetadataStoreRow`; drift-only writes so steady-state ticks
  never bump `updated_at`/the change token; the state-CAS write is the
  reshard fence — the runner owns those columns mid-flip). External rows
  are provision-time inputs; only their credential Secret ref is mirrored.
- **Auth is a SEPARATE, scoped surface** (`RegisterDiscoveryAPI` + its own
  group in `multitenant.go` behind `admin.AnyTokenAuthMiddleware`): the
  read-only discovery secret (`--read-only-secret` /
  `DUCKGRES_READ_ONLY_SECRET`, sent in `X-Duckgres-Internal-Secret`, same
  fallback-rotation semantics as the internal secret) works ONLY on these
  two GETs; the admin internal secret also works here (operator/debug +
  rotation window). Never register discovery routes inside the admin
  `api` group and never accept `readOnlyTokens` anywhere else — external
  writer pods carry this credential, and its blast radius must stay "read
  the tenant list and its connection topology (RDS endpoints, bucket
  names, k8s Secret names — never values)". Tripwires:
  `TestAnyTokenAuthMiddlewareScoping` (token matrix incl. cross-surface
  rejection) and `TestReadOnlyGroupTopology` (the group's exact route
  set, against the real `registerReadOnlyGroup` wiring). A discovery
  value equal to the internal secret (or any fallback) FAILS STARTUP —
  `validateDistinctReadOnlySecret` — because a shared value silently
  un-scopes the credential.
- Touching the payload shape, states, team derivation, or the generation →
  update `controlplane/provisioning/discovery_test.go`,
  `tests/configstore/org_teams_postgres_test.go`, AND the
  `discovery_endpoints` assertion in `tests/mw-dev/e2e/harness.sh`.

## Resharding (metadata-store migrations) — LOAD-BEARING CONTRACT

Operator-driven moves of an org's DuckLake catalog between metadata stores
(cnpg↔cnpg, ext→cnpg, cnpg→ext escape hatch), admin-console-driven with a
verbose op log. Full design: `docs/design/resharding.md`. Pieces:
`configstore/reshard.go` (+ migrations `000018`/`000021`/`000022`),
`provisioner/reshard_runner.go` + `catalog_copy.go` + `catalog_backup.go`
(the step machine, executed in a DEDICATED per-op pod — see below),
`controlplane/reshard_runner_mode.go` (`--mode reshard-runner`, the pod's
entrypoint), `controlplane/reshard_pod.go` (spawner) +
`reshard_reconciler.go` (leader-only pod janitor), `admin/reshard.go`, UI
`ReshardForm.tsx`/`ReshardOperation.tsx`. Invariants:

- **Reshards execute in a dedicated per-operation pod, NEVER in a CP
  process** (`duckgres-reshard-op-<id>`, labels `app=duckgres-reshard` +
  `duckgres-op-id`, restartPolicy Never, TGPS 600, requests=limits from the
  env-only `DUCKGRES_RESHARD_POD_CPU`/`DUCKGRES_RESHARD_POD_MEMORY`, default
  2/8Gi): a catalog pg_dump/copy must not compete with live traffic for CP
  resources (a ~20k-table dump OOM-killed a 512Mi CP pod). The pod runs the
  CP's OWN image + ServiceAccount (no new RBAC) and inherits an ALLOWLIST of
  the CP's env spec verbatim (`reshardPodEnvAllowlist` — secretKeyRefs stay
  refs; nothing secret lands in a pod spec). The start handler creates the op
  PENDING and spawns the pod; the pod claims the row via the standard CAS,
  runs the step machine to a terminal state, and exits 0 — the op row is the
  OUTCOME's source of truth (failed/rolled-back still exits 0; only infra
  errors — store unreachable, claim lost/fenced — exit nonzero). A leader-only
  reconciler (attached under the janitor lease) respawns the pod of a
  pending-past-grace or stale-heartbeat op (bounded, 3 attempts, then
  force-fail with an operator-facing error) and reaps pods of terminal ops.
  The e2e reshard pod netpol lives in `k8s/networkpolicy.yaml`
  (`duckgres-reshard-runner-boundaries`, egress-only: DNS/5432/443/8080); the
  production chart needs the equivalent (charts repo).

- **The sound connection barrier is the lease-GRANT check**, not the
  connect-time 57P03 gates: the grant transaction refuses `resharding` orgs
  under the same per-org advisory lock `SetWarehouseResharding` takes for the
  `ready→resharding` CAS. Never rely on the snapshot-polled connect gate
  alone — a lease can be granted up to a queue-timeout after it ran.
- **Drain, never kill**: live queries always finish. Drain = leases==0 AND
  queue==0 (one tx) AND zero live org workers (each runs a catalog-writing
  `DuckLakeCheckpointer`). Lingering hot-idle workers are retired via the
  standard CAS retire path only — never raw pod deletes.
- **Flip semantics differ by direction**: a `cnpgShard` change re-points
  role/DB in place (source ORPHANED — explicit cleanup after verify); a TYPE
  flip to external un-renders the cnpg MRs, and whether Crossplane then DELETES
  or ORPHANS the role/DB depends on `spec.metadataStore.retainCnpgOnFlip`
  (charts). **External stores are never modified/deleted.**
- **cnpg→ext escape hatch = orphan-adopt then verified-delete** (charts
  `retainCnpgOnFlip`): copy → verify source → set `retainCnpgOnFlip=true` AND
  poll the cnpg Role/Database MRs until they carry the no-Delete policy (two-step
  flip, closes the un-render-before-policy race; this MR read needs an explicit
  get grant on roles+databases in `postgresql.sql.m.crossplane.io` — NOT covered
  by duckling-reader; a Forbidden fails the wait immediately with the missing
  grant named, and the periodic wait log + timeout error carry the observed
  `managementPolicies`) → flip type to external (now
  ORPHANS, not deletes, the cnpg role/DB) → verify the external catalog row
  counts match the copy EXACTLY → only THEN `DROP DATABASE` the retained source +
  clear the flag. ANY failure before that drop → flip back to cnpg-shard +
  clear flag in one patch (`SetMetadataStoreCnpgAdopt`), provider-sql re-ADOPTS
  the still-present role/DB by external-name — NO copy-back, NO empty-recreate
  (replaces the old recreate+copy-back recovery that caused a data-loss
  incident). **XRD-compat**: if the read-back shows the cluster's XRD lacks
  `retainCnpgOnFlip` (patch pruned), REFUSE the reshard ("deploy charts first")
  — safer than the destructive delete-on-flip.
- **DEPROVISION-UNAFFECTED (non-negotiable)**: `retainCnpgOnFlip` defaults false
  and is true only transiently mid-reshard; a normal never-resharded cnpg tenant
  always has it false → its cnpg Role/DB render with full lifecycle `["*"]`
  (Delete) → deprovision (Duckling delete → finalizer) drops them exactly as
  before. The orphan is bound to the reshard type-flip, NEVER to Duckling
  deletion. `lifecycle_teardown_cnpg` in the e2e is the regression net.
- **Pre-flip catalog backup (safety net, `backup_catalog` step,
  `catalog_backup.go`)**: after drain + pause-compaction + `recordSource` and
  BEFORE any flip (for EVERY direction), the runner `pg_dump`s the SOURCE catalog
  to the org's OWN S3 data bucket under
  `s3://<bucket>/_reshard_catalog_backups/op-<id>-<ts>.dump` (custom format →
  `pg_restore`; password via `PGPASSWORD`, never argv; bucket/region/IAM-role
  from the duckling CR status; upload creds via STS AssumeRole of the org's own
  role, injected as an `AssumeRoleFunc` from the reshard-runner mode
  (`controlplane/reshard_runner_mode.go`) to avoid the import cycle). The dump
  is STREAMED: pg_dump stdout flows straight into an S3 multipart upload
  (16MiB parts × concurrency 2 bounds memory regardless of catalog size — no
  temp file, no whole-dump buffering; a nonzero pg_dump exit or empty dump
  deletes the partial object). **Gate by direction**: the destructive cnpg→external direction
  (its verified-delete drops the source) HARD-FAILS the op before the flip if the
  backup fails; non-destructive directions (source survives) log a warning and
  continue. The URI is recorded on `backup_s3_uri` (migration `000021`) + the op
  log, with the exact `pg_restore` recovery command. Retention is an S3 lifecycle
  rule on the reserved key prefix (30d suggested) — no in-app GC; backups are
  kept on success. The objects carry NO object tag: PutObject with x-amz-tagging
  needs `s3:PutObjectTagging`, which the org duckling roles do not grant (a
  tagged upload 403s on the real cluster — mw-dev e2e regression). pg_dump/pg_restore ship in the CP image
  (Wolfi's `postgresql-18-client` apk, in BOTH `Dockerfile` and
  `Dockerfile.controlplane`). Full design + restore procedure:
  `docs/design/resharding.md`.
- **Source identity: the duckling STATUS is authoritative, validated at
  submit.** The start handler derives source_kind/from_shard/the external
  source block from the duckling STATUS (where the catalog actually lives) and
  400s an op whose identity is incomplete or contradicts the config-store row
  (kind drift, cnpg source with unresolvable shard, external source with an
  incomplete row block). The runner re-checks at recordSource (pre-flip).
  Never re-introduce the old "empty row kind defaults to cnpg-shard" fallback
  — it pointed a flip-before-copy reshard at a phantom cnpg source while the
  org lived on an external RDS (prod incident: org re-pointed onto an empty
  catalog, rollback patch rejected).
- **Metadata credentials come from a referenced Secret, never CR status.**
  `status.metadataStore.credentialSecretRef` names one key in a Secret in the
  `ducklings` namespace; `DucklingClient.Get` resolves it before returning the
  status used by activation, probes, metering, and resharding. References to
  any other namespace are rejected. The name, namespace, and key are all
  mandatory; an incomplete reference or an unreadable/missing Secret is a hard
  error. Duckgres never reads credential material from Duckling CR status.
- **Rollback patches the source shard VALUE back — never removes the key**
  (precedence would fall through to the freshly-stamped bogus status pin);
  ext→cnpg rollback must null `cnpgShard` (XRD CEL forbids it on external).
  **Never emit a patch the XRD would reject**: an empty/invalid recorded
  from_shard (`isValidCnpgShardName`, mirrors the XRD pattern) or an
  incomplete external source block skips the flip-back/adopt patch, logs
  ERROR operator instructions, and **intentionally leaves the warehouse
  blocked in `resharding`** — unblocking onto the wrong store is worse than a
  blocked org (see docs/design/resharding.md's never-stranded carve-out).
  Takeover/endpoint reconstructions derive sslmode from the metadata-store
  KIND via `sslModeFor` (cnpg → disable, external → require) — never
  hardcode an sslmode.
- **The compaction pause must never let XRD defaulting disable DuckLake**:
  `SetCompactionEnabled` pins the org's effective `spec.ducklake.enabled`
  (legacy type coupling, as the activator derives it) into the patch when the
  CR has no `spec.ducklake` object — otherwise materializing the object gets
  `enabled: false` stamped by the XRD default and every later activation
  fails ("tenant activation requires a ducklake metadata_store"). The pinned
  value is never removed afterwards (deliberate — see
  `TestSetCompactionEnabledPinsDuckLakeEnablement`).
- **The ext target password is ephemeral**: request → creating replica's
  in-memory stash → one-shot pull by the runner pod → runner memory; never in
  the op row, log, audit, k8s Secret, or any pod spec. The runner pod fetches
  it at startup from `GET /api/v1/reshards/:id/password` on the CREATING
  replica's pod IP (the URL — never the password — is persisted on
  `password_url`, migration `000022`, so a reconciler respawn re-wires the
  same handoff). The endpoint is INTERNAL-SECRET identity ONLY (an SSO admin
  is 403'd — operators must never read tenant credentials); the stash is
  pruned once the op turns terminal. A pull that 404s (creating replica gone)
  is not an infra error: the runner proceeds stashless and the step machine
  fails the op with the clear "password is not available … cancel and re-run"
  message, then rolls back. (This replaces the old claim-on-create /
  `CreateReshardOperationClaimed`+`AdoptClaimedOperation` path — with the op
  no longer executing in a CP process, pinning it to the creating replica is
  neither possible nor needed.)
- **The ext SM secret must be ESO-readable and a raw string**: the ESO IAM
  policy only allows `posthog-*`/`duckling-*` names, so the start handler
  enforces a POSITIVE allowlist (`esoReadableSecretPrefixes`) — a name outside
  it is 400'd, with RDS-managed `rds!…`/`rds/…/master` names
  (`rdsManagedSecretNamePattern`) detected first for a more specific message.
  The composition's ExternalSecret copies the whole value verbatim (no JSON
  property). An unreadable name that slips through just hangs the cutover wait
  until the per-op timeout, then recovers (flip-back + copy-back). The form
  teaches the same rules (`ui/src/lib/reshard.ts::classifySecretName`).
- **cnpg→ext fails fast at submit, before the destructive flip**: the flip
  DELETEs the cnpg source, so both submit-time gates (the ESO-name allowlist
  above + a bounded `SELECT 1` pre-flight connection check to the external
  target, `admin.ExternalTargetProber` over `provisioner.PGCatalogCopier.Probe`,
  `sslmode=require`, wired in `multitenant.go`) 400 a doomed op before anything
  is created. The prober nil-degrades (tests / non-k8s → check skipped; the
  runner's copy still catches a bad credential); when present and the connect
  fails, the op is refused. The 400 never echoes the password.
- **Runner fencing**: claim bumps `runner_epoch`; every runner write is
  CAS-fenced on (runner, epoch); stale-heartbeat (>5m) ops are takeover-able;
  the copy holds a target-DB advisory lock.
- **Takeover rollback reconstructs progress from the persisted row.** The
  in-process rollback flags (`blocked`, `compactionPaused`, `flipped`,
  `retainRequested`) start false on a fresh `opRun`. A runner that CLAIMS an op a
  prior epoch advanced (crash-takeover or replica switch) MUST call
  `reconstructProgress()` before `run()` — otherwise a cancel/failure that
  short-circuits before the steps re-execute (e.g. `run()`'s first
  `cancelRequested()` check) rolls back with all-false flags and silently skips
  unblocking the warehouse (org stuck in `resharding`) and restoring compaction.
  Reconstruction is conservative: `blocked` ← `blocked_at` set OR step past
  `blocking`; `compactionPaused` ← step past `pausing_compaction` (proves the
  prior setting was recorded, else restore could wrongly re-enable compaction);
  `flipped`/`retainRequested` ← step reached `cutover`/`orphaning_source`
  (over-marking is safe — the flip-back/adopt patches are idempotent no-ops when
  the store never moved). This was a real mw-dev incident: the blocking runner
  OOM-crashed mid-backup, a sibling replica took over, saw the cancel flag, and
  marked the op `cancelled` while leaving the warehouse blocked.
- Touching any of this → update `tests/configstore/reshard_postgres_test.go`,
  `provisioner/reshard_runner_test.go` (incl. the `TestReshardBackup*` cases),
  `provisioner/k8s_client_test.go` (the compaction/ducklake-pin cases),
  `admin/reshard_test.go`, `controlplane/reshard_pod_test.go` +
  `reshard_reconciler_test.go` + `reshard_runner_mode_test.go`, the
  migration asserts in `tests/configstore/migrations_postgres_test.go`, the
  netpol assert in `tests/manifests/manifests_test.go`, AND
  the `reshard_*` + `lifecycle_teardown_cnpg` assertions in
  `tests/mw-dev/e2e/harness.sh` (validation, cancel-during-drain,
  bogus-shard-rollback, ext→cnpg positive path incl. the pre-flip backup
  assertion + the runner-pod appear/reap assertions, and the
  deprovision-unaffected net). The cnpg→ext orphan-adopt
  charts side (composition managementPolicies + `retainCnpgOnFlip` XRD field)
  lives in the `charts` repo and is covered by
  `charts/charts/crossplane-config/tests/composition_retain_cnpg_test.sh`.
  cnpg→ext positive path is unit-only (harness lacks the RDS password);
  cnpg→cnpg positive path needs a second mw-dev shard (follow-up).

## Trino Cells (customer-facing SQL over DuckLake, `kubernetes` tag)

An org enabled for Trino gets a catalog, a login, authorization and resource
limits on a shared multi-tenant Trino cluster called a **cell**. The control
plane is the only writer of that state: `provisioner/trino_provisioner.go`
projects it every controller tick from `duckgres_managed_warehouse_trino` +
the org's warehouse row + its Duckling CR. Enablement is env-inferred —
`DUCKGRES_TRINO_COORDINATOR_URL` set means on (`controlplane/trino_inputs.go`);
unset means the branch never wires and nothing changes. **Trino is binary: if
you asked for it, a wiring failure is fatal at startup**, because silently
skipping leaves the cell's OPA sidecar serving a last-good bundle while
password/tenant/catalog changes never propagate.

- **The catalog is DuckLake and carries NO secret.** Per org:
  `connector.name=ducklake`, `ducklake.metadata.connection-url` (a JDBC URL
  whose `sslmode` follows the store kind — `disable` for in-cluster
  `cnpg-shard`, `require` for `external` and any future kind),
  `ducklake.metadata.connection-user`,
  **`ducklake.metadata.connection-password-file`**, `ducklake.data-path`,
  `fs.s3.enabled` (NOT `fs.native-s3.enabled` — that spelling was rejected at
  CREATE CATALOG and cost a release), `s3.region`, `s3.auth-type=IAM_ROLE`,
  `s3.iam-role` (the per-org duckling role: the tenant S3 boundary), and a
  small `s3.max-connections`. Every value comes from the `ManagedWarehouse`
  row's `metadata_store_*` / `s3_*` / `worker_identity_*` blocks. **Trino logs
  the full `CREATE CATALOG` statement, renders catalog properties in its web
  UI, and ships them to workers — a password in a property is readable by
  anyone who can see a query listing.** Hence the file indirection; never add
  `ducklake.metadata.connection-password`.
- **Tenant passwords live in one Secret, keyed by org id**
  (`TrinoTenantSecretName`, mounted at `TenantSecretMountPath`). The value is
  read from the org's Duckling CR status (`credentialSecretRef`) through the
  SAME resolver the worker activation path uses, so Trino and the DuckDB
  workers can never authenticate a tenant's metadata store with two different
  credentials. The projection is AUTHORITATIVE, not additive: a disabled org's
  key is removed on the next tick.
- **`Reconcile` order is load-bearing**: cluster secrets → auth files →
  resource groups → OPA bundle → tenant passwords → catalogs, and the
  `globalErr` gate SKIPS the catalog step if any projection failed. A
  coordinator that just lost its `password.db` keys 401s every catalog REST
  call, which would surface as a misleading "catalog reconcile failed" masking
  the real problem.
- **`ensureClusterSecrets` is write-once with a sentinel.** The K8s Secret is
  the source of truth for each cluster credential; the configstore holds only
  a one-bit `duckgres_trino_cluster_bootstrap` row per namespace. Missing
  Secret + not bootstrapped ⇒ generate; missing Secret + already bootstrapped
  ⇒ **fail loud**, because regenerating the env-projected
  internal-communication shared secret would split-brain a running cluster.
  The admin password/hash pair is the deliberate exception (no external
  consumer ⇒ regenerate-if-missing self-heals).
- **Catalog reconcile is `SHOW CATALOGS` first**: create only what's missing,
  drop only names matching `opa.ManagedCatalogPattern` that aren't wanted, so
  `system`, `jmx` and hand-made catalogs survive. An org whose password is
  momentarily unresolvable keeps its existing catalog (never dropped) but is
  NOT reported ready.
- **Catalog naming is a THREE-way contract**: `TrinoCatalogName` (`org_` +
  sanitized org id, no `_iceberg` suffix — warehouses are DuckLake),
  `opa.ManagedCatalogPattern`, and the regex literal inside `policy.rego`.
  `TestTrinoCatalogNameMatchesManagedNamePattern` +
  `TestPolicyRegoContainsManagedNamePattern` fail if any one moves alone.
- **The Rego policy is the tenant-isolation boundary.** The cell can assume
  every per-org duckling role, so nothing below OPA stops org A reading org
  B's catalog. Treat `provisioner/opa/policy.rego` as security review.
  **Query visibility is same-org only**: `ViewQueryOwnedBy`,
  `FilterViewQueryOwnedBy` and `KillQueryOwnedBy` (the plugin's exact
  operation strings — the filter one is NOT `FilterViewQuery`) are allowed
  only when the requester and the query OWNER share a bundle-known
  `org_<sanitized>` group, derived from the same `data.group_catalogs`
  ownership map every other decision uses. This matters because
  `ExecuteQuery` is unconditionally allowed, so without it org A reads org
  B's SQL text — table names, filter literals, customer identifiers — via
  `system.runtime.queries` and the web UI, and can kill B's queries. The
  owner arrives as `input.action.resource.user.{user,groups}` (an
  `@JsonUnwrapped` `TrinoIdentity`); `ImpersonateUser` uses a different,
  groups-less shape for the same field — do not conflate them. **The admin
  principal deliberately gets NO cross-tenant query visibility** (only its
  own queries): the reconcile loop issues only `SHOW`/`CREATE`/`DROP
  CATALOG` and never reads `system.runtime.queries`, so the grant would buy
  nothing and leak every tenant's SQL. **Every filter op needs a `batch`
  rule as well as an `allow` rule.** `opa.policy.batched-uri` is enabled
  (without it, filtering a catalog with >1024 tables overruns the OPA
  client's queue), and `filterViewQueryOwnedBy` goes through it like the
  catalog filters do. A missing `batch` rule fails closed, which for query
  visibility looked like "org-mates' queries are missing" rather than an
  error — Trino short-circuits self-ownership before OPA, so each tenant
  still saw its own query. `TestBatchedQueryFilteringMatchesNonBatched`
  pins the two shapes to the same decision.
- **The observer principal (`__duckgres_observer`) is the admin console's
  read-only identity**, and is deliberately NOT the provisioner's admin.
  Split authority: admin does CREATE/DROP CATALOG and sees only its own
  queries; the observer gets cluster-wide `ViewQueryOwnedBy` /
  `FilterViewQueryOwnedBy` / `KillQueryOwnedBy` plus `ReadSystemInformation`
  (one op gating EVERY MANAGEMENT_READ resource — `/v1/node` and
  `/v1/resourceGroupState`, which the console reads, plus `/v1/thread`,
  `/v1/announce` GET, `/v1/maxActiveSplits`, `/v1/integrations/gateway`; all
  GETs of operational state, enumerated in policy.rego) and
  holds NO TENANT catalog — no `data.group_catalogs` entry, and the observer
  group is excluded from `tenant_owns_catalog` and from the same-org query
  match, so even a mistaken bundle entry grants no tenant data access. Its
  ONE data grant is `system.runtime.nodes`: `AccessCatalog` on `system` plus
  `SelectFromColumns` pinned to that single table, because a cell on the
  default `discovery.type` serves no `/v1/node` and `/v1/announce` carries
  neither health nor version. `AccessCatalog` alone opens nothing — every
  read still passes `SelectFromColumns` — so `system.runtime.queries`
  (tenant SQL), `system.metadata.*` and `system.jdbc.*` (which would
  enumerate every tenant catalog, schema, table and column) all stay denied;
  `TestObserverSystemGrantIsPinnedToTheNodesTable` pins that. `is_observer` is
  the same username-AND-group conjunction as `is_admin`. Its credential is
  a second regenerate-if-missing pair on `trino-auth`
  (`ensureCredentialPair`), projected into password.db/group.db and read by
  the console through `TrinoProvisioner.ObserverCredential` on every call
  (never captured — a self-heal would otherwise 401 forever). Console reads
  redact SQL before it leaves the CP. See `controlplane/admin/README.md`.
- **Resource groups must keep a selector for BOTH operational principals**
  (`root.admin.__admin_provisioner` and `root.admin.__duckgres_observer`).
  The final selector matches user `(?<org>.*)`, which matches anything, so a
  principal without its own lane is admitted as a tenant into
  `root.tenants.free.<principal>` — and those leaves are `JmxExport: true`,
  so it would appear as a phantom tenant in the per-tenant resource-group
  metrics.
- **Resource groups must keep the `root.admin.__admin_provisioner` selector.**
  Trino rejects a query matching no resource group, so dropping it silently
  breaks every reconcile tick's own DDL.
- **Cells, minimally**: `trino_cell_id` on the row names the owning cell
  (`DUCKGRES_TRINO_CELL_ID`, default `configstore.DefaultTrinoCellID`). A
  provisioner claims unassigned orgs (`AssignTrinoCell`, conditional in SQL so
  no cell can steal another's tenant), reconciles its own, and ignores the
  rest — including writing NO state for them. There is exactly ONE cell today
  and deliberately no assignment policy, capacity model, rebalancer or cell
  drain; `resolveTrinoCell` becoming `resolveTrinoCells` is the whole shape of
  adding a second.
- **The bundle endpoint is mounted OUTSIDE `/api/v1`** (`/bundles/trino`) with
  its own bearer auth, and `buildTrinoWiring` bootstraps SYNCHRONOUSLY so the
  handler is constructed with the real token — there is no window where it
  serves under a placeholder.
- Touching any of this → update `provisioner/trino_provisioner_test.go`,
  `provisioner/trino_cluster_secrets_test.go`, `provisioner/opa/*_test.go`,
  `provisioning/api_test.go`, `tests/configstore/trino_postgres_test.go` +
  `trino_cluster_secrets_test.go`, the migration asserts in
  `tests/configstore/migrations_postgres_test.go`, and — for anything the
  admin console reads — `controlplane/admin/trino{,_client}_test.go` plus
  the `ui/src/lib/trino.test.ts` derivations. **There is no
  `tests/mw-dev/e2e/harness.sh` coverage yet**, and that is a stated gap, not
  an oversight: mw-dev runs no Trino cell, so there is nothing for the
  in-cluster Job to talk to. The harness assertion (enable an org, poll the
  row to `ready`, query the catalog as the org's `root`) lands with the chart
  that deploys the cell.

## TODO Reference

`TODO.md` is a lightweight backlog for ideas that do not yet have a better
home. It is not the PostgreSQL compatibility source of truth; use
`docs/postgres-compatibility.md` for compatibility status, test citations, and
known gaps.
