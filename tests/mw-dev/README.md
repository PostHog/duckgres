# mw-dev harness

Shared isolated-stack harness for dev-backed e2e and scenario tests. It owns
the real mw-dev infrastructure boundary that `tests/k8s/` (kind) cannot model:
real Cilium network policies, real Crossplane Duckling provisioning, and real
cnpg-shard metadata stores.

## Flow

1. **Build** the arm64-only all-in-one `duckgres` image (`_image-build.yml`),
   pushed to ECR. mw-dev runs ONE image for both the control plane
   (`--mode control-plane`) and the workers (`DUCKGRES_K8S_WORKER_IMAGE` =
   same image), so the harness deploys one image for both roles.
2. **Tailscale** join via OIDC/WIF → reach the private mw-dev EKS API.
3. **Deploy** an isolated `duckgres-ci-pr-<N>` namespace: throwaway config-store
   Postgres + a control-plane Deployment on the test image, spawning worker pods
   in the same namespace.
4. **Test** via an in-cluster payload Job hitting the CP ClusterIP service.
   The regular deployment workflow runs only `trino`, exercising Hoglake through
   Trino. Its numeric identity is `3<base>`, where `base` is the PR number or
   workflow run ID. Existing neutral, DuckDB, and reshard scripts and unit tests
   remain available but are not regular deployment matrix lanes. The aggregate
   `e2e` job keeps the existing branch-protection check name.
   `test-e2e` runs `e2e/harness.sh`; `test-scenario` runs the scenario named by
   `SCENARIO_NAME`, which defaults to `full-suite`. Scenario artifacts are
   copied to `SCENARIO_ARTIFACTS_DIR`, which defaults to
   `artifacts/scenario-dev/` at the repository root. Each invocation gets a
   unique `<scenario>-<token>/` directory; failed or incomplete copies are
   preserved as visible `<scenario>-<token>.partial/` directories with an
   `artifact_collection_error.txt` marker.
   Successful CNPG scenarios receive `DUCKGRES_SCENARIO_ORG_ID` as
   `ci-pr-<N>-cnpg`, where `<N>` is `PR_NUMBER` (the workflow run id for
   `scenario-dev`). This exact identity is shared by Crossplane's scoped
   credential RoleBinding and the harness cleanup path; it is not a separate
   user-configurable default.
5. **Teardown** always: deprovision the ci-pr ducklings (clean shared-infra
   footprint) then delete the namespace.

The harness stores its generated internal secret, rotation fallback, and user
secret key under `DUCKGRES_CI_SECRET_DIR` (default `/tmp`). Tests override this
with a private temporary directory so they cannot alter credentials belonging
to a concurrent local run. A missing configured directory is created with mode
`0700`, and newly generated secret files use mode `0600`.
`SCENARIO_JOB_CLEANUP_TIMEOUT_SECONDS` and
`SCENARIO_POD_START_TIMEOUT_SECONDS` both default to `180` seconds and can be
raised for unusually slow local clusters.

The isolated control plane's default worker request is configurable through
`DUCKGRES_K8S_WORKER_CPU_REQUEST` and
`DUCKGRES_K8S_WORKER_MEMORY_REQUEST`; `run.sh` defaults them to `750m` and
`1536Mi`, respectively, preserving the e2e harness's worker-packing behavior.
`scenario-dev.yml` explicitly overrides them to 3 CPU and 12Gi for the frozen
perf workload. Direct `run.sh` callers can make the same explicit override.

### Control-plane rollout retirement

The isolated Deployment uses `maxUnavailable: 0`, `maxSurge: 1`, a five-second
`preStop` hook, and `terminationGracePeriodSeconds: 35`. These are explicit
fixture settings: the hook keeps the API listening while Kubernetes withdraws
the terminating pod from Service routing, then leaves the previous 30-second
budget for session drain after SIGTERM. An idle control plane can otherwise
exit before client-node routing updates and reject a post-rollout connection.

Trino configuration transitions capture the old pod names before each patch,
wait for Deployment rollout completion, then wait for those exact pods to be
deleted. Each wait is bounded at 180 seconds. Deployment replica counters alone
can report success while a previous pod is still running its retirement hook
and serving the previous configuration. The cells assertion remains a single
authenticated request; transport failures and incorrect cell contents both
fail the test, with distinct diagnostics.

Run `just test-mw-fixtures` locally to exercise retirement ordering and error
handling without a cluster. For a stalled live transition, inspect the named
old pods and their events/logs for a stuck hook or session drain. Do not force
delete them or add retries to the content assertion. For connection failures,
capture old/new pod logs, EndpointSlice transitions, and client-node Service
routing during the rollout; post-failure pod readiness alone misses the race.
See [Control-plane rollout](../../docs/runbooks/control-plane-rollout.md).

### Scenario Trino readiness

Scenarios that opt an org into Trino in their `provision_warehouse` request can
use a `wait_trino_ready` step before a Trino workload:

```yaml
- id: wait_trino_ready
  type: wait_trino_ready
  depends_on: [wait_ready]
  with:
    org_id: ${env:DUCKGRES_SCENARIO_ORG_ID}
    timeout: 5m
    poll_interval: 5s
```

The step polls the authenticated org detail API (`GET
/api/v1/orgs/:id/trino`) until Trino is enabled, its latest reconcile state is
`ready`, its principal and catalog are resolved for the reported cell, and the
control plane can reach that cell. It fails immediately when the reconcile
state is `failed`, preserving `status_message` in the scenario error. The
scenario runner defaults are a 15-minute timeout and 10-second poll interval;
a step can override them with `timeout`, `poll_interval`, or `max_attempts`.

Tenant readiness also checks coordinator login-file projection and waits out
its configured password/group refresh periods before reporting ready. A newly
created per-user login is asynchronous and has no separate ready endpoint: its
startup check must wait for the intended authorized catalog read, not just
`SELECT 1`. On failure, inspect the org detail response first: `failed` is a
catalog/projection failure, while `ready` plus `available: false` points to the
cell or its observer credentials.

A `perf_queries` step targeting Trino consumes that stored readiness state. It
uses `status.principal` as the Trino username (not `root`), `status.catalog` as
the catalog, the provision response's root password, and schema `posthog` by
default. The coordinator URL comes from `cell.coordinator_url`. Isolated cells
must mount their per-run CA and set `trino_ca_cert_file`; the driver requires
verified HTTPS and never disables certificate verification.

Optional perf-step settings are:

- `worker_cpu` (default empty, leaving PGWire worker selection to the server)
- `worker_memory` (default empty, leaving PGWire worker selection to the server)
- `trino_schema` (default `posthog`)
- `trino_ca_cert_file` (default empty, using system roots)
- `trino_startup_timeout` (default `2m`)
- `trino_startup_poll_interval` (default `2s`)
- `athena_catalog` (default `AwsDataCatalog`)
- `athena_poll_interval` (default `500ms`)
- `athena_query_timeout` (default `30m`)

An Athena target additionally requires explicit `athena_region`,
`athena_workgroup`, `athena_database`, and `athena_output_location` settings.
The output location must be the same `s3://` prefix enforced by the workgroup.
Athena uses unique query execution IDs for result object names.

The startup window contains an authenticated `SELECT 1` retry and completes
before warmup or measured statements run. For the isolated mw-dev cell, use
`trino_ca_cert_file: /trino-ca/ca.crt`.

`posthog_frozen_perf` enables Trino and Athena and selects the isolated Trino suite. Its
scenario Job mounts the per-run CA from `duckgres-trino-tls` and passes that
path through `DUCKGRES_SCENARIO_TRINO_CA_CERT`; the perf adapter verifies the
coordinator certificate and retries its first authenticated query for the
bounded Secret-projection window. It also passes the deployed
`DUCKGRES_K8S_WORKER_CPU_REQUEST` and `DUCKGRES_K8S_WORKER_MEMORY_REQUEST` into
the perf step, which requests that exact shape through PGWire startup options
and therefore bypasses the exploratory worker tier. The paired catalog remains the single SQL
source: direct-Parquet `raw_view` members run only through PGWire,
production-shaped `ducklake_table` members run through PGWire and Trino, and
`athena_external` members run through Athena against Glue tables over the same
immutable Parquet objects. Athena is on-demand, result reuse is disabled, and
the harness records service-side timing and scanned bytes in
`query_service_metrics.csv`.
To reproduce the scheduled run, deploy and test with `E2E_SUITE=trino` and the
same `TRINO_POD_IDENTITY_ROLE` required by the isolated Trino lane. Teardown and
the scheduled cleanup sweep remove both namespace-local workloads and their
Pod Identity associations.

## Isolated Trino lane

The Trino lane never uses the shared mw-dev Trino namespace or coordinator.
It deploys one coordinator, three workers, and an OPA sidecar in the lane's
`duckgres-ci-pr-<N>` namespace. The control plane projects fixed-name auth,
tenant-secret, OPA-token, and resource-group objects into that same namespace;
its cell id and PostgreSQL catalog store are also PR-local. This isolation is
load-bearing: pointing a PR control plane at the shared cell could overwrite
authoritative projections or drop catalogs absent from the PR's config store.

The lane defaults `TRINO_IMAGE` to the pinned PostHog fork promoted for these
tests. That fork contains atomic Hoglake writes and the PostgreSQL dynamic catalog
store; upstream `trinodb/trino` is not compatible. Update the default in
`run.sh` and `e2e-mw-dev.yml` together when promoting the regular E2E
Trino build. The frozen benchmark (`posthog_frozen_perf`) is not pinned: it
always tests the newest PostHog/trino master build, which
`scripts/resolve_trino_master_image.sh` resolves to a digest-pinned reference
from the fork's source-ordered `r<position>-<sha>` GHCR tags. `scenario-dev.yml`
resolves it once per run, records it in the job summary, and accepts a
`trino_image` dispatch input to benchmark a specific build instead.
On statement failure, the harness reports the query ID, error codes and a bounded
exception-class chain alongside the existing top-level message. It excludes nested
messages, stack traces and response URLs because these can contain credentials or
internal infrastructure details. A failure still stops the test without retrying
the statement; these diagnostics do not classify a storage failure as transient.
The suite asserts per-user logins on every run: an org user authenticates as
`<database_name>.<username>` with its pgwire password, reads only its own org's
catalog, is attributed to its org in the admin query list, and stops
authenticating once disabled. The host-qualified login, where the same user
types only `<username>` against `<database_name>.<domain>`, is asserted only
when `TRINO_HOST_QUALIFIED_DOMAIN` is set, and is logged as skipped otherwise.
It needs a fork build with
`http-server.authentication.password.host-qualified-user.domains`, which the
pinned image predates. When promoting such a build, set that property to the
same domain on the lane's coordinator and pass the domain to the harness Job.
The harness sends the tenant host as the `Host` header against the lane's own
TLS name, so no DNS or certificate for the tenant host is needed.
The provisioner cancels a statement it abandons at its reconcile deadline
(`DELETE` on the pending `nextUri`), so abandoned queries cannot fill the
`root.admin.__admin_provisioner` resource group. The lane does not assert this:
it needs a coordinator that holds `SHOW CATALOGS` past the 30-second reconcile
budget, which the Job cannot produce deterministically. The contract is pinned
by `TestTrinoStatementDrainCancelsAbandonedStatement` against a fake
coordinator whose statement never leaves the queue.
Each Trino worker has requests and limits of 1 CPU and 4Gi. Together they
match the frozen perf Duckgres worker's aggregate 3 CPU and 12Gi execution
budget while exercising Trino's distributed execution path. Trino permits 2GB
of query memory per worker and 6GB cluster-wide. The coordinator does not
execute query tasks (`node-scheduler.include-coordinator=false`) and is
additional Trino control-plane overhead rather than part of the matched
execution budget.

The isolated lane explicitly sets `task.max-worker-threads=8` and
`task.min-drivers=16` per worker, four times the pinned engine's expected
one-CPU defaults of 2 and 4. The latter is a leaf-driver target, not a hard
limit or a count of simultaneous storage requests. These settings apply to
both the isolated Trino E2E suite and `posthog_frozen_perf`; they do not change
shared or production deployments. Worker resources, 3G heaps, query-memory
limits, coordinator configuration, and aggregation `task.concurrency` remain
unchanged. This tests whether more overlapping scan work improves throughput
when the existing CPU budget is underused; it is not a proven speedup.

To validate a branch against the existing frozen benchmark:

```bash
gh workflow run scenario-dev.yml --ref <branch> -f scenario=posthog_frozen_perf
```

Compare per-query Trino medians against the prior baseline, checking for query
errors, worker restarts, CPU throttling, and memory pressure. Higher concurrency
can increase heap usage and context switching. If it regresses, remove the two
worker properties in `manifests.trino.tmpl.yaml`
to restore CPU-derived defaults, then rerun in a fresh isolated deployment.
Do not patch a running benchmark; let workflow teardown clean up its stack.

`TRINO_TLS_PASSWORD` defaults to `duckgres-e2e-keystore`; it protects only the
random, two-day, per-run PKCS12 file. `run.sh` generates a fresh CA and leaf
certificate under `DUCKGRES_CI_SECRET_DIR`, mounts the CA into the PR control
plane through `SSL_CERT_FILE`, and uses verified HTTPS for provisioning and
tenant password authentication.

Repository setup requires the `MW_DEV_TRINO_POD_IDENTITY_ROLE` Actions secret.
The `github-duckgres-e2e` role must be able to pass that role and create/list/
delete EKS Pod Identity associations. Deploy creates a second association for
the PR-local `trino` ServiceAccount before scaling Trino above zero replicas;
teardown and the six-hour cleanup sweep delete all associations in the PR
namespace.

Failure recovery:

1. Inspect `run.sh diagnostics` output for the control plane, coordinator,
   worker, and OPA logs. Auth/catalog failures usually mean a missing projected
   Secret/ConfigMap; S3 failures usually mean Trino Pod Identity was not
   injected at pod admission.
2. Re-run the workflow. Deploy first deletes the prior namespace, Ducklings,
   CNPG roles/databases, and namespace Pod Identity associations.
3. If a canceled run survives, invoke the scheduled `e2e-cleanup` workflow or
   run `tests/mw-dev/run.sh e2e-cleanup` with the documented cluster access.
   Never redirect a PR control plane to the shared Trino cell as a workaround.

A scheduled (`cron`) **e2e-cleanup** job (`run.sh e2e-cleanup`) runs every 6h and
reaps any `duckgres-ci-pr-*` namespace older than 6h — a backstop for runs that
died hard before their `always()` teardown could fire. (Named e2e-cleanup, not
"janitor", to avoid colliding with duckgres's own control-plane janitor.)

## What the e2e payload asserts (`e2e/harness.sh`)

This suite is the **successor to the retired kind suite** (`tests/k8s/`): its
portable black-box behavior is re-asserted here against real mw-dev using
CNPG-backed metadata stores. The
in-cluster Job runs as the `duckgres` SA and uses `kubectl` (in-cluster config
from its mounted SA token) for the pod-level checks the Go suite made via
client-go:

- **wire/query** — `SELECT 1` round-trips; 5 concurrent connections stay
  distinct (ported from `TestK8sMultipleConcurrentConnections`); a malformed
  post-TLS startup-message length (negative / ~2GiB / truncated, injected via
  `openssl s_client -starttls postgres`) gets a clean connection close — the CP
  pod must not restart and must keep serving (regression for #715).
- **pipeline error recovery** (#718) — a pipelined extended-query batch (psql
  18 `\startpipeline`) whose first statement errors must have its queued
  statements **discarded until Sync** (the queued INSERT must not execute);
  the statement after `\syncpipeline` must execute normally. This is why the
  harness Job image is `postgres:18-alpine` (pipeline meta-commands are
  psql 18+). The same wire lane also asserts that a pgwire CancelRequest leaves
  the same session immediately reusable.
- **server-side cursors** — DECLARE → `FETCH n` → `MOVE n` (advances without
  returning rows) → `FETCH ALL` (remaining rows only) → CLOSE, with exact value
  assertions on a live worker; then ROLLBACK while a second cursor is still
  partially read must return promptly and leave the session usable (an open
  cursor rowset pins the worker session's single DuckDB connection — the
  pre-fix behavior deadlocked the session at transaction end).
- **cold-burst absorption** — there is no warm pool, so a burst of cold sessions
  spawns workers on demand; if it outruns the org/global cap the surplus gets a
  graceful client-visible hint (`no Duckgres worker … retry in about 45 seconds`
  / `timed out waiting for an available worker`) rather than a hang/500/drop, and
  the pool must then serve a retrying connection. The harness logs whether
  backpressure was observed **and** handles it (queries retry through it).
- **activation** — DuckLake catalogs attach, read/write, and run
  `EXPLAIN`/`EXPLAIN ANALYZE` on CNPG-backed tenants.
- **native metadata Postgres proxy** — the Job reaches the actual proxy branch
  over the control-plane ClusterIP while libpq sends a dedicated, non-resolving
  TLS SNI name (`<org>.md.ci.duckgres.local`). It proves a ready CNPG org is
  denied by default, enables only that org through the admin warehouse API,
  initializes the DuckLake catalog once through the normal worker path, then
  queries `public.ducklake_metadata` through the proxy and hidden per-tenant
  CNPG credentials (the proxy query itself never uses a worker). It also checks
  that both a normal Duckgres database and an explicitly empty startup database
  are rejected (only exact
  `dbname=metadata` is valid). A second ready org on the same shard remains
  denied, then disabling the opted-in org blocks new connections again.
- **binary COPY** — a `psql`-generated PostgreSQL binary fixture traverses the
  deployed pgwire → control-plane → Flight → worker → `postgres_scanner` →
  DuckLake path. It checks every natively routed scalar type, reordered/subset
  columns, NULLs/defaults, DECIMAL scale normalization, fresh-session
  persistence, the unsupported-type legacy fallback, and transaction rollback.
- **worker sizing** (TTL-pool model, `docs/design/worker-ttl-pool.md`) — a
  client-sized connection (`duckgres.worker_cpu`/`worker_memory`/`worker_ttl`
  startup options, sent via `PGOPTIONS`; CP runs `allowClientWorkerProfile=true`
  with clamps) spawns a worker pod whose `duckdb-worker` container carries the
  requested CPU+memory on **both** requests and limits — proving the shape flows
  control-plane → k8s pod spec, not BestEffort. A same-shape reconnect **reuses**
  that hot-idle worker (no respawn — the count of that-shape pods stays 1).
  Asserted on cnpg for the ducklake catalog. There is no warm pool, so the only
  workers are the ones a request sizes + spawns on demand.
  Clamp enforcement itself is unit-covered (`controlplane/worker_profile_test.go`).
- **org default worker profile** — an operator-set per-org default worker shape
  + hot-idle TTL (config-store columns `default_worker_cpu`/`memory`/`ttl`, set
  via the admin API `PUT /orgs/:id`) must size a **plain** connection — one that
  sends no `duckgres.worker_*` startup options at all (the external-customer
  case). The harness sets `2/8Gi/10m` on a dedicated CNPG-backed org, asserts
  the admin API round-trips the fields and 400s garbage values, then connects
  without `PGOPTIONS` and asserts the worker pod carries the org default on requests
  **and** limits; finally it clears the default (explicit empty strings) and
  asserts a fresh plain connection no longer produces org-default-shaped pods.
  Per-field client-GUC-over-org-default precedence and the
  AllowClientWorkerProfile-independence of org defaults are unit-covered
  (`controlplane/worker_profile_test.go`).
- **extension forks** — the bundled `ducklake`/`httpfs` extensions are the
  PostHog forks, not upstream (ported from the `*IsBundledFork` tests).
- **worker pods** — labels (`app`, `duckgres/control-plane`,
  `duckgres/worker-id`), securityContext (`runAsNonRoot`, uid 1000, no
  priv-esc), Downward-API `POD_NAME`/`NODE_NAME` env, and **no** ambient
  SA-token mount.
- **resilience** — worker-pod kill → crash recovery; DuckLake durability across
  a worker restart; concurrent writers (fork conflict-retry, the test that was
  flaking on main); graceful drain (a worker SIGTERM'd mid-query drains — the
  in-flight query completes correctly while the pod is Terminating — then retires
  cleanly; regression net for the worker drain protocol, #690); one session per
  worker (two concurrent queries for one org land on two distinct worker pods,
  never sharing a pod's DuckDB — workers run DUCKGRES_DUCKDB_MAX_SESSIONS=1 so a
  query owns the whole pod's resources). The org-at-max-workers clear error and
  the under-cap hold-for-spawn / FIFO anti-snatch paths are covered by unit tests
  (controlplane/org_reserved_pool_test.go, org_acquire_gate_test.go) — exercising
  them in-Job would need a dedicated max_workers=1 org and deterministic cold-spawn
  timing the shared cluster can't guarantee.
- **persistent user secrets** — `CREATE PERSISTENT SECRET` survives across
  sessions (the CP stores the statement encrypted
  in the config store and replays it at session creation — worker pods are
  ephemeral); reserved system names (`ducklake_s3`, …) and unnamed persistent
  secrets are rejected; `DROP PERSISTENT SECRET` removes it durably; and a
  second user of the same org never sees the first user's secret, even on a
  reused hot-idle worker (cnpg lane).
- **isolation** — two CNPG-backed tenants see distinct catalogs; a
  cross-tenant read is denied.
- **lifecycle** — deprovision → `warehouse=deleted` → the Crossplane Duckling
  CR **fully** deletes (`kubectl wait --for=delete`, asserting the finalizer
  cascade that drops the cnpg role+db completed). Right after the deprovision
  202 the warehouse status is watchable — state `deleting` with
  `status_message="Deprovisioning..."` — so a UI can poll `warehouse/status`
  until `deleted`. Then `DELETE /orgs/:id`
  cascades the terminal `deleted` warehouse row + the org row away and the org's
  `database_name` becomes available again (`database-name/check` flips
  `false`→`true`) — the regression net for the name being squatted forever after
  a completed deprovision. Same-id **re-provision** is
  *not* done in-Job: a clean slate needs DROPping a possibly-stranded cnpg role,
  which only `run.sh` (on the runner, with cnpg-shards exec) can do — so the
  stranded-cnpg-role regression (#649/#650/#11518/#11522) is covered **across
  runs** (`run.sh deploy` drops the role for a clean slate; `run.sh teardown`
  waits the CR `--for=delete`), not within one Job.

**Static-manifest asserts** (`k8s/rbac.yaml`, `k8s/networkpolicy.yaml`) that the
kind suite carried as unit tests now live in `tests/manifests/` and run in the
normal `go test ./...` lane.

- **query log round trip** — a marked query must appear in
  `ducklake.system.query_log` with a populated `query_id`, the right
  `type`/`query_kind`/`user_name`. This is the only coverage of the full
  query-log path (CP builds the entry → Flight `DoAction` to the worker → the
  worker's batched sink INSERTs into tenant metadata Postgres → the DuckLake
  view reads it back), and every column added to the registry in
  `server/querylog_schema.go` rides it. Asserting `query_id` specifically also
  catches a **stale view**: a tenant's view is created once with
  `CREATE VIEW IF NOT EXISTS`, so a view that was not replaced when the column
  set drifted would still be missing the column here. The marker travels in a
  SQL comment, which only survives if the ORIGINAL inbound text is logged (the
  transpiler deparses from the AST and drops comments).

- **query log access metadata** — a `SELECT` and an `INSERT` over the same table
  must log different `access_kinds` and land in `read_relations` /
  `write_relations` respectively; a DuckDB-native statement the PostgreSQL
  parser rejects must log `metadata_complete=false` rather than an empty
  relation list; and a `CALL` must log `access_kinds=unknown` with
  `metadata_complete=true` — parseable but opaque, which is a different fact
  from unparseable even though both deny. These are the signals a future authorization policy will be
  evaluated against, so "referenced nothing" and "we could not tell" have to
  stay distinguishable on real traffic, not just in unit fixtures.

### Deliberately not covered here

- **Hot-idle claim refused on a disrupted pod** (`k8s_pool_doomed_claim.go`) —
  the check fires when a claimed hot-idle worker's pod already carries a
  `deletionTimestamp` or its node carries Karpenter's `karpenter.sh/disrupted`
  taint at the instant of adoption. Staging that in-Job would need the harness
  to delete a specific hot-idle worker pod (or taint its node) in the
  sub-second window between the CP's durable claim and its pod read, and the
  e2e Job holds no pod-delete/node-taint RBAC (deliberately — the harness must
  never be able to disrupt the shared mw-dev fleet). The decision matrix and
  the retire-and-fall-through path are covered by
  `controlplane/k8s_pool_doomed_claim_test.go` against a fake clientset and a
  capturing runtime store.

- **Portal suspension (extended-query Execute row limit)** — the harness
  drives all SQL through psql, and libpq never sends a nonzero Execute row
  limit, so a paging client (JDBC `setFetchSize`, Hex) cannot be simulated
  in-Job. The behavior is deterministically covered against a real server by
  `tests/integration/portal_suspension_test.go::TestPortalSuspensionPaging`
  (raw pgproto3 frontend paging a 1000-row result set over TLS) and by the
  server unit tests in `server/conn_portal_suspension_test.go`; the path is
  identical on the remote backend (suspension lives entirely in the CP's
  pgwire layer above the executor).

- **Native metadata proxy denial for an external metadata-store org** — the
  live suite intentionally provisions only CNPG-backed orgs and has no RDS
  credential with which to create an external-store tenant. The backend-kind
  gate is deterministic and covered by
  `TestOrgMetadataProxyEnabledFailsClosed`; the live proxy check still proves
  the independent access boundary by leaving a second ready tenant on the same
  CNPG shard opted out.

- **The query log on an EXTERNAL (RDS) metadata store** — the suite provisions
  only cnpg-shard orgs, so `query_log_round_trip` runs on cnpg alone. The
  query-log storage path does not branch on backend: the sink resolves one DSN
  from the org's DuckLake metadata store and issues the same DDL and INSERT
  either way (`server/querylog_postgres.go`), so the cnpg run exercises the same
  code. What an ext org would add is DSN/sslmode resolution, which
  `reshard_targets` and the activation assertions already cover.

- **Mid-statement STS credential recovery (patched PostHog httpfs)** —
  the worker image bundles the PostHog httpfs fork patch that re-resolves the
  latest committed `ducklake_s3` secret and retries on ExpiredToken read/write
  auth failures, letting a statement outlive the STS token it started with.
  Proving real in-statement
  expiry in-Job needs a statement longer than the shortest AssumeRole token
  (900s AWS floor), which blows the Job time budget. The behavior is
  deterministically covered by the MinIO rotation tests in
  `tests/integration/credential_rotation_pin_test.go` (stock httpfs dies /
  patched httpfs survives rotation + revocation mid-scan); the harness's
  `assert_fork_extensions` pins that workers actually run the patched build
  (`EXPECT_HTTPFS_SHA`), and every DuckLake assertion here exercises the patched
  request paths against real S3.
- **Version-mismatch worker reaper** — needs a mid-run image bump, so it stays
  covered by `controlplane/` unit tests rather than in-Job.
- **External-store resharding** — the live harness provisions CNPG metadata
  stores only. External-source/target cutovers, rollback/recovery, ESO errors,
  and stale or concurrently-created index replay are covered by
  `controlplane/admin` and `provisioner` unit tests.
- **Successful cnpg→cnpg cutover and the cutover milestone columns** — the
  reshard lane runs `reshard_targets`, `reshard_validation`,
  `reshard_cancel_during_drain` and `reshard_forced_cutover_rollback`, none of
  which carries a cnpg→cnpg reshard through to a successful cutover, so the
  `target_rendered_at` / `target_login_ready_at` stamps are never reached
  in-Job. Asserting them would mean adding a full successful reshard (copy,
  drain, flip, source drop) of a live warehouse to the Job, which costs minutes
  and leaves the tenant on the new shard for the rest of the run. The stamp
  ordering (the login milestone comes from the successful tenant probe, not from
  a duckling status read) is covered by
  `provisioner/reshard_runner_test.go::TestReshardCnpgCutoverLoginMilestoneWaitsForTheProbe`.
- **Reshard runner-pod crash respawn** — the live reshard operations run in
  dedicated `duckgres-reshard-op-<id>` pods, but deliberately do NOT kill a
  runner pod mid-operation to exercise the leader reconciler's respawn/takeover: a
  mid-copy kill races the drain/flip waits and would add minutes of
  deterministic-flake risk per run. Respawn, the retry bound + force-fail, and
  the stale-heartbeat takeover claim (incl. `reconstructProgress`) are covered
  by `controlplane/reshard_reconciler_test.go` and
  `provisioner/reshard_runner_test.go`.
- **Physical object-store-prefix isolation** — the Go suite listed the MinIO
  prefix to prove writes land only in a tenant's own path. Against real mw-dev
  S3 the Job holds no list creds, so isolation is asserted **logically** (the
  cross-tenant read is denied) rather than by enumerating S3 objects.
- **Cilium egress allow/deny probing** — asserting a worker reaches the cnpg
  pooler but not a denied destination needs a stable exec-into-worker probe;
  deferred (high flake risk). The policies themselves are asserted statically
  in `tests/manifests/`.
- **Native crash handler (`internal/crashhandler`)** — a SIGSEGV on a
  DuckDB-created thread must kill the worker with a native backtrace on stderr
  instead of wedging the process (the Go runtime's badsignal path can leave a
  crashed C thread's locks held forever). Asserting this in-Job would require
  deterministically segfaulting a real worker pod — there is no SQL statement
  that does that on purpose. Covered by `internal/crashhandler` package tests,
  which re-exec the test binary and crash it on a C-created thread, in a cgo
  call, and in pure Go code, asserting death-by-signal + the stderr marker
  (and that Go panics stay ordinary panics).
- **Dynamic headroom placeholders (`controlplane/headroom.go`)** — the e2e CP
  deliberately sets no `DUCKGRES_K8S_PLACEHOLDER_PRIORITY_CLASS`, so headroom
  stays disabled in-Job: real placeholder pods would consume Karpenter capacity
  in the shared mw-dev cluster and outlive the per-PR CP that owns them
  (nothing deletes them after teardown — the reconcile that converges them to
  zero dies with the CP). Slot-count/size/cap/scale-down behavior is covered by
  the fake-clientset unit tests in `controlplane/headroom_test.go`; the
  spawn-log SQL by `tests/configstore/spawn_log_postgres_test.go` (real
  postgres); spawn recording itself runs in-Job on every worker spawn
  (best-effort, so it cannot fail activation).
- **Oversized Bind-parameter rejection (#717)** — rejecting a Bind message whose
  declared parameter length exceeds the remaining message body requires crafting
  a malformed wire-protocol packet on a raw socket (through TLS + auth); libpq
  clients like psql always emit well-formed lengths, and the Job image carries no
  raw-packet tooling. Covered by the unit regression test in
  `server/conn_bind_test.go` instead.
- **Concurrent worker operations on one session** — the worker rejects
  overlapping same-session Flight operations with `FailedPrecondition`, but the
  harness enters through pgwire, where one client connection maps to one worker
  session and operations are serialized by the control plane. Driving this
  specific defense-in-depth path end-to-end would need a bespoke concurrent
  direct worker Flight client, so the rejection contract, required
  GetFlightInfo-to-DoGet handoffs, and abandoned-continuation cleanup are covered
  by `duckdbservice` unit tests. The harness still asserts the cluster invariant
  this protects: one active session owns one worker.
- **Worker DoGet close acknowledgement internals** — the harness covers the
  black-box pgwire behavior (CancelRequest then immediate same-session reuse),
  but not the exact internal wait point. Pausing the worker exactly after it
  observes gRPC cancellation but before it releases the session operation token
  would need a bespoke worker/Flight fault-injection client. Covered by
  `server/flightclient` and `duckdbservice` unit tests instead.
- **Malformed Bind message validation (#720)** — negative count/length fields
  in a Bind message must return a clean `08P01` instead of panicking. Every
  real client (psql, lib/pq, ...) only emits well-formed Bind messages, so
  triggering this needs a raw pgwire client that completes TLS + SCRAM auth
  and then sends crafted bytes — tooling the alpine Job image (psql/curl/jq)
  doesn't have. Covered by `server/conn_bind_test.go` unit tests
  feeding malformed payloads directly to `handleBind`.
- **STS credential freshness floor** (`stsCacheSafetyMargin` /
  `credentialRefreshLookahead`, `controlplane/sts_broker.go`) — the guarantee
  is temporal: every statement starts with ≥35min of STS token validity (at
  the default 1h session), and the refresh scheduler re-pushes worker secrets
  30min ahead of expiry. Asserting it in-Job would mean holding a query open
  across a real token-expiry boundary: even time-compressed via the env-only
  `DUCKGRES_STS_SESSION_DURATION` knob, AWS's 900s AssumeRole floor puts the
  shortest meaningful wait far past the Job budget, and the shared cluster
  gives no deterministic control over when the scheduler tick lands. Covered
  by `controlplane/sts_broker_test.go` +
  `shared_worker_activator_credentials_test.go` (cache margin, lookahead
  invariant, expiry surfacing on both activation paths) and the integration
  pin `tests/integration/credential_rotation_pin_test.go`, which proves
  against a real MinIO that on STOCK httpfs an in-flight scan does NOT
  survive credential rotation (DuckDB resolves secrets through the
  statement's MVCC snapshot, and scan-workload file opens skip the HEAD that
  could trigger httpfs' refresh-on-403). With the bundled
  `v1.5.5-cred-refresh-write-retry` fork build the floor is defense-in-depth rather than
  the only protection — see the mid-statement recovery bullet above.
- **PostHog product-analytics events** (`internal/analytics`,
  `warehouse_provision_begin`/`_success`/`_failed`,
  `warehouse_deprovision_begin`/`_success`/`_failed`, `warehouse_password_reset`,
  `query_initiated`/`query_failed`) — the harness already drives every path that
  fires these (it provisions, deprovisions, resets, and runs queries), but the
  events are sent asynchronously to PostHog's
  external capture API and the in-cluster Job holds no PostHog query-API creds
  to read them back, so ingestion cannot be asserted in-Job. The emission logic
  (event name, org group-analytics attribution, properties, failure-category
  classification, and "no event on handler failure") is covered by
  `internal/analytics/analytics_test.go`,
  `controlplane/provisioning/analytics_events_test.go` (the `_begin` admin-API
  events), `controlplane/provisioner/controller_analytics_test.go` (the
  terminal `_success`/`_failed` events the provisioner controller emits on the
  Ready/Failed/Deleted transitions), and `server/conn_analytics_test.go`.
  The same applies to WHICH exporter a given key enables: `POSTHOG_API_KEY`
  turns on analytics *and* the OTLP log export, while
  `POSTHOG_ANALYTICS_API_KEY` turns on analytics alone (so query text is not
  exported). Confirming that split end-to-end means reading both PostHog Logs
  and the event stream back, which the Job cannot do for the reason above; the
  key resolution is covered by `TestAnalyticsAPIKeyPrefersDedicatedKey` in
  `internal/cliboot/analytics_test.go`.
- **Orphaned-draining worker reaping is unit-only, deliberately.** The bug it
  fixes needs a worker spawned by CP replica A, claimed (hot-idle adopted) by
  replica B, then SIGTERM'd and exited while B owns it — B's pod informer is
  label-scoped to A's spawns, so B never sees the exit and used to probe the
  dead pod every tick forever. The e2e Job runs against a single-replica
  control plane, has no way to force which replica adopts a parked worker,
  and has no config-store access to stage a stale `draining` row for the
  janitor sweep. Both halves are pinned by unit tests that drive the real
  `HealthCheckLoop` against a fake clientset with the pod absent/present
  (`controlplane/k8s_pool_draining_orphan_test.go`) and the real janitor
  `runOnce` against fixture rows (`controlplane/janitor_draining_orphan_test.go`).
  The drain path the harness DOES cover (`hot_idle_retired`, the pod-delete
  → retire chain on the spawning CP) is unchanged by the fix.
- **`--statement-timeout` (`DUCKGRES_STATEMENT_TIMEOUT`) is unit-only, deliberately.**
  The knob is server-global and defaults to `0` (unbounded), so asserting it
  in-Job needs one of two bad options: set a short global timeout on the e2e
  control plane, which would start killing the harness's own legitimately slow
  assertions (reshard copies, DuckLake round-trips, concurrent-writer checks);
  or leave it generous and burn that many minutes on a deliberately slow query
  just to watch it expire. Neither buys more confidence than the unit tests,
  which drive the real protocol handlers with a wedging fake executor and
  cover the behaviour that can actually regress: the deadline reaches the
  engine on every execution path (simple, batched, extended Execute, the
  writable-CTE rewrites on both protocols, the extended Describe probes, COPY
  in both directions, cursors), it rides the STATEMENT context and never the
  connection context, `0` leaves statements unbounded, an expired deadline
  classifies as `57014` with PostgreSQL's exact `canceling statement due to
  statement timeout` wording (drivers string-match it), a user cancel keeps
  the `due to user request` wording, an internal deadline with the knob off
  never becomes 57014, a suspended portal keeps its context alive across
  Execute legs, and a cursor is bounded over its whole lifetime. See
  `server/statement_timeout_test.go`. If the knob ever becomes
  per-connection (a client-honored `statement_timeout` GUC is the named
  follow-up), it becomes cheaply assertable in-Job and should get a
  harness assertion then.

- **PostHog Logs (OTLP)** — `assert_worker_pod` checks plumbing only: the
  worker must not carry a plaintext `POSTHOG_API_KEY` `value:`, and when the
  CP container's *named* env has a `secretKeyRef` the worker must copy the
  same secret name+key. Until charts set that named env the copy branch is
  skipped; the plaintext assert still runs. **Ingest cannot be asserted
  in-Job** for the same reason as analytics: the Job holds no PostHog
  query-API creds and cannot read the destination project. Do not require a
  successful export. The first mw-dev `service.name=duckgres-worker` record
  in the **analytics** project is the human egress proof; in-repo
  NetworkPolicy 443 is not production Cilium.

## Isolation model

### Concurrent Trino credential bootstrap

Before provisioning tenants, the Trino lane stops its PR-local control plane
and waits for every old pod to disappear. It removes only the admin and observer
password/hash keys with a Secret resource-version precondition, then starts
three replicas together. The test requires three ready pods with zero restarts,
successful admin and observer authentication against real Trino, and unchanged
credential pairs after returning to one replica and provisioning the first
tenant. Other Secret keys, the internal shared secret, and the OPA token remain
unchanged. Credentials and their fingerprints stay in memory and are not logged.

This exercises real concurrent startup and Kubernetes writes; it does not force
the exact historical race interleaving. Provisioner unit tests provide that
deterministic regression. The fixture grants patch only on its named control-plane
Deployment and `trino-auth` Secret. Normal workflow teardown removes the fixture
on failure, including any extra replicas. No shared environment is modified.

### Trino multicell lane

The full Trino E2E lane adds a second disposable namespace,
`duckgres-ci-pr-0<N>`, alongside the canonical `duckgres-ci-pr-<N>` identity.
Canonical lane identities must be positive numbers without a leading zero.
The secondary namespace carries the original lane label and a `trino-cell`
component label. It owns no control plane, config-store, or Duckling resources.
Performance scenarios keep the existing single-cell fixture and resource budget.

The additional `cell-test` starts with one blue coordinator and one worker;
green remains at zero replicas. Both colors use distinct internal credentials,
node environments, discovery Services, and catalog-store keys. Their shared
cell-local auth, tenant-password, OPA, and resource-group projections are
managed by the primary control plane through a scoped RoleBinding.

`e2e/trino-multicell.sh` provisions a new warehouse without Trino, selects its
initial cell, enables Trino, and verifies real Hoglake writes and reads. It
asserts that legacy remains queryable, credentials and OPA tokens cannot cross
cells, and an existing legacy assignment cannot be changed. It then starts
green, updates the startup-loaded registry, restarts the control plane, and
queries the same data through green's independently hydrated catalog. Direct
coordinator URLs are fixture-only; this does not test Gateway routing or a
maintenance move of an existing warehouse.

After these checks, the lane restarts its control plane in registry-only mode,
verifies both registered backends still query the warehouse, rejects legacy
ownership and implicit cell selection, and verifies the legacy bundle endpoint
is absent. It restores the original configuration on success. Workflow teardown
removes the disposable namespaces on failure, including during this phase.
The registry-only phase also checks enablement admission for existing registered
and legacy-owned warehouses. Initial assignment without a legacy default is
covered by startup, admin, provisioning, and projection package tests; the
real initial-placement flow runs earlier while both cells are configured.

The fixture preserves the existing CI network-policy posture. It does not
create network policies or add cluster-wide RBAC grants. Isolation assertions
cover application authentication and OPA authorization, not network isolation.
If an existing cluster policy blocks the fixture, investigate that policy;
do not weaken it to make the test pass.

All lanes generate a random config-store password in `DUCKGRES_CI_SECRET_DIR`
and reuse it for that run. PostgreSQL, the control plane, and benchmark Jobs
read Kubernetes Secret references; Trino receives the same password through
its catalog-store Secret. Credentials never appear as literal pod environment
values. GitHub Actions masks the generated password before deployment.

Run `just test-mw-fixtures` for local rendering and cleanup guard tests. The
real acceptance gate is the PR's Trino E2E workflow. A rendered fixture is not
proof that CI has the required cross-namespace RBAC and Pod Identity grants.
On failure, keep the PR in draft and inspect its isolated job/deployment logs.
Do not redirect the suite to an existing shared cell.

Normal reset/teardown and stale cleanup delete the secondary namespace only
after its name, original lane label, component label, and UID match. Namespace
deletion uses a UID precondition. Secondary cleanup removes its Pod Identity
association but never independently deletes the primary lane's warehouses.

Dedicated CP + throwaway config-store **per e2e lane**, provisioning three **real**
CNPG-backed ducklings (org IDs `ci-pr-<N>-cnpg` plus the ducklake-only
resilience-lane orgs `ci-pr-<N>-res1`/`-res2`) through the **shared**
Crossplane / cnpg-shards infra. The config-store
uses a namespace-scoped PVC so a pod recreation during the harness does not
erase provisioned org rows. Everything PR-specific lives in the namespace and
is deleted; the shared-infra footprint is removed by deprovisioning the
ducklings first.

Each deploy first reaps any existing resources for the same lane identity (namespace,
Duckling CRs, pod identity association, cross-namespace bindings, and cnpg role).
It fails before applying manifests if the same-PR Duckling CRs do not fully
delete, so a rerun never reuses PR-scoped bucket/org names while Crossplane
finalizers are still running.

## One-time repo configuration

| kind | name | purpose |
|---|---|---|
| var | `TS_WIF_CLIENT_ID_MW_DEV` | Tailscale OAuth WIF client (mirror of hogland's) |
| var | `TS_WIF_AUDIENCE_MW_DEV` | Tailscale WIF audience |
| secret | `MW_DEV_ACCOUNT_ID` | mw-dev AWS account id (kept out of committed code; ARNs are built from it) |
| secret | `MW_DEV_TRINO_POD_IDENTITY_ROLE` | full ARN of the dedicated mw-dev Trino Pod Identity role (consumed only by the Trino lane) |
| secret | `AWS_ECR_PUBLISH_IAM_ROLE` | ECR push (already exists; used by CD) |
| (role) | `github-duckgres-e2e` | dedicated stripped role in the mw-dev account (posthog-cloud-infra) — `eks:DescribeCluster` + Pod Identity association calls + `iam:PassRole`/`iam:GetRole` on the CP and dedicated Trino roles + an EKS access entry for kubectl. The workflow assumes `arn:aws:iam::<MW_DEV_ACCOUNT_ID>:role/github-duckgres-e2e`. |
| repo setting | "Require approval for all outside collaborators" | the access gate (see below) |

Athena adds no one-time GitHub configuration. Its Terraform unit publishes the
SSM String parameter `/duckgres/perf/athena` and grants the existing workflow
OIDC role `ssm:GetParameter` on that exact parameter. The JSON keys are
`pod_identity_role_arn`, `workgroup_name`, `glue_database_name`, and
`results_s3_uri`, all derived from the deployed resources. The frozen-perf
workflow loads them with `bash scripts/scenario_athena_config.sh` and exports
the existing scenario environment variables before deployment. The loader
requires AWS CLI, jq, and an explicit `AWS_REGION`; it fails without exporting
partial settings if fetching or validation fails. Apply the infrastructure
first, and fix configuration in Terraform rather than editing SSM manually.
Direct local scenario invocations still accept the documented explicit Athena
environment variables.

The `scenario-dev` workflow requests a 16,200-second session from
`github-duckgres-e2e`, matching its 270-minute job timeout. The role's
`max_session_duration` in posthog-cloud-infra must be at least 16,200 seconds
before this workflow setting is deployed.

The Tailscale tailnet ACL must allow `tag:github-runner` to reach the mw-dev
VPC subnet router (same pattern hogland set up for its dev cluster).

## Access control — "external people cannot run this"

Same model as the AWS/OIDC job in `ci.yml`: the gate is the repo setting
**"Require approval for all outside collaborators"**. Members' PRs run
automatically; fork PRs from outside collaborators get no secrets and don't run
until a maintainer clicks *approve-and-run*, so they can't reach the cluster or
assume the IAM role unapproved.

No per-workflow guard job or required-reviewer Environment: a guard on
`author_association` would block external PRs *even after a maintainer approves*
(the opposite of the intent), and a required-reviewer Environment would force an
approval click on every maintainer push. The repo setting gives exactly
"members auto, outsiders need approval".

## Validated locally against mw-dev (dry-run with the shipped `duckgres` image)

Running `run.sh` from a laptop on the VPN (kubectl `--context posthog-mw-dev`)
got through, in order — each was a real fix:
1. ✅ deploy — namespace, throwaway config-store, CP, cross-ns RBAC all apply.
2. ✅ worker boot — needed `data_dir: /data` in the worker ConfigMap (the CP
   factory mounts an emptyDir at `/data`; default `./data` → `mkdir
   data/extensions: permission denied` and the worker exits 1).
3. ✅ CP secret reconciler — needed `list`/`watch` on secrets in the Role.
4. ✅ SNI routing — the CP rejects non-SNI connections
   (`this server requires connecting via <org-id>.<managed-suffix>`). Fixed by
   `DUCKGRES_MANAGED_HOSTNAME_SUFFIXES=.ci.duckgres.local` +
   `DUCKGRES_SNI_ROUTING_MODE=passthrough`, and connecting with libpq
   `host=<org>.<suffix>` (SNI) + `hostaddr=<CP ClusterIP>` (TCP).
5. ✅ catalog selection — `dbname` must be `ducklake` or the org's own Trino
   catalog name (`org_<database_name>`, a logical alias for the same catalog),
   never an arbitrary name (PR #651: *database = catalog selection*).
   harness.sh covers both, in `logical_catalog_alias`.

6. ✅ **activation (cnpg DuckLake)** — the control plane resolves the metadata
   password from the Secret referenced by Duckling status. Activation failed at
   **S3 STS brokering** because the isolated CP had no AWS identity. Fixed by
   binding the per-PR `duckgres` SA to the **same EKS Pod
   Identity role the real mw-dev control plane uses**
   (`duckgres-control-plane-dev`), via `aws eks create-pod-identity-association`
   in `run.sh` deploy (deleted in teardown). With it, the CP brokers per-duckling
   S3 creds exactly like prod: validated the warehouse reaches `ready`, the pod
   gets the EKS creds endpoint, and a psql session authenticates + attaches the
   DuckLake catalog. (Full CREATE/INSERT/SELECT reconfirm was interrupted by a
   VPN drop; the activation path itself is proven.)

The CP pod-identity role is `role/duckgres-control-plane-dev` in the mw-dev
account; the workflow builds the ARN from `secrets.MW_DEV_ACCOUNT_ID` (no account
id committed).

## Other open items

- **`github-duckgres-e2e` role (posthog-cloud-infra).** AWS policy:
  `eks:DescribeCluster`, `eks:{Create,List,Delete,Describe}PodIdentityAssociation`
  on the cluster + its associations, and `iam:PassRole` on both
  `duckgres-control-plane-dev` and the dedicated role referenced by
  `MW_DEV_TRINO_POD_IDENTITY_ROLE`. Trust: `repo:PostHog/duckgres:*`. Plus an EKS
  **access entry** binding the role to k8s RBAC that can create namespaces, the
  cross-namespace bindings, and the in-namespace resources (the kubectl the
  harness runs). Scope as tightly as the cluster admins allow — deliberately
  NOT the account-admin `github-terraform-infra-role`.
- **Don't hammer auth.** The CP rate-limiter bans the source IP after a few
  failed auths (~15 min). The harness uses the provision-time password and
  settles one config-poll interval before connecting — keep it that way; a
  reset-password + tight retry loop will trip the ban.
- **Trino password rotation is eventually consistent.** The Trino lane allows
  up to 180 seconds for the control-plane reconcile, kubelet Secret-volume
  projection, and Trino's password-file reload. If it times out, inspect the
  control-plane reconcile and coordinator logs before rerunning; repeated
  immediate authentication attempts do not make the mounted Secret refresh
  faster.
- **Teardown / recreate are now CR-synchronous.** `run.sh deploy`, `run.sh
  teardown`, and the in-harness same-org recreate all `kubectl wait --for=delete`
  on the Duckling CR, whose finalizers run the Crossplane DROP of the cnpg
  role+db, before returning / re-provisioning. Deploy and teardown fail if the
  same-PR CRs do not delete within the timeout; the scheduled `e2e-cleanup` sweep
  logs a narrow stuck-CR summary but keeps going so one old namespace does not
  block the janitor. `drop_cnpg_role` is still called at deploy + at teardown as a
  belt-and-suspenders idempotent backstop; it sweeps the `mdstore_<org>`
  identifiers. (Composition `managementPolicies:
  ["*"]` from charts#11522 does the drop; the `--for=delete` wait is what makes
  it synchronous from our side.)
- **Shared-infra contention.** Concurrent PRs provision real ducklings against
  the same cnpg-shards infra. Org-ID prefix keeps them
  distinct; watch quay.io / cnpg pooler limits under parallelism.
- **Parallel-lane recovery.** Every matrix lane has a matching teardown entry
  and `fail-fast: false`, so one lane failing does not cancel the others or skip
  their cleanup. A cancelled workflow can still strand a
  lane temporarily; rerunning reaps that lane identity before deploy, and the
  scheduled `e2e-cleanup` sweep remains the final backstop. Reshard status polls
  bound each API attempt to 15 seconds and retry transient service-DNS failures;
  a persistent outage still exhausts the operation's overall wait budget and
  fails with the operation log.
- **e2e-cleanup** is wired: the `e2e-mw-dev.yml` `schedule` trigger runs
  `run.sh e2e-cleanup` every 6h, reaping `duckgres-ci-pr-*` namespaces older than
  6h (`E2E_CLEANUP_MAX_AGE_HOURS`) along with their ducklings, cnpg role+db, Pod
  Identity association, and ci-pr-labelled cross-ns bindings.
- **Remaining deferrals** are listed under "Deliberately not covered here"
  above (warm-pool activation + version-reaper, physical S3-prefix isolation,
  Cilium egress allow/deny probing).

## Local dry-run

Needs VPN to the private mw-dev API and `AWS_PROFILE=mw-dev` for the `aws eks`
pod-identity calls. Both images point at the same all-in-one ref.

```sh
IMG=<ecr>/duckgres:<tag>
AWS_PROFILE=mw-dev \
NAMESPACE="duckgres-ci-pr-${LANE_ID:?}" PR_NUMBER="$LANE_ID" KUBE_CONTEXT=posthog-mw-dev \
  WORKER_IMAGE=$IMG CONTROLPLANE_IMAGE=$IMG \
  CP_POD_IDENTITY_ROLE=arn:aws:iam::<mw-dev-account-id>:role/duckgres-control-plane-dev \
  bash tests/mw-dev/run.sh deploy
```

### Running the combined Trino perf comparison

The scheduled workflow and manual `posthog_frozen_perf` selection run all five
benchmark targets in one scenario, namespace, and result set. Dataset and Hoglake
registration run once. Measurements run sequentially: `pgwire_uncached`,
`pgwire_cached`, `trino`, `trino_cached`, then `athena`.
The former `posthog_frozen_perf_trino_cached` selection has been removed; use
`posthog_frozen_perf` instead. Omit the workflow's `duckgres_image` override so the
control-plane build explicitly persists the expected baseline cache setting.

For local invocation, set `SCENARIO_NAME=posthog_frozen_perf` and `E2E_SUITE=trino`
before both `tests/mw-dev/run.sh deploy` and `test-scenario`. Use the existing lane
credentials, images, and namespace requirements. The harness starts three Trino
clusters: the managed onboarding cluster, an uncached benchmark cluster, and a
cached benchmark cluster. Each has one coordinator and three 1-CPU/4Gi workers;
the total worker reservation is 9 CPU/36Gi, while each measured target retains a
3-CPU/12Gi execution budget. Separate discovery services, catalog-store cells,
and ephemeral volumes isolate their state.

The managed tenant catalog is left untouched. The runner reads its properties,
then creates fixture catalogs in the two benchmark cells with the same authorized
tenant catalog name. Both point to the shared fixture Hoglake catalog and use the
read-only Pod Identity; only their explicit cache setting differs. Tenant
credentials and OPA authorization remain unchanged. The admin credential is used
only for creation; measurements authenticate as the tenant. Existing catalog
properties must match exactly, and no catalog is dropped or switched between
benchmark phases.

`run.sh` supplies these runner settings automatically. Direct runner invocations
must supply them for the isolated deployment:

| Variable | Harness value / purpose |
| --- | --- |
| `DUCKGRES_SCENARIO_TRINO_CATALOG_STORE_DSN` | Throwaway catalog database connection |
| `DUCKGRES_SCENARIO_TRINO_PERF_URL` | HTTPS endpoint of `duckgres-trino-perf` |
| `DUCKGRES_SCENARIO_TRINO_PERF_CELL_ID` | Managed cell ID with `-perf` suffix |
| `DUCKGRES_SCENARIO_TRINO_CACHED_URL` | HTTPS endpoint of `duckgres-trino-cached` |
| `DUCKGRES_SCENARIO_TRINO_CACHED_CELL_ID` | Baseline cell ID with `-cached` suffix |
| `DUCKGRES_SCENARIO_TRINO_ADMIN_PASSWORD_FILE` | `/trino-admin/admin-password` |

There are no implicit cached endpoint or credential defaults. Never point this
setup at a shared dev/prod catalog store or coordinator. A `perf_queries` step may
select both Trino targets; `with.targets` can select a subset for focused local
runs, while the checked-in scenario runs all five.

The current pinned Hoglake connector ignores `fs.cache.enabled`; the two labels
currently distinguish requested configuration, not verified caching behavior.
See `tests/perf/README.md` for cache budgets and this existing connector limitation.

If setup or readiness fails, preserve artifacts and inspect logs for
`duckgres-trino`, `duckgres-trino-perf`, and `duckgres-trino-cached` deployments. Check catalog-store cell
IDs, TLS service names, auth projections, and cache-manager configuration. Run
`tests/mw-dev/run.sh diagnostics`, then the normal `teardown`, and redeploy a fresh
namespace before retrying. Namespace teardown removes all three clusters, fixture
catalog rows in the throwaway database, and all ephemeral cache volumes.

### Shared-pool startup acceptance

New compute instances use `cell-<eight random hexadecimal digits>` independently
of their logical pool ID. Their Deployments are `<instance>-coordinator` and
`<instance>-worker`; Kubernetes still adds its normal pod suffixes. Existing
instances keep their persisted names and endpoints until normal replacement.
The config-store primary key prevents identity reuse across pools, including
retired instances. A collision fails before creating Kubernetes resources; the
next reconciliation generates a fresh random suffix.

Set `E2E_TRINO_POOL_SHORT_NAMES=1` with `E2E_TRINO_POOL=1` after all old instances
have retired to verify Deployment names and Kubernetes-managed pod suffixes.
The naming assertion defaults off so a mixed old/new fleet remains supported.
The default in-Job fixture has no pooled workload, so this requires an authorized
deployment of the candidate image and a completed instance replacement before
it can provide live evidence. No namespace move or resource rename is performed
by the naming change. Namespace moves require a separate maintenance procedure;
do not change the configured namespace while old instance snapshots remain live.

The existing `trino_shared_pool_active` assertion in `e2e/harness.sh` requires
`E2E_TRINO_POOL=1` and a separately configured shared-pool deployment. Its
structure stage waits for ready, independent coordinator instances; subsequent
opt-in stages check warehouse admission and a real query with an existing login.
Run it after rolling a candidate control-plane image with a shared-pool registry,
the Gateway token file, and no `DUCKGRES_TRINO_ROLLOUT_CANARIES_FILE`. Reusing
the token must not activate the obsolete fixed-slot canary endpoint or crash
control-plane startup.
When the Gateway uses form authentication, configure the existing API-role
username through `DUCKGRES_TRINO_MANAGED_GATEWAY_USERNAME`. The same active
acceptance path requires successful API and capability authentication before
the operator can create and admit compute instances.

Internal HTTP coordinator probes declare forwarded HTTPS on port 443. Trino
uses that advertised origin in statement continuation URLs. Duckgres validates
the same coordinator hostname, HTTPS port 443, and statement-result path before
mapping those continuations back to the configured internal HTTP endpoint.
Other origins, user information, query strings, fragments, and HTTP redirects
remain rejected. Fixed HTTPS coordinator probes retain their exact-origin rule.
No new configuration or credential is needed.

`TestPoolCandidateConsumesForwardedHTTPSContinuation` exercises full candidate
validation with queued and executing result pages and both HTTPS port spellings.
The fixture follows Trino's request-derived URI construction, including forwarded
headers; returning all node rows from the initial POST would miss this failure.
Run the active-pool acceptance stage after deployment to verify real coordinator
admission and querying. This local regression does not replace that cluster check.

The default in-Job fixture does not configure a shared pool, and its harness runs
only after the control plane starts. It cannot reproduce this startup failure by
changing its own environment: the control plane reads these settings at process
startup. `TestTrinoRolloutReadinessScopesFixedCells` covers the startup selection
and malformed fixed/mixed configurations locally. The active-pool harness remains
the real-cluster acceptance check; passing unit tests alone does not prove it ran.

A corrected blueprint supersedes a never-admitted `PREPARING` candidate when
its release ID or blueprint digest changes, including configuration-only changes
with the same image. Duckgres records `FAILED_PREPARING`, obtains the Gateway's
guarded retirement claim, and deletes only that candidate's UID-bound resources.
It keeps the capacity slot until resource absence is verified. `VALIDATING`,
`ADMITTED`, and `SERVING` instances do not use this shortcut: admission may
already have happened, so their ordinary admission/drain protocol still applies.
No manual database edits or pod deletion are required for superseded candidates.

For recovery acceptance, publish a corrected blueprint while an old candidate
cannot pass validation, then run the active-pool stage after replacement.
The default in-Job fixture has no shared-pool controller or authority to change
its desired blueprint. `TestSupersededPreparingCandidateRecovers` exercises that
configuration change, immutable snapshots, lost retirement response, verified
absence, and replacement locally. The companion tests preserve admission
ambiguity, Gateway retirement refusals, configuration freeze, and lease fencing.

### Shared-pool rollout sealing acceptance

A DRAINING member with no remaining obligations reports `readyToSeal=true` and
`drained=false`. Duckgres uses that readiness to request sealing; it does not
wait for the post-seal `drained` result before making the first request. If the
seal commits but its response is lost, a subsequent `drained=true` observation
allows replay with the original operation identity and expected generation.
Pending requests, open transactions, and active or retained queries still block
sealing. Only Gateway-authorized retirement permits resource deletion.

For live acceptance, keep an existing read-only transaction open while a desired
release update admits a replacement and drains its predecessor. The old member
must remain present while that transaction continues to query successfully.
After committing and waiting for result retention to finish, verify the old
member seals and retires while the pool retains its configured serving floor.
Run this only through the deployment controller during an authorized test window;
restarting a coordinator does not test graceful retirement.

The default in-Job fixture does not own a shared pool or publish its desired
release, so it cannot perform that rollout safely. `TestPoolSeal*` covers the
Gateway obligation contract, each blocker, refusal, and lost-response replay
locally. These tests do not establish live rollout continuity by themselves.

Configuration rollback must also work within one control-plane leadership term.
Each change to the Gateway configuration gets a new attempt ID, including a
return to an earlier release or tenant-admission setting. Unchanged settings
reuse their attempt. An unknown result must settle before a newer configuration
can proceed; resolving an obsolete attempt does not authorize lifecycle work
against settings the Gateway has not applied yet. A new authority epoch resets
the attempt sequence and fences requests from the previous term.
The routing group must remain stable within a term. Changing it fails closed;
an unresolved request must never be redirected to a different pool.

`TestPoolConfigure*` exercises repeated rollback, unchanged replay, ambiguous
responses, delayed requests, rejection, and leadership changes locally. The
in-Job fixture cannot change shared-pool desired configuration, so live acceptance
requires an authorized desired-release A-to-B-to-A rollout while queries continue.
Verify the Gateway's desired revision as well as member replacement and serving
capacity. Do not restart the control plane between configuration changes: that
would hide a same-term rollback regression.

### Optional shared catalog rollout lane

`TRINO_SHARED_CATALOGS_ENABLED=true` adds an isolated, real Gateway to the
`E2E_SUITE=trino`, `SCENARIO_NAME=full-suite` fixture. The default remains false;
the existing static multicell tests still run first. This option requires:

- A control-plane image with the managed shared-catalog runtime, lifecycle store,
  provisioning freeze/release endpoints, and readiness endpoint.
- `TRINO_GATEWAY_IMAGE` containing an explicit `@sha256:` digest from a build
  with fenced rollout administration. A mutable tag is rejected before any cluster
  operation. Verify the image's source revision before supplying its digest.
- A fresh private `DUCKGRES_CI_SECRET_DIR`, so the per-run TLS certificate includes
  the isolated Gateway service. Existing certificates without this identity fail
  closed. No customer credentials or production secrets are reused.

Set those variables on the existing `run.sh deploy` and `run.sh test-e2e` commands,
using the same disposable namespace, PR identity, and explicit test context as the
normal fixture. The regular CI lane does not automatically enable this option.
The Gateway PR's Docker check alone is not a published-image prerequisite: the
main-only image publisher must have produced the reviewed candidate first, or a
separately reviewed build must supply that exact source as a digest-pinned image.

The optional lane pauses catalog management, waits for old control-plane pods to
terminate, stops green, and points green at blue's persisted catalog identity.
It then exercises the actual Gateway operation and control-plane admission APIs:

1. Freeze admission before starting green. A newly enabled fixture warehouse
   cannot become Ready or create a catalog while frozen.
2. Start green from zero pods. Its startup reads the shared catalog without
   changing the existing row's version, properties, or update timestamp.
3. Require the prepared certificate to match the target's actual process and
   admitted roster, then query existing DuckLake data on green.
4. Cut over and release admission. The new warehouse becomes Ready only on green;
   the still-running blue coordinator does not gain its catalog. Require its
   persisted connector name to be `ducklake`, without SQL identifier quotes,
   and read its `main` schema through Gateway using the new tenant's credentials.
5. Drain and seal blue, require actual pod absence, and complete the operation.

Before and after cutover, warehouse DuckLake queries pass through the real
Gateway. An administrator's node query must identify the active coordinator.
Every advertised continuation must remain on the verified Gateway origin; the
harness rejects direct-backend or foreign-host continuations before sending
credentials. The two fixture coordinators enable forwarded-header processing
only when this option is selected; the legacy fixture remains unchanged.

The Gateway uses its own schema in the disposable PostgreSQL database. Generated
Gateway keys, signing material, and the dedicated fixture canary remain Kubernetes
Secrets. No mock Gateway is accepted for this lane. The publication evidence is
synthetic because this test does not create Git branches or PRs; native Kargo/Git
publication safety is covered separately. This lane does not claim transaction
affinity or load-test coverage.

On failure, retain only redacted diagnostics and use the normal fixture teardown.
Do not clear a held catalog mutation or bypass the freeze to make a test pass.
The additional fixture warehouse is included in the existing cleanup inventory.
Local `just test-mw-fixtures` validates opt-in checks, real rendering, and shell
request construction; it is not proof that this real-cluster lane has executed.

## Regular Trino + Hoglake prerequisites

Normal managed onboarding creates each tenant’s Hoglake catalog and namespace.
The lane verifies CREATE/INSERT/CTAS, wide decimals through real compaction,
tenant isolation, disable/re-enable persistence, and deprovision protection.

The regular lane uses isolated Hoglake PostgreSQL and server deployments, plus
three Trino workers. Apply [the CI-only Hoglake storage identity and CI deployer](https://github.com/PostHog/posthog-cloud-infra/pull/10521)
permissions, the Duckling permissions boundary, and Crossplane tenant-prefix
grants before running it. After AWS OIDC authentication, deployment, teardown,
and scheduled cleanup run `discover-hoglake.sh`. It reads the dedicated CI role
with `iam:GetRole` and its `hoglake-ci-storage` inline policy with
`iam:GetRolePolicy`, deriving the shared `s3://<bucket>/trino/` base from the
single permitted `trino/ci-pr-*` write scope. Missing or ambiguous configuration
fails before mutation. Values are masked before being exported through
`GITHUB_ENV`; no Hoglake GitHub secrets or variables are required.

For local runs, continue supplying `HOGLAKE_CI_POD_IDENTITY_ROLE` and
`HOGLAKE_DATA_PATH` explicitly. After the discovery permission is applied and
CI discovery succeeds, remove the obsolete `MW_DEV_HOGLAKE_CI_POD_IDENTITY_ROLE`
and `MW_DEV_HOGLAKE_DATA_PATH` repository secrets.

Missing prerequisites fail deployment before namespace mutation. Cleanup waits
for fixture writers to terminate and deletes only the numeric PR's exact prefix.
The initialized metadata-loss protection has unit regression coverage; this
lane does not corrupt the server database to simulate metadata loss.

The frozen performance workflow also discovers the dedicated managed storage
base and configures managed Hoglake admission. It uses the same Hoglake server
pin, which supports atomic table creation, but keeps the frozen-data read-only
Pod Identity for fixture reads. The control plane owns the tenant's managed
catalog; the importer registers immutable Parquet in a separate `<org>-frozen`
catalog containing `posthog` and `properties_perf` namespaces. Its data path is
the frozen bucket root, while each import enumerates only its exact source prefix.
The two isolated benchmark cells use the read-only Pod Identity instead of assuming
the managed tenant storage role. It does not copy, rewrite or grant writes to the frozen dataset.

For local frozen runs, supply `HOGLAKE_DATA_PATH` with the dedicated
`s3://<bucket>/trino/` base before deployment. Missing configuration fails before
namespace mutation. Managed Hoglake rejects public deprovision by design, so the
frozen scenario leaves cleanup to the workflow's always-run `run.sh teardown`.
Direct/local scenario invocations must run that teardown even after failures;
it removes the isolated Duckling and namespace before cleaning the scoped managed
storage prefix. Do not use the frozen source prefix as `HOGLAKE_DATA_PATH`.
Collect `run.sh diagnostics` and preserve scenario artifacts before teardown, then
redeploy a fresh isolated stack before retrying.
