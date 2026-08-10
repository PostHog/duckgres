# Duckgres Scenario Runner

## Scope

The scenario runner executes end-to-end managed-warehouse flows against a configured dev environment. The first smoke scenario provisions a warehouse, waits for readiness, runs `SELECT 1` over PGWire with managed-hostname SNI, then deprovisions and verifies cleanup.

The default full workload uses one `full-suite.yaml` scenario: it provisions a fresh dev warehouse, creates read-only views over frozen persons/events parquet supplied by `DUCKGRES_SCENARIO_FROZEN_S3_URI`, runs metadata exploration, perf queries, and dbt models, then deprovisions. `fast-suite.yaml` follows the same flow without dbt. The standalone provisioning, frozen metadata, perf, and dbt scenarios remain available for focused debugging.

The targeted frozen-perf scenario additionally creates production-shaped DuckLake
tables, `posthog.events` and `posthog.persons`, from those raw views. It uses the
PostHog backfill schema pinned at `056583335dc739b9e025efede811c9b4f5e153f5`,
rewritten inserts, and `year(timestamp), month(timestamp), day(timestamp)` /
`year(_timestamp), month(_timestamp)` partitioning. The raw views are deliberately
retained as the later paired-query performance-control target.

`project_id` is the one mapping exception: it is derived from `team_id`, exactly
as the pinned production exporter does, so no `project_id` fixture column is needed.

## Required Environment

Set these before running a real scenario:

```bash
export DUCKGRES_SCENARIO_API_BASE="<control-plane-api-base-url>"
export DUCKGRES_SCENARIO_INTERNAL_SECRET="<internal-secret>"
export DUCKGRES_SCENARIO_ORG_ID="<authorized-disposable-org-id>"
export DUCKGRES_SCENARIO_PG_HOST="<pgwire-direct-tcp-host>"
export DUCKGRES_SCENARIO_SNI_SUFFIX="<managed-hostname-suffix>"
```

`DUCKGRES_SCENARIO_PG_HOST` is the direct PGWire TCP host. The Go SQL and perf clients dial it through a connector while retaining `host=<org><suffix>` for TLS SNI and managed-warehouse routing. The dbt client exports a numeric value as `PGHOSTADDR`, which libpq supports.

Optional:

```bash
export DUCKGRES_SCENARIO_OUTPUT_BASE="artifacts/scenario"
export DUCKGRES_SCENARIO_RUN_ID="scenario-smoke-manual"
export DUCKGRES_SCENARIO_PG_PORT="5432"
export DUCKGRES_SCENARIO_PG_CONNECT_TIMEOUT="10"
export DUCKGRES_SCENARIO_DBT_BIN="dbt"
export DUCKGRES_SCENARIO_MAX_RUNTIME="30m"
export DUCKGRES_SCENARIO_GO_TEST_TIMEOUT="60m"
```

Frozen dataset scenarios additionally require:

```bash
export DUCKGRES_SCENARIO_FROZEN_S3_URI="s3://<dev-managed-bucket>/frozen_v1/"
```

The full suite, fast suite, and targeted frozen perf scenarios exercise PGWire
only. `posthog_frozen_trino_perf.yaml` compares the same DuckLake tables over
PGWire and Trino; its raw-Parquet-view control queries remain PGWire-only.
Frozen perf records per-query success and failure rows in
`query_results.csv`.
Measured query errors fail the perf DAG step after its artifacts are written;
independent sibling steps continue to run.

A `perf_queries` step can set `with.targets` to a non-empty subset of the
catalog's targets. Duckgres perf catalogs and scenarios are pgwire-only.

Do not commit concrete dev endpoints, secrets, org IDs, or private bucket names.

## Run

Validate configuration without running:

```bash
./scripts/scenario_run.sh --check-env
```

Run the dev smoke:

```bash
just scenario-smoke
```

Run the composed full suite:

```bash
just scenario scenario=tests/mw-dev/scenario/scenarios/full-suite.yaml
```

Run the fast suite without dbt:

```bash
just scenario scenario=tests/mw-dev/scenario/scenarios/fast-suite.yaml
```

Run targeted frozen metadata exploration:

```bash
just scenario-frozen-metadata
```

Run frozen perf queries:

```bash
just scenario-frozen-perf
```

Run the paired PGWire/Trino comparison:

```bash
just scenario-frozen-trino-perf
```

The scenario provisions a four-worker Trino cluster, waits for it to become
ready, benchmarks both protocols, and always tears Trino down before the
warehouse. It gets nothing from the control plane but a cluster ID, a lifecycle
state, the in-cluster endpoint, the worker counts, and the pinned image
reference — see "Trino benchmark lifecycle" below.

This runs, in order: raw-view setup, source-column preflight, explicit PostHog
table DDL, registration of the frozen Parquet files in DuckLake, then partition
and file-metadata validation. Registration reads Parquet footers but does not
rewrite the fixture rows, so the raw-view and DuckLake-table queries use the
same frozen S3 objects. Validation checks the declared schema and partition
metadata plus exact source/registered file-list equality. Neither `fast-suite`
nor `full-suite` enables these tables yet.

Run frozen dbt lifecycle:

```bash
just scenario-frozen-dbt
```

Run a specific scenario file:

```bash
just scenario scenario=tests/mw-dev/scenario/scenarios/provision_smoke.yaml
```

Artifacts are written under `artifacts/scenario/<run_id>/`.

The default full-suite scenario uses:

- `tests/mw-dev/scenario/scenarios/full-suite.yaml`
- `tests/mw-dev/scenario/sql/setup_frozen_views.sql`
- `tests/mw-dev/scenario/sql/metadata_catalog.yaml`
- `tests/perf/queries/ducklake_frozen.yaml`
- `tests/mw-dev/scenario/dbt/posthog_frozen_project/`

It runs serially in topological order: `provision` → `wait_ready` → `setup_frozen_views`, then `metadata_exploration`, `perf_queries`, and `dbt_models`. Each workload branch depends only on setup, so a failure in one branch does not suppress the others; `deprovision` always runs after the branches.

The targeted frozen metadata scenario uses:

- `tests/mw-dev/scenario/scenarios/posthog_frozen_metadata.yaml`
- `tests/mw-dev/scenario/sql/setup_frozen_views.sql`
- `tests/mw-dev/scenario/sql/metadata_catalog.yaml`

The frozen perf scenario uses:

- `tests/mw-dev/scenario/scenarios/posthog_frozen_perf.yaml`
- `tests/perf/queries/ducklake_posthog_tables.yaml`

Perf artifacts are written under `artifacts/scenario/<run_id>/perf/` using the existing `tests/perf/core` artifact schema, including `query_results.csv`, `summary.json`, and `server_metrics.prom`.

The paired Trino scenario uses
`tests/mw-dev/scenario/scenarios/posthog_frozen_trino_perf.yaml` with the same
table setup and query catalog. Its artifact contains a row per protocol, so
`query_results.csv` directly compares the existing PGWire workload with Trino
on the identical DuckLake snapshot. Its explicit lifecycle calls are the only
place that may create or delete Trino; the scenario YAML and runner do not
receive metadata or S3 credentials.

### Trino benchmark lifecycle

**This feature is disabled and fail-closed until the companion charts release
is deployed.** Duckgres alone cannot run the paired scenario: it needs a
per-Duckling read-only S3 role, a dedicated metadata-Postgres reader
role/password Secret, and RBAC letting the control plane read that Secret by
exact name. Until those exist, provisioning fails with a configuration error and
the API answers `503`. There is deliberately no fallback to the tenant's writer
credentials.

**Configuration** (env-only on the control plane, resolved in
`configresolve/resolve.go`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `DUCKGRES_TRINO_BENCHMARK_ENABLED` | `false` | Master gate. Enabled alone is not enough. |
| `DUCKGRES_TRINO_BENCHMARK_IMAGE` | `""` | Pinned Trino+Brikk image; **required** when enabled. Prefer a digest reference — it is what the artifact records. |
| `DUCKGRES_TRINO_BENCHMARK_IMAGE_PULL_POLICY` | `IfNotPresent` | Pull policy for coordinator and workers. |
| `DUCKGRES_TRINO_BENCHMARK_SERVICE_ACCOUNT` | `""` | ServiceAccount whose IAM identity may assume the read-only S3 role. |
| `DUCKGRES_TRINO_BENCHMARK_WORKERS` | `4` | Default worker replicas when a request omits `workers`. |
| `DUCKGRES_TRINO_BENCHMARK_COORDINATOR_CPU` / `_COORDINATOR_MEMORY` | `2` / `8Gi` | Coordinator shape. |
| `DUCKGRES_TRINO_BENCHMARK_WORKER_CPU` / `_WORKER_MEMORY` | `2` / `8Gi` | Per-worker shape. |

Requests equal limits (Guaranteed QoS) for every benchmark pod, so Trino neither
bursts into nor is throttled by the Duckgres worker it is being compared with.
A request may ask for at most 16 workers.

The mw-dev harness passes the image through
`DUCKGRES_TRINO_BENCHMARK_IMAGE` and enables the lifecycle only when that image
is set (`tests/mw-dev/run.sh`); the `scenario-dev` workflow exposes it as the
optional `trino_benchmark_image` input. Neither ever carries a credential.

**Lifecycle API** (internal, admin-authenticated, under the existing
`/api/v1` router):

| Route | Success | Notes |
| --- | --- | --- |
| `POST /api/v1/trino-benchmarks/orgs/:org_id/provision` | `202` created, `200` idempotent repeat | Body is optional `{"workers": N, "run_id": "..."}`; unknown fields are rejected. |
| `GET /api/v1/trino-benchmarks/status/:cluster_id` | `200` | `state` is `pending`, `ready`, or `failed`. |
| `POST /api/v1/trino-benchmarks/deprovision/:cluster_id` | `204` | Idempotent; an already-absent cluster is also `204`. |

Errors: `400` invalid request, `404` unknown cluster, `409` a same-named cluster
exists with different ownership or configuration, `503` the feature is disabled
or the reader identity is not configured, `500` otherwise. Error bodies are
fixed strings — infrastructure detail is logged on the control plane, never
returned.

Provisioning creates, in the control plane's namespace and all labelled with the
cluster ID and owning org: a ClusterIP Service selecting only the coordinator,
coordinator/worker/catalog ConfigMaps, a short-lived Secret holding only the
charts-created metadata reader password, a one-replica coordinator Deployment,
and a worker Deployment with exactly the requested replicas. Status is `ready`
only when the coordinator is ready **and** every requested worker replica is
ready. Cleanup deletes only objects carrying those ownership labels, so it is
safe after a partial provision and cannot touch another cluster, a Duckgres
worker, or the charts-created reader Secret.

**Artifacts.** `summary.json` gains an `environments` array, one entry per
protocol: engine and version (Trino's from the coordinator's own `/v1/info`),
connector version, the pinned image reference, requested/ready worker counts,
catalog and schema, and the `UTC` session time zone. `query_results.csv` carries
a row per protocol per query, so the two engines are compared directly. No
thresholds and no CI gating are attached to any of it.

**Failure recovery.**

- `503` from provision: the feature is off, no image is pinned, or the charts
  reader resources are missing. Check the control-plane log for the named
  missing field. Do not work around it with writer credentials.
- `409` from provision: a cluster for that org already exists with a different
  image or worker count — usually a leftover from an interrupted run. Deprovision
  it (`POST .../deprovision/trino-bench-<org>`) and retry.
- Readiness times out: the wait step reports the attempt count and last observed
  state. Inspect the Deployments with
  `kubectl get deploy -l duckgres.posthog.com/trino-benchmark-cluster=trino-bench-<org>`.
  A `failed` state is terminal — the poller stops rather than burning the budget.
- Leftover cluster after an aborted run: `deprovision_trino` is `always_run` and
  precedes warehouse teardown, so this should be rare. Clean up manually with the
  deprovision route, or by deleting the labelled objects.

The frozen dbt scenario uses:

- `tests/mw-dev/scenario/scenarios/posthog_frozen_dbt.yaml`
- `tests/mw-dev/scenario/dbt/posthog_frozen_project/`

dbt artifacts are written under `artifacts/scenario/<run_id>/dbt/`, including per-command stdout/stderr logs, `target/` artifacts, and dbt logs. Install `dbt-postgres` locally or set `DUCKGRES_SCENARIO_DBT_BIN` to the dbt executable to use.

The frozen dbt workload requests a 2 CPU, 4Gi worker through the dbt connection's `duckgres.worker_cpu` and `duckgres.worker_memory` startup options. It also sets `with.connect_timeout: 360`, long enough for the control plane's five-minute worker queue to provision a cold Karpenter node. Other scenario workloads use the isolated control plane's default worker size; `scenario-dev` sets that default to 2 CPU and 8Gi to add process headroom for repeated frozen pgwire aggregates. A `dbt_run` step can opt into a different size or connection window with `with.worker_cpu`, `with.worker_memory`, and `with.connect_timeout`.

`perf_queries` defaults `with.fail_on_query_errors` to `true`. A measured query
error therefore marks that DAG step failed and appears in the scenario result,
while independent sibling steps continue. Set it to `false` only for a
diagnostic scenario whose query failures are intentionally non-verdict data.

dbt retry is opt-in per scenario step:

```yaml
retry:
  enabled: true
  max_attempts: 2
```

When enabled, a failed `run`, `test`, or `docs_generate` command is retried with `dbt retry`. The scenario records the original failure and retry as separate attempts in `events.jsonl`, marks the step `success_after_retry` if recovery succeeds, and writes per-attempt logs under `artifacts/scenario/<run_id>/dbt/attempts/<command>/attempt_<n>/`. `retry.enabled` defaults to `false`; `max_attempts` counts the original command attempt.

`DUCKGRES_SCENARIO_FROZEN_S3_URI` must point at a dev-owned frozen dataset prefix with `persons/` and `events/` parquet children.
The provisioned Duckgres worker role also needs read/list access to that prefix; the runner process only supplies the URI, while the worker performs the S3 reads during `read_parquet`.

## Leaked Dev Warehouse Recovery

The smoke scenario has an `always_run` deprovision step, but an interrupted process can still leave dev resources behind. To clean up:

1. Recover the `DUCKGRES_SCENARIO_ORG_ID` value supplied for the interrupted run.
2. Call the control-plane deprovision endpoint with the internal secret.
3. Poll `/warehouse/status` until the state is `deleted` or the warehouse returns `404`.
4. If deletion does not complete, inspect the dev control-plane logs and the managed warehouse deprovision runbook.

Use placeholders in shared notes and PRs; keep concrete dev values local.

## Frozen PostHog Table Setup Recovery

If the targeted frozen-perf setup or validation fails, retain the scenario error:
it identifies missing required fixture column names, schema/partition drift, or a
non-sensitive parity check. Do not edit the raw views to mask it. The warehouse is
disposable and the next targeted run creates a new one; within a retried setup the
transaction deletes both destination tables before inserting, so it cannot duplicate
rows after partial setup. Refresh the fixture only after confirming the pinned
backfill mapping remains appropriate; change the pin and mappings together when
upstream production schema changes.
