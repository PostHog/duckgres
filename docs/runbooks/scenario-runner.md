# Duckgres Scenario Runner

## Scope

The scenario runner executes end-to-end managed-warehouse flows against a configured dev environment. The first smoke scenario provisions a warehouse, waits for readiness, runs `SELECT 1` over PGWire with managed-hostname SNI, then deprovisions and verifies cleanup.

The default full workload uses one `full-suite.yaml` scenario: it provisions a fresh dev warehouse, creates read-only views over frozen persons/events parquet supplied by `DUCKGRES_SCENARIO_FROZEN_S3_URI`, runs metadata exploration, perf queries, and dbt models, then deprovisions. `fast-suite.yaml` follows the same flow without dbt. The standalone provisioning, frozen metadata, perf, and dbt scenarios remain available for focused debugging.

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

The full suite, fast suite, targeted frozen perf scenario, and row-group A/B
scenario exercise PGWire only. Frozen perf records per-query success and failure rows in
`query_results.csv`.
Measured query errors fail the perf DAG step after its artifacts are written;
independent sibling steps continue to run.

A `perf_queries` step can set `with.targets` to a non-empty subset of the
catalog's targets. If omitted, every target declared by the catalog runs. The
frozen scenarios set `targets: [pgwire]` so they do not call the deprecated
DuckHog Flight endpoint while the shared catalog remains usable by legacy perf
jobs during the deprecation period.

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

Run the manual frozen-events row-group A/B:

```bash
just scenario-events-rowgroup-perf
```

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
- `tests/perf/queries/ducklake_frozen.yaml`

Perf artifacts are written under `artifacts/scenario/<run_id>/perf/` using the existing `tests/perf/core` artifact schema, including `query_results.csv`, `summary.json`, and `server_metrics.prom`.

### Frozen events row-group A/B

`events_rowgroup_perf.yaml` is a standalone, manual experiment; it is not part of the scheduled full or fast suite. It takes the first 524,288 rows from one stable day in the existing frozen events fixture and writes the same 25 columns twice:

- `rowgroup_ab.events_rg_128mib`: 128 MiB maximum row-group byte size
- `rowgroup_ab.events_rg_1gib`: 1 GiB maximum row-group byte size

Both writes use the same 250,000-row upper bound, one thread, disabled insertion-order preservation as required by DuckDB's row-group byte guard, Zstandard compression, and 10 GB target file size. Each produces one output file. The 1 GiB table is populated from the completed 128 MiB table, so the candidate cannot select a different source slice. The scenario uses a 4 CPU, 16Gi worker to leave headroom while constructing the larger groups from this wide events schema.

The perf catalog is PGWire-only and covers five workloads: a five-minute timestamp count, minute time series, event aggregation, distinct-person count, and a wide properties scan. It reverses the full query order on alternating iterations, so each 128 MiB/1 GiB pair runs first and second three times. The perf session disables DuckDB's external file cache before one warmup and six measured iterations, so repeated scans continue to exercise object-store range reads. Compare query IDs ending in `_rg128mib` and `_rg1gib` within the same `intent_id` in `query_results.csv`; timings are recorded but never used as a CI pass/fail threshold.

Validation runs before the timed queries so an ineffective rewrite fails fast. It checks both tables have 524,288 rows, hashes all 25 columns to confirm identical contents, verifies one data file each, checks the persisted writer options, and inspects the Parquet footers to confirm that the larger byte cap produces fewer physical row groups. The benchmark then disables and clears DuckDB's external file cache on its pinned PGWire connection; the isolated scenario deployment has no node cache proxy, so validation reads do not warm the measured object-store path. Exact row counts per group are deliberately not assumed because the wide variable-length columns make the byte limit decisive. The recipe defaults the scenario runtime to 60 minutes and the enclosing Go test timeout to 75 minutes; override `DUCKGRES_SCENARIO_MAX_RUNTIME` or `DUCKGRES_SCENARIO_GO_TEST_TIMEOUT` if the dev environment is slower.

The frozen dbt scenario uses:

- `tests/mw-dev/scenario/scenarios/posthog_frozen_dbt.yaml`
- `tests/mw-dev/scenario/dbt/posthog_frozen_project/`

dbt artifacts are written under `artifacts/scenario/<run_id>/dbt/`, including per-command stdout/stderr logs, `target/` artifacts, and dbt logs. Install `dbt-postgres` locally or set `DUCKGRES_SCENARIO_DBT_BIN` to the dbt executable to use.

The frozen dbt workload requests a 2 CPU, 4Gi worker through the dbt connection's `duckgres.worker_cpu` and `duckgres.worker_memory` startup options. It also sets `with.connect_timeout: 360`, long enough for the control plane's five-minute worker queue to provision a cold Karpenter node. Other scenario workloads use the isolated control plane's default worker size; `scenario-dev` sets that default to 2 CPU and 8Gi to add process headroom for repeated frozen pgwire aggregates. SQL, PGWire perf, and dbt steps can opt into a worker profile with `with.worker_cpu` and `with.worker_memory`; dbt can also set `with.connect_timeout`. Flight perf connections do not currently carry these PGWire startup options.

`perf_queries` defaults `with.fail_on_query_errors` to `true`. A measured query
error therefore marks that DAG step failed and appears in the scenario result,
while independent sibling steps continue. Set it to `false` only for a
diagnostic scenario whose query failures are intentionally non-verdict data.

A PGWire `perf_queries` step can set `with.disable_external_file_cache: true` to run `SET enable_external_file_cache = false` on its pinned benchmark connection before warmup. The runner resets the setting before closing that connection. The option defaults to `false`; use it when the benchmark is intended to retain remote object-store I/O across iterations.

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
