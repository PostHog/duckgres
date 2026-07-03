# Duckgres Scenario Runner

## Scope

The scenario runner executes end-to-end managed-warehouse flows against a configured dev environment. The first smoke scenario provisions a warehouse, waits for readiness, runs `SELECT 1` over PGWire with managed-hostname SNI, then deprovisions and verifies cleanup.

The frozen metadata, perf, and dbt scenarios provision the same fresh dev warehouse shape, create read-only views over frozen persons/events parquet supplied by `DUCKGRES_SCENARIO_FROZEN_S3_URI`, run their workload, then deprovision.

## Required Environment

Set these before running a real scenario:

```bash
export DUCKGRES_SCENARIO_API_BASE="<control-plane-api-base-url>"
export DUCKGRES_SCENARIO_INTERNAL_SECRET="<internal-secret>"
export DUCKGRES_SCENARIO_PG_HOST="<pgwire-direct-address-for-libpq-hostaddr>"
export DUCKGRES_SCENARIO_SNI_SUFFIX="<managed-hostname-suffix>"
```

`DUCKGRES_SCENARIO_PG_HOST` is used as libpq `hostaddr`; the runner separately sets `host=<org><suffix>` so TLS SNI carries the managed warehouse identity.

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

Frozen perf scenarios additionally require the Flight SQL address because the perf catalog exercises both PGWire and Flight:

```bash
export DUCKGRES_SCENARIO_FLIGHT_ADDR="<flight-sql-address>"
export DUCKGRES_SCENARIO_FLIGHT_INSECURE_SKIP_VERIFY="true"
```

Frozen perf records per-query success and failure rows in `query_results.csv`.
The scenario is configured to preserve those query failures as reported perf results instead of failing the scenario step.

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

Run frozen metadata exploration:

```bash
just scenario-frozen-metadata
```

Run frozen perf queries:

```bash
just scenario-frozen-perf
```

Run frozen dbt lifecycle:

```bash
just scenario-frozen-dbt
```

Run a specific scenario file:

```bash
just scenario scenario=tests/scenario/scenarios/provision_smoke.yaml
```

Artifacts are written under `artifacts/scenario/<run_id>/`.

The frozen metadata scenario uses:

- `tests/scenario/scenarios/posthog_frozen_metadata.yaml`
- `tests/scenario/sql/setup_frozen_views.sql`
- `tests/scenario/sql/metadata_catalog.yaml`

The frozen perf scenario uses:

- `tests/scenario/scenarios/posthog_frozen_perf.yaml`
- `tests/perf/queries/ducklake_frozen.yaml`

Perf artifacts are written under `artifacts/scenario/<run_id>/perf/` using the existing `tests/perf/core` artifact schema, including `query_results.csv`, `summary.json`, and `server_metrics.prom`.

The frozen dbt scenario uses:

- `tests/scenario/scenarios/posthog_frozen_dbt.yaml`
- `tests/scenario/dbt/posthog_frozen_project/`

dbt artifacts are written under `artifacts/scenario/<run_id>/dbt/`, including per-command stdout/stderr logs, `target/` artifacts, and dbt logs. Install `dbt-postgres` locally or set `DUCKGRES_SCENARIO_DBT_BIN` to the dbt executable to use.

`DUCKGRES_SCENARIO_FROZEN_S3_URI` must point at a dev-owned frozen dataset prefix with `persons/` and `events/` parquet children.
The provisioned Duckgres worker role also needs read/list access to that prefix; the runner process only supplies the URI, while the worker performs the S3 reads during `read_parquet`.

## Leaked Dev Warehouse Recovery

The smoke scenario has an `always_run` deprovision step, but an interrupted process can still leave dev resources behind. To clean up:

1. Identify the scenario org ID from the scenario file and artifact directory.
2. Call the control-plane deprovision endpoint with the internal secret.
3. Poll `/warehouse/status` until the state is `deleted` or the warehouse returns `404`.
4. If deletion does not complete, inspect the dev control-plane logs and the managed warehouse deprovision runbook.

Use placeholders in shared notes and PRs; keep concrete dev values local.
