# Duckgres Scenario Runner

## Scope

The scenario runner executes end-to-end managed-warehouse flows against a configured dev environment. The first smoke scenario provisions a warehouse, waits for readiness, runs `SELECT 1` over PGWire with managed-hostname SNI, then deprovisions and verifies cleanup.

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
export DUCKGRES_SCENARIO_MAX_RUNTIME="30m"
export DUCKGRES_SCENARIO_GO_TEST_TIMEOUT="60m"
```

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

Run a specific scenario file:

```bash
just scenario scenario=tests/scenario/scenarios/provision_smoke.yaml
```

Artifacts are written under `artifacts/scenario/<run_id>/`.

## Leaked Dev Warehouse Recovery

The smoke scenario has an `always_run` deprovision step, but an interrupted process can still leave dev resources behind. To clean up:

1. Identify the scenario org ID from the scenario file and artifact directory.
2. Call the control-plane deprovision endpoint with the internal secret.
3. Poll `/warehouse/status` until the state is `deleted` or the warehouse returns `404`.
4. If deletion does not complete, inspect the dev control-plane logs and the managed warehouse deprovision runbook.

Use placeholders in shared notes and PRs; keep concrete dev values local.
