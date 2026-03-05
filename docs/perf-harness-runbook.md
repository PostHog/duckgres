# Perf Harness Runbook

## Scope

Golden-query performance signal collection for `pgwire` and `flight` protocols.
This is observability-only; there is no pass/fail performance gate.

## Local Prerequisites

- Run commands from the Duckgres repository root.
- Go toolchain must be installed and available on `PATH` (`go test` and `go build` are used by the harness).
- `/tmp` must be writable (used for temporary control-plane artifacts).
- Local metrics port `:9095` must be free for the harness runner.
- On first run, Go may download module dependencies (requires network access or a prewarmed module cache).

## Local Operator Steps

1. Run default local synthetic smoke:

```bash
./scripts/perf_smoke.sh
```

This default mode auto-starts a temporary local Duckgres control-plane with Flight ingress; you do not need to pre-start Duckgres.

2. Optional: use a pre-started Duckgres instance by setting both endpoint env vars:

```bash
DUCKGRES_PERF_PGWIRE_DSN="host=127.0.0.1 port=5432 user=perfuser dbname=test sslmode=require" \
DUCKGRES_PERF_FLIGHT_ADDR="127.0.0.1:50051" \
./scripts/perf_smoke.sh
```

3. For frozen dataset smoke, also set dataset version (plus both endpoint env vars):

```bash
DUCKGRES_PERF_DATASET_VERSION=v1 \
DUCKGRES_PERF_PGWIRE_DSN="host=127.0.0.1 port=5432 user=perfuser dbname=test sslmode=require" \
DUCKGRES_PERF_FLIGHT_ADDR="127.0.0.1:50051" \
./scripts/perf_smoke.sh
```

4. Inspect `artifacts/perf/<run_id>/summary.json`, `query_results.csv`, and (for frozen dataset mode) `dataset_manifest.json`.

If `:9095` is already in use, run the harness directly with a different metrics address:

```bash
go test ./tests/perf \
  -run TestGoldenQueryPerformanceHarness \
  -perf-run \
  -perf-catalog tests/perf/queries/smoke.yaml \
  -perf-metrics-addr :19095
```

## Perf Runner Execution (Code-first)

The perf runner is configured by code in `posthog-cloud-infra`:

- Terraform creates `duckling-perf-runner`, `duckling-perf-duckgres`, and perf artifact bucket resources.
- Ansible role `ansible/roles/perf_runner` installs the runner script plus a systemd timer.

From `posthog-cloud-infra` repository root, configure the runner:

```bash
ansible-playbook ansible/perf-runner-setup-playbook.yml \
  -i ansible/hosts/duckgres.yml \
  -e "target_hosts=managed_warehouse_perf_runner"
```

Run the perf job manually (code-first trigger, no SSH needed):

```bash
ansible-playbook ansible/perf-runner-run-playbook.yml \
  -i ansible/hosts/duckgres.yml \
  -e "target_hosts=managed_warehouse_perf_runner"
```

Nightly runs are handled by systemd timer `duckgres-perf-nightly.timer` installed by that same role.

GitHub Actions equivalents in `posthog-cloud-infra`:

- `Duckgres Config (Managed Warehouse)` configures duckgres + perf runner on deploy/update.
- `Duckgres Perf Runner` is a manual workflow dispatch that starts an on-demand perf run now.

## AWS Setup (Generic)

Provision and update AWS resources through `posthog-cloud-infra` (Terraform/Terragrunt), not ad-hoc AWS CLI scripts.

Use the managed warehouse stack:

- `terraform/environments/aws-accnt-managed-warehouse/us-east-1/duckling-example`

From the `posthog-cloud-infra` repository root:

```bash
set -euo pipefail

STACK_DIR="terraform/environments/aws-accnt-managed-warehouse/us-east-1/duckling-example"
cd "$STACK_DIR"

terragrunt plan
terragrunt apply
```

After apply, retrieve the perf-harness connection/storage values from Terragrunt outputs:

```bash
set -euo pipefail

RDS_ENDPOINT="$(terragrunt output -raw rds_endpoint)"
DUCKLAKE_BUCKET="$(terragrunt output -raw bucket_name)"
PERF_ARTIFACTS_ROOT_URI="$(terragrunt output -raw perf_artifacts_root_uri)"

echo "RDS endpoint: ${RDS_ENDPOINT}"
echo "DuckLake object store bucket: s3://${DUCKLAKE_BUCKET}/data/"
echo "Perf artifacts root URI: ${PERF_ARTIFACTS_ROOT_URI}"
```

Use `PERF_ARTIFACTS_ROOT_URI` for publishing nightly run artifacts.

## Athena Path Contract (v1)

Nightly uploads publish the canonical Athena source object at:

`s3://<perf-artifacts-bucket>/perf-athena/query_results/dataset_version=<v>/run_date=<YYYY-MM-DD>/query_results-<run_id>.csv`

The source CSV schema is fixed to:

`query_id,intent_id,protocol,status,error,error_class,rows,duration_ms,started_at`

`summary.json` is also uploaded for future dashboard expansion under:

`s3://<perf-artifacts-bucket>/perf-athena/summary/dataset_version=<v>/run_date=<YYYY-MM-DD>/summary-<run_id>.json`

## Grafana (Manual Athena Datasource Setup)

1. Apply the managed warehouse Athena stack:
   `terraform/environments/aws-accnt-managed-warehouse/us-east-1/athena-perf`
2. Get role ARN from output `duckgres_perf_athena_reader_role_arn`.
3. In Internal Grafana, create an Athena datasource:
   - Datasource type: `Amazon Athena`
   - Region: `us-east-1`
   - Catalog: `AwsDataCatalog`
   - Database: `duckgres_perf`
   - Workgroup: `duckgres-perf`
   - Assume role ARN: `duckgres_perf_athena_reader_role_arn`
4. Set datasource UID to match dashboard variable default (`athena-duckgres-perf`) or update dashboard variable `ds_athena` accordingly.

## Frozen Dataset Bootstrap

Use repository helper script (no cloud provisioning) to deterministically regenerate data and publish dataset metadata:

```bash
DUCKGRES_PERF_PGWIRE_DSN="host=127.0.0.1 port=5432 user=perfuser dbname=test sslmode=require" \
DUCKGRES_PERF_DATASET_VERSION=v1 \
./scripts/bootstrap_ducklake_frozen_dataset.sh
```

Optional generation controls:

- `DUCKGRES_PERF_CATEGORIES_COUNT`
- `DUCKGRES_PERF_PRODUCTS_COUNT`
- `DUCKGRES_PERF_CUSTOMERS_COUNT`
- `DUCKGRES_PERF_ORDERS_COUNT`
- `DUCKGRES_PERF_ITEMS_PER_ORDER`
- `DUCKGRES_PERF_EVENTS_COUNT`
- `DUCKGRES_PERF_PAGE_VIEWS_COUNT`
- `DUCKGRES_PERF_GENERATOR_VERSION`
- `DUCKGRES_PERF_ALLOW_OVERWRITE=1` (replace an existing dataset version row)

The script writes metadata to `ducklake.main.dataset_manifest` (override with `DUCKGRES_PERF_DATASET_MANIFEST_TABLE`).

## Manifest Expectations

- `DUCKGRES_PERF_DATASET_VERSION` must resolve to at least one row in `DUCKGRES_PERF_DATASET_MANIFEST_TABLE`.
- Query corpus for frozen runs must remain SELECT-only and target `ducklake.main.*`.
- Run artifacts should include `dataset_manifest.json` with dataset version and manifest table used.

## Failure Recovery

1. If run hangs: collect `runner.log`, Duckgres logs, and `server_metrics.prom`.
2. If protocol errors spike: rerun with explicit catalog (`tests/perf/queries/smoke.yaml` or `tests/perf/queries/ducklake_frozen.yaml`) and inspect per-query rows/errors.
3. If manifest check fails: verify `ducklake.main.dataset_manifest` includes the dataset version and rerun.
4. If artifact corruption occurs: rerun with a new `DUCKGRES_PERF_RUN_ID` and preserve the failed run directory.
