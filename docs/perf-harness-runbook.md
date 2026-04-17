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

4. Inspect `artifacts/perf/<run_id>/summary.json`, `query_results.csv`, and (for frozen dataset mode) `dataset_manifest.json`. The harness writes and validates the frozen dataset metadata artifact before publishing.

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

- The runner host remains the dedicated EC2 instance `duckling-perf-runner` in `aws-accnt-managed-warehouse-prod-us`.
- The benchmark target is the prod-us k8s Duckgres endpoint, currently `duckgres-db-mw-prod-us-e635a11acac3ee9c.elb.us-east-1.amazonaws.com`.
- Nightly result publishing writes the canonical `query_results.csv` artifact back into the prod-us k8s `perf` database in schema `duckgres_perf`.
- Ansible role `ansible/roles/perf_runner` installs the runner script plus a systemd timer.

From `posthog-cloud-infra` repository root, configure the runner:

```bash
ansible-playbook ansible/perf-runner-setup-playbook.yml \
  -i ansible/hosts/duckgres-perf-runner.yml \
  -e "target_hosts=managed_warehouse_perf_runner"
```

Treat this setup playbook as runner reconciliation. Run it after perf-runner code/config changes, or manually if the runner looks stale or drifted from the expected benchmark setup.

Run the perf job manually (code-first trigger, no SSH needed):

```bash
ansible-playbook ansible/perf-runner-run-playbook.yml \
  -i ansible/hosts/duckgres-perf-runner.yml \
  -e "target_hosts=managed_warehouse_perf_runner"
```

Nightly runs are handled by systemd timer `duckgres-perf-nightly.timer` installed by that same role.

GitHub Actions equivalents in `posthog-cloud-infra`:

- `Duckgres Perf Runner Config` reconciles the perf runner on perf-runner config changes or manual dispatch.
- `Duckgres Perf Runner` is a manual workflow dispatch that starts an on-demand perf run now.

## AWS Setup (Generic)

Provision and update AWS resources through `posthog-cloud-infra` (Terraform/Terragrunt), not ad-hoc AWS CLI scripts.

Use the prod-us managed warehouse runner stack:

- `terraform/environments/aws-accnt-managed-warehouse-prod-us/us-east-1/duckling-perf-runner`

Do not provision a dedicated `duckling-perf` EC2 benchmark target stack. The benchmark target is the prod-us k8s Duckgres deployment plus the `perf` org/team provisioned through the Duckgres control-plane/admin path.

From the `posthog-cloud-infra` repository root, if you need to reprovision or update the runner host itself:

```bash
set -euo pipefail

terragrunt run-all plan \
  --working-dir terraform/environments/aws-accnt-managed-warehouse-prod-us/us-east-1 \
  --queue-include-dir duckling-perf-runner

terragrunt run-all apply \
  --working-dir terraform/environments/aws-accnt-managed-warehouse-prod-us/us-east-1 \
  --queue-include-dir duckling-perf-runner
```

After runner changes, reconcile the perf runner from the same `posthog-cloud-infra` repo:

```bash
ansible-playbook ansible/perf-runner-setup-playbook.yml \
  -i ansible/hosts/duckgres-perf-runner.yml \
  -e "target_hosts=managed_warehouse_perf_runner"
```

This configures the runner with:

- benchmark target: prod-us k8s Duckgres endpoint
- publish target: the same prod-us k8s `perf` database
- publish table: `duckgres_perf.query_results`

## Publish Path Contract (v2)

Nightly runs still emit canonical local artifacts under:

`artifacts/perf/<run_id>/`

When publisher config is set, the perf harness publishes `query_results.csv` into:

`duckgres_perf.query_results`

on `duckling-posthog` over PGWire.

Rows are stored with:

- `run_id`
- `query_id`
- `intent_id`
- `protocol`
- `status`
- `error`
- `error_class`
- `rows`
- `duration_ms`
- `started_at`
- `dataset_version`
- `run_date`

The source CSV schema is fixed to:

`query_id,intent_id,measure_iteration,protocol,status,error,error_class,rows,duration_ms,started_at`

`summary.json` remains a local run artifact and is also consumed by the publisher to populate run-level dashboard metadata.
For frozen runs, `dataset_manifest.json` is written and validated by the harness before publisher writes begin.

Publisher configuration:

- `DUCKGRES_PERF_PUBLISH_DSN`
- `DUCKGRES_PERF_PUBLISH_PASSWORD`
- `DUCKGRES_PERF_PUBLISH_SCHEMA`
- `DUCKGRES_PERF_PUBLISH_BOOTSTRAP_SCHEMA`

## Grafana

The repo-managed dashboard now lives in `grafana-dashboards` under `managed-warehouse/duckgres-perf.json` and queries
`duckgres_perf.query_results` through the existing prod-us datasource.

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
2. If the runner configuration looks stale or drifted after code or environment changes: rerun `ansible/perf-runner-setup-playbook.yml` against `ansible/hosts/duckgres-perf-runner.yml` before retrying the benchmark.
3. If protocol errors spike: rerun with explicit catalog (`tests/perf/queries/smoke.yaml` or `tests/perf/queries/ducklake_frozen.yaml`) and inspect per-query rows/errors.
4. If manifest check fails: verify `ducklake.main.dataset_manifest` includes the dataset version and rerun.
5. If artifact corruption occurs: rerun with a new `DUCKGRES_PERF_RUN_ID` and preserve the failed run directory.
