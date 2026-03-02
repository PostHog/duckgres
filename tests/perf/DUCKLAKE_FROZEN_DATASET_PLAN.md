# DuckLake Frozen Dataset Plan (Nightly Perf)

## Goal

Run nightly perf on an immutable DuckLake dataset version so results are reproducible and comparable over time.

## Fixed Decisions

- Workload mode: read-only nightly perf.
- Data strategy: deterministic synthetic generation.
- Initial size target: ~10M fact rows.
- Schema strategy: reuse the existing schema model from `scripts/seed_ducklake.sh` (`categories`, `products`, `customers`, `orders`, `order_items`, `events`, `page_views`).

Note: "existing schema" means table definitions from the repo, not pre-existing data in the new store.

## Minimal Architecture

- One dedicated frozen perf DuckLake catalog (dedicated S3 + dedicated metadata DB).
- Dataset published as immutable versions (`v1`, `v2`, ...).
- Nightly job reads one selected version via `DUCKGRES_PERF_DATASET_VERSION`.

## Execution Status (2026-02-27)

- [ ] 1. Provision dedicated frozen perf infrastructure.
- Pending: dedicated S3 bucket/prefix for frozen DuckLake objects.
- Pending: dedicated Postgres/RDS metadata DB for frozen catalog.
- [x] 2. Add repo-side deterministic bootstrap helper for schema + synthetic data generation.
- Implemented: `scripts/bootstrap_ducklake_frozen_dataset.sh`.
- [x] 3. Add manifest metadata publication flow.
- Implemented: helper publishes to `ducklake.main.dataset_manifest` (or `DUCKGRES_PERF_DATASET_MANIFEST_TABLE`).
- [x] 4. Add frozen SELECT-only query catalog using DuckLake schema tables.
- Implemented: `tests/perf/queries/ducklake_frozen.yaml`.
- [x] 5. Wire smoke/nightly scripts to dataset version + manifest expectations.
- Implemented: `scripts/perf_smoke.sh` and `scripts/perf_nightly.sh` enforce version-aware catalog/manifest behavior.
- [x] 6. Document operator workflow and env contract.
- Implemented: `tests/perf/README.md` and `docs/perf-harness-runbook.md`.
- [ ] 7. Execute/publish first immutable dataset version (`v1`) on dedicated infra.
- Pending: requires Step 1 infrastructure provisioning.

## Step-by-Step TODO

1. Provision dedicated frozen perf infrastructure:
- Create S3 bucket/prefix for frozen DuckLake objects.
- Create dedicated Postgres/RDS metadata DB for the frozen catalog.

2. Bootstrap schema into the frozen catalog:
- Create the 7 tables listed above in the frozen catalog.

3. Implement deterministic large-scale data generator:
- Generate data from fixed seed and deterministic formulas.
- Target first baseline: `customers` ~1M, `products` ~100k, `orders` ~10M, `order_items` ~30M.

4. Load and freeze first version:
- Load generated data into frozen catalog.
- Publish as immutable `dataset_version` (for example `v1`).

5. Add dataset manifest metadata table:
- Store `dataset_version`, `created_at`, `generator_version`, table row counts.

6. Wire nightly perf job to dataset version:
- Require `DUCKGRES_PERF_DATASET_VERSION`.
- Fail fast if version missing in manifest.
- Run perf queries read-only against that version.

7. Add safety guardrails:
- Enforce SELECT-only query corpus for nightly mode.
- Block writes to frozen dataset version by policy.

8. Record version in perf artifacts:
- Include `dataset_version` in nightly summary output for traceability.

## Out of Scope (Current)

- Per-run wipe/restore pipelines.
- Mutable run catalogs for write benchmarks.
