# Perf Harness

This package contains the golden-query performance harness.

## Paired Query Catalogs

Existing catalogs continue to use `queries:` unchanged. A catalog may contain
legacy `queries:`, `paired_queries:`, or both. Paired definitions let one
semantic SQL template run against the frozen raw Parquet views and the
production-shaped DuckLake tables without changing the runner or artifact
contracts:

```yaml
relation_variants:
  raw_view:
    events: frozen_v1.events_file_view
    persons: frozen_v1.persons_file_view
  managed_table:
    events: posthog.events
    persons: posthog.persons

paired_queries:
  - query_id_base: q_events_daily
    intent_id: ph.events.daily.v1
    tags: [posthog, events, time-series]
    params: {}
    sql_template: |
      SELECT date_trunc('day', "timestamp") AS day, COUNT(*) AS events
      FROM {{ relation "events" }}
      WHERE "timestamp" >= TIMESTAMPTZ '2026-03-01 00:00:00+00'
        AND "timestamp" < TIMESTAMPTZ '2026-03-18 00:00:00+00'
      GROUP BY 1
      ORDER BY 1
```

Paired catalogs must declare exactly the `raw_view` and `managed_table`
variants. A template expands in declaration order, with `raw_view` before
`managed_table`, into `q_events_daily__raw_view` and
`q_events_daily__managed_table`. Generated queries retain the same
`intent_id`, tags, parameters, and semantic template; only declared relation
placeholders differ. They carry in-memory storage-target metadata, so later
code does not need to infer the target from the generated ID. Legacy queries
remain unpaired. The v1 artifact and publisher schemas remain unchanged, so
artifact rows distinguish paired targets only by these generated query IDs;
they do not include a storage-target column.

Templating is intentionally limited to `{{ relation "<role>" }}`. Each role
must have a binding in both variants, and multiple roles may be used in one
template. Bindings are unquoted, dot-separated identifiers such as
`posthog.events`; the loader validates every identifier segment and emits it
as a safely quoted relation. SQL expressions, comments, semicolons,
whitespace, quoted identifiers, and malformed names are rejected in bindings;
all template actions other than the relation placeholder are rejected. The
rendered SQL must be a single read-only `SELECT` statement and is copied into
both current protocol SQL fields.

This is catalog abstraction only. Fair scheduling, migration of the real
frozen PostHog query catalog, paired artifacts, dashboards, and Grafana work
are deliberately deferred to later PRs.

## Local Smoke Run

```bash
./scripts/perf_smoke.sh
```

This runs:

```bash
go test ./tests/perf \
  -run TestGoldenQueryPerformanceHarness \
  -perf-run \
  -perf-catalog tests/perf/queries/smoke.yaml
```

By default the harness auto-starts a temporary local Duckgres control-plane
instance with Flight ingress, executes queries over both protocols, then shuts
it down after artifact generation.

For frozen DuckLake dataset smoke runs, set:

```bash
DUCKGRES_PERF_DATASET_VERSION=v1 \
DUCKGRES_PERF_PGWIRE_DSN="host=127.0.0.1 port=5432 user=perfuser dbname=test sslmode=require" \
DUCKGRES_PERF_FLIGHT_ADDR="127.0.0.1:50051" \
./scripts/perf_smoke.sh
```

When `DUCKGRES_PERF_DATASET_VERSION` is set:

- default catalog switches to `tests/perf/queries/ducklake_frozen.yaml`
- manifest verification is required in `ducklake.main.dataset_manifest` (override with `DUCKGRES_PERF_DATASET_MANIFEST_TABLE`)
- the harness writes and validates `dataset_manifest.json` under `artifacts/perf/<run_id>/` before any configured publish step

Artifacts are written to `artifacts/perf/<run_id>`:

- `summary.json`
- `query_results.csv`
- `server_metrics.prom`
- `runner.log`
- `dataset_manifest.json` (only when `DUCKGRES_PERF_DATASET_VERSION` is set)

## Artifact Schema Contract (v1)

`query_results.csv` is the canonical per-query artifact and its columns are fixed in v1:

- `query_id`
- `intent_id`
- `measure_iteration`
- `protocol`
- `status`
- `error`
- `error_class`
- `rows`
- `duration_ms`
- `started_at`

`measure_iteration` is the 1-based measured repetition within a run (`0` is reserved for non-measured warmup work and is not emitted to the CSV today).
`duration_ms` is emitted as milliseconds with fixed precision, and `started_at` is UTC RFC3339Nano.
No CSV schema mutation is expected in this phase.

## Nightly Run

```bash
./scripts/perf_nightly.sh
```

Nightly uses lock/timeout guards:

- `DUCKGRES_PERF_LOCK_FILE` (default: `/tmp/duckgres-perf-nightly.lock`)
- `DUCKGRES_PERF_MAX_RUNTIME_SECONDS` (default: `3600`)

Nightly frozen dataset requirements:

- `DUCKGRES_PERF_DATASET_VERSION` is required
- `DUCKGRES_PERF_PGWIRE_DSN` and `DUCKGRES_PERF_FLIGHT_ADDR` are required
- default catalog is `tests/perf/queries/ducklake_frozen.yaml`
- `dataset_manifest.json` must exist after run and match the configured dataset version

Optional artifact publisher:

- `DUCKGRES_PERF_PUBLISH_DSN`: enables post-run publishing into a Duckgres writer.
- `DUCKGRES_PERF_PUBLISH_PASSWORD`: optional password override for the publisher connection.
- `DUCKGRES_PERF_PUBLISH_SCHEMA`: target schema for published rows. Default: `duckgres_perf`.
- `DUCKGRES_PERF_PUBLISH_BOOTSTRAP_SCHEMA`: when `true`, create/extend publisher tables before inserting.

## Useful Flags

- `-perf-run`: executes the harness test (otherwise it is skipped).
- `-perf-catalog`: catalog YAML path.
- `-perf-output-base`: base output directory.
- `-perf-run-id`: fixed run id.
- `-perf-pgwire-dsn`: use an existing PGWire endpoint instead of auto-start.
- `-perf-flight-addr`: use an existing Flight endpoint instead of auto-start.
