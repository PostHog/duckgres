# Perf Harness

This package contains the golden-query performance harness.

## Protocol Drivers

Catalogs may target `pgwire`, `trino`, or both. Both drivers execute the same
rendered statement stored in the existing `pgwire_sql` catalog field; the
legacy field name is retained for catalog compatibility and must not be used
to create a second, protocol-specific query definition. Keep shared benchmark
SQL within the intersection supported by DuckDB and Trino.

The Trino driver requires an HTTPS coordinator and always verifies its TLS
certificate. It uses system roots by default, or the explicitly configured CA
certificate file for an isolated cluster. Before measurement it runs an
authenticated `SELECT 1` outside query timing, retrying for up to 2 minutes at
2-second intervals by default. This absorbs the bounded delay between Trino
readiness, Kubernetes Secret projection, and file-authenticator refresh.

When a catalog targets multiple protocols, the runner completes all warmup and
measured iterations for one protocol before starting the next protocol in the
catalog's declared target order. This keeps each protocol's connection and
worker cache active throughout its measurements and prevents slow queries in
one protocol from changing another protocol's cache context.

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
  ducklake_table:
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

Paired catalogs must declare exactly the `raw_view` and `ducklake_table`
variants. A template expands in declaration order, with `raw_view` before
`ducklake_table`, into `q_events_daily__raw_view` and
`q_events_daily__ducklake_table`. Generated queries retain the same
`intent_id`, tags, parameters, and semantic template; only declared relation
placeholders differ. They carry in-memory storage-target metadata, so later
code does not need to infer the target from the generated ID. Legacy queries
remain unpaired. The v1 artifact and publisher schemas remain unchanged, so
artifact rows distinguish paired targets only by these generated query IDs;
they do not include a storage-target column.

During measured execution, the runner alternates every generated pair by
iteration: odd iterations run `raw_view` then `ducklake_table`, and even
iterations run `ducklake_table` then `raw_view`. Paired benchmark catalogs
should therefore use an even `measure_iterations` value so each target runs
first the same number of times. The catalog loader rejects odd measurement
counts for paired catalogs. Warmup work and legacy queries retain catalog
order. Query and intent IDs must be versioned when their measurement
methodology changes so historical latency series do not mix different cache
contexts, including dashboards that aggregate by intent.

Templating is intentionally limited to `{{ relation "<role>" }}`. Each role
must have a binding in both variants, and multiple roles may be used in one
template. Bindings are unquoted, dot-separated identifiers such as
`posthog.events`; the loader validates every identifier segment and emits it
as a safely quoted relation. SQL expressions, comments, semicolons,
whitespace, quoted identifiers, and malformed names are rejected in bindings;
all template actions other than the relation placeholder are rejected.
Placeholder syntax inside SQL strings, quoted identifiers, or comments is also
rejected so a target cannot be mislabeled without changing the executed
relation. The rendered SQL must be a single read-only `SELECT` statement and is
stored in the PGWire SQL field.

This abstraction preserves the artifact contract while allowing downstream
dashboards to compare paired targets by their generated query-ID suffixes.

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

By default the harness auto-starts a temporary local Duckgres control plane,
executes queries over pgwire, then shuts it down after artifact generation.

For frozen DuckLake dataset smoke runs, set:

```bash
DUCKGRES_PERF_DATASET_VERSION=v1 \
DUCKGRES_PERF_PGWIRE_DSN="host=127.0.0.1 port=5432 user=perfuser dbname=test sslmode=require" \
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
- `DUCKGRES_PERF_PGWIRE_DSN` is required
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
