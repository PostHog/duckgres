# Perf Harness

This package contains the golden-query performance harness.

## Protocol Drivers

Catalogs may target `pgwire`, `pgwire_uncached`, `pgwire_cached`, `trino`,
`athena`, or any supported combination. All drivers execute the same
rendered statement stored in the existing `pgwire_sql` catalog field; the
legacy field name is retained for catalog compatibility and must not be used
to create a second, protocol-specific query definition. Keep shared benchmark
SQL within the intersection supported by DuckDB, Trino, and Athena.

The Athena driver uses on-demand capacity in an explicitly configured
workgroup, database, and S3 result prefix. It disables result reuse for every
execution and rejects a response which reports reuse. Timing is end to end:
it includes queueing, engine execution, and paginated result retrieval. The
driver defaults to catalog `AwsDataCatalog`, a 500ms status poll interval, and
a 30-minute query timeout. Workgroup, database, result prefix, and AWS region
are explicit scenario settings. Cancellation attempts to stop any unfinished
query; Athena's independent server-side timeout still applies if that request
fails. Available terminal service statistics are retained even when execution,
result validation, or result retrieval fails. Executions without service
statistics do not emit a service-metrics row.

The Trino driver requires an HTTPS coordinator and always verifies its TLS
certificate. It uses system roots by default, or the explicitly configured CA
certificate file for an isolated cluster. Before measurement it runs an
authenticated `SELECT 1` outside query timing, retrying for up to 2 minutes at
2-second intervals by default. This absorbs the bounded delay between Trino
readiness, Kubernetes Secret projection, and file-authenticator refresh.

When a catalog targets multiple protocols, the runner completes all warmup and
measured iterations for one protocol before starting the next protocol in the
catalog's declared target order. This keeps each protocol's connection and
worker context active throughout its measurements and prevents slow queries in
one protocol from changing another protocol's cache context.

### DuckDB cache comparison

`posthog_frozen_perf` runs four separately labeled targets in one result set,
in order: **`pgwire_uncached` (baseline)**, `pgwire_cached`, `trino`, `athena`.
Both DuckDB variants use identical queries, worker resource requests, warmup
counts, and measured iterations. Each variant finishes before the next begins;
Trino and Athena run once, not once per cache mode. Legacy `pgwire` catalogs
keep their existing behavior and are not relabeled as uncached history.

Only the perf driver changes cache settings. It pins its PGWire connection and
applies `SET GLOBAL` before that variant's first warmup and outside query timing:

| Setting | Uncached baseline | Cached |
| --- | --- | --- |
| `enable_external_file_cache` | `false` | `true` |
| `parquet_metadata_cache` | `false` | `false` |
| `enable_http_metadata_cache` | `false` | `false` |

The cached variant matches DuckDB's default remote-file caching policy rather
than enabling metadata caches that ordinary workers leave off. Setup is lazy,
so the cached driver's construction cannot enable caching during the uncached
phase. A lost pinned connection fails the query instead of silently connecting
to an unconfigured worker. Worker startup and production configuration are
unchanged.

Use the isolated scenario workflow for this comparison. Local
`just scenario-frozen-perf` runs require a dedicated test warehouse: these
settings affect the underlying worker globally, and the two variants must not
run concurrently against a shared worker. The scenario's sequential phases
also work when connections reuse the same worker. If cache setup fails, inspect
the artifact errors and recreate the test stack; do not publish fallback runs
under either explicit cache-mode label. Teardown removes the test workers.

Warmup does not imply that the entire dataset fits in memory. Query-local
buffering, prefetching, DuckLake catalog caching, and separate cache
proxies/extensions are unaffected, so "uncached" here is not a fully cold
end-to-end read path. The paired query and intent IDs use `balanced_v4` to
separate this methodology from `balanced_v3`; the dataset is unchanged.

### Trino worker shape experiments

The `scenario-dev` workflow accepts a manual `trino_perf_shape` choice for
`posthog_frozen_perf`. Scheduled runs and callers that omit the choice retain
`baseline`. The three experiments vary worker size and total execution
resources independently:

| Shape | Workers | CPU / memory per worker | Total CPU / memory | Heap per worker | Query memory per worker / cluster |
| --- | ---: | --- | --- | --- | --- |
| `baseline` | 3 | 1 / 4 GiB | 3 / 12 GiB | 3 GiB | 2 / 6 GiB |
| `large` | 1 | 3 / 12 GiB | 3 / 12 GiB | 9 GiB | 6 / 6 GiB |
| `scaleout` | 6 | 1 / 4 GiB | 6 / 24 GiB | 3 GiB | 2 / 12 GiB |
| `large-scaleout` | 2 | 3 / 12 GiB | 6 / 24 GiB | 9 GiB | 6 / 12 GiB |

Worker CPU and memory requests equal their limits. The coordinator keeps its
existing resources, 2 GiB heap, and 1 GiB per-node query limit; its cluster
query-memory setting follows the selected budget. JVM heap headroom retains
Trino's default of 30% of the heap. Duckgres remains at 3 CPU / 12 GiB in the
workflow, so only `baseline` and `large` match its execution resource budget.
Coordinator and supporting-service resources are additional to the table.

Run the entire comparison with one dispatch on the PR branch (or `main` after
merging):

```bash
gh workflow run scenario-dev.yml --ref codex/trino-perf-shape-experiments \
  -f scenario=posthog_frozen_perf \
  -f trino_perf_shape=all
```

`all` builds the runner and Duckgres images once, then runs the four shapes as
a sequential matrix (`max-parallel: 1`). Each shape gets one temporary stack,
the existing warmup and four measured iterations, and teardown before the next
job starts. Numeric suffixes `1` through `4` on the workflow run ID give each
shape a separate namespace and warehouse identity. Each job has its own
270-minute timeout and fresh credentials. A failed shape does not cancel the
remaining jobs. This is one measurement round, with no repeated deployments
per shape. The Trino image remains pinned by the workflow.

The baseline runs the full cross-engine benchmark (uncached and cached
Duckgres, Trino, and Athena). The three nonbaseline shapes run only Trino
measurements, avoiding redundant Duckgres and Athena work. Each still performs
the same isolated warehouse provisioning, frozen-data setup, and table/view
validation. Trino SQL, query order, warmup, and four measured iterations are
unchanged. Nonbaseline shapes do not load Athena configuration or create the
scenario runner's Athena Pod Identity association.

The final `compare-shapes` job presents Trino medians, baseline-relative
speedups, and CPU-budget efficiency in one workflow summary and a downloadable
`trino-shape-comparison-<run>-<attempt>` artifact. Missing or failed results and
teardown failures are marked incomplete and fail the comparison job. Each
shape's raw artifact is named `scenario-dev-<run>-<attempt>-<shape>` and includes
`shape-result.json` with deployment, scenario, and teardown outcomes.

Individual choices remain available: replace `all` with `baseline`, `large`,
`scaleout`, or `large-scaleout`. Default and scheduled invocations execute just
`baseline`, with the full workload. Individual nonbaseline choices also run
Trino-only measurements. Compare identical query IDs and protocol labels from the current
`balanced_v4` catalog; do not mix them with older methodology.

The workflow title and summary identify the shape. The downloadable scenario
artifact includes `trino-perf-shape.json` with configured resource and image
provenance and `perf_mode` (`full` or `trino-only`), including on deployment
failure. A copy accompanies the collected
scenario results. Nonbaseline experiments and the entire `all` comparison
(including its baseline member) are artifact-only and do not publish to the
daily baseline's historical tables. Trino SQL, cache settings, warmup count,
and measured iteration count are identical across shapes; other protocols are
measured only in the baseline.

Use per-query median latency and allocated CPU-seconds (total worker CPU times
elapsed seconds) to compare speed and resource efficiency. The distinct-person
query is the primary diagnostic case. The harness excludes warmups from
`query_results.csv`; these results are measured iterations, not a separately
instrumented cold-cache benchmark. A topology speedup alone does not distinguish
GC, throttling, exchange traffic, scan throughput, or memory pressure: correlate
with execution telemetry before attributing its cause.

For local harness development, set `TRINO_PERF_SHAPE` to one concrete shape
alongside `SCENARIO_NAME=posthog_frozen_perf`, `E2E_SUITE=trino`, and the usual
isolated-stack environment, and use that same environment for `run.sh deploy`
and `run.sh test-scenario`. The test invocation checks any saved deployment
provenance, including the measurement mode derived from the selected shape.
`DUCKGRES_SCENARIO_PERF_MODE` is set by the harness and cannot override that
selection. The harness rejects a different shape, resource budget, or deployment
image; restore the deployment environment or redeploy before continuing. Use a separate
`SCENARIO_ARTIFACTS_DIR` for each local stack. `all` is a workflow selection,
not a shape accepted by `run.sh`. Install `jq` on the machine running `run.sh` to
write the JSON provenance (the GitHub runner already includes it). Explicitly
set the Duckgres worker variables to
`3` and `12Gi` to reproduce the workflow. The generic harness retains its
smaller Duckgres defaults. `just scenario-frozen-perf` uses an existing
warehouse and does not resize Trino. Unknown shapes and nonbaseline shapes
outside the isolated frozen-perf scenario are rejected before cloud mutations.

If deployment or testing fails, inspect the selected shape artifact and pod
events, then use the normal `run.sh teardown` with the same namespace and run
identity. Teardown remains available even with a malformed shape selection.
For an `all` run, use the suffixed `PR_NUMBER` and namespace shown in the failed
shape job, not the unsuffixed workflow run ID. Rerun the whole workflow for a
complete combined report; rerunning only failed jobs produces a new attempt
with missing shape artifacts, which the comparison intentionally rejects.
Rerun in a fresh isolated stack; a partially started worker pool is not a valid
measurement. Restore `baseline` to return to the daily configuration.

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

Paired catalogs without Athena declare exactly the `raw_view` and
`ducklake_table` variants. Athena catalogs add `athena_external`, whose generic
table names are resolved in the configured Glue database. A template expands
in stable order: `raw_view`, `ducklake_table`, then `athena_external`. Generated queries retain the same
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
PGWire executes `raw_view` and `ducklake_table`; Trino executes only
`ducklake_table`; Athena executes only `athena_external`.

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
- `query_service_metrics.csv`
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

`query_service_metrics.csv` is an additive sidecar. Provider-backed rows record
queue, planning, engine, and service time; bytes scanned; DPU count when the
service returns it; result reuse; and engine version. `query_results.csv`
remains the canonical latency/status artifact and keeps its v1 header
unchanged.

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
