# Perf Harness

This package contains the golden-query performance harness.

Manual-only extended coverage scenarios exercise twelve additional workload shapes in
`queries/ducklake_posthog_coverage.yaml`. They retain separate `perf-coverage/`
artifacts and versioned intent IDs. These queries are excluded from standard
nightly runs and execute only when explicitly dispatched. For the focused uncached DuckDB run,
baseline recording, runtime estimates, and failure recovery, see the
[coverage runbook](../../docs/runbooks/perf-coverage.md).

## Protocol Drivers

Catalogs may target `pgwire`, `pgwire_uncached`, `pgwire_cached`, `trino`, `trino_cached`,
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

`posthog_frozen_perf` runs five separately labeled targets in one result set,
in order: **`pgwire_uncached` (baseline)**, `pgwire_cached`, `trino`, `trino_cached`, `athena`.
Both DuckDB variants use identical queries, worker resource requests, warmup
counts, and measured iterations. Each variant finishes before the next begins.
Dataset registration runs once and both Trino clusters share the same Hoglake
catalog. Legacy `pgwire` catalogs
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
end-to-end read path. The full-corpus paired catalog uses `_v5` query and
intent IDs to separate its methodology from `balanced_v4`, which also measured
raw Parquet views on PGWire and alternated them with the DuckLake tables on the
same worker, so the table measurements shared cache state with their raw twin.
The frozen
scenario retains this catalog and optionally appends the properties comparison described below.

## Paired Query Catalogs

Existing catalogs continue to use `queries:` unchanged. A catalog may contain
legacy `queries:`, `paired_queries:`, or both. Paired definitions let one
semantic SQL template run against the production-shaped relation each engine
exposes over the same frozen Parquet files, without changing the runner or
artifact contracts:

```yaml
relation_variants:
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

Paired catalogs always declare `ducklake_table`. Catalogs targeting Trino add
`hoglake_table`, and Athena catalogs add `athena_external`, whose generic table
names are resolved in the configured Glue database. A template expands in
stable order: `ducklake_table`, `hoglake_table`, then `athena_external`. Generated queries retain the same
`intent_id`, tags, parameters, and semantic template; only declared relation
placeholders differ. They carry in-memory storage-target metadata, so later
code does not need to infer the target from the generated ID. Legacy queries
remain unpaired. The v1 artifact and publisher schemas remain unchanged, so
artifact rows distinguish paired targets only by these generated query IDs;
they do not include a storage-target column.

Each protocol runs exactly one variant, so every iteration executes queries in
catalog order. Query and intent IDs must be versioned when their measurement
methodology changes so historical latency series do not mix different cache
contexts, including dashboards that aggregate by intent.

Templating is intentionally limited to `{{ relation "<role>" }}`. Each role
must have a binding in every declared variant, and multiple roles may be used in one
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
PGWire executes only `ducklake_table`; Trino executes only `hoglake_table`;
Athena executes only `athena_external`.

## Perf Gate Expectations

Any legacy or paired query may declare optional `expectations:`: per-protocol
bounds on the provider statistics in `query_service_metrics.csv`. Catalogs
without them are not checked.

```yaml
paired_queries:
  - query_id_base: q_events_total_v5
    intent_id: intent_events_total_v5
    sql_template: SELECT COUNT(*) AS events FROM {{ relation "events" }}
    expectations:
      trino:
        max_total_splits: 200
        max_bytes_scanned: 10MiB
```

Supported bounds are `max_total_splits` (Trino split count; Trino targets
only) and `max_bytes_scanned` (Trino physical input or Athena data scanned; a
byte count or a size in `KiB`, `MiB`, `GiB`, or `TiB`). Bounds are inclusive.
Latency is deliberately not boundable. The loader rejects unknown bound names,
protocols the query does not target, and entries without a bound, so a typo
cannot silently disable the gate. A paired query's bounds attach to the
variant each protocol executes (Trino: `hoglake_table`).

After the measured iterations, the `perf_queries` scenario step checks every
bound against each successful measured iteration of its (query, protocol)
pair. A bound that any iteration exceeds, or whose metric was not captured
for an iteration, fails the step with a message naming the query, protocol,
observed value, bound, and the worst iteration's Trino query ID, for example:

```text
perf gate failed: 1 expectation(s) violated:
- q_events_total_v5__hoglake_table on trino: total_splits 7300 exceeds max_total_splits 200 in 4 of 4 measured iterations (worst: iteration 1, Trino query <query-id>)
```

The check runs after the artifacts are written and is independent of
`fail_on_query_errors`; warmups and failed iterations are not checked, since
query errors are reported on their own.

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

## Artifact Schema Contract (v2)

`query_results.csv` is the canonical per-query artifact and its columns are:

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
- `representation` (empty for existing workloads; `json`, `struct`, or `variant` for properties)
- `run_label` (display name for the properties comparison; `protocol` retains its routing identifier)

`measure_iteration` is the 1-based measured repetition within a run (`0` is reserved for non-measured warmup work and is not emitted to the CSV today).
`duration_ms` is emitted as milliseconds with fixed precision, and `started_at` is UTC RFC3339Nano.
The publisher accepts both the original ten-column v1 header and the v2 header with the appended representation column.

`query_service_metrics.csv` is an additive sidecar. Provider-backed rows record
queue, planning, engine, and service time; bytes scanned; DPU count when the
service returns it; result reuse; and engine version. `query_results.csv`
remains the canonical latency/status artifact. Both CSVs append the representation label; stable query IDs also include it.

Trino rows (one per measured Trino iteration) come from the coordinator's
query info (`GET /v1/query/{queryId}?pruned=true`), read after the timed
window with the benchmark credentials. The driver learns each query's ID from
its client-protocol statement responses through a wrapping HTTP transport, so
no extra SQL runs. Column mapping: `queue_ms` = `queuedTime`, `planning_ms` =
`analysisTime` + `planningTime`, `engine_ms` = `executionTime`, `service_ms` =
`elapsedTime`, `bytes_scanned` = `physicalInputDataSize`. Columns appended
after `run_label`, blank on Athena rows:

- `total_splits`, `completed_splits` (Trino `totalDrivers`/`completedDrivers`,
  which the client protocol and web UI call splits)
- `physical_input_rows` (`physicalInputPositions`)
- `cpu_ms` (`totalCpuTime`)
- `peak_memory_bytes` (`peakUserMemoryReservation`)
- `engine_query_id` (the Trino query ID)
- `stats_source`: `query_info`, or `statement_stats` when the coordinator did
  not answer and the final statement response's statistics were recorded
  instead; that fallback has no `engine_ms` or `physical_input_rows`

Trino rows leave `dpu_count`, `result_reused`, and `engine_version` blank. A
query whose statistics could not be read at all has no row.

The publisher loads the sidecar when it exists (both the original 14-column
header and the current one) and replaces the run's rows in
`<schema>.query_service_metrics`. It only touches that table when the run has
service metrics rows, so PGWire-only runs keep publishing to schemas that were
bootstrapped before the table existed.

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

### Cached Trino comparison

The scheduled workflow and manual `posthog_frozen_perf` scenario use separate
uncached and cached benchmark clusters, in addition to the managed onboarding
cluster. Each has its own discovery service, catalog-store cell and cache volumes.
The managed catalog supplies the baseline properties but is never modified. Both
benchmark catalogs retain the tenant's authorized name and credentials, point to
one fixture Hoglake catalog containing `posthog` and `properties_perf` namespaces,
and read immutable S3 objects through the fixture Pod Identity. The runner creates
them once with explicit `fs.cache.enabled=false` and `true` respectively, and
verifies persisted properties before measurements. Teardown removes the isolated
stack and catalog metadata; it never deletes fixture objects.
Both clusters load the same Alluxio and memory cache managers.
Each node has a 16GB disk-cache budget in a 20Gi ephemeral volume, with 64kB pages
and a seven-day TTL. For connectors that use this cache, entries persist between
queries/iterations until eviction or teardown; one warm-up does not guarantee
every replica holds the complete working set. Worker CPU/memory limits remain
three workers at 1 CPU/4Gi per cluster; cache volumes reserve additional storage.

The frozen suite runs the newest PostHog/trino master build, whose Hoglake
connector honors `fs.cache.enabled` through Trino's shared filesystem module
(PostHog/trino#43). Builds before that change ignored the property, so every
`trino_cached` result from the pinned build, through the 2026-09-23 08:35 UTC
nightly, measured an uncached cluster under the cached label. The baseline also does not guarantee cold JVM, OS, or
storage-service caches. Keep the protocol labels separate in history.

Trino `physicalInputBytes` can include bytes served from cache. Use cache-manager
external-read/hit counters to distinguish storage traffic from cached reads.

## Published properties workload

The frozen-perf scenario runs its existing catalog and frozen dataset first,
unchanged. A `properties_comparison` step then compares browser properties on
its generated single-day fixture. By default, it reads the S3 location of the
existing `properties_events_supported` Athena table. Scheduled runs therefore
include properties without a workflow input. Override the location with
`DUCKGRES_SCENARIO_PROPERTIES_S3_URI` (workflow input `properties_s3_uri`); the
selected prefix must still match that table. A missing default fails the phase
instead of silently omitting measurements. No new repository secret is required.

Generate matching files for a modest full day before changing the table location.
Queries cover the entire selected prefix, with one warmup and four measured
iterations. Merely filtering a large mixed-day file set does not guarantee small
scans. The runner does not generate data or enforce a row-count limit.

The properties catalog measures JSON with `duckgres (vanilla)`, `duckgres (cache)`,
`trino (vanilla)` and `trino (cache)`, plus STRUCT with Athena, so every engine
covers every properties intent. Athena executes only STRUCT;
its complete ordered results must match the shared Duckgres/Trino JSON baseline
for each intent before properties measurements start. `trino (cache+variant)` is explicitly
unsupported until Hoglake supports VARIANT: its two comparisons emit `skipped`
rows with a reason and no timings. Skipped rows use iteration zero and are
excluded from measured/warmup query counts.

Both suites of one nightly publish under the same dataset version
(`posthog-file-views-v1`, shared through a YAML anchor in the scenario), with
three columns on `runs` and `query_results`:

- `suite` (`tables` | `properties` | `coverage`, a closed set in `core`, validated when the
  step starts so a typo fails before hours of measurement);
- `nightly_run_id`, the table-suite run's ID, which pairs a nightly's suites
  explicitly (a standalone run is its own nightly);
- `fixture_version`, the hash of the selected properties object inventory.

The properties suite keeps its own `perf-properties/` directory and a distinct
`-properties` run ID, so the publisher never overwrites one result set with the
other. `fixture_version` records which fixture each properties run measured; it
does not by itself stop a history chart from spanning a fixture change, so
regenerating the fixture is a deliberate history break. The publisher's schema
bootstrap adds the columns and classifies rows published before they existed
(by the `-properties` suffix they used to carry); rows with explicit values are
never touched. A summary without a suite publishes as `tables`. Only
main-branch runs publish to the shared database.

All properties preparation happens after the original result files are complete,
so a properties setup or validation failure cannot prevent their publication.
Failures still fail the scenario and trigger cleanup. See the
[properties runbook](../../docs/runbooks/properties-perf.md) for fixture preparation,
catalog registration, and recovery.

Scenario Trino connections require `DUCKGRES_SCENARIO_TRINO_CATALOG_STORE_CELL_ID`
(no default), matching the baseline coordinator's `catalog-store.cell-id`.
The isolated workflow supplies it; local runs must set it. The public readiness
API cell ID is used only for API identity validation, not catalog-store lookups.
