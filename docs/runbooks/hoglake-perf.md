# Hoglake full-corpus benchmark

The `trino_hoglake` target runs every paired query in
`tests/perf/queries/ducklake_posthog_tables.yaml` through the native Hoglake
connector. SQL templates are shared with DuckLake. The current corpus has seven
intents, one warmup iteration and four measured iterations (28 measured results).
Adding a paired query automatically adds it to the Hoglake run.

## Prepare the dataset and catalogs

Use a Trino image containing `plugin/hoglake`, pinned by digest, and run the
Hoglake repository's `server` integration suite with
`./gradlew :trino:test -PhoglakeTrinoImage=<image>` before collecting a baseline.
Record the image digest, Hoglake revision, dataset version, worker count, CPU and
memory with the run. Match the other benchmark deployments' resource budgets.

The coordinator needs two catalogs accessible by the benchmark principal:

- A `hoglake` connector catalog containing `posthog.events` and `posthog.persons`.
- A `ducklake` connector reference catalog containing the same immutable rows and
  column values under the same schema/table names.

Configure the first catalog using the connector's documented `hoglake.uri`,
`hoglake.catalog`, and S3 settings. Supply credentials through the deployment's
secret mechanism. Hoglake provisioning is explicit: the existing warehouse
provisioner creates DuckLake catalogs and cannot create this catalog for you.

The fixture importer in `tests/perf/datasets/hoglake/` copies immutable source
Parquet bytes into a dedicated destination prefix and registers them through
Hoglake's REST API. It does not omit columns that the current queries happen not
to select. Unsupported or unsafe schemas fail setup explicitly; in particular,
unsigned 64-bit values must be proven to fit signed 64-bit integers before the
connector can read them without loss. See the importer's README for setup and
recovery instructions.

Keep the dataset fixed throughout validation and measurement. The connector
currently pins each table handle separately; concurrent writes can invalidate a
cross-catalog correctness comparison. Lowercase fixture names avoid its current
mixed-case identifier limitation. Deletion-vector files are unsupported.

## Run

Set these environment variables through your local environment or secret store:

| Variable | Value |
| --- | --- |
| `DUCKGRES_SCENARIO_ORG_ID` | Benchmark run identity |
| `DUCKGRES_SCENARIO_HOGLAKE_TRINO_URL` | HTTPS coordinator origin |
| `DUCKGRES_SCENARIO_HOGLAKE_TRINO_CATALOG` | Hoglake connector catalog name |
| `DUCKGRES_SCENARIO_HOGLAKE_REFERENCE_CATALOG` | Different DuckLake reference catalog name |
| `DUCKGRES_SCENARIO_HOGLAKE_TRINO_USERNAME` | Principal allowed to read both catalogs |
| `DUCKGRES_SCENARIO_HOGLAKE_TRINO_PASSWORD` | Principal password |
| `DUCKGRES_SCENARIO_TRINO_CA_CERT` | CA certificate file for verified TLS |
| `DUCKGRES_SCENARIO_DATASET_VERSION` | Immutable fixture version |

```bash
just scenario scenario=tests/mw-dev/scenario/scenarios/posthog_frozen_perf_hoglake.yaml
```

This standalone scenario uses the existing deployment. It neither provisions
nor deprovisions a warehouse, and does not require control-plane or PGWire
connection variables. Startup checks that the target and reference catalogs use
the expected connectors. Default schema is `posthog`; startup timeout is two
minutes and polling interval is two seconds. `DUCKGRES_SCENARIO_MAX_RUNTIME`
bounds the entire scenario (default 30 minutes).

## Correctness and coverage

Before timing each distinct SQL/argument combination, the driver executes it on
both catalogs and compares all returned values as a multiset, including duplicate
rows. It retains hashes instead of result values. Comparison ignores row order,
normalizes timestamp zones, and compares numeric values exactly. An approximate
aggregate with engine-dependent rounding may therefore report a mismatch; do not
silently exclude it. Successful validation is reused for later iterations in the
same run. Validation also warms the data, so these are warmed-query measurements,
not cold-start measurements.

Every query remains in the corpus. Unsupported queries and correctness mismatches
are recorded as failures; the remaining queries still execute. The scenario fails
after writing artifacts if any measured query failed. Failed durations are not
valid latency samples. Predicate pushdown is not implemented by the connector;
selective queries remain in the corpus to measure the resulting cost.

Results use protocol `trino_hoglake` and query suffix `__hoglake_table`, preserving
the shared intent IDs for comparison. Inspect `query_results.csv` and
`summary.json` under the scenario artifact directory. Confirm all seven intents
and all 28 measured results are present for the current corpus. Trino currently
reports elapsed query duration and output row count; provider-side planning time
and scanned-byte metrics are not populated by this driver.

If startup fails, verify the image/plugin, catalog names, TLS trust and principal
access. If correctness fails, investigate the dataset/schema or connector before
using its timings. An interrupted run leaves the explicitly managed deployment
and imported fixture in place; use a new run ID when retrying and keep incomplete
artifacts separate from completed runs. Do not publish credentials, source paths,
customer values or internal endpoints in benchmark artifacts or PRs.
