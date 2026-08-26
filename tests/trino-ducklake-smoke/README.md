# Trino DuckLake Smoke Test

`just trino-ducklake-smoke` creates the existing local DuckLake fixture through
Duckgres, then starts one pinned Trino coordinator with the pinned Brikk
DuckLake connector. It verifies that read-only Trino credentials can discover
and query the same PostgreSQL metadata catalog and MinIO data path.

The test is intentionally separate from the normal integration suite because
it downloads and starts Trino. It uses only local Docker services and test
credentials; it does not contact managed-warehouse infrastructure.

The `just` recipe writes the version artifact to
`artifacts/trino-ducklake-smoke/`. Set `TRINO_DUCKLAKE_SMOKE_ARTIFACT_DIR` to
override it, for example:

```bash
TRINO_DUCKLAKE_SMOKE_ARTIFACT_DIR=artifacts/my-trino-ducklake-smoke \
  just trino-ducklake-smoke
```

## Local DuckLake performance comparison

Run the opt-in local comparison between Duckgres PGWire and Trino HTTP against
the same synthetic DuckLake events table:

```bash
just perf-trino-ducklake
```

It seeds 1,000,000 deterministic events by default, warms each engine once,
checks that result fingerprints match, then records three measured executions
per query. Set `TRINO_DUCKLAKE_PERF_ROWS` to a value from 1 through 5,000,000
to change the dataset size. Artifacts are written to
`artifacts/trino-ducklake-perf/` unless `TRINO_DUCKLAKE_PERF_ARTIFACT_DIR` is
set.

This is a local end-to-end comparison, including PGWire or HTTP client overhead
and local Postgres/MinIO access. It is not a production capacity benchmark.

For wide, PostHog-shaped synthetic events, use the explicit realistic profile:

```bash
just perf-trino-ducklake-realistic
```

The default `realistic-smoke` profile writes 100,000 events across timestamp
partitions. It includes wide valid JSON event payloads, person payloads,
optional group payloads, and an events/persons join. Set
`TRINO_DUCKLAKE_REALISTIC_PERF_PROFILE=realistic-local` for 1,000,000 events,
or override `TRINO_DUCKLAKE_REALISTIC_PERF_ROWS` (up to 1,000,000).

If a previous local run leaves the coordinator unhealthy, inspect its logs and
recreate only the test services:

```bash
docker compose -f tests/integration/docker-compose.yml logs trino
docker compose -f tests/integration/docker-compose.yml rm -sf trino trino-metadata-reader-init
```
