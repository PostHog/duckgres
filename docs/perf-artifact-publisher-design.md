# Perf Artifact Publisher Design

## Summary

Replace the current shell-based artifact publish hook with a dedicated publisher
owned by the Duckgres perf harness. The publisher is an optional post-run step
that reads the generated perf artifacts from disk and writes run-level and
query-level results into a configured Duckgres instance.

This keeps artifact production and artifact ingestion in the same codebase,
removes `psql`/shell parsing failure modes, and makes the publish path testable.

## Problem

The current publish path is driven by:

- `scripts/perf_nightly.sh`
- harness-owned publish config
- runner configuration in `posthog-cloud-infra`

That approach is brittle because:

- it depends on shell quoting and environment propagation
- it mixes `psql` meta-commands and SQL in one heredoc
- it handles typed data through CSV import edge cases
- it creates schema and publishes data in a backend-specific script
- it is hard to test end-to-end from the Duckgres repository

The recent failures are examples of architectural fragility rather than isolated
syntax bugs:

- unsupported temp-table semantics
- `\copy` parser quirks
- file-path interpolation issues
- null/type conversion issues during CSV ingestion

## Goals

- Make publish a first-class optional feature of the perf harness.
- Read the canonical local artifacts from disk instead of relying on in-memory
  result state.
- Upload both `summary.json` and `query_results.csv`.
- Support idempotent re-publish for the same `run_id`.
- Keep local smoke runs working with publishing disabled by default.
- Keep nightly runs able to publish automatically when configured.
- Make the publish path unit-testable with fixture artifacts.

## Non-Goals

- Building a general-purpose ingestion framework.
- Replacing Grafana, Terraform, or runner orchestration.
- Designing a generic schema migration system for all Duckgres components.
- Uploading `server_metrics.prom` in the first phase.

## Proposed Architecture

Add a typed publisher package inside the perf harness and invoke it directly from
the harness after artifact generation when publish config is present.

Suggested structure:

- `tests/perf/publisher/`
  - artifact loader
  - config parsing
  - schema bootstrap helpers
  - transactional publish logic
  - tests
- `tests/perf/publisher/publisher.go`
- `tests/perf/publisher/publisher_test.go`

The harness remains responsible for:

- running the benchmark
- writing artifacts to `artifacts/perf/<run_id>/`
- deciding whether publishing is enabled

The publisher becomes responsible for:

- loading `summary.json`
- loading `query_results.csv`
- validating artifact schema
- connecting to the configured Duckgres writer endpoint
- ensuring the required tables exist
- replacing rows for the current `run_id` in one transaction

## Data Flow

1. Harness executes the perf catalog.
2. `core.ArtifactSink` writes:
   - `summary.json`
   - `query_results.csv`
   - `server_metrics.prom`
   - `runner.log`
3. Harness closes the sink and verifies artifacts exist.
4. If publishing is configured, harness calls publisher with `run_dir`.
5. Publisher reads artifacts from `run_dir`.
6. Publisher writes:
   - one row into `duckgres_perf.runs`
   - many rows into `duckgres_perf.query_results`
7. Harness reports publish success or fails the run, depending on configured mode.

## Why Read Artifacts From Disk

The publisher should consume the generated artifacts rather than internal Go
objects because:

- the artifacts are the published contract
- this validates that the publisher matches the real artifact format
- retries can be performed from an existing run directory
- local debugging is simpler because artifacts are inspectable
- publish logic is decoupled from runner internals

## Configuration

Publishing is disabled by default. It is enabled only when explicit publisher config
is provided.

Suggested flags:

- `-perf-publish-dsn`
- `-perf-publish-password`
- `-perf-publish-schema` default `duckgres_perf`
- `-perf-publish-bootstrap-schema` default `true`

Suggested environment variables for scripts:

- `DUCKGRES_PERF_PUBLISH_DSN`
- `DUCKGRES_PERF_PUBLISH_PASSWORD`
- `DUCKGRES_PERF_PUBLISH_SCHEMA`
- `DUCKGRES_PERF_PUBLISH_BOOTSTRAP_SCHEMA`

Behavior:

- if `dsn` is empty, publishing is skipped
- if `dsn` is set, publisher runs
- if publish fails, the harness fails the run with the publisher error

## Harness Integration

Add publisher configuration to `tests/perf/harness_test.go` flags alongside the
existing run configuration.

After artifact generation succeeds:

```text
if publishing is configured:
    publisher.Publish(runDir)
```

This is a direct Go call, not a shell hook.

For nightly runs:

- `scripts/perf_nightly.sh` should pass publisher env vars through to the harness

This keeps orchestration in shell but moves ingestion into typed code.

## Artifact Contract

Publisher input files:

- `summary.json`
- `query_results.csv`

Required CSV columns:

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

Required summary fields:

- `run_id`
- `dataset_version`
- `started_at`
- `finished_at`
- `total_queries`
- `total_errors`
- `warmup_queries`

Publisher should validate that:

- the `run_id` in `summary.json` matches the run directory identity used by the harness
- `dataset_version` is present, even if empty-string is disallowed by policy
- timestamps parse cleanly
- CSV headers match the expected schema exactly in v1

## Database Contract

Phase 1 target tables:

- `duckgres_perf.query_results`
- `duckgres_perf.runs`

Suggested semantics:

- `runs.run_id` is primary key
- `query_results` is keyed logically by `(run_id, query_id, measure_iteration, protocol)`
- publish is idempotent by deleting existing rows for `run_id` and reinserting within one transaction

## Schema Management

For phase 1, the publisher may include a small bootstrap step:

- `CREATE SCHEMA IF NOT EXISTS`
- `CREATE TABLE IF NOT EXISTS`
- additive `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` only where needed

That is acceptable because it is simple and self-contained for perf telemetry.

Longer term, if the schema evolves materially, schema management should move to
a separately versioned migration path. The publisher should remain responsible
for validating schema compatibility, not performing arbitrary destructive DDL.

## Publish Strategy

Preferred approach:

- use a normal Go SQL client over PGWire
- use prepared statements or batched inserts
- keep all writes inside one transaction

Recommended sequence:

1. Open transaction.
2. Bootstrap schema if enabled.
3. Delete `duckgres_perf.query_results` rows for `run_id`.
4. Delete `duckgres_perf.runs` row for `run_id`.
5. Insert the `runs` row.
6. Insert all `query_results` rows.
7. Commit.

This is simpler and more portable than temp tables plus external `\copy`.

For the expected row counts in current perf runs, row-by-row prepared inserts are
acceptable. If runs grow significantly, batched multi-row inserts can be added
later without changing the public interface.

## Error Handling

Uploader errors should be explicit and classified:

- artifact missing
- artifact parse error
- artifact schema mismatch
- connection/auth failure
- schema bootstrap failure
- transaction failure
- partial insert failure

The error message should include:

- `run_id`
- artifact path
- table name when relevant
- failing operation

Avoid backend-specific parser noise when a clearer application-level error can
be returned earlier.

## Retry and Recovery

Because the publisher reads from a completed run directory, recovery is simple:

- rerun publisher against the same `run_dir`
- transaction semantics guarantee either full replacement or no change

This also enables future tooling such as:

- `go test ./tests/perf -run Test... -perf-publish-only <run_dir>`
- a small standalone CLI wrapper for backfills

The core design should keep that option open even if phase 1 only wires publishing
through the harness.

## Testing Plan

Tests should live in the Duckgres repository and cover:

- successful publish from fixture artifacts
- missing `summary.json`
- missing `query_results.csv`
- malformed summary JSON
- unexpected CSV header
- null/empty `error`, `error_class`, and `rows`
- idempotent re-publish of the same `run_id`

At least one integration-style test should exercise:

- write fixture artifacts to a temp dir
- run publisher against a test database
- verify `runs` and `query_results` contents

## Implementation Plan

### [x] Phase 1: Add publisher package

- Create `tests/perf/publisher`.
- Add typed artifact reader helpers.
- Add publish config struct.
- Add transactional publish implementation.
- Add unit tests using fixture artifacts.

### [x] Phase 2: Integrate into harness

- Add perf publish flags to `tests/perf/harness_test.go`.
- Invoke publisher after artifact generation.
- Fail the run on publish error with a clear harness error.
- Document the new flags and env vars.

### [x] Phase 3: Update scripts and docs

- Update `scripts/perf_nightly.sh` to pass publisher config to the harness.
- Update `tests/perf/README.md` and `docs/perf-harness-runbook.md`.

### [x] Phase 4: Clean up external publisher script

- Remove the shell-based ingestion script from infra once the harness-integrated
  publisher is deployed and verified.

## Open Questions

- Should publish failure fail nightly runs by default, or only log and continue?
- Should schema bootstrap be enabled in production by default, or only in dev?
- Do we want a future `server_metrics.prom` ingestion path, or should that stay
  filesystem-only?
- Should the publisher support direct Flight writes in the future, or standardize
  on PGWire for ingest?

## Recommendation

Implement the publisher as a Go package called directly by the perf harness, with
publishing disabled unless explicitly configured. Read the artifacts from disk,
write both `runs` and `query_results` in one transaction, and keep schema
bootstrap narrow and additive. This gives a smaller, more testable, and more
robust system than continuing to evolve the shell/`psql` hook.
