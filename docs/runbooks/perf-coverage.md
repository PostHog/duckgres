# Workload coverage benchmark

The workload coverage catalog supplements the original frozen table and
properties comparisons. It measures filtered unique users, grouped trends,
joins, event browsing, actor retrieval, a funnel, sampled-week retention,
behavioral cohorts, and read-side modeling queries. Modeling queries measure
the SELECT computation, not writes or a complete materialization job.

## Run

The regular `posthog_frozen_perf` scenario includes all five configurations.
For a focused DuckDB baseline, dispatch the same workflow on the PR branch:

```sh
gh workflow run scenario-dev.yml --ref <branch> \
  -f scenario=posthog_frozen_perf_coverage_uncached
```

The focused scenario builds the revision being tested, deploys an isolated
warehouse, registers the same frozen files, and executes only
`pgwire_uncached`. It does not require a running local database. Both scenarios
use one warmup and four measured repetitions per query, sequential execution,
and the workflow's explicit worker resource settings. "Uncached" disables the
DuckDB external file cache; it does not guarantee cold OS or storage caches.
See [cache settings](../../tests/perf/README.md#duckdb-cache-comparison).

Artifacts live in `perf-coverage/`, with a `-coverage` run ID suffix. The
original `perf/` and `perf-properties/` histories are preserved. Main-branch
runs publish coverage separately; PR runs retain downloadable artifacts.

## Dataset limits and validation

The frozen events are sampled days, not a continuous event history. Coverage
queries use populated fixture windows. In particular, retention measures a
return between sampled weeks; it is a performance workload, not a production
retention estimate. Do not interpret absent days as zero user activity.
Person enrichment uses tenant and person keys and handles duplicate person
records. Fixture checks must reject empty populations rather than silently
turning joins or funnels into cheap empty-result benchmarks.

## Record a comparison baseline

Keep the successful focused run URL, tested commit, catalog version/hash,
dataset version, worker resources, cache settings, and repetition counts
alongside sanitized per-query timing samples. Do not commit query result
values, fixture locations, credentials, tenant identifiers, or raw run logs.
GitHub artifacts expire, so retained timing samples must be sufficient to
recalculate each query's mean, median, and observed range.

Compare future runs by matching intent ID, protocol, fixture, resources, and
methodology. A query or window change needs a new versioned intent ID and a
new baseline. Report per-query ratios as well as the sum; a summed ratio is
weighted toward slow queries. Four repetitions do not establish tail latency.
The uncached DuckDB baseline is measured evidence only for that configuration;
it is not a measured Trino, cached DuckDB, or Athena baseline.

## Estimate the expanded full suite

For each added query, use its measured uncached mean as the initial planning
estimate for the warmup and four measured executions. With twelve queries and
five configurations, the equal-speed estimate is:

```text
additional query time = sum(uncached means) * 5 repetitions * 5 configurations
full workflow estimate = original full workflow time + additional query time
```

Report this explicitly as an equal-speed estimate, not a forecast of engine
performance. Include a sensitivity range for the other four configurations.
Account separately for fixture validation, setup, collection, and teardown;
the focused workflow's build/deployment time must not be multiplied by five.
Use the measured coverage phase wall time to check the sum-based estimate,
because warmups and harness overhead are not in `query_results.csv`.

## Failure recovery

Keep failed artifacts and query errors. An empty fixture population, SQL
compatibility failure, timeout, or platform error invalidates that query's
baseline; never record a failure as zero latency or change assertions to make
a failing platform look successful. Fix the cause and dispatch a fresh isolated
run. The workflow always tears down its stack; if cancellation interrupts
cleanup, follow the existing scenario runner cleanup runbook. Do not patch a
running benchmark or reuse a partially configured worker for the baseline.
