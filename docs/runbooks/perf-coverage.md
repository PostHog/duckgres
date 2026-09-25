# Extended workload coverage benchmark

The workload coverage catalog supplements the original frozen table and
properties comparisons. It measures filtered unique users, grouped trends,
joins, event browsing, actor retrieval, a funnel, sampled-week retention,
behavioral cohorts, and read-side modeling queries. Modeling queries measure
the SELECT computation, not writes or a complete materialization job.

## Run

The regular `posthog_frozen_perf` nightly retains the original table and
properties workloads. The twelve additional queries are an **extended, manual-only
run**: scheduled nightly runs never include them. Start them explicitly through
`workflow_dispatch`. This command runs all five configurations:

```sh
gh workflow run scenario-dev.yml --ref <branch> \
  -f scenario=posthog_frozen_perf_extended
```

`coverage_target` defaults to `all`. To select one configuration, add
`-f coverage_target=pgwire_uncached`, `pgwire_cached`, `trino`, `trino_cached`,
or `athena`. Each selected configuration gets its own job, isolated deployment,
and artifact. Jobs execute sequentially to avoid benchmark resource contention;
a failed target does not cancel the others. Images are built once, and both
Trino configurations use the same resolved image digest.

Trino targets select the declarative `posthog_frozen_perf_extended_trino.yaml`
backing scenario and deploy the Trino lane. Other targets use the neutral lane.
Both reuse the existing frozen setup; Athena configuration also supplies the
frozen runner identity.

### Timeouts

Each target has a four-hour scenario limit and a 270-minute workflow job limit.
The Go process limit is 4h15m, leaving time for artifact collection and cleanup.
These limits apply independently to each job, not to the sum of all target jobs.
A single target can still time out; peer runtimes have not yet been measured.
The measured uncached DuckDB coverage phase took 87m06s.

Running all five targets sequentially inside one job would likely time out:
the equal-speed query-work estimate alone is 7h16m. The matrix avoids that
combined timeout without reducing queries or repetitions. Under the same
planning assumptions and shared initial builds, allow roughly **8h16m for the
extended workflow alone**, excluding queue delays. Standard nightly queries
are not included; running them as well brings the estimate to about **9h19m**.

For the focused uncached DuckDB baseline:

```sh
gh workflow run scenario-dev.yml --ref <branch> \
  -f scenario=posthog_frozen_perf_extended_uncached
```

Each target job executes all twelve queries with one warmup and four
measured repetitions, sequentially, using the same frozen files. The explicit
3 CPU / 12 GiB settings describe DuckDB workers, not equivalent resources across
engines. "Uncached" disables the DuckDB external file cache; it does not
guarantee cold OS or storage caches. See
[cache settings](../../tests/perf/README.md#duckdb-cache-comparison).

Target jobs have distinct benchmark run and nightly IDs. Group an all-target
campaign by its parent workflow URL and target-suffixed artifacts. Compare
matching recorded commits, catalog versions, dataset versions, resource settings,
and methodology. Across separate workflow dispatches, pin the same `trino_image`
digest when comparing Trino configurations.

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

The initial uncached DuckDB comparison base is the
[2026-09-25 coverage v1 baseline](../../tests/perf/baselines/coverage-v1-duckdb-uncached-2026-09-25/README.md).
It retains 48 successful measurements and the complete methodology. The query
phase took 87m06s; the workflow took 1h42m45s. The equal-speed expanded campaign
estimate recorded with that baseline was 9h37m across separate dispatches plus
the original suite. The current shared-build matrix lowers that planning estimate
to about 9h19m; this is not a new measured result.


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
full campaign estimate = original workflow time + additional query time
                         + shared build time + sum(non-build overhead for each target job)
```

Report this explicitly as an equal-speed estimate, not a forecast of engine
performance. Include a sensitivity range for the other four configurations.
The workflow builds images once; each target job repeats deployment, fixture
validation, collection, and teardown. Estimate those separately from query work.
For the initial baseline, the workflow's initial build phase was approximately
272 seconds and total time outside the query phase was 939.50 seconds. Using
`272 + 5 * (939.50 - 272)` for shared-build overhead gives the approximate
8h16m extended-only estimate above. This assumes other engines' setup and warmup
costs match the focused DuckDB run; actual costs can differ.
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
