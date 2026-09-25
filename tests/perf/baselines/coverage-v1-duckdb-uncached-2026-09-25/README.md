# Coverage v1: uncached DuckDB baseline

This is the initial comparison base for the twelve coverage v1 intents on
`pgwire_uncached`. [Successful source run](https://github.com/PostHog/duckgres/actions/runs/36159233201)
measured commit `9c9f5f305f87b7f0d41c0b54d64e5648aeb8ef83` on 2026-09-25.
The focused scenario and catalog are unchanged by subsequent launch-path edits.

[Samples](samples.csv) retain all 48 successful measurements;
[manifest](manifest.json) records the catalog hash, dataset version, cache
settings, resource requests, methodology, and timing calculations. No query
result values or internal fixture locations are retained.

## Measured times

- Coverage phase: **87m06s**, including 12 warmups and 48 measurements; zero errors.
- Complete workflow: **1h42m45s** (creation through completed-run update).
- Time outside the coverage phase: **15m39s**, including build, deployment,
  fixture validation, artifact collection, and teardown. Coverage validation
  itself took **3m50s**.
- Sum of per-query means: **17m26s**. The two person joins account for **57.1%**.

| Query family | Mean (s) | Median (s) | Observed min–max (s) |
|---|---:|---:|---:|
| Daily unique users | 61.35 | 61.68 | 57.99–64.03 |
| Event trends | 38.86 | 39.16 | 36.72–40.39 |
| Person enrichment join | 361.08 | 362.75 | 348.63–370.20 |
| Person activity join | 235.86 | 235.82 | 229.27–242.52 |
| Recent events | 72.50 | 72.99 | 68.57–75.46 |
| Active actors | 41.93 | 42.27 | 40.39–42.77 |
| Ordered funnel | 67.24 | 67.42 | 64.07–70.07 |
| Sampled-week retention | 42.17 | 43.28 | 37.92–44.19 |
| Repeat-activity cohort | 21.09 | 21.76 | 18.82–22.01 |
| Exclusion cohort | 27.72 | 28.50 | 25.09–28.80 |
| Daily-person model | 22.81 | 23.04 | 20.99–24.17 |
| Session model | 53.31 | 52.58 | 51.32–56.74 |

## Expanded campaign estimate

Keep the original nightly suite separate and dispatch coverage once per
configuration. With one warmup plus four measurements, equal query speeds
would add **7h16m of query work** across five configurations. Using the measured
15m39s overhead for each dispatch adds another **1h18m**. Adding the
[original 63m20s workflow](https://github.com/PostHog/duckgres/actions/runs/36113843696)
gives **about 9h37m total**, or **8h34m additional**.

This is a serial planning estimate, not a measured result for the other engines.
Their setup, warmup, and query costs may differ. It excludes queue delays.
Holding DuckDB uncached fixed and varying the other four configurations:

| Other configurations' query durations relative to uncached DuckDB | Estimated campaign |
|---|---:|
| 0.5× | 6h43m |
| 1× | 9h37m |
| 2× | 15h26m |

These are scenarios, not statistical confidence bounds. Even the equal-speed
query work exceeds the four-hour scenario limit, which is why coverage uses
separate target dispatches. This baseline does not establish Trino or Athena
performance. Resource requests are 3 CPU / 12Gi for DuckDB workers only.

## Future comparisons

Compare matching intent IDs and protocol using the same catalog hash, fixture,
resource requests, and cache settings. Report per-query mean/median ratios and
absolute times; a total ratio is dominated by slow queries. A changed query or
window requires a new intent version and baseline. Four repetitions do not
establish tail latency. See the [runbook](../../../../docs/runbooks/perf-coverage.md).
