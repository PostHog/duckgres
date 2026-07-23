# Events Row-Group Byte-Cap Benchmark

This is a one-off comparison of 128 MiB and 1 GiB Parquet row-group byte caps for the wide, variable-length events schema. It was run from temporary branch `codex/events-rowgroup-perf-scenario` at commit `b7202de88ceed023d8fb09e0ab18ea55b5c5309c` in [scenario-dev run 30033179471](https://github.com/PostHog/duckgres/actions/runs/30033179471) on 2026-07-23.

## Design

- Both DuckLake tables contain the same 524,288 frozen event rows and all 25 columns.
- Both tables use one file, Zstandard compression, Parquet V2, a 250,000-row upper bound, and a 10 GB target file size.
- The only row-group setting difference is `parquet_row_group_size_bytes`: 128 MiB for the baseline and 1 GiB for the candidate.
- The candidate is copied from the completed baseline table rather than independently selecting source rows.
- The isolated worker has 4 CPU and 16 GiB memory. The deployment has no node cache proxy, and the pinned benchmark connection disables DuckDB's external file cache.
- The run performs one warmup and six measured iterations. It reverses the full query order on alternating iterations, so each variant runs first three times per workload.
- Preflight validation confirmed equal row counts and full-row fingerprints, one file per table, the expected persisted writer options, and fewer physical row groups in the 1 GiB table. The artifact did not retain the exact row-group counts.

## Results

All six scenario steps and all 60 measured PGWire queries succeeded. There were no retries or query errors, and paired result-row counts were stable.

| Workload | 128 MiB median | 1 GiB median | Median paired speedup | Median-latency change | 1 GiB wins |
|---|---:|---:|---:|---:|---:|
| Five-minute timestamp count | 799 ms | 330 ms | 2.35x | 58.6% faster | 6/6 |
| One-hour minute series | 802 ms | 309 ms | 2.61x | 61.5% faster | 6/6 |
| Events grouped by name | 2,623 ms | 891 ms | 2.99x | 66.0% faster | 6/6 |
| Distinct persons | 1,375 ms | 436 ms | 3.23x | 68.3% faster | 6/6 |
| Wide properties scan | 6,193 ms | 7,036 ms | 0.90x | 13.6% slower | 0/6 |

The geometric mean of the five per-workload geometric-mean speedups is 2.22x, but that aggregate should not hide the wide-scan regression. Within each workload, the median speedup was similar whether the 128 MiB or 1 GiB variant ran first, so the result does not show a material order bias.

## Interpretation

The experiment confirms that larger row groups substantially reduce latency for selective and projected events queries under direct object-store access. It does not support 1 GiB as a universal optimum: the query that reads all large property columns consistently regressed, plausibly because larger groups reduce scan parallelism and increase each decode unit's working set.

The next useful comparison is 128 MiB versus intermediate 256 MiB and 512 MiB caps, keeping the 250,000-row ceiling. That can identify whether most of the projected-query benefit is available without the full 1 GiB wide-scan cost.

This is a balanced remote-read steady-state benchmark, not a pristine process-level cold start. DuckDB's external file cache and the node cache proxy were absent, but the test intentionally reused one worker and ran a warmup. It also covers one 524,288-row, one-file frozen sample rather than production's full file count and event-width distribution.
