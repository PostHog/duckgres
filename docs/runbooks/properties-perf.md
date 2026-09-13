# Properties in frozen performance scenarios

The existing `posthog_frozen_perf` and `posthog_frozen_perf_trino_cached`
scenarios register generated properties Parquet alongside the full frozen corpus.
Set `DUCKGRES_SCENARIO_PROPERTIES_S3_URI` to the directory containing the generated
Parquet files (the published dataset's `data/` prefix). There is no default.
No completion manifest or new repository secret is required.

## Prepare and run

```sh
just prepare-properties-perf "$DUCKGRES_SCENARIO_PROPERTIES_S3_URI"
just scenario-frozen-perf
```

Preparation lists Parquet objects recursively under that prefix and writes
`setup.sql`, `athena.sql`, `catalog.yaml`, and `dataset-version.txt` to
`/tmp/properties-perf`. The recipe's second argument changes the output directory;
scenario runs use `DUCKGRES_SCENARIO_PROPERTIES_OUTPUT_DIR`. Preparation has a
five-minute timeout. Keep generated files and dataset locations outside this
public repository.

In GitHub Actions, dispatch `scenario-dev` on the desired branch with
`scenario=posthog_frozen_perf` and `properties_s3_uri` set to the Parquet prefix.
This runs both ordinary and cached-Trino scenarios. To run cached Trino alone,
select `posthog_frozen_perf_trino_cached`. The workflow masks the input in logs
and uses its existing AWS role.

Duckgres registers `properties_perf.events_supported` and
`properties_perf.events_variant`; Hoglake registers the corresponding projections
in the scenario's catalog. Athena uses the precreated
`properties_events_supported` table in the configured benchmark database.
Provision that table with the generated `athena.sql` before the ordinary scenario;
preparation verifies its mapping. The cached-Trino scenario requires no Athena
configuration. All projections refer to the same Parquet prefix.

Queries cover the entire selected dataset, with one warmup and four measured
iterations. Measured queries select VARIANT for Duckgres and Trino and STRUCT
for Athena. JSON supplies untimed correctness baselines. Complete query results
must agree before measurements start. Hoglake currently rejects VARIANT types;
a live run is needed to establish the exact failure. Unsupported types are not
silently substituted.

## Results and recovery

Full-corpus results remain in `perf/`; properties results use `perf-properties/`
and a distinct run ID ending in `-properties`. Dataset versions derive from the
selected object inventory. Keep the prefix immutable to compare runs reliably.
The existing publisher preserves representation labels and both workloads.

For empty or inaccessible prefixes, correct the input or access and rerun.
For Athena mapping failures, correct the table using the generated SQL.
Keep raw values and private paths out of published diagnostics.
Scenario cleanup deprovisions the owned warehouse, and workflow teardown removes
the isolated stack. For interrupted runs, follow the scenario recovery runbook
using only that run's recorded identity and preserve artifacts before cleanup.
