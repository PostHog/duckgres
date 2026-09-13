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

Duckgres registers only `properties_perf.events_supported`; Hoglake registers its projections
in the scenario's catalog. Uncached Trino registers only the JSON projection;
cached Trino additionally registers VARIANT. Scenario steps specify this through
`representation`; the Python helper defaults `--properties-representation` to
`variant` for direct invocations. Athena uses the precreated
`properties_events_supported` table in the configured benchmark database.
Provision that table with the generated `athena.sql` before the ordinary scenario;
preparation verifies its mapping. The cached-Trino scenario requires no Athena
configuration. All projections refer to the same Parquet prefix.

Queries cover the entire selected dataset, with one warmup and four measured
iterations. The measured properties comparisons are:

| Run label | Cache | Properties representation |
| --- | --- | --- |
| duckgres (vanilla) | Off | JSON |
| duckgres (cache) | On | JSON |
| trino (vanilla) | Off | JSON |
| trino (cache+variant) | On | VARIANT |
| Athena | — | STRUCT |

These names are emitted as `run_label` in result and service-metric CSVs and
published query results. The `protocol` identifiers remain stable. Original
full-corpus rows keep their existing labels because they do not use VARIANT.

Cached Trino and Athena also run untimed JSON baselines. Complete query results
must agree before measurements start. The first live branch run failed earlier
in DuckLake file registration: `Expected VARIANT, found type STRUCT` for
`properties_variant`. Hoglake registration and properties queries were skipped.
Duckgres now uses JSON in both cache modes and does not register the VARIANT
column, avoiding that path. Cached Trino still requires VARIANT support in Hoglake.
Unsupported types are not silently substituted.

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
