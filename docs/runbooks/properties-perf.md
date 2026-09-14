# Properties in frozen performance scenarios

The existing `posthog_frozen_perf` scenario benchmarks the generated properties
Parquet dataset. This replaces its original full-corpus benchmark phase; it runs one
`perf_queries` step after dataset registration and validation.
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
This runs all five targets in one scenario, with separate Trino coordinators
for the two cache modes. The workflow masks the input in logs and uses its existing AWS role.

Duckgres registers only `properties_perf.events_supported`; Hoglake registers its projections
in the scenario's catalog, whose `data_path` is initialized from the selected
properties Parquet prefix. All registered files must be beneath that path.
Both Trino coordinators share the JSON projection. The scenario explicitly sets
`representation: json`; the Python helper defaults `--properties-representation`
to `variant` for direct invocations. Athena uses the
`properties_events_supported` table in the configured benchmark database.
The workflow creates this external table if missing, using its existing AWS role;
it leaves existing tables intact and preparation verifies their mapping. This
requires `glue:GetTable` and `glue:CreateTable` on the benchmark database/table.
For direct local runs, provision it with the generated `athena.sql` first.
All projections refer to the same Parquet prefix.

Queries cover the entire selected dataset, with one warmup and four measured
iterations. The unified scenario requests these five comparisons:

| Run label | Cache | Properties representation |
| --- | --- | --- |
| duckgres (vanilla) | Off | JSON |
| duckgres (cache) | On | JSON |
| trino (vanilla) | Off | JSON |
| trino (cache+variant) | On | VARIANT |
| Athena | — | STRUCT |

These names are emitted as `run_label` in result and service-metric CSVs and
published query results. The `protocol` identifiers remain stable.

Cached Trino and Athena also run untimed JSON baselines. Complete query results
must agree before measurements start. Duckgres registers only its JSON projection
in both cache modes. Cached Trino still requests VARIANT, which Hoglake does not
support. The shared JSON setup does not register `events_variant`, so that target
currently fails correctness validation. Because validation is shared, this can
prevent measurements for all targets. Unsupported types are not silently substituted.

## Results and recovery

The scenario writes its sole benchmark result set to `perf/` under its existing
run ID. Dataset versions derive from the selected object inventory. Keep the
prefix immutable to compare runs reliably. The existing publisher preserves
representation labels. Workflow artifact upload preserves available diagnostics
on failure; branch runs do not publish to the shared dashboard.

For empty or inaccessible prefixes, correct the input or access and rerun.
For Athena mapping failures, correct the table using the generated SQL.
For a Hoglake `data_path` rejection, verify that catalog initialization and
properties registration use the same selected prefix; recreate the isolated
scenario catalog with that prefix before retrying.
Keep raw values and private paths out of published diagnostics.
Scenario cleanup deprovisions the owned warehouse, and workflow teardown removes
the isolated stack. For interrupted runs, follow the scenario recovery runbook
using only that run's recorded identity and preserve artifacts before cleanup.
