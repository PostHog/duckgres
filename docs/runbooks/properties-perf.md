# Published properties performance workload

The `posthog_properties_perf` scenario compares JSON and STRUCT on DuckDB,
Trino, and Athena, plus VARIANT on DuckDB, using the same published Parquet
files. It provisions and removes an isolated warehouse. Existing full-corpus
queries and the scheduled workflow selection are unchanged.

## Prepare and run

Set `DUCKGRES_SCENARIO_PROPERTIES_MANIFEST` to the private S3 URI of the
published `complete.json`. Keep operational configuration and generated files
outside this public repository. Never select incomplete output with a wildcard.

`just prepare-properties-perf "$DUCKGRES_SCENARIO_PROPERTIES_MANIFEST"` validates
the fixture and writes `setup.sql`, `athena.sql`, `catalog.yaml`, and
`dataset-version.txt` to `/tmp/properties-perf` by default. Its second argument
overrides that directory. Preparation reads the published fixture; it does not
generate or rewrite source data. Completion-manifest value checks are sampled;
the workload's full query-result comparisons provide a separate correctness gate.

An operator with Glue table-creation permission must provision
`properties_events_supported` in the configured Athena database using the
generated `athena.sql` before running. This logical schema omits VARIANT while
pointing at the same physical fixture. The ordinary scenario identity has
read-only Glue access. Do not expand its privileges or replace existing shared
benchmark tables to bypass a setup failure.

With an explicitly configured disposable warehouse identity and the environment
required by the scenario YAML, run:

```sh
just scenario-properties-perf
```

The runner prepares the fixture before provisioning, verifies the Athena table
against the selected fixture, and exports its manifest-derived dataset version.
`DUCKGRES_SCENARIO_PROPERTIES_OUTPUT_DIR` overrides `/tmp/properties-perf`;
relative paths are resolved from the repository root. `--check-env` checks
required settings without reading S3, preparing files, or provisioning.

Configure the repository Actions secret `DUCKGRES_PROPERTIES_MANIFEST_URI` with
the private completed manifest URI. Do not pass it as a public workflow input.
For the normal isolated CI stack, manually select `posthog_properties_perf` in
`scenario-dev`. It enables the isolated Trino cell and the existing Athena Pod
Identity configuration. The properties workload is opt-in; it is not added to
the daily selection or the existing full suite. Do not run it against an
unrelated existing dev warehouse. The scenario's SQL setup runs once with `exec_only: true`, which drains every statement and propagates later validation errors without replaying the script. Other SQL steps default to `exec_only: false`.

The generated catalog defaults to one warmup and four measured iterations per implementation. Date bounds come from the manifest as explicit UTC instants. JSON and STRUCT use the supported logical table; VARIANT runs only on PGWire.

## Results and recovery

The existing perf artifacts contain representation labels and the selected
manifest's dataset version. Correctness checks precede all timed iterations and
compare returned keys and counts against JSON. Do not report timing for a failed
correctness gate. Engine compatibility is established by successfully reading
the supported columns; an EXPLAIN plan alone does not establish read efficiency.

If manifest, object inventory, or Athena mapping validation fails, fix the
selection or precreated metadata and rerun preparation. Never edit the manifest,
regenerate this fixture, or silently omit a failing engine. If a reader rejects
the mixed Parquet schema or a property type, preserve the exact error privately
and report the platform blocker. Keep public diagnostics free of object paths,
customer identifiers, and raw property values.

The scenario always attempts to deprovision its warehouse after execution. The
isolated workflow also runs its normal namespace teardown. For an interrupted
local run, follow the scenario runner recovery runbook using only the recorded
owned warehouse identity; do not run shared-infrastructure cleanup. Preserve
artifacts before deleting an interrupted run's resources.
