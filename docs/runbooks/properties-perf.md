# Published properties in the full performance scenario

The existing `posthog_frozen_perf` and `posthog_frozen_perf_trino_cached`
scenarios include the published properties workload after their full-corpus
queries. They reuse each scenario's isolated warehouse and normal cleanup.
Existing full-corpus table registration, Hoglake setup, and queries remain intact.
There is no separate properties scenario or infrastructure suite.

The requested measured representations are VARIANT on DuckDB and Trino, and
STRUCT on Athena. JSON queries serve only as untimed correctness baselines.
The cached scenario runs the properties workload on `trino_cached`; the main
scenario uses PGWire uncached/cached, Trino, and Athena. Reader compatibility
must succeed before measurement; never substitute another representation to
bypass an unsupported type.

## Prepare and run

Set `DUCKGRES_SCENARIO_PROPERTIES_MANIFEST` to the private URI or local path of
the published `complete.json`, alongside the existing scenario environment.
This is a required input, not a new repository secret. Keep configuration and
generated files outside this public repository. Preparation validates manifest
completion and the exact live object inventory without generating or rewriting
data. Completion-manifest value checks are sampled; the workload's complete
query-result comparisons provide a separate correctness gate.

```sh
just prepare-properties-perf "$DUCKGRES_SCENARIO_PROPERTIES_MANIFEST"
```

Preparation writes `setup.sql`, `athena.sql`, `catalog.yaml`, and
`dataset-version.txt` and `hoglake-properties.json` to `/tmp/properties-perf`
by default. The recipe's second argument changes the output directory. For ordinary scenario execution,
`DUCKGRES_SCENARIO_PROPERTIES_OUTPUT_DIR` overrides the same default; relative
paths resolve from the repository root.

An authorized operator must provision `properties_events_supported` in the
configured Athena database using the generated `athena.sql` before running the
main scenario. Its logical schema omits VARIANT while referring to the same
physical files. The ordinary scenario preparation checks this existing Glue
mapping read-only, including column names, location, and Parquet reader settings.
The cached Trino scenario does not require Athena metadata or credentials.

Run the existing scenario entry point with its normal isolated configuration:

```sh
./scripts/scenario_run.sh tests/mw-dev/scenario/scenarios/posthog_frozen_perf.yaml
./scripts/scenario_run.sh tests/mw-dev/scenario/scenarios/posthog_frozen_perf_trino_cached.yaml
```

Preparation occurs before warehouse provisioning. `--check-env` verifies required
settings without reading S3, creating files, or provisioning. Supply the private
manifest through the runner environment; the harness does not create secrets or
configuration resources. In Actions, the existing paired scenario selection and
infrastructure handling apply. A missing manifest fails the environment check.

The properties SQL setup runs once with `exec_only: true`, draining every
statement and propagating later errors without replaying registration. The
existing `setup_hoglake.py` also consumes the generated private properties plan
in a separate `setup_hoglake_properties` step. It registers only the selected
manifest files, using the existing Hoglake client type mapper. Both properties
perf steps depend on successful registration. Native VARIANT schema mapping and reader
support in that client and the Trino connector are prerequisites; unsupported
types fail registration explicitly before properties timings.

The catalog defaults to one warmup and four measured iterations. Date bounds come
from the manifest as explicit UTC instants.

## Results and recovery

Full-corpus artifacts remain in `perf/`; properties artifacts use
`perf-properties/` and a run ID ending in `-properties`. The distinct run ID
prevents historical publication from replacing the full-corpus results. The
workflow publishes both directories through the existing publisher.
Representation labels and the manifest-derived dataset version identify the
properties results.

Correctness checks run before timed iterations and compare complete returned
keys and counts against JSON. Do not report timing for a failed gate. Successful
reads establish compatibility; EXPLAIN alone does not establish read efficiency.
The requested Trino VARIANT mapping is blocked: the currently pinned Hoglake
client API and Trino connector reject VARIANT. Registration and reader
compatibility for this mapping have not been verified. Do not claim a completed Trino run or
silently fall back to STRUCT while this blocker remains.

If manifest, object inventory, or Athena mapping validation fails, correct the
selection or precreated metadata and rerun preparation. Never edit the manifest
or regenerate the fixture to bypass a failure. Preserve reader errors privately
and report platform blockers without publishing paths, customer identifiers, or
raw values.

Cleanup depends on both perf steps and always attempts to deprovision the owned
warehouse. The isolated workflow retains its normal namespace teardown. For an
interrupted local run, use the scenario recovery runbook with only the recorded
owned warehouse identity, preserving artifacts before cleanup. Leave unrelated
warehouses and running workloads alone.
