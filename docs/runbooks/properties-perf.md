# Properties in frozen performance scenarios

`posthog_frozen_perf` first runs the existing frozen corpus on all five targets.
An optional properties comparison follows in the same scenario and isolated
stack. Leave `DUCKGRES_SCENARIO_PROPERTIES_S3_URI` empty (the default) to run only
the original suite. No properties S3/Glue access or preparation occurs in that
case. No completion manifest or new repository secret is required.

## Select the fixture

Generate an immutable Parquet fixture for a modest full UTC day before enabling
the comparison. Every representation must contain the same events and property
semantics. Select a day using source row counts and property sizes; validate its
runtime before adopting it as the benchmark. Do not use the large earlier
fixture as the default or assume a timestamp filter will prune mixed-day files.
The benchmark scans the entire supplied prefix; it does not generate data,
select dates, or enforce a row-count limit. Keep dates, source locations, and
fixture-derived statistics in private generation notes, outside this repository.

## Prepare and run

```sh
# Original benchmarks only:
just scenario-frozen-perf

# After the replacement single-day fixture has been generated:
export DUCKGRES_SCENARIO_PROPERTIES_S3_URI="<generated-parquet-prefix>"
just scenario-frozen-perf
```

For GitHub Actions, dispatch `scenario-dev` with
`scenario=posthog_frozen_perf`. Supply `properties_s3_uri` only once the fixture
is ready. The workflow masks this input and uses its existing AWS role.

The properties step discovers the object inventory and prepares private SQL and
catalog files in a temporary directory, removed when the step returns. Discovery
and Athena verification have a five-minute deadline. Provision the fixed
`properties_events_supported` Glue table as part of fixture generation using
the emitted `athena.sql`; its mapping must match the selected prefix and schema.
The runner only verifies this mapping using `glue:GetTable`. When replacing a
fixture, update this mapping deliberately as part of generation; the benchmark
does not create or overwrite Glue tables.

For manual preparation,
`just prepare-properties-perf "$DUCKGRES_SCENARIO_PROPERTIES_S3_URI"` writes `setup.sql`, `athena.sql`,
`catalog.yaml`, and `dataset-version.txt` to `/tmp/properties-perf` (the optional
second argument changes this directory). This is a diagnostic convenience;
scenario execution prepares its own inputs after the original benchmarks.
Generated files contain private locations and must stay out of public artifacts.

## Catalogs and comparisons

Duckgres adds the supported JSON/STRUCT projection under
`properties_perf.events_supported`, without changing the original tables.
Hoglake registers the JSON projection in an isolated catalog suffixed
`-properties`, whose `data_path` is the selected properties prefix. After the
original benchmarks, the disposable tenant Trino catalog is recreated with only
its `hoglake.catalog` mapping changed to this properties catalog. Tenant
credentials, permissions, and cache settings are preserved. The original
Hoglake catalog and its fixture root remain intact. The original benchmark step
explicitly selects the original mapping when rerun. Both locations must be
readable by the isolated stack's existing AWS identity.

| Properties run label | Cache | Representation |
| --- | --- | --- |
| duckgres (vanilla) | Off | JSON |
| duckgres (cache) | On | JSON |
| trino (vanilla) | Off | JSON |
| trino (cache+variant) | On | VARIANT — explicitly skipped |
| Athena | — | STRUCT |

There are two intents: browser counts and event counts filtered to Chrome.
Supported engines must return identical complete ordered results; Athena runs
only STRUCT, validated against the shared Duckgres/Trino JSON baseline for each
intent. Measurements use one warmup and four measured
iterations. Hoglake does not support VARIANT, so its comparisons emit `skipped`
results with `unsupported_representation`, iteration zero, and no timing. They
are excluded from measured/warmup counts and never enter the correctness gate.
Enable them only after native support and fixture registration are available;
do not substitute JSON under a VARIANT label.

## Results and recovery

Original results retain `perf/`, the original run ID, and dataset version.
Properties use `perf-properties/`, a `-properties` run ID suffix, and an
inventory-derived dataset version. Both summaries are handled by the existing
publisher; only main-branch runs publish to the historical database. A
properties failure still fails the scenario, but previously completed original
results remain available for upload and publication. No timing result is
claimed for a skipped comparison. Dashboard comparisons should filter
`status = 'ok'` and use the matching properties intent; original aggregate
panels retain their existing query selection.

For missing/inaccessible prefixes, correct the selection or access and rerun.
For Athena mapping failures, use the generated SQL and approved fixture
configuration to correct the table before retrying. If catalog switching fails,
recreate the isolated stack rather than timing an unverified mapping. Preserve
private diagnostics before cleanup. Scenario deprovisioning and workflow
teardown remove the owned warehouse and isolated stack; follow the existing
scenario recovery runbook for interrupted runs using only that run's identity.

The runner requires `DUCKGRES_SCENARIO_TRINO_CATALOG_STORE_CELL_ID` to match the
baseline coordinator's `catalog-store.cell-id`; the isolated workflow supplies
it automatically. It has no default. Local runs must set it explicitly. The API's
public cell identity (for example, `legacy`) is not the persisted catalog-store
identity. Cache verification, catalog switching, and cached-catalog cloning all
use the explicit stored identity. For a missing-catalog error, check this value
against the baseline coordinator configuration before retrying.

Cached Trino startup logs changed failure signatures for catalog creation and
tenant metadata readiness separately. Timeout diagnostics retain both attempt
counts and the last structured errors, including server error codes and Java
cause/method symbols. Free-form messages, SQL, URLs, and credentials are omitted
from public artifacts. These diagnostics distinguish a catalog that was never
created from one that exists but cannot be read; cancellation does not overwrite
the preceding server failure.
