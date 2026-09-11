# Frozen Hoglake fixture

`register.py` populates `posthog.events` and `posthog.persons` for the complete
`ducklake_posthog_tables.yaml` corpus. It inspects every source Parquet footer,
copies every matching object byte-for-byte to a dedicated destination, and
registers both tables in one Hoglake commit. It does not rewrite Parquet,
filter rows, or omit columns. S3 reads and copies use source ETag conditions.
The manifest records the schema, file counts, row counts, and commit snapshot.

Requirements: Python 3.10+, the packages in `requirements.txt`, an available
Hoglake REST server, and standard AWS credentials with source list/read and
destination list/write permissions. The server and Trino workers need read
access to the destination. The CLI uses the standard boto3 region/credential
chain; optional `HOGLAKE_TOKEN` supplies a REST bearer token. HTTPS is required
except for a server forwarded to localhost. No secrets are command arguments.

```sh
python3 -m venv /tmp/hoglake-perf-venv
/tmp/hoglake-perf-venv/bin/pip install -r tests/perf/datasets/hoglake/requirements.txt
/tmp/hoglake-perf-venv/bin/python tests/perf/datasets/hoglake/register.py \
  --source "$FROZEN_S3_URI" \
  --destination "$HOGLAKE_FIXTURE_S3_URI" \
  --catalog "$HOGLAKE_FIXTURE_CATALOG" \
  --uri "$HOGLAKE_URI" \
  --manifest /tmp/hoglake-fixture-manifest.json
```

Source files must match `events/*.parquet` and `persons/*.parquet`, as in the
existing DuckLake fixture setup. Destination must be empty and must not
overlap the source prefix. Catalog must not already exist: a conflict stops
before any copies. Use a unique destination and catalog for each attempt;
the script never drops catalogs or deletes objects. Keep manifests and real
fixture paths outside this public repository.

Schema preflight finishes before creating the catalog. Missing evolved
columns remain nullable. Compatible int32/int64 and float32/float64 evolution
widens to long and double. Unsupported or conflicting types fail explicitly.
Existing Parquet field IDs must match the resulting schema's sequential IDs;
id-less files bind by name. Lowercase identifiers are required.

Hoglake has no unsigned integer type. A uint64 column (such as
`person_version`) maps to long only when **every nonempty, non-all-null row
group** has min/max statistics proving its values fit signed int64. Missing
statistics or values above `2^63 - 1` fail before writes. A decimal declaration
cannot reinterpret unsigned Parquet bytes through this connector. Such data
needs a separately designed lossless rewrite, which changes the physical
fixture and must be labeled accordingly; this script does not fall back.

Tables are unpartitioned and stats are initially pending for server hydration.
This preserves the frozen objects and is sufficient for the native connector,
which currently does no predicate pushdown. Disable compaction and keep this
catalog immutable during measurement. Before measuring, run the entire corpus
and compare results to the DuckLake fixture. Verify all Trino workers load the
Hoglake connector and use the catalog name from the manifest.

On failure after catalog creation, the isolated destination may contain copies
and the new catalog may contain empty tables. Preserve the error and choose a
new catalog/prefix for retry. Clean up only the failed run's dedicated data
after inspection; never point Hoglake cleanup at the frozen source prefix.

Synthetic tests (no S3 or server required):

```sh
just test-hoglake-fixture /tmp/hoglake-perf-venv/bin/python
```
