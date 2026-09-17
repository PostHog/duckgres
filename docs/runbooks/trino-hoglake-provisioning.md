# Managed Trino Hoglake provisioning

## Configuration and selection

Managed Trino tenants default to `ducklake`. Before the first enable, an operator
can choose `hoglake` in the organization's Trino settings or pass
`"backend": "hoglake"` to the enable/provision API. Cell assignment and backend
selection are separate. Successful enable locks the backend; disabling and
re-enabling retains it. Omitted backend values preserve the stored selection.
Existing Trino configuration rows migrate to locked DuckLake selections,
including disabled rows whose previous enablement history is unknown.

Hoglake creates a separate, initially empty catalog. It does not migrate the
organization's DuckLake warehouse or change Duckgres query storage.
Use a new dedicated test tenant for a pilot, rather than changing an existing
DuckLake tenant's backend.

The control plane requires these environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `DUCKGRES_TRINO_MANAGED_HOGLAKE_URI` | empty, disabled | Hoglake HTTP(S) origin, without credentials or an API path |
| `DUCKGRES_TRINO_HOGLAKE_DATA_PATH` | empty | Reserved `s3://bucket/prefix/` base, with a trailing slash |
| `DUCKGRES_TRINO_HOGLAKE_NAMESPACE` | `main` | Namespace to create and verify |

An explicit Hoglake request is rejected before database mutations if managed
Hoglake is unavailable. Reserve the configured S3 prefix exclusively for this
service. Do not reuse DuckLake roots or immutable performance fixtures.

## Provisioning and ownership

The Trino catalog retains its `org_<sanitized-database-name>` name and existing
OPA tenant ownership mapping. The Hoglake catalog is named after the organization
ID. Its S3 path is `<configured-base>/<warehouse-DucklingName>/`, matching the
Crossplane tenant-role policy. The provisioner uses the Duckling storage status
and tenant IAM role without reading a DuckLake metadata password.

Reconciliation reads the Hoglake catalog, creates it if absent, and verifies its
exact data path. Concurrent-create conflicts are resolved by rereading and
checking ownership. It verifies `atomic-table-creation-v1`, then creates and
verifies the configured namespace. Trino receives the Hoglake connector and
`s3.auth-type=IAM_ROLE` with the tenant's role.

An existing Trino catalog must report an operational Hoglake connector. A
DuckLake catalog with the same name fails readiness and requires explicit
recovery. Reconciliation never silently replaces it. Trino's connector inventory
does not expose all catalog properties, so it cannot certify an externally
modified catalog's URI or role. Investigate manual configuration changes before
returning the tenant to service.

Readiness also requires the existing authentication and cell gates. It verifies
metadata and connector availability, but does not perform S3 writes. The smoke
test below provides that verification. The administrative OPA grant permits
connector inventory; it does not grant tenant data writes.

Disabling Trino removes its registration and access through the normal lifecycle.
Hoglake metadata and S3 files remain. Neither disabling nor clearing deployment
configuration changes the selected backend or migrates data. Repair configuration
or ownership mismatches explicitly; do not drop catalogs to force a switch.

## Rollout prerequisites

1. Deploy the compatible control-plane version across the fleet with managed
   Hoglake configuration disabled. Complete this before enabling the feature;
   older binaries do not implement the new provisioning path.
2. Deploy a Hoglake server and Trino connector supporting atomic table creation.
   Apply their infrastructure and verify the service can maintain the reserved
   S3 prefix.
3. Apply tenant-role policies granting each tenant access only to its own
   Duckling-name child prefix. The Trino pod role must be allowed to assume that
   role. Custom roles outside the managed composition require equivalent grants.
4. Allow the control plane and Trino cells to reach the Hoglake REST service, and
   allow Trino and Hoglake to reach the required storage services.
5. Supply registered-cell rollout canary credentials and verify healthy control
   plane and cell readiness. Creating an empty secret resource is insufficient.
6. Enable the managed configuration, create a dedicated pilot tenant, select its
   cell and Hoglake backend, then enable Trino. Wait for reconciled readiness.
7. Run the tenant smoke test. Keep pilot enablement limited until it succeeds.

The existing `DUCKGRES_TRINO_HOGLAKE_URI` setting is separate. It applies to the
legacy provisioner for externally bootstrapped performance fixtures, not
registered managed cells. Preserve that workflow's explicit fixture setup; see
the [scenario runbook](scenario-runner.md).

## Live smoke test

`just test-trino-hoglake-smoke` runs HTTP client regression tests and skips live
operations by default. To opt in, provide these variables through the approved
runtime credential mechanism:

```text
HOGLAKE_SMOKE_TEST=1
TRINO_SERVER=https://trino.example
TRINO_USER=<tenant-principal>
TRINO_PASSWORD=<tenant-password>
TRINO_CATALOG=<tenant-trino-catalog>
TRINO_ROUTING_GROUP=<cell-routing-group-if-required>
HOGLAKE_URI=https://lake.example
HOGLAKE_CATALOG=<tenant-hoglake-catalog>
HOGLAKE_NAMESPACE=main
```

Run only against a dedicated test tenant: compaction applies to the entire
Hoglake catalog. Coordinate automatic maintenance so it does not consume the
multi-file baseline before the assertion. The runner needs authorized network
access to both endpoints. Use tenant credentials, not provisioner administrator
credentials, for Trino operations.

The test creates randomly named tables, inserts eight separate batches including
a decimal exceeding INT64, checks exact values, runs CTAS, and triggers Hoglake
compaction. It checks unchanged source rows and a reduced source file count.
Cleanup deletes only its generated tables using the Hoglake REST API, since the
Trino connector does not currently implement DROP TABLE. A cleanup failure is
reported as a test failure and requires explicit operator cleanup. No mutation
is blindly retried after an uncertain response.

Passing unit tests is not evidence of a successful deployment. Record live smoke
results separately after the prerequisites are applied.
