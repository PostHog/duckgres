# Managed Trino Hoglake provisioning

## Client backend policy

Existing Trino clients retain DuckLake. New Trino clients use the deployment's
new-client backend: Hoglake where managed Hoglake is configured, DuckLake where
it is not. The admin UI reports the backend without offering a choice. API
callers can omit `backend`: the server preserves an existing selection or
assigns the new-client backend. A request for any other backend on a new client
is rejected (an explicit `hoglake` on a deployment without it is 503, not
configured). Disable and re-enable retain the selected backend. Manual
migration of existing clients is outside this rollout.

The DuckLake fallback exists because onboarding enables Trino in the provision
call. Without it, a deployment with no managed Hoglake (prod-us today) rejected
every new org's provision with 503. A DuckLake client's Trino catalog is the
org's existing warehouse; configuring managed Hoglake later changes only what
NEW clients receive, never a pinned selection.

Existing Trino configuration rows migrate to locked DuckLake selections,
including disabled rows whose previous enablement history is unknown. This
conservative boundary also includes old cell-only rows: the database cannot
prove they were never enabled. Organizations without a previous Trino
configuration receive Hoglake when first enabled.

Hoglake creates a separate, initially empty catalog. It does not migrate the
organization's DuckLake warehouse or change Duckgres query storage.

The control plane requires these environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `DUCKGRES_TRINO_MANAGED_HOGLAKE_URI` | empty, disabled | Hoglake HTTP(S) origin, without credentials or an API path |
| `DUCKGRES_TRINO_HOGLAKE_DATA_PATH` | empty | Reserved `s3://bucket/prefix/` base, with a trailing slash |
| `DUCKGRES_TRINO_HOGLAKE_NAMESPACE` | `main` | Namespace to create and verify |

New-client onboarding is rejected before database mutations if managed
Hoglake is unavailable; existing DuckLake clients can still re-enable. Reserve
the configured S3 prefix exclusively for this service. Do not reuse DuckLake roots or immutable performance fixtures.

## Provisioning and ownership

The Trino catalog retains its `org_<sanitized-database-name>` name and existing
OPA tenant ownership mapping. The Hoglake catalog is named after the organization
ID. Its S3 path is `<configured-base>/<warehouse-DucklingName>/`, matching the
Crossplane tenant-role policy. The provisioner uses the Duckling storage status
and tenant IAM role without reading a DuckLake metadata password.

Reconciliation reads the Hoglake catalog, creates it if absent, and verifies its
exact data path. Concurrent-create conflicts are resolved by rereading and
checking ownership. It verifies `atomic-table-creation-v1`, then creates and
verifies the configured namespace. Before registering Trino, it durably records
that Hoglake initialization completed. This marker survives disable/re-enable
and loss of the Trino registration. After initialization, missing catalogs or
namespaces fail readiness and require metadata recovery; reconciliation and
rollout certification never recreate them as empty resources. An uncertain
initial create or marker write is retried by verifying the remote resources
before recording initialization, without admitting the tenant prematurely.
Trino receives the Hoglake connector and `s3.auth-type=IAM_ROLE` with the tenant's
role.

An existing Trino catalog must report an operational Hoglake connector. A
DuckLake catalog with the same name fails readiness and requires explicit
recovery. Reconciliation never silently replaces it. Trino's connector inventory
does not expose all catalog properties, so it cannot certify an externally
modified catalog's URI or role. Investigate manual configuration changes before
returning the tenant to service. Each backend reads one connector inventory per
reconcile, plus one refresh when it creates Hoglake catalogs. New catalogs are
admitted only after that refresh verifies them. Rollout certification also uses
one inventory for the whole admitted tenant set.

Authentication readiness reads the provider files from the coordinator container's
explicit ConfigMap `subPath` mounts for `password-authenticator.properties` and
`group-provider.properties`. Both `/etc/trino` and `/etc/trino/coordinator`
layouts are supported without an additional setting. Missing, ambiguous, or
unsupported mounts fail readiness. Custom deployments must use this same
single-file mount contract and configure Trino to load those files. If readiness
reports a provider observation failure, check that provider's mount and the
control plane's pod-exec access; changing tenant passwords will not repair it.

Readiness also requires the existing authentication and cell gates. It verifies
metadata and connector availability, but does not perform S3 writes. The smoke
test below provides that verification. The administrative OPA grant permits
connector inventory; it does not grant tenant data writes.

Hoglake warehouse deprovisioning, organization deletion, and warehouse replacement
are blocked, including for disabled Trino clients. These operations must wait for
an explicit Hoglake retirement workflow that fences the retained catalog and S3
ownership before releasing the organization name. Ordinary worker image updates
and Trino disable remain supported. Previously queued warehouse deletions also
stop while Hoglake ownership exists.

Disabling Trino removes its registration and access through the normal lifecycle.
Hoglake metadata and S3 files remain. Neither disabling nor clearing deployment
configuration changes the selected backend or migrates data. Repair configuration
or ownership mismatches explicitly; do not drop catalogs to force a switch.

## Rollout prerequisites

1. Deploy the compatible control-plane version across the fleet with managed
   Hoglake configuration disabled. Complete this before enabling the feature;
   older binaries do not implement the new provisioning path. Pause new Trino
   onboarding during the rolling update. Existing clients continue to operate;
   new onboarding remains unavailable until managed configuration is supplied.
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
   cell, then enable Trino. Hoglake is assigned automatically. Wait for
   reconciled readiness.
7. Run the tenant smoke test. Keep pilot enablement limited until it succeeds.

The historical global `DUCKGRES_TRINO_HOGLAKE_URI` switch is deprecated and
ignored, with a startup warning. It cannot override a client's stored backend.
Existing catalogs are retained, while new clients use the managed Hoglake path.
The frozen performance runner still sets the historical switch: its old setup
is insufficient for new-client onboarding. Updating that runner's storage and
fixture setup is separate work; do not repurpose immutable fixture prefixes
as managed write paths.

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
