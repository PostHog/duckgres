# Trino Hoglake provisioning

## Current contract

`DUCKGRES_TRINO_HOGLAKE_URI` defaults to empty. Empty selects DuckLake when
creating a missing Trino catalog. A nonempty HTTP(S) base URI selects Hoglake
for every missing catalog in that provisioner's scope. The URI must not contain
credentials, a query, or a fragment; use the server base URI without `/v1`.
This setting currently supports the frozen performance scenarios, whose runner
bootstraps the Hoglake dataset separately. It is not a managed tenant rollout
switch.

The two catalog names are different:

- The Trino catalog is `org_<sanitized-database-name>`, preserving its tenant
  identity and OPA ownership mapping.
- The Hoglake catalog is the raw organization ID. Setting the URI does not
  create this catalog, its namespaces, or its tables.

The scenario runner's `setup_hoglake` step registers the externally supplied
immutable fixture S3 prefix and creates the `posthog` namespace. The properties
comparison uses its own catalog and `properties_perf` namespace. These are
scenario choices, not general tenant defaults. See the
[scenario runbook](scenario-runner.md).

Hoglake properties currently use the Trino pod's AWS credentials. Unlike the
DuckLake branch, they do not select the tenant's IAM role. The
`hoglake.s3.region` property is a supported compatibility alias for `s3.region`.
The existing Duckling metadata/password projection and readiness gates still
apply even though Hoglake does not consume the DuckLake metadata password.
Readiness checks do not validate the Hoglake catalog, namespace, or S3 access.

## Existing catalogs and recovery

Reconciliation lists catalogs and retains each enabled tenant's existing catalog
by name. It does not compare connector properties. Consequently:

- Setting the URI leaves existing DuckLake catalogs as DuckLake.
- New or recreated catalogs use Hoglake while the setting is nonempty.
- Clearing the setting leaves existing Hoglake catalogs as Hoglake; subsequent
  creations use DuckLake.

Do not drop an existing catalog to force a connector switch. This is not a data
migration: the two services have separate metadata, and the Hoglake data path
and namespace may not exist. Restore an accidentally changed setting before
allowing more catalog creations, inspect which catalogs were created during the
change, and plan their recovery explicitly. Clearing the setting alone does not
restore their prior backend.

For disposable performance environments, bootstrap fixtures using the scenario
runner and its explicit S3 source. If fixture registration or connectivity fails,
repair that input and rerun the disposable scenario; a Trino `Ready` status alone
is not evidence that the Hoglake dataset exists.

## Operator access

The provisioner administrator can manage only `org_*` catalog names. The
reconciler drops managed-name catalogs that have no enabled tenant owner.
Administrator reads require an OPA bundle grant, and this identity cannot write
tables or schemas. Therefore manually creating an arbitrary test catalog is not
a substitute for provisioning a tenant.

Run CREATE TABLE, INSERT, and CTAS checks with a dedicated tenant's authorized
unscoped credentials after that tenant's catalog and OPA mapping are provisioned.
Do not disable OPA or broaden the administrator's permissions for a smoke test.

## Requirements for managed tenant enablement

Before using Hoglake on a shared managed deployment, implement and validate:

1. Explicit tenant backend selection, with DuckLake as the default and existing
   catalog/backend mismatches reported instead of silently treated as migrated.
2. Explicit ownership of a dedicated S3 data path. Do not infer that an existing
   DuckLake root or a performance fixture prefix is available for Hoglake writes
   and cleanup. Paths must be disjoint from other Hoglake catalogs and must not
   give two metadata systems ownership of the same files.
3. Idempotent catalog and namespace bootstrap before Trino registration. Read
   existing resources and verify the intended data path; after a concurrent-create
   conflict, reread and verify instead of accepting any same-name catalog. Keep
   namespace choices explicit and preserve the external fixture bootstrap flow.
4. Per-tenant S3 role configuration for Trino, plus the required access for
   Hoglake maintenance. Verify scoped network access from the provisioner and
   Trino to the Hoglake service and from both engines to the chosen S3 path.
5. Readiness that verifies the selected backend, catalog, namespace, and required
   write capability. Validate storage access with a disposable tenant smoke test:
   CREATE TABLE, INSERT, CTAS, and compaction with unchanged query results and
   a reduced data-file count.

These requirements are not implemented by the URI setting. Keep shared
provisioners on the default until the managed path is available; use isolated
performance environments for the existing externally bootstrapped flow.
