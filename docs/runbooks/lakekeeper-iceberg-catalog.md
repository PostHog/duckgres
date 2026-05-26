# Lakekeeper Iceberg Catalog

How the per-org [Lakekeeper](https://docs.lakekeeper.io/) Iceberg REST catalog
backend works, how to activate it for a tenant, and how to diagnose the
failure modes we hit bringing it up. This is the alternative to the AWS S3
Tables Iceberg backend — selected per tenant via `iceberg.backend`.

## Architecture

Each tenant that opts into the Lakekeeper backend gets its **own** single-tenant
Lakekeeper instance, provisioned by the control plane through the
[lakekeeper-operator](https://github.com/lakekeeper/lakekeeper-operator) and a
`Lakekeeper` custom resource. All instances share one Kubernetes namespace
(`lakekeeper`); isolation is per-CR (Deployment + Service + ServiceAccount +
Secret + a migrate Job), not per-namespace.

```
duckgres control plane ──(provisioner)──▶ Lakekeeper CR ──(operator)──▶ lakekeeper-<org> Deployment/Service
        │                                                                         │
        │ persists endpoint/warehouse/client_id into the config store            │ serves Iceberg REST catalog (metadata only)
        ▼                                                                         ▼
   worker activation ──▶ ATTACH '<warehouse>' AS iceberg (TYPE ICEBERG, ENDPOINT '<endpoint>', …)
                              data read/write goes straight to S3 with the worker's own credentials
```

The relevant code:

- `controlplane/provisioner/lakekeeper_provisioner.go` — reconciles the
  `Lakekeeper` CR, bootstraps the server, and creates the per-org warehouse.
- `controlplane/provisioner/lakekeeper_k8s.go` — renders the CR, ServiceAccount,
  and client-credentials Secret.
- `server/iceberg/migration.go` — builds the `ATTACH` and `CREATE SECRET`
  statements the worker runs on activation.
- `server/server.go::attachLakekeeperCatalog` — wires the worker's S3 credentials
  into the attach.

## Credential model (important)

**Lakekeeper serves catalog metadata only. The worker reads and writes table
data in S3 with its own credentials — Lakekeeper does not vend credentials to
the client.**

On activation the worker creates a `TYPE S3` secret named `iceberg_sigv4` from
the credentials in its activation payload (the same per-org role/bucket the
DuckLake S3 secret uses), then `ATTACH`es the Lakekeeper REST catalog. DuckDB
signs S3 data requests with that secret.

Credential **vending** (the Iceberg REST `vended-credentials` delegation, where
the catalog hands per-table S3 credentials back to the client) is deliberately
**disabled**:

- The Lakekeeper instance is configured with STS vending off; its STS
  down-scoping session policy overflowed AWS's packed-policy size limit
  (`PackedPolicyTooLargeException`).
- It is unnecessary — the worker already holds S3 credentials for the warehouse
  bucket.

### `ACCESS_DELEGATION_MODE 'none'` is mandatory, not optional

DuckDB's iceberg extension **defaults `ACCESS_DELEGATION_MODE` to
`'vended_credentials'`**. *Omitting* the option does **not** disable vending.

If the client requests delegation but the server has vending disabled,
Lakekeeper returns a per-table storage *config* (region/endpoint) with **no
credentials**. DuckDB still materializes that into a path-scoped `__internal_ic_*`
S3 secret with empty credentials. Because its scope (the table's S3 prefix) is
*more specific* than `iceberg_sigv4`'s (`s3://`), it **shadows** the working
secret — and every data read/write goes out anonymous, so S3 returns
`403 Forbidden`. Metadata operations (CREATE SCHEMA/TABLE) still succeed because
they go through the REST API, which masks the problem during provisioning.

`BuildLakekeeperAttachStmt` therefore sets `ACCESS_DELEGATION_MODE 'none'`
explicitly on every `ATTACH`. With delegation off, DuckDB falls back to the
ambient `iceberg_sigv4` secret. A quick check that this is in effect:

```sql
SELECT count(*) FILTER (WHERE name LIKE '__internal_ic%') AS vended,
       count(*) FILTER (WHERE name = 'iceberg_sigv4')     AS sigv4
FROM duckdb_secrets();
-- expect: vended = 0, sigv4 = 1
```

## Prerequisites

- The `lakekeeper-operator` is deployed in the same cluster as the tenants'
  workers (it must be co-located with the `Lakekeeper` CRs the control plane
  creates).
- The control plane runs with `DUCKGRES_LAKEKEEPER_PROVISIONER_ENABLED=true`,
  and the `lakekeeper` namespace plus the provisioner's RBAC exist.
- The per-org Lakekeeper ServiceAccount has cloud credentials (e.g. via an EKS
  Pod Identity association mapping it to the tenant's IAM role) so the catalog
  can perform its own metadata IO to S3.

## Activate the Lakekeeper backend for a tenant

Enable Iceberg on the tenant's managed warehouse with the `lakekeeper` backend
via the admin API:

```sh
curl -X PUT "$ADMIN_API/api/v1/orgs/<org>/warehouse" \
  -H "X-Duckgres-Internal-Secret: $INTERNAL_SECRET" \
  -H 'Content-Type: application/json' \
  -d '{"iceberg":{"enabled":true,"backend":"lakekeeper"}}'
```

The control plane detects the drift, patches the Duckling CR, and the operator
provisions the Lakekeeper instance (typically ready in well under a minute).
Confirm via the warehouse record:

```sh
curl -s "$ADMIN_API/api/v1/orgs/<org>/warehouse" \
  -H "X-Duckgres-Internal-Secret: $INTERNAL_SECRET" | jq .iceberg, .iceberg_state
# iceberg_state: "ready"; iceberg.lakekeeper_endpoint / lakekeeper_warehouse / lakekeeper_client_id populated
```

## Verify end to end

Connect as the tenant and round-trip data through the catalog:

```sql
CREATE SCHEMA  IF NOT EXISTS iceberg.smoke;
CREATE TABLE   iceberg.smoke.t (id INTEGER, note VARCHAR);
INSERT INTO    iceberg.smoke.t VALUES (1, 'hello'), (2, 'world');
SELECT count(*) FROM iceberg.smoke.t;   -- 2
```

A successful `INSERT` writes a parquet data file plus Iceberg metadata
(`*.metadata.json`, manifest/snapshot `*.avro`) under the warehouse prefix in
the tenant's S3 bucket.

## `information_schema.columns`

DuckDB's Iceberg catalog can list tables before it has loaded each table's
schema, so raw `information_schema.columns` may expose placeholder
`__` / `unknown` rows. Duckgres hides those placeholders and, when
`iceberg.lakekeeper_metadata_dsn` / `DUCKGRES_ICEBERG_LAKEKEEPER_METADATA_DSN`
is configured alongside the Lakekeeper warehouse name, bulk-loads current
column metadata directly from Lakekeeper's Postgres catalog
(`table_current_schema` + `table_schema`). The default is empty, which keeps
the compatibility path on the table-metadata fallback.

## Troubleshooting

| Symptom | Likely cause | Action |
| :------ | :----------- | :----- |
| `CREATE TABLE` works but `INSERT` fails with `Unable to connect to URL s3://… Forbidden (HTTP 403)` | DuckDB requested vended credentials (the `ACCESS_DELEGATION_MODE` default) and is using an empty `__internal_ic_*` shadow secret | Confirm the `ATTACH` includes `ACCESS_DELEGATION_MODE 'none'` (see `BuildLakekeeperAttachStmt`) and that `duckdb_secrets()` shows `vended = 0`. Re-activate the worker on a build that includes the fix. |
| `iceberg_state` never leaves `pending`; no `lakekeeper-<org>` Deployment appears | operator not running in this cluster, or provisioner disabled | Check the operator pod and that the control plane has `DUCKGRES_LAKEKEEPER_PROVISIONER_ENABLED=true`; check the `Lakekeeper` CR's status. |
| Warehouse-create fails with a credentials/`SystemIdentity` error | the Lakekeeper pod can't reach cloud credentials for its own metadata IO | Verify the per-org ServiceAccount's Pod Identity association exists and the pod has credential env injected (the pod must start *after* the association is created). |
| `column "iceberg_lakekeeper_oauth2_server_uri" does not exist` on persist | GORM snake-cases `OAuth2` to `o_auth2`; a config write used the wrong column name | The column is `iceberg_lakekeeper_o_auth2_server_uri`. |

## See also

- `server/iceberg/migration.go` — the attach/secret statement builders and the
  delegation-mode rationale in the doc comments.
- [Delta Catalog Activation](delta-catalog-activation.md) — the analogous
  runbook for the DuckLake Delta catalog.
