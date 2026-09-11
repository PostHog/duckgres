# Trino cell registration and initial placement

The control plane can reconcile the existing `legacy` cell alongside additional
logical cells. A logical cell has one namespace and up to two independent Trino
backends, usually blue and green. Catalogs and credentials are projected only
for warehouses assigned to that cell. This is compute placement: DuckLake's
metadata database, S3 objects, and customer password do not move.

## Configuration

`DUCKGRES_TRINO_CELLS_FILE` defaults to unset. Set it to a mounted JSON file to
register up to 16 additional cells. Configuration is loaded at startup, not
hot-reloaded. All control-plane replicas must receive the same configuration.
Keep the existing `DUCKGRES_TRINO_COORDINATOR_URL`, namespace, TLS name, and cell
ID unchanged; they continue to describe legacy. A registry without a legacy
coordinator is rejected, rather than silently abandoning existing warehouses.

For a deployment that has no legacy coordinator, explicitly set
`DUCKGRES_TRINO_REGISTRY_ONLY=true` (default `false`). This requires a nonempty,
valid `DUCKGRES_TRINO_CELLS_FILE` and rejects a configured legacy coordinator
URL. It bootstraps only registered cells and exposes no legacy bundle endpoint.
Existing legacy ownership is never reinterpreted or migrated.

```json
{
  "cells": [
    {
      "id": "cell-001",
      "namespace": "trino-cell-001",
      "client_url": "https://warehouse.example.test",
      "routing_group": "cell-001",
      "backends": [
        {
          "id": "blue",
          "coordinator_url": "https://blue.example.test",
          "running": true,
          "routing_active": true,
          "internal_secret_name": "trino-blue-internal"
        },
        {
          "id": "green",
          "coordinator_url": "https://green.example.test",
          "running": false,
          "routing_active": false,
          "internal_secret_name": "trino-green-internal"
        }
      ]
    }
  ]
}
```

URLs must use HTTPS without embedded credentials, paths, queries, or fragments.
An optional backend `tls_server_name` pins certificate verification when the
coordinator URL uses a different name. Cells must have distinct namespaces,
routing groups, and coordinator endpoints, including relative to legacy.
Each cell must have exactly one routing-active backend, and that backend must
be marked running. Registry fields describe operator intent; they do not scale
workloads or change Gateway routing.

The existing per-namespace projection names remain `trino-auth`,
`trino-tenant-secrets`, `trino-resource-groups`, and `trino-opa-bundle-token`.
Blue and green share these cell-local projections, but have distinct,
deployment-managed internal-communication Secrets. Duckgres reads those
references and refuses missing keys; it never creates or modifies them.
They must exist even for a stopped backend. Grant the control-plane service
account the corresponding namespace-scoped projection permissions first.

New-cell OPA sidecars poll `/bundles/trino/<cell-id>` with that namespace's
bundle token. Legacy keeps `/bundles/trino`. Tokens cannot read another cell's
bundle. The observer credential remains separate from the catalog administrator.

## Initial assignment

Existing stored ownership is unchanged. In particular, a legacy row whose
stored ID is `cell-001` still belongs to legacy. New registry cells use the
reserved storage prefix `registered:`, so logical `cell-001` stores
`registered:cell-001`. Do not edit these values directly.

Use the authenticated operator console to select a cell **before first enabling
Trino**, or use its admin-only `PUT /api/v1/orgs/<org>/trino/cell` endpoint with
`{"cell":"cell-001"}`. Selection itself does not enable Trino. Unselected new
warehouses retain the existing default: legacy claims them when enabled.
In registry-only mode there is no default placement: both enablement endpoints
reject an unassigned warehouse with "select an initial Trino cell before
enabling Trino". Provision without Trino, select the initial cell, then enable.
Already-enabled unassigned rows remain unprovisioned; disable them before
initial selection. Unknown stored ownership fails closed without mutation.
Assignments survive disable/re-enable. Already owned warehouses cannot change
cells through this endpoint, even when disabled.

Operational API calls require `?cell=<logical-id>` when no legacy cell exists.
The Trino pages provide an explicit cell selector; they never select an arbitrary
registered cell. The legacy default remains unchanged when legacy is configured.

`client_url` is the opaque client endpoint, not a cell-selection instruction to
customers. This PR does not implement authenticated Gateway assignment lookup.
Do not advertise a shared Gateway URL as usable for a new cell until server-side
routing has been separately wired and tested with an authenticated query.
Coordinator/catalog readiness alone does not prove Gateway reachability.

## Blue running, green stopped

Both backends receive shared authentication, tenant-password, and OPA
projections. Only backends configured `running: true` receive catalog API calls
or live observer polls. A stopped green does not make blue's tenants unhealthy.
When green starts, update the registry and restart the control plane. Every
running backend must reconcile successfully before a tenant is reported ready;
one successful coordinator cannot conceal another's missing catalog or failure.
The console observes the configured routing-active backend. Usage collection
polls each running backend independently under the existing leader lease.

Each backend's catalog reconciliation has a 30-second context budget. Cell
reconciliation runs independently with a 90-second external-API budget.
Existing config-store methods retain their database timeout behavior; these
budgets are not a hard deadline for stalled database calls.

## Local verification and recovery

Run `just test-trino`, `just test-trino-admin`, `just ui-test`, and `just lint`.
The PostgreSQL-backed tests exercise initial-selection races against legacy
claiming and enablement. The isolated Trino CI lane exercises the real query
and projection path; never redirect it to a shared coordinator.

For a failed rollout, correct invalid JSON, duplicate identities, missing
namespace permissions, missing chart-managed internal Secrets, or TLS errors
and restart the control plane. Do not delete bootstrap sentinels or regenerate
internal credentials as a recovery shortcut. Preserve the registry while any
warehouse remains assigned to it. Removing a configured cell does not migrate
its warehouses: unknown ownership fails closed in the console.

## Existing-warehouse migration follow-up

Initial assignment is deliberately not a live-migration API. Disabling Trino
does not prove that cached credentials, open transactions, or direct coordinator
clients can no longer submit work. A maintenance move needs an enforced source
admission barrier, verified drain, destination provisioning, an explicit
assignment/routing switch, and source cleanup. Until that barrier exists, use a
previously non-Trino-enabled test warehouse for new-cell testing. Do not change
an existing warehouse's row to simulate a completed move.

Gateway public exposure is also a separate gate: authenticate every externally
reachable API and UI before publishing it; keep unauthenticated probes internal.
