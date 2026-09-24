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
Mounted credential readiness additionally requires `get`/`list` on `pods` and
`create` on `pods/exec` in each Trino namespace. Apply those chart permissions
before deploying the readiness-aware control plane.

New-cell OPA sidecars poll `/bundles/trino/<cell-id>` with that namespace's
bundle token. Legacy keeps `/bundles/trino`. Tokens cannot read another cell's
bundle. The observer credential remains separate from the catalog administrator.
A shared-pool cell has no fixed coordinator, so its observer reads the pool's
current instances from the config store on each call and asks every instance
that can hold queries (ADMITTED, SERVING, DRAINING, SEALED or SUSPECT) on its
own Service, declaring the Gateway's forwarded HTTPS hop. The console and usage
metering see the union; one unreachable member does not hide the others.
In registry-only mode, `/bundles/trino` is absent and returns HTTP 404. Missing
bundle URLs never fall back to the admin UI page.

## Initial assignment

Existing stored ownership is unchanged. In particular, a legacy row whose
stored ID is `cell-001` still belongs to legacy. New registry cells use the
reserved storage prefix `registered:`, so logical `cell-001` stores
`registered:cell-001`. Do not edit these values directly.

Use the authenticated operator console to select a cell **before first enabling
Trino**, or use its admin-only `PUT /api/v1/orgs/<org>/trino/cell` endpoint with
`{"cell":"cell-001"}`. Selection itself does not enable Trino. Unselected new
warehouses retain the existing default when `DUCKGRES_TRINO_DEFAULT_CELL` is unset:
legacy claims them when enabled. In registry-only mode with no configured default, both enablement endpoints
reject an unassigned warehouse with "select an initial Trino cell before
enabling Trino". Provision without Trino, select the initial cell, then enable.
Already-enabled unassigned rows remain unprovisioned; disable them before
initial selection. Unknown stored ownership fails closed without mutation.
Assignments survive disable/re-enable. Already owned warehouses cannot change
cells through this endpoint, even when disabled; use the move endpoint below.

### Automatic placement runbook

Set `DUCKGRES_TRINO_DEFAULT_CELL` (default unset) to a registered shared-pool ID.
The pool must have `tenant_admission=true`, with the pool, operator, and catalog
writer enabled. Invalid configuration fails startup; fix or revert it and restart.

Both provision and standalone Trino enablement assign this default atomically,
only when the org has no owner. Existing assignments always win, including across
retries and disable/re-enable. An unavailable pool stays pending without fallback.

For local or deployment acceptance, restart with the configuration and provision a
fresh synthetic org; verify `/api/v1/orgs/<org>/trino` reports the expected `cell.id`,
then test an authenticated query. The mw-dev harness supports this placement check
with `E2E_TRINO_DEFAULT_CELL=<id>` and `E2E_TRINO_DEFAULT_CELL_ORG=<fresh-org>`;
it retains the test warehouse because Hoglake retirement is unsupported.
Fully roll out the backend before setting the default, and finish the configuration
rollout before acceptance: pods with old configuration can still assign legacy.
Changing or reverting the default never migrates existing assignments.

Operational API calls require `?cell=<logical-id>` when no legacy cell exists.
The Trino pages provide an explicit cell selector; they never select an arbitrary
registered cell. The legacy console selection remains unchanged when legacy is configured;
this is separate from the warehouse placement default.

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
Catalog creation is followed by a check of the current tenant password file on
every active member reported by `system.runtime.nodes`, including a coordinator
and at least one worker. Node and pod identities are checked again after the
observation; replacements or projection lag keep the tenant `provisioning`.
Existing catalogs undergo the same check on every reconcile. See the
[readiness runbook](runbooks/trino-readiness.md) for the precise contract and
failure recovery.
The console observes the configured routing-active backend. Usage collection
polls each running backend independently under the existing leader lease.

Each backend's catalog reconciliation has a 30-second context budget. Cell
reconciliation runs independently with a 90-second external-API budget.
Existing config-store methods retain their database timeout behavior; these
budgets are not a hard deadline for stalled database calls.

## Local verification and recovery

For opt-in shared-store blue/green cells, use the
[shared catalog runbook](runbooks/trino-shared-catalogs.md). This mode freezes
new provisioning before target startup and uses the Gateway's active route.
It does not replay catalog CREATE statements on the standby. The static
registry behavior documented above remains the default.

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

## Moving an existing warehouse

`POST /api/v1/orgs/<org>/trino/cell/move` with `{"from":"legacy","to":"cell-001"}`
reassigns an org that a cell already owns. It is admin-only and audited, and it
names both cells by their console id. The move is a compare-and-swap on the
owner, taken under the org's admission lock: it applies only while the org is
still on `from` (otherwise 409), so a stale console cannot move an org it has
not seen, and repeating a completed move is a no-op. The row returns to
`pending` with `ready_at`/`failed_at` cleared, because the destination has not
provisioned the org yet.

Nothing else is special about a move. On its next tick the source cell finds
the org absent from its wanted set and removes what it projected (catalog,
tenant password, `password.db` lines, policy group), exactly as it does for a
disabled org. The destination provisions the org like a new assignment; a pool
with tenant admission holds it at provisioning until its publication commits.
Per-org state writes are fenced to the owning cell, so a source tick that
listed the org before the move cannot mark the moved row ready.

**A move is not a live migration.** Between the source's cleanup and the
destination's readiness the org has no Trino, typically a reconcile tick or
two. Queries running on the source when its catalog is dropped fail, and
clients that address the source directly (for example the legacy
`trino.dw.<env>.postwh.com` endpoint) stop working for that org. Clients on the
org's tenant host follow the Gateway to the destination. Tell the org's Trino
users before moving it. A zero-downtime handover would need an enforced source
admission barrier, a verified drain and overlapping ownership; none of that
exists. Do not edit `trino_cell_id` by hand to simulate a move: it skips the
readiness reset and the ownership check.

Gateway public exposure is also a separate gate: authenticate every externally
reachable API and UI before publishing it; keep unauthenticated probes internal.

### Query obligations that outlive their clients

A client can stop polling before the Gateway observes the query's terminal response.
For draining pool members, the operator checks up to ten query obligations per tick, within a five-second read budget and a one-second timeout per coordinator probe.
The cursor rotates so long-running queries do not prevent checking later candidates.
These limits are fixed safety defaults, with no additional activation flag beyond the existing pool operator switch.

The operator uses the pool's observer credential against the exact member's `/v1/query/{queryId}/drain-status` endpoint.
Only an explicit `absent: true` response with the registered node and coordinator identities permits submission to Gateway reconciliation.
Gateway independently verifies the member identity, fences the controller epoch and generation, and checks that admissions have not changed.
It retains reconciled terminal records for the normal retry window.
The operator reads obligations again on a later tick before attempting the existing seal and retirement protocol.
Open transactions, live queries, retained results, and pending or uncertain admissions still prevent draining.

Deploy the Gateway reconciliation API and the Trino drain-status endpoint before relying on this recovery path.
Configure finite completed-query history retention, such as `query.max-history-age=15m`, on supporting Trino images so a quiet member can eventually prove absence.
An older endpoint returning HTTP 404, missing proof fields, authentication failures, timeouts, or a coordinator identity change leaves the query obligation intact.
No elapsed-time cutoff marks a query complete.
A member already running an older Trino image cannot use this path to unblock its own replacement.
Recover such a drain through the Gateway's authorized owner cancellation procedure before expecting the new image to serve it.
For a blocked drain, inspect the bounded `Trino drain proof unavailable` diagnostic and the existing Gateway obligation counters; verify both endpoint versions and observer authorization.
Successful recovery logs `Trino drain query obligations reconciled` with an aggregate count, without query text or identifiers.
Do not remove ledger rows or bypass the Gateway's seal refusal.
