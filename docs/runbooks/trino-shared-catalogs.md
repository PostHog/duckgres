# Shared catalog cells

This mode requires the reviewed lifecycle migration, strict catalog client,
Gateway rollout APIs, and rollout-readiness support. It is disabled by default.
Legacy cells retain their existing behavior.

## Configuration and activation

Each registered cell accepts `catalog_management`:

- Omitted: existing static backend reconciliation.
- `paused`: refresh authentication, resource groups, tenant credentials and
  each replica's OPA bundle, but submit no catalog DDL or provisioning state updates.
- `gateway-shared`: use the Gateway's authoritative active backend for catalog
  mutations. Keep both slot clients credential-ready, regardless of static
  `running` flags. Never reconcile catalog DDL against the inactive slot.

Managed cells must contain exactly `blue` and `green`. They require
`DUCKGRES_TRINO_MANAGED_GATEWAY_URL`, `DUCKGRES_TRINO_MANAGED_GATEWAY_USERNAME`,
and optionally `DUCKGRES_TRINO_MANAGED_GATEWAY_SERVER_NAME`. The capability in
`DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE` supplies both the API-only Basic password
and the transaction-admin header. These are one capability, not two factors.
`DUCKGRES_TRINO_ROLLOUT_CANARIES_FILE` uses the dedicated per-cell canary format
in [rollout readiness](trino-rollout-readiness.md).

First deploy the compatible image with registered cells `paused`. Let every
older unfenced controller stop, including terminating replicas; verify that
no old catalog mutation remains in flight. Do not kill unrelated client
connections or use a whole-control-plane `Recreate` deployment. The existing
SIGTERM path stops the provisioner independently of client connection drain.
Create dedicated canaries and private credentials while paused, then enable
`gateway-shared`. A pending canary becomes Ready through normal active-backend
provisioning before the first cell rollout. Never reuse customer passwords.

## Rollout protocol

1. Acquire the Gateway's durable cell rollout operation.
2. Read the current admission epoch and freeze provisioning **before** starting
   the target. Wait until the earlier catalog owner has safely finished.
3. Start the target with the same logical-cell persistent catalog store.
   Startup loads the existing definitions; no per-tenant CREATE replay runs.
4. The control plane bulk-checks the target catalogs and verifies mounted
   credentials for every enabled previously admitted warehouse. Historical
   admissions remain in this check after temporary state or credential errors.
   It binds the certificate to the exact live target process and canary.
5. Cut over the Gateway route, then release the provisioning freeze.
6. Provision new catalogs only on the new active backend while the old source drains.

New warehouse enablement can wait through target startup and checks. Existing
queries continue. Backend health alone does not revoke an existing Ready
admission; genuine shared-input errors still surface. Authentication refresh
retains its existing eventual projection semantics during freezes and holds.

## Internal API

All endpoints require the handler-specific transaction-admin capability.
They accept a registered routing group, not a caller-supplied backend URL.

- `GET /internal/trino/rollout-provisioning/{routingGroup}` returns
  `operationId`, `admissionEpoch`, `frozen`, `stable`, `prepared`,
  `targetBackend`, `nodeId`, `coordinatorId`, `rosterHash`, and `admittedCount`.
- `POST .../freeze` accepts `operationId`, `planHash`, and
  `expectedAdmissionEpoch`. A new freeze increments the epoch exactly once.
  `202` means the prior owner has not completed; `200` acknowledges stability.
- `POST .../release` accepts `operationId`, `planHash`, and `admissionEpoch`.
  It verifies the Gateway cutover and current target process, then increments
  the epoch again. Identical release retries return the durable receipt.

Never reread a new epoch to force a stale operation through a conflict.
GET is read-only; it does not provision or alter assignments.

## Failure handling

Every catalog mutation has a durable, one-shot intent before submission.
Only verified `FINISHED` success or rejection before submission permits
clearing it. A remote `FAILED` query can still have a synchronous catalog
mutation running; cancellation is not a completion fence. Transport failures,
ambiguous responses, controller crashes, and remote query failures retain
ownership. There is no timeout takeover or automatic force-unlock endpoint.

Before explicit recovery, fence the original controller and any potentially
running coordinator mutation. Inspect the persisted catalog definitions and
current process identities. Reconcile the intended outcome before releasing
ownership through a separately reviewed operator procedure. Do not delete
lifecycle rows, bootstrap sentinels, or regenerate credentials as a shortcut.
An immutable certificate whose target process changed requires recovery;
the control plane must not silently overwrite it.

## Scale and verification

Catalog inventory is one bounded bulk SQL query, not one query per warehouse.
Unchanged catalogs require no CREATE or warehouse-property lookup. Mounted
credential checks still scale with warehouse count and live members; they use
bounded batches and deadlines. The 10,000-catalog unit regression proves no
DDL replay, not a production throughput benchmark. Existing secret projection
and metadata resolution costs remain; this change does not eliminate them.

Only catalog/provisioning selection follows the Gateway in this change.
Existing admin live-query observers and usage collectors still use static
registry selections. Updating those observers after a cell cutover is a
separate follow-up; do not interpret their stale backend health as lost
catalog admission.

Run `just test-trino Managed`, `just test-trino SharedCatalog`,
`just test-trino-admin`, `just test-controlplane-k8s`, and `just lint`.
The retained isolated end-to-end lane must exercise the actual Gateway and
Trino images before activation. Configuration PRs are not deployment approval.
