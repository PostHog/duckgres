# Trino service credentials

Internal jobs mint one short-lived service grant per job through the existing authenticated provisioning API.
The grant's audit principal is metadata, not the login name.
The login remains stable across refreshes so query polling and cancellation retain the original query owner.

## Authentication contract

The Trino password authenticator posts `{"username":"acme.svc_<24 lowercase hex>","password":"<secret>"}` to `/auth/trino/service-credentials` with a dedicated cell-scoped bearer token.
The token identifies the caller's immutable configured cell ID; no request field can select another cell.
On success, the response contains `user`, `groups`, and `expires_at`.
The authenticated user is the exact qualified grant identifier.
The groups contain the organization's catalog group and resource tier, preserving the same tenant limits as persistent users.
Service grants are organization-wide, matching their existing Duckgres permissions.

The control plane reads the organization, current cell assignment, Trino enabled state, grant hash, expiration, revocation, and tier from Postgres on every request.
It rejects a grant when the organization is assigned to a different cell from the one authenticated by the bearer token.
It caches only successful bcrypt comparisons, indexed by HMAC-SHA-256 of the stored hash and supplied secret using an ephemeral process-local random key, for up to five minutes and 4,096 entries.
A cached comparison cannot bypass expiry, revocation, secret rotation, tenant disablement, or a tier change.
Database errors fail closed with HTTP 503; invalid credentials return HTTP 401.
The request body is bounded to 4 KiB and database work has a three-second deadline.
Do not log authorization headers or request bodies.

Unlike Duckgres pgwire, Trino authenticates every HTTP request, including polling and cancellation.
Clients must renew before expiry using `rotate_secret: false` on the existing refresh endpoint.
Renewal extends only live, non-revoked grants and preserves both login and secret, including the gateway query-owner fingerprint.
The response explicitly contains `secret_rotated: false` and omits `credential_secret`; retain the in-memory secret only after checking this marker.
An older server may ignore the new request field and rotate the secret, so reject any renewal response that does not confirm non-rotation.
Default refresh still rotates the secret for existing pgwire clients.
Do not rotate a grant used by an active Trino query, and stop if renewal fails.
Revocation or expiry blocks subsequent requests without automatically cancelling already-running queries.
Use an authorized operator connection if a revoked job needs cancellation.

## Cell token configuration

`DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE` names a control-plane-only JSON file:

```json
{
  "cells": [
    {"cell_id": "registered:cell-a", "tokens": ["<cell-a-current-token>", "<cell-a-previous-token>"]},
    {"cell_id": "registered:cell-b", "tokens": ["<cell-b-current-token>"]}
  ]
}
```

The example contains nonfunctional placeholders, not valid tokens.
Use a distinct cryptographically random token of at least 32 bytes for each cell.
Each `cell_id` must exactly match its immutable stored ID in the active configured fleet; public display IDs and aliases are not accepted.
Legacy cells use their exact configured stored ID, not an assumed `legacy` value.
The file supports up to four rotation tokens per cell and 64 KiB in total.
Unknown IDs, duplicate cells, tokens shared across cells, and reused admin or discovery tokens fail startup.
An unmapped cell does not receive `trino_connect` in mint or renewal responses.

Mount only a cell's own tokens on that cell's coordinators, in a separate plain-text file with the current token first.
Never mount the complete JSON map or another cell's tokens on a coordinator.
Trino reads the first nonempty token line on each request; the control plane loads the JSON map at startup.
Workers and application clients do not receive validation tokens.

## Rollout

1. Deploy the gateway's service-grant lookup through existing bare database root-principal bindings to every serving replica before enabling service-credential clients.
   Principal publication and recorded operation-step schemas remain unchanged; no tenant republication or admission reset is required.
2. Deploy the Trino service-credential authenticator and its static-file reserved-user rule.
   Static password authentication must reject only qualified minted grant names matching `<database_name>.svc_<24 lowercase hex>`, including stale file entries, so an expired grant cannot fall through to a persistent user with the same name.
   Other persistent login names such as `svc_airbyte` remain supported.
3. Configure the control-plane cell map and the separate cell-specific coordinator token files.
   Leave `DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE` unset until both sides and the gateway support the protocol.
   Configure the authenticator endpoint through a protected service network with TLS; any local plaintext transport requires explicit deployment opt-in.
4. Deploy the control plane with `DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE` set.
   Mint and refresh now return `trino_connect` only for ready assigned cells included in the token map.
   Pooled tenant principal lists and binding revisions remain unchanged when service authentication is enabled or disabled.
   The gateway resolves qualified grants through the existing root-principal mapping and checks the same tenant admission gate.
5. Deploy internal clients that use the returned connection block and renew the grant without rotation during long-running queries.
   An initial mint without `trino_connect` must fail closed rather than reading a stored root password.
   A successful nonrotating renewal may omit the optional connection block; clients retain the original validated target.

The normal pgwire `connect` block remains unchanged.
On a tenant-qualified Trino hostname the returned username is the bare `svc_` grant; on a shared hostname it is `<database_name>.svc_...`.
Do not qualify an already qualified username again.

## Rotation and recovery

Rotate one cell at a time: add its new token to that cell's entry alongside the previous token, then restart all control-plane replicas.
Replace only that cell's coordinator token files so the new token is the first line.
After all of the cell's authenticators use the new token, remove its previous token from the JSON map and restart control-plane replicas again.
Do not move tokens between cells or reuse an admin or discovery token.

For HTTP 401, check that the coordinator token maps to the organization's current assigned cell, token agreement, the qualified grant's tenant, grant expiration and revocation, and whether the organization still has Trino enabled.
For HTTP 503, check control-plane database connectivity and latency.
For a missing `trino_connect`, check that service auth is enabled, the assigned cell is configured and ready, and its exact stored ID has an entry in the token map.
For gateway rejection, check that the existing bare database root principal is published for the correct tenant, that the tenant is admitted, and that every serving gateway supports service-grant lookup.
Conflicting exact-grant and root-principal tenant mappings fail closed.
The service identity must remain inside the organization's existing resource-group leaf, rather than allocating capacity per grant.

## Local validation

Run `just test-trino-service-credentials` for endpoint, token, target and binding tests.
Start the local integration Postgres using `tests/integration/docker-compose.yml`, then run `just test-configstore-integration` for mint, refresh, cross-tenant denial, disablement, tier changes, revocation and expiration.
Run `just lint` before publishing.
Cross-service tests must additionally exercise Trino POST, polling GET, cancellation DELETE, refresh, and persistent-user compatibility through the gateway; unit tests alone do not verify a deployment.

The disposable `tests/mw-dev/e2e/trino.sh` lane accepts `TRINO_SERVICE_CREDENTIALS_ENABLED=true` through `tests/mw-dev/run.sh`.
It then runs `e2e/trino-service-credentials.sh` against only its two freshly provisioned fixture tenants, testing statement submission, renewal without rotation, continuation polling, catalog isolation, wrong-tenant authentication, and revocation.
It fails if the candidate control plane omits `trino_connect`; it never silently skips missing auth wiring.
Before using this opt-in, configure the isolated fixture control plane and candidate Trino image with the cell-specific authentication token, control-plane cell map, and endpoint above.
The fixture map uses the exact stored ID `ci-pr-<PR number>`; its public status label `legacy` is not the stored cell ID.
The lane checks the persisted assignment before minting and does not generate or mount these authentication files.
The lane cannot prove cross-service behavior against an old Trino image that lacks the authenticator.
