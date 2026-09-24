# Trino service credentials

Internal jobs mint one short-lived service grant per job through the existing authenticated provisioning API.
The grant's audit principal is metadata, not the login name.
The login remains stable across refreshes so query polling and cancellation retain the original query owner.

## Authentication contract

The Trino password authenticator posts `{"username":"acme.svc_<24 lowercase hex>","password":"<secret>"}` to `/auth/trino/service-credentials` with a dedicated bearer token.
On success, the response contains `user`, `groups`, and `expires_at`.
The authenticated user is the exact qualified grant identifier.
The groups contain the organization's catalog group and resource tier, preserving the same tenant limits as persistent users.
Service grants are organization-wide, matching their existing Duckgres permissions.

The control plane reads the organization, Trino enabled state, grant hash, expiration, revocation, and tier from Postgres on every request.
It caches only successful bcrypt comparisons, keyed by a digest of the stored hash and supplied secret, for up to five minutes and 4,096 entries.
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

## Rollout

1. Deploy the gateway's explicit `service_principal_prefix` binding support before enabling control-plane publication of service identities.
2. Deploy the Trino service-credential authenticator and its static-file reserved-user rule.
   Static password authentication must reject only qualified minted grant names matching `<database_name>.svc_<24 lowercase hex>`, including stale file entries, so an expired grant cannot fall through to a persistent user with the same name.
   Other persistent login names such as `svc_airbyte` remain supported.
3. Configure a distinct validation token in the control plane and Trino using secret files.
   Leave `DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE` unset until both sides and the gateway support the protocol.
   Configure the authenticator endpoint through a protected service network with TLS; any local plaintext transport requires explicit deployment opt-in.
4. Deploy the control plane with `DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE` set.
   Mint and refresh now return `trino_connect` for ready assigned cells.
   Pooled tenant bindings include an explicit `<database_name>.svc_` prefix and a changed binding revision; the gateway still checks tenant admission before routing these requests.
5. Deploy internal clients that use the returned connection block and renew the grant without rotation during long-running queries.
   Missing `trino_connect` must fail closed rather than reading a stored root password.

The normal pgwire `connect` block remains unchanged.
On a tenant-qualified Trino hostname the returned username is the bare `svc_` grant; on a shared hostname it is `<database_name>.svc_...`.
Do not qualify an already qualified username again.

## Rotation and recovery

To rotate validation tokens, first place old and new tokens in the control-plane file and restart all control-plane replicas.
Then replace the Trino token file's first line with the new token.
After every authenticator uses the new token, remove the old token from the control-plane file and restart replicas again.
Neither token may equal an admin or discovery token.

For HTTP 401, check token agreement, the qualified grant's tenant, grant expiration and revocation, and whether the organization still has Trino enabled.
For HTTP 503, check control-plane database connectivity and latency.
For a missing `trino_connect`, check that service auth is enabled and the assigned cell is configured and ready.
For gateway rejection, check that the prefix binding's new revision reached the gateway and that the tenant is admitted.
The service identity must remain inside the organization's existing resource-group leaf, rather than allocating capacity per grant.

## Local validation

Run `just test-trino-service-credentials` for endpoint, token, target and binding tests.
Start the local integration Postgres using `tests/integration/docker-compose.yml`, then run `just test-configstore-integration` for mint, refresh, cross-tenant denial, disablement, tier changes, revocation and expiration.
Run `just lint` before publishing.
Cross-service tests must additionally exercise Trino POST, polling GET, cancellation DELETE, refresh, and persistent-user compatibility through the gateway; unit tests alone do not verify a deployment.

The disposable `tests/mw-dev/e2e/trino.sh` lane accepts `TRINO_SERVICE_CREDENTIALS_ENABLED=true` through `tests/mw-dev/run.sh`.
It then runs `e2e/trino-service-credentials.sh` against only its two freshly provisioned fixture tenants, testing statement submission, renewal without rotation, continuation polling, catalog isolation, wrong-tenant authentication, and revocation.
It fails if the candidate control plane omits `trino_connect`; it never silently skips missing auth wiring.
Before using this opt-in, configure the isolated fixture control plane and candidate Trino image with the dedicated authentication token and endpoint above.
The lane cannot prove cross-service behavior against an old Trino image that lacks the authenticator.
