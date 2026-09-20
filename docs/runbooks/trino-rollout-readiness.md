# Trino rollout readiness

This optional, read-only endpoint observes a registered blue or green backend:

`GET /internal/trino/rollout-readiness/{routingGroup}/{color}`

It requires exactly one `X-Gateway-Transaction-Admin-Token` header. The token is
specific to this handler and does not grant general control-plane API access.
The endpoint does not change replicas, warehouse assignments, catalogs,
provisioning state, or Gateway routes.

## Configuration

Both variables are empty by default; the endpoint is then not registered.
Supplying only one variable, invalid content, or incomplete cell configuration
fails startup for fixed-cell readiness.

Shared-pool Gateway authentication reuses `DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE`,
but this does not enable the fixed-slot endpoint. When all registered cells use
`mode: shared-pool` and no canary file is configured, the endpoint stays disabled;
the legacy single coordinator can coexist with those pools. The pool Gateway
client still validates its token independently. A mixed registry requires canaries
only for its fixed cells. Explicit canary configuration remains fail-closed, and
a canary entry for a shared-pool cell is rejected.

For a Gateway with form authentication enabled, also set
`DUCKGRES_TRINO_MANAGED_GATEWAY_USERNAME` to its existing API-role identity.
The pool client sends Basic authentication using that username and the existing
token as password. It also sends `X-Gateway-Transaction-Admin-Token` for the
separate capability check. Without a username, the client retains Bearer
authentication; that mode does not satisfy a form-authenticated Gateway API.

| Variable | Content |
| --- | --- |
| `DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE` | Mounted private capability value, at least 32 characters. |
| `DUCKGRES_TRINO_ROLLOUT_CANARIES_FILE` | Mounted private JSON document containing one dedicated canary per registered cell. |

The canary file has this shape. These are illustrative values, not credentials:

```json
{"canaries":[{"cell":"cell-test","orgID":"canary-org","principal":"canary-test","password":"replace-through-private-secret-management"}]}
```

Provision each dedicated canary through the normal warehouse API and retain its
one-time credential only in private secret management. Assign and enable it on
the intended registered cell. Do not reset or reuse an existing customer's
password. The config store retains a password hash, not a retrievable client
password. Before probing, the handler checks the current warehouse and Trino
readiness, assignment, principal, and root-user enabled state with a fresh query.
Unknown, disabled, or wrong-cell canaries fail without sending their credentials.

The process loads these files at startup. Credential rotation requires a
coordinated restart; there is no automatic credential generation or rotation.
Only registered cells with exactly `blue` and `green` backends are supported.
Backend URLs, TLS server names, namespaces, and pod selectors come from the
server registry. Callers cannot supply a URL, SQL, namespace, or credential.

## Response and limits

The response contains `schemaVersion: 1`, the logical `cell`, explicit
`routingGroup`, color, and backend identity,
`observedAt`, and pod counts. Pod inventory includes terminating and non-ready
pods. `pods.images` is the deduplicated set of each main Trino container's
`specImage` and `runtimeImageID`; sidecar images do not enter this set. An OCI
index digest in the pod specification can legitimately differ from the runtime
platform manifest digest. Compare the selected artifact against `specImage`.

An empty inventory returns HTTP 200 with `pods.total: 0` and no coordinator or
canary facts. This proves observed pod absence, not warm readiness. A warm
response requires exactly one ready coordinator, ready workers, authenticated
active node membership mapped exactly to the selected pods' IPs and roles,
stable coordinator process identity, and a successful
tenant catalog-metadata query. `coordinator.registeredWorkers` must equal the
observed worker count. The caller additionally checks its desired worker count,
expected artifact pin, and expected backend identity.

Logical cell IDs and routing groups can differ. Use `routingGroup` for this
endpoint's path and routing assertion; `cell` reports the registered ownership
identity. Advertised node addresses must be pod IP literals, as required by the
existing mounted-credential observer. Duplicate, extra, missing, wrong-role, or
other-slot members fail readiness even when total worker counts match.

Checks are uncached. Each request has a 10-second total deadline and the process
allows four concurrent probes. Use a client timeout longer than 10 seconds.
Inventory is limited to 1,000 pods and 1,000 nodes. SQL reads have a 32-page,
4-MiB total, 1-MiB-per-page, and 4,096-row limit. HTTP requests reject redirects
and result URLs outside the configured HTTPS coordinator origin. This scoped
cluster transport does not use ambient proxy settings or change `NO_PROXY`.

Responses disable caching. Failures return generic codes, never raw coordinator
errors, catalog names, warehouse identities, passwords, or authorization values.
Busy, incomplete, expired, or failed observations return HTTP 503. Retry after
the underlying condition changes; never interpret an error as pod absence.

## Guarantee boundary

A canary checks one representative tenant's authentication and catalog metadata.
It does not scan tenant data or prove that every admitted tenant is projected.
This endpoint alone is not a safe-cutover authorization. Automatic rollout also
needs authoritative ongoing backend provisioning, complete target preparation,
and a fence against concurrent new admission while preparation is certified.
Those operations must remain separate from this GET endpoint.

## Verification

Run `just test-trino TrinoRolloutReadiness`, `just test-trino-admin`, and
`just lint`. The tests cover real TLS HTTP exchanges, foreign continuations and
redirects, capability failures, image identity, stopped inventory, cancellation,
concurrency, and PostgreSQL-backed canary eligibility. These local tests are not
evidence that an environment's real coordinator or private canary is configured.
Verify both warm and stopped observations against an isolated deployment before
using this endpoint as an orchestration prerequisite.
