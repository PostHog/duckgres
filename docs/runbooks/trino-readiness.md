# Trino tenant readiness

Duckgres reports a tenant ready only after its catalog reconciles and its
current metadata password has been observed in the mounted Secret on every
active member of every configured running backend. This includes an active
coordinator and at least one active worker. Successful `CREATE CATALOG` checks
the coordinator; it does not acknowledge workers' independent Secret mounts.

The provisioner queries `system.runtime.nodes`, matches member addresses to
pods in the cell namespace, and runs a read-only SHA-256 check inside the Trino
container. It compares hashes to the Secret bytes written during that reconcile.
Passwords and hashes are never logged or placed in status messages. A missing
or stale file leaves only the affected tenant `provisioning`. API, permission,
malformed response, or mount configuration errors fail readiness explicitly.
Pod replacements, restarts, and membership changes during the check defer
readiness to the next reconcile.

This records observed credential readiness. It is not a guarantee against a
worker joining or failing afterward, proof that every lazy catalog has loaded,
or an end-to-end Gateway health check. Membership and files are checked again
on every reconcile, including for existing catalogs and ready tenants.

## Deployment requirements

- Apply namespace-scoped `pods` `get`/`list` and `pods/exec` `create` permissions
  for the control-plane service account before deploying this code. Kubernetes
  grants exec capability; the provisioner uses it only for the fixed read-only
  observation command.
- Mount `trino-tenant-secrets` read-only as a whole Secret volume in every Trino
  container. The default mount is `/etc/trino/tenant-secrets`; an explicit
  `DUCKGRES_TRINO_TENANT_SECRET_MOUNT_PATH` must match the actual mount. A
  `subPath` mount cannot receive projected Secret updates and is rejected.
- The Trino image must contain `/bin/sh` and `sha256sum`. Member HTTP URIs must
  identify pod IPs in that namespace. Ambiguous container mappings are rejected.
- Publish the updated Duckgres OPA bundle. The provisioner administrator needs
  narrow read access to `system.runtime.nodes`; tenant access remains denied.

Checks batch at most 128 tenant files per exec and use at most four concurrent
execs per backend. Each exec has a five-second deadline within the existing
30-second backend reconciliation budget and 90-second cell API budget. These
are fixed defaults, with no readiness bypass flag. Stopped backends are skipped.

## Local verification

Run `just test-trino`, `just test-controlplane-k8s`, `just test-mw-fixtures`, and
`just lint`. The readiness regressions reproduce successful coordinator catalog
creation while a worker still has missing or stale credentials, then verify the
transition to ready once that worker observes the current file. Tests also cover
multiple running backends, member replacement, exec failures, and timeouts.

Run the isolated Trino CI lane for the real Kubernetes projection and query
path. It waits for the existing Duckgres readiness API and immediately performs
tenant queries; do not add sleeps or query retries to hide provisioning errors.

## Recovery

For `provisioning`, read the status message and inspect active membership and
pod readiness in the affected cell. Missing or stale files should recover on
the next reconcile after Kubelet projects the Secret. Do not print Secret
contents or hashes while diagnosing projection lag.

For a failed observation, check the control-plane service account's namespaced
pod read/exec permissions, API connectivity, the Trino container's mount path,
and the required shell utilities. For a denied node inventory query, verify the
OPA bundle has refreshed and the provisioner identity is authenticated. Correct
the configuration and allow reconciliation to retry. Keep Trino's missing-file
catalog validation enabled; do not bypass readiness or mark the tenant ready
manually.
