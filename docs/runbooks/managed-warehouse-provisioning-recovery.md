# Runbook: Managed Warehouse Provisioning Recovery

Use this runbook when a managed warehouse is `failed` in the Duckgres API or
admin UI after its Duckling infrastructure was repaired externally. The checks
below are read-only. Replace every placeholder explicitly; never rely on the
current kubectl context.

## Setup

```bash
TARGET_CONTEXT=<target-k8s-context>
ORG=<org-id>
DUCKLING=<duckling-cr-name>
API_BASE=<admin-api-base-url>
```

## Diagnose

1. Confirm the exact context, namespace, and Duckling aggregate conditions.

```bash
kubectl --context "$TARGET_CONTEXT" config current-context
kubectl --context "$TARGET_CONTEXT" -n ducklings get ducklings.k8s.posthog.com "$DUCKLING"
kubectl --context "$TARGET_CONTEXT" -n ducklings get ducklings.k8s.posthog.com "$DUCKLING" \
  -o jsonpath='{range .status.conditions[*]}{.type}{"="}{.status}{" reason="}{.reason}{" message="}{.message}{"\n"}{end}'
```

The Duckling must report both `Synced=True` and `Ready=True`. If either is
false, inspect the referenced composed resources before expecting Duckgres to
recover:

```bash
kubectl --context "$TARGET_CONTEXT" -n ducklings get ducklings.k8s.posthog.com "$DUCKLING" \
  -o jsonpath='{range .spec.crossplane.resourceRefs[*]}{.apiVersion}{" "}{.kind}{" "}{.name}{"\n"}{end}'
```

2. Check the persisted warehouse state through the API.

```bash
curl -sS "$API_BASE/api/v1/orgs/$ORG/warehouse/status" \
  -H "X-Duckgres-Internal-Secret: $DUCKGRES_INTERNAL_SECRET" \
  | jq '{state,status_message,ready_at,failed_at}'
```

3. Wait for automatic convergence. The provisioner observes failed warehouses
every 10 seconds. It requires the Duckling aggregate Ready condition, the S3
bucket, metadata endpoint and credential, worker IAM role, and a successful
metadata-store connection probe. It keeps the row failed until all gates pass,
then atomically changes it to ready and clears `failed_at`. While the probe is
blocked, `status_message` reports a fixed, credential-safe category such as
authentication, DNS resolution, or timeout; it never persists the raw probe
error or connection string.

```bash
while true; do
  curl -sS "$API_BASE/api/v1/orgs/$ORG/warehouse/status" \
    -H "X-Duckgres-Internal-Secret: $DUCKGRES_INTERNAL_SECRET" \
    | jq '{state,status_message,ready_at,failed_at}'
  sleep 10
done
```

The admin UI refreshes a visible failed warehouse every 5 seconds. A page
reload should not be necessary.

## If Recovery Does Not Complete

- `Ready=False`: continue with the Duckling or composed-resource message; the
  controller intentionally preserves `failed`.
- `Ready=True` but Duckgres remains failed: verify that the status contains the
  bucket, metadata connection fields and credential Secret reference, and IAM
  role. Then inspect control-plane logs for the end-to-end metadata probe error.
- Kubernetes read or Secret resolution errors: fix API/RBAC/Secret availability;
  the next controller tick retries observation.
- Do not edit the config-store state by hand. That bypasses the same readiness
  and connection checks that protect worker activation.

## Local Verification

The regression and full Kubernetes-tagged control-plane suite run with:

```bash
just test-controlplane-k8s
```

The admin polling behavior, typecheck, and production UI build run with:

```bash
just ui-test
```
