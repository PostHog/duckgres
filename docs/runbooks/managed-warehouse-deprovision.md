# Runbook: Managed Warehouse Deprovision

Use this runbook to delete a managed warehouse and then remove the org from the control-plane config store. This is destructive: confirm the org ID, cluster, AWS account, and data-retention expectations before starting.

## Setup

```bash
ORG=<org-id>
API_BASE=http://127.0.0.1:8080
CANONICAL=${ORG//-/}
TARGET_CONTEXT=<target-k8s-context>

kubectl --context "$TARGET_CONTEXT" get ns duckgres ducklings
aws sts get-caller-identity
```

In a separate terminal, expose the admin API locally:

```bash
kubectl --context "$TARGET_CONTEXT" -n duckgres port-forward svc/duckgres-api 8080:80
```

Set the Duckling CR name used for Kubernetes verification:

```bash
if kubectl --context "$TARGET_CONTEXT" -n ducklings get ducklings.k8s.posthog.com "$CANONICAL" >/dev/null 2>&1; then
  CR_NAME=$CANONICAL
elif kubectl --context "$TARGET_CONTEXT" -n ducklings get ducklings.k8s.posthog.com "$ORG" >/dev/null 2>&1; then
  CR_NAME=$ORG
else
  CR_NAME=$CANONICAL
fi
echo "Using Duckling CR name: $CR_NAME"
```

Set `API_BASE` to the production admin API endpoint. Keep the default only when using the local port-forward above.

## Procedure

1. Start warehouse deprovisioning.

```bash
curl -sS -X POST "$API_BASE/api/v1/orgs/$ORG/deprovision" \
  -H "X-Duckgres-Internal-Secret: $DUCKGRES_INTERNAL_SECRET" \
  | jq .
```

2. Watch config-store state.

```bash
curl -sS "$API_BASE/api/v1/orgs/$ORG/warehouse/status" \
  -H "X-Duckgres-Internal-Secret: $DUCKGRES_INTERNAL_SECRET" \
  | jq .
```

The status endpoint is not enough by itself. The controller can mark config-store state deleted after deleting the Duckling CR while Crossplane resources are still terminating.

3. Verify Kubernetes and Crossplane cleanup.

```bash
kubectl --context "$TARGET_CONTEXT" -n ducklings get ducklings.k8s.posthog.com "$CR_NAME" || true
kubectl --context "$TARGET_CONTEXT" get managed -A | grep "$CR_NAME" || true
```

Repeat until the Duckling CR is gone and no managed resources remain. On macOS without `watch`:

```bash
while true; do
  date
  kubectl --context "$TARGET_CONTEXT" -n ducklings get ducklings.k8s.posthog.com "$CR_NAME" || true
  kubectl --context "$TARGET_CONTEXT" get managed -A | grep "$CR_NAME" || echo "no managed resources found"
  sleep 10
done
```

4. If an S3 bucket is stuck, empty it only after confirming data destruction is intended.

```bash
BUCKET=posthog-duckling-$CR_NAME-mw-prod-us

kubectl --context "$TARGET_CONTEXT" -n ducklings describe bucket.s3.aws.m.upbound.io "$BUCKET"
aws s3 ls "s3://$BUCKET" --recursive --summarize
aws s3 rm "s3://$BUCKET" --recursive
```

If versioning is enabled or suspended, delete object versions and delete markers in batches until the listing is empty:

```bash
aws s3api get-bucket-versioning --bucket "$BUCKET"
DELETE_PAYLOAD="/tmp/$BUCKET-delete-objects.json"

while true; do
  aws s3api list-object-versions --bucket "$BUCKET" --max-items 1000 --output json \
    | jq '{Objects: (((.Versions // []) + (.DeleteMarkers // [])) | map({Key, VersionId}))}' \
    > "$DELETE_PAYLOAD"
  [ "$(jq '.Objects | length' "$DELETE_PAYLOAD")" -eq 0 ] && break
  aws s3api delete-objects --bucket "$BUCKET" --delete "file://$DELETE_PAYLOAD"
done
```

5. Delete the org row only after the Duckling CR and managed resources are gone.

```bash
curl -sS -X DELETE "$API_BASE/api/v1/orgs/$ORG" \
  -H "X-Duckgres-Internal-Secret: $DUCKGRES_INTERNAL_SECRET" \
  | jq .
```

6. Verify the org is gone.

```bash
curl -sS "$API_BASE/api/v1/orgs" \
  -H "X-Duckgres-Internal-Secret: $DUCKGRES_INTERNAL_SECRET" \
  | jq -r '.[] | [.name, .database_name] | @tsv'

kubectl --context "$TARGET_CONTEXT" -n ducklings get ducklings.k8s.posthog.com "$CR_NAME" || true
kubectl --context "$TARGET_CONTEXT" get managed -A | grep "$CR_NAME" || true
```

## Legacy Hyphenated CRs

UUID-shaped Duckling CRs may render compact dehyphenated bucket names so composed S3 buckets fit AWS length limits. Manual cleanup should still snapshot and delete the exact existing Duckling CR:

```bash
kubectl --context "$TARGET_CONTEXT" -n ducklings get ducklings.k8s.posthog.com "$CR_NAME" -o yaml > "/tmp/$ORG-duckling.yaml"
kubectl --context "$TARGET_CONTEXT" get managed -A | grep "$CR_NAME" > "/tmp/$ORG-managed.txt"
kubectl --context "$TARGET_CONTEXT" -n ducklings delete ducklings.k8s.posthog.com "$CR_NAME"
```

Then continue with Crossplane verification, S3 cleanup if needed, and org deletion.
