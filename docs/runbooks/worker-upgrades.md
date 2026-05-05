# Worker Upgrades and Canaries

Duckgres supports running multiple versions of DuckDB and DuckLake workers simultaneously on the same Kubernetes control plane. This allows for safe, gradual rollouts (canarying) and per-tenant build pinning.

## How it Works

The control plane manages a heterogeneous pool of workers. The target build for any given connection is resolved with the following precedence:

1.  **Tenant Override:** The `image` and `ducklake_version` columns on the tenant's row in `duckgres_managed_warehouses` (Config Store).
2.  **Global Fallback:** The `k8s.worker_image` and `ducklake.default_spec_version` set in the Control Plane's `duckgres.yaml`.

### 1. Setting Global Defaults

Maintainers define the "stable" fleet version in the Control Plane configuration:

```yaml
# duckgres.yaml
k8s:
  # Use the multi-arch GHCR manifest (or its ECR mirror). The bare :<sha>
  # and :latest tags always point at the default DuckDB version (1.5.2 today)
  # produced by the matrix CD.
  worker_image: "ghcr.io/posthog/duckgres-worker:latest"
  worker_image_pull_policy: "IfNotPresent"

ducklake:
  # Default DuckLake spec version for migration checks (major.minor only)
  default_spec_version: "1.0"
```

When the Control Plane starts, the **Janitor** ensures that the shared "neutral" warm pool is populated with pods running this image.

### 2. Available Worker Builds

Worker images are produced by [`.github/workflows/container-image-worker-cd.yml`](../../.github/workflows/container-image-worker-cd.yml) on every push to `main`. Each push produces a multi-arch (arm64 + amd64) manifest per DuckDB version in the matrix.

**Tag conventions:**

| Tag                                              | Points at                                   |
|--------------------------------------------------|---------------------------------------------|
| `ghcr.io/posthog/duckgres-worker:<sha>`          | Default DuckDB version, this commit         |
| `ghcr.io/posthog/duckgres-worker:latest`         | Default DuckDB version, latest `main`       |
| `ghcr.io/posthog/duckgres-worker:<sha>-duckdb<X.Y.Z>` | Specific DuckDB version, this commit   |

The same tags exist mirrored in ECR at `795637471508.dkr.ecr.us-east-1.amazonaws.com/duckgres-worker:...`.

To list what's currently available:

```bash
# GHCR
gh api /users/posthog/packages/container/duckgres-worker/versions --jq '.[].metadata.container.tags[]' | sort -u

# ECR (requires AWS auth)
aws ecr list-images --repository-name duckgres-worker --region us-east-1 \
  --query 'imageIds[].imageTag' --output text | tr '\t' '\n' | sort -u
```

The DuckDB versions in the matrix are defined in `container-image-worker-cd.yml`'s `strategy.matrix.duckdb`. To pin a tenant to a non-default DuckDB version, you need a `:<sha>-duckdb<version>` tag from a commit where that version was in the matrix.

### 3. Canarying a New Build

To test a new build (e.g., a DuckDB upgrade) for a specific subset of tenants without affecting the rest of the fleet, use the admin API. **Do not run direct `UPDATE duckgres_managed_warehouses` SQL** — the API endpoint goes through a row-locked transaction so it serializes correctly with concurrent operator mutations.

```bash
# Pin org-uuid-123 to a specific DuckDB 1.5.1 build
curl -X PATCH "https://<cp-host>/api/v1/orgs/org-uuid-123/warehouse/pinning" \
  -H "Authorization: Bearer $DUCKGRES_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "image": "ghcr.io/posthog/duckgres-worker:abc1234-duckdb1.5.1"
  }'
```

The response is the updated managed warehouse JSON. The endpoint accepts:

- `image` (string, optional): the worker image. Pass `""` to clear back to the global default.
- `ducklake_version` (string, optional): the DuckLake spec version (`major.minor` only — e.g., `"0.4"`, `"1.0"`, `"1.1"`). Pass `""` to clear back to the global default.

At least one of the two fields must be present; an empty body is rejected. Missing fields preserve the stored value (this is a true `PATCH`, not a `PUT`).

**Automatic Recycling:** the Control Plane detects the change on the next connection attempt:
*   It will **refuse to reuse** any existing "hot-idle" workers for that org if their image doesn't match the new target.
*   It spawns a **fresh worker pod** using the canary image.
*   The old workers are naturally retired by the Janitor.

### 4. Managing DuckLake Spec Upgrades

If a new worker build requires a newer DuckLake metadata schema (e.g., moving from 1.0 to 1.1), pin both fields together so the migration logic kicks in:

```bash
curl -X PATCH "https://<cp-host>/api/v1/orgs/org-uuid-123/warehouse/pinning" \
  -H "Authorization: Bearer $DUCKGRES_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "image": "ghcr.io/posthog/duckgres-worker:def5678-duckdb1.5.2",
    "ducklake_version": "1.1"
  }'
```

**The Control Plane Authority:**
The Control Plane (CP) performs a "pre-flight" migration check before activating a worker.
*   If `ducklake_version` in the DB (or the global default) is newer than the version detected in the tenant's metadata store, the CP triggers a **backup**.
*   The CP then signals the worker to perform `AUTOMATIC_MIGRATION TRUE` during attachment.

> [!CAUTION]
> **One-Way Door:** DuckLake migrations are irreversible. Once a tenant's metadata is upgraded to 1.1, older workers (1.0) will no longer be able to attach to it. Pin `image` and `ducklake_version` together so a tenant doesn't accidentally flip-flop between incompatible builds.

### 5. Reverting a Pin

To roll a tenant back to the global default for either or both knobs, send empty strings:

```bash
# Clear both: tenant follows global k8s.worker_image and ducklake.default_spec_version
curl -X PATCH "https://<cp-host>/api/v1/orgs/org-uuid-123/warehouse/pinning" \
  -H "Authorization: Bearer $DUCKGRES_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"image": "", "ducklake_version": ""}'
```

Note: clearing `ducklake_version` does NOT downgrade the metadata schema (one-way door); it just stops asserting a per-tenant target. The tenant's existing schema remains at whatever level it last migrated to.

## Operational Safety

### Hot-Idle Recycling
The system is "version-aware" during worker reclamation. If a tenant is sitting on a "hot" worker (DuckLake already attached) and you change their target image via the pinning endpoint, the control plane will:
1.  See the mismatch.
2.  Log: `Hot-idle worker image mismatch, skipping reclamation.`
3.  Retire the old worker.
4.  Spawn a new one with the correct build.

### Neutral Pool
The global "Neutral Warm Pool" (idle workers waiting for any org) **always** uses the global default image. This ensures that the most common path is always fast (sub-second connection). Tenants on custom/canary builds will experience a "cold start" (pod spawn penalty) on their first connection.

## Background

The duckgres binary was split into two specialized images in the PRs around #500–#503:

- **`duckgres-worker`** — the per-DuckDB-version image, links libduckdb. Comes in a matrix of DuckDB versions (currently 1.5.1 + 1.5.2) so operators can pin tenants for canary or rollback.
- **`duckgres-controlplane`** — duckdb-go-free, version-agnostic. One image fits all worker fleets.

The `posthog/duckgres` repo (the legacy all-in-one image) is still produced and used by deployments that need both the CP and worker bundled, but new tenant pinning should target the `duckgres-worker` images.
