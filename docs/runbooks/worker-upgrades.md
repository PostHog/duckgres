# Worker Upgrades and Canaries

Duckgres supports running multiple versions of DuckDB and DuckLake workers simultaneously on the same Kubernetes control plane. This allows for safe, gradual rollouts (canarying) and per-tenant build pinning.

## How it Works

The control plane manages a heterogeneous pool of workers. The target build for any given connection is resolved with the following precedence:

1.  **Tenant Override:** The `image` and `ducklake_version` fields in the `managed_warehouses` table (Config Store).
2.  **Global Fallback:** The `k8s.worker_image` and `ducklake_default_spec_version` set in the Control Plane's `duckgres.yaml`.

### 1. Setting Global Defaults

Maintainers define the "stable" fleet version in the Control Plane configuration:

```yaml
# duckgres.yaml
k8s:
  worker_image: "posthog/duckgres:v1.1.0"
  image_pull_policy: "IfNotPresent"

# Default DuckLake spec version for migration checks
ducklake_default_spec_version: "1.0"
```

When the Control Plane starts, the **Janitor** will ensure that the shared "neutral" warm pool is populated with pods running this image.

### 2. Canarying a New Build

To test a new build (e.g., a DuckDB upgrade) for a specific subset of tenants without affecting the rest of the fleet:

1.  **Update the Tenant Config:** Set the `image` field for the target org in the Config Store database.

    ```sql
    UPDATE managed_warehouses 
    SET image = 'posthog/duckgres:v1.2.0-beta.1' 
    WHERE org_id = 'org-uuid-123';
    ```

2.  **Automatic Recycling:** The Control Plane detects this change during the next connection attempt:
    *   It will **refuse to reuse** any existing "hot-idle" workers for that org if their image doesn't match the new target.
    *   It will spawn a **fresh worker pod** using the specific canary image.
    *   The old workers will be naturally retired by the Janitor.

### 3. Managing DuckLake Spec Upgrades

If a new worker build requires a newer DuckLake metadata schema (e.g., moving from v1.0 to v1.1), you must also designate the target spec version to trigger the migration logic.

```sql
UPDATE managed_warehouses 
SET image = 'posthog/duckgres:v1.2.0',
    ducklake_version = '1.1'
WHERE org_id = 'org-uuid-123';
```

**The Control Plane Authority:**
The Control Plane (CP) performs a "pre-flight" migration check before activating a worker. 
*   If `ducklake_version` in the DB (or the global default) is newer than the version detected in the tenant's metadata store, the CP triggers a **backup**.
*   The CP then signals the worker to perform `AUTOMATIC_MIGRATION TRUE` during attachment.

> [!CAUTION]
> **One-Way Door:** DuckLake migrations are irreversible. Once a tenant's metadata is upgraded to v1.1, older workers (v1.0) will no longer be able to attach to it. Pinning the `image` and `ducklake_version` together ensures a tenant doesn't accidentally flip-flop between incompatible builds.

## Operational Safety

### Hot-Idle Recycling
The system is "version-aware" during worker reclamation. If a tenant is sitting on a "hot" worker (DuckLake already attached) and you update their target image in the DB, the control plane will:
1.  See the mismatch.
2.  Log: `Hot-idle worker image mismatch, skipping reclamation.`
3.  Retire the old worker.
4.  Spawn a new one with the correct build.

### Neutral Pool
The global "Neutral Warm Pool" (idle workers waiting for any org) **always** uses the global default image. This ensures that the most common path is always fast (sub-second connection). Tenants on custom/canary builds will experience a "cold start" (pod spawn penalty) on their first connection.
