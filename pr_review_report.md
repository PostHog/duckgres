# Pull Request Review Report: Warm Worker Pool Removal

This report reviews the changes in the branch `ben/duckgres-remove-warm-pool` compared to the `main` branch. 

---

## 1. Architectural Summary

The goal of this branch is to remove the legacy **warm worker pool** model (where a pool of unassigned, "neutral" workers was kept pre-spawned/running in Kubernetes) and replace it with a **size-on-request, hot-idle reuse, and node headroom** model for the remote (K8s) worker backend.

### The Old Model vs. The New Model

| Feature | Old Model | New Model (This PR) |
| :--- | :--- | :--- |
| **Worker Allocation** | Pre-spawned "neutral" warm workers in the background. On demand, a neutral worker was claimed, assigned an org, and activated. | Spawned **on-demand** for a specific org, sized from the query profile request. |
| **Worker Reuse** | Workers were moved to `hot_idle` on session end, and could be reclaimed by the same org or eventually retired. | Workers transition to `hot_idle` on session end, carrying an explicit `org_id` and version, and can be reclaimed by that org until their individual `ttl` expires. |
| **Startup Latency Mitigation** | Kept a pool of generic warm workers running (very expensive). | Runs a **headroom controller** (`reconcileHeadroom` janitor hook) keeping a configurable % of cluster capacity held by low-priority "pause" placeholder pods. Real worker spawns preempt these placeholders to schedule instantly. |
| **Configuration** | Complex flags around shared warm targets, dynamic warm capacity miss windows, and warm shapes. | Simplified size-on-request parameters. Warm pool scaling flags are removed in favor of headroom percent settings. |

---

## 2. Component-Level Changes

### A. Configuration & CLI Flags
* **Removed Flags**: All warm-capacity, shared-warm-target, colocated worker profile, and tier settings were deleted from [cliflags.go](file:///Users/ben/code/posthog/duckgres/configresolve/cliflags.go) and the YAML config in [file_config.go](file:///Users/ben/code/posthog/duckgres/configloader/file_config.go#L57-L68).
* **Added Flags**: Headroom configurations and default sizing conventions (`DUCKGRES_K8S_ALLOW_CLIENT_WORKER_SIZING`, `DUCKGRES_K8S_WORKER_DEFAULT_CPU`, `_DEFAULT_MEMORY`, `_DEFAULT_TTL`, `_HEADROOM_PERCENT`, etc.).
* **Process Backend**: The single-host/process worker backend remains unchanged, keeping its local pre-warm (`min_workers`) configuration.

### B. Worker Pools (`controlplane/`)
* [k8s_pool.go](file:///Users/ben/code/posthog/duckgres/controlplane/k8s_pool.go):
  * [ReserveSharedWorker](file:///Users/ben/code/posthog/duckgres/controlplane/k8s_pool.go#L1553-L1613) is simplified to claim a `hot_idle` worker for the org or spawn a fresh one on-demand. All logic to claim a "neutral" warm worker is removed.
  * [spawnReservedWorker](file:///Users/ben/code/posthog/duckgres/controlplane/k8s_pool.go#L1640-L1719) is now the only spawn path; it reserves the slot in the DB first and then creates the pod.
  * [SpawnMinWorkers](file:///Users/ben/code/posthog/duckgres/controlplane/k8s_pool.go#L2130-L2132) is turned into a no-op since no neutral warm pool is populated at startup.
* [org_reserved_pool.go](file:///Users/ben/code/posthog/duckgres/controlplane/org_reserved_pool.go):
  * Simplified to call `ReserveSharedWorker` directly under the organization's acquire gate.

### C. Configstore Coordination (`controlplane/configstore/`)
* [models.go](file:///Users/ben/code/posthog/duckgres/controlplane/configstore/models.go#L508-L543):
  * Removed the `profile_colocate` column from `WorkerRecord`.
* [store.go](file:///Users/ben/code/posthog/duckgres/controlplane/configstore/store.go):
  * Removed warm pool helpers like `CountNeutralWarmWorkers` and `ClaimWarmWorker`.
  * Updated `UpsertWorkerRecord` to drop colocation fields from conflict resolution.

### D. Janitor & Observability
* [janitor.go](file:///Users/ben/code/posthog/duckgres/controlplane/janitor.go):
  * Deleted the `reconcileWarmCapacity` call.
  * Added `reconcileHeadroom` hook to manage the low-priority placeholder pods.
  * The janitor now manages worker lifecycles purely through the snapshot/lease-typed API (`ListExpiredHotIdleSnapshots`, `RetireFromSnapshot`, etc.).

---

## 3. Detailed Review Feedback & Code Hygiene Issues

While the refactoring is extremely clean and successfully collapses the warm pool logic, there are several stale comments, dead variables, and outdated documentation files left behind.

### ⚠️ Critical Feedback: Outdated Runbooks
The runbooks in the repository still refer heavily to the old warm pool concepts and will mislead operators:
1. **[replenish-capacity.md](file:///Users/ben/code/posthog/duckgres/docs/runbooks/replenish-capacity.md)**:
   * **Issue**: The entire runbook is obsolete. It instructs operators to increase `minWorkers` to "replenish the warm pool" and references metric `duckgres_worker_lifecycle_count{binding="neutral"}`. 
   * **Action**: Delete this runbook or completely rewrite it to cover **Headroom Replenishment** (tuning `DUCKGRES_K8S_HEADROOM_PERCENT` and placeholder pod troubleshooting).
2. **[drain-hot-workers.md](file:///Users/ben/code/posthog/duckgres/docs/runbooks/drain-hot-workers.md)**:
   * **Issue**: Lines 15, 21, 23, 25, and 31-32 reference `minWorkers`, neutral binding, and scaling pre-warmed capacity.
   * **Action**: Remove these obsolete references.
3. **[worker-upgrades.md](file:///Users/ben/code/posthog/duckgres/docs/runbooks/worker-upgrades.md)**:
   * **Issue**: Lines 30 and 130-131 still describe the "Neutral Warm Pool" using the global default image to provide sub-second connections.
   * **Action**: Remove these outdated descriptions.

---

### 🔍 Code & Comment Hygiene Issues

#### 1. Leftover Test Comment in [k8s_pool_test.go](file:///Users/ben/code/posthog/duckgres/controlplane/k8s_pool_test.go#L1186-L1190)
* **Location**: Right above `TestK8sPoolReserveClaimedWorkerRejectsDuplicateActivatingClaim`.
* **Stale Text**:
  ```go
  // TestK8sPoolReserveSharedWorkerSkipsWarmWorkerWithMismatchedImageWithoutRuntimeStore
  // ensures the runtime-store-less warm-pool path honors per-org image pinning.
  // Without the image filter on findReservableWarmWorkerLocked,
  // ReserveSharedWorker would return a default-image warm worker to a pinned org
  // and the subsequent activation would fail with a version-mismatch error.
  ```
* **Reason**: The test `TestK8sPoolReserveSharedWorkerSkipsWarmWorkerWithMismatchedImageWithoutRuntimeStore` was deleted in this PR, but its documentation block was left behind and is now floating over a completely different test case.
* **Action**: Delete lines 1186-1190.

#### 2. Dead Code: `recordDynamicDemand` in [capacity_policy.go](file:///Users/ben/code/posthog/duckgres/controlplane/capacity_policy.go#L22)
* **Location**: [capacity_policy.go:L22](file:///Users/ben/code/posthog/duckgres/controlplane/capacity_policy.go#L22) and [capacity_policy.go:L31](file:///Users/ben/code/posthog/duckgres/controlplane/capacity_policy.go#L31).
* **Issue**: `recordDynamicDemand bool` in `capacityMissPolicy` is a leftover from the dynamic warm-capacity targeting. It is populated but never read anywhere in the production code. It is only asserted in a test case in [control_cancel_test.go:L151-L174](file:///Users/ben/code/posthog/duckgres/controlplane/control_cancel_test.go#L151-L174).
* **Action**: Remove the field from `capacityMissPolicy`, its instantiation, and clean up the test assertions in `control_cancel_test.go`.

#### 3. Stale Comment referencing `reconcileWarmCapacity`
* **Locations**:
  * [k8s_pool.go:L259](file:///Users/ben/code/posthog/duckgres/controlplane/k8s_pool.go#L259): `// warm-pool replenishment via reconcileWarmCapacity, old-version workers are`
  * [worker-ttl-pool.md:L96](file:///Users/ben/code/posthog/duckgres/docs/design/worker-ttl-pool.md#L96): `janitor.runOnce() alongside reconcileWarmCapacity...`
* **Reason**: `reconcileWarmCapacity` was deleted.
* **Action**: Update these comments to refer to `reconcileHeadroom` or remove the stale function names.

#### 4. Stale Comments referencing `countNeutralWarmWorkers` and neutral warm rows in [store.go](file:///Users/ben/code/posthog/duckgres/controlplane/configstore/store.go)
* **Locations**:
  * [store.go:L1595-L1597](file:///Users/ben/code/posthog/duckgres/controlplane/configstore/store.go#L1595-L1597): `...blocking warm-pool replenishment because countNeutralWarmWorkers still counts them.`
  * [store.go:L1284-L1286](file:///Users/ben/code/posthog/duckgres/controlplane/configstore/store.go#L1284-L1286): `...covers two cases: warm-pool rows that haven't been activated yet...`
  * [store.go:L1307-L1308](file:///Users/ben/code/posthog/duckgres/controlplane/configstore/store.go#L1307-L1308): `// Org-bound rows only: a neutral warm row (org_id='') hasn't been activated...`
* **Reason**: `countNeutralWarmWorkers` was deleted, and neutral warm rows are no longer spawned since all spawns are now org-bound.
* **Action**: Rewrite these comment blocks to match the new size-on-request on-demand model.

#### 5. Deprecate / Document `"neutral"` label in `duckgres_worker_lifecycle_count`
* **Location**: [pool_metrics.go:L69](file:///Users/ben/code/posthog/duckgres/controlplane/pool_metrics.go#L69).
* **Issue**: The `observedWorkerLifecycleBindings` still defines `"neutral"` alongside `"org_bound"`, and [ListWorkerLifecycleStats](file:///Users/ben/code/posthog/duckgres/controlplane/configstore/store.go#L942) still maps empty `org_id` to `"neutral"`. Under the new design, remote workers are always org-bound from the start, so `binding="neutral"` will always report 0 in production.
* **Action**: Document that `binding="neutral"` is legacy/deprecated, or clean up the labels if metrics backwards compatibility allows.

---

## 4. Verdict & Recommendations

### **Status**: Approve with Suggestions / Requests for Changes (Documentation and Dead Code)

The core code changes are solid, robust, and correctly implement the designed on-demand size-on-request worker pool. However, **this PR should not merge without updating the operational runbooks**, otherwise on-call engineers will waste time trying to scale configurations (`minWorkers` / `shared_warm_target`) that no longer exist or reading obsolete metrics. 

**Recommended Action Plan**:
1. Remove the dead `recordDynamicDemand` field from `capacity_policy.go` and its associated test assertions.
2. Clean up the stale code comments and the orphaned test comment in `k8s_pool_test.go`.
3. Delete the obsolete `replenish-capacity.md` runbook and replace it with a headroom troubleshooting guide.
4. Update the other runbooks to remove references to `minWorkers` and neutral warm pools.
