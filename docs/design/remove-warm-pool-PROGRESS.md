# Remove-warm-pool refactor — staging progress

Branch `ben/duckgres-remove-warm-pool` (off main after #709). Goal: remove ALL
warm-worker-pool machinery from the remote/k8s backend (workers are only
hot-active or hot-idle, spawned on demand, reused by exact-profile match),
simplify naming/flags/params, **keep the process backend (FlightWorkerPool)
untouched**, rename surviving capacity symbols. One branch, staged green commits,
one PR at the end.

## Done

- **Stage 1 — committed `6cc55eb` (build+vet+controlplane tests green, both tag sets).**
  Removed dynamic warm-capacity *targeting* + the demand (warm-miss) signal:
  deleted `warm_capacity_targets*.go` (+tests) + `multitenant_warm_capacity_test.go`;
  removed `OrgRouter.reconcileWarmCapacity`/`computeBaseWarmCapacityTargets`/
  `computePerImageWarmTargets`, multitenant `computeEffectiveWarmCapacityTarget*`/
  `warmCapacityGlobalCapBlocksDemand`/`reconcileWarmCapacityImageTargets`,
  `K8sWorkerPool.recordWarmCapacityMiss`, the janitor warm-miss-bucket pruner +
  `reconcileWarmCapacity` hook (→ slim `observeWorkerLifecycle`). Renamed
  `warm_capacity_metrics.go`→`pool_metrics.go`, `warm_pool_metrics_test.go`→
  `pool_metrics_test.go` (kept only the worker-lifecycle gauge).

## In progress — Stage 2 (acquire path), STASHED at `stash@{0}` ("stage2-wip-acquire-simplification")

Touches `controlplane/k8s_pool.go` + `org_reserved_pool.go`. The code compiles
green; it is stashed because it breaks ~5–10 ReserveSharedWorker unit tests that
assert the OLD contract (default request → backpressure-without-spawn) which is
now (default request → spawn on demand).

What the stash does:
- `ReserveSharedWorker` simplified to **claim-hot-idle (org reuse) → else spawn**.
  Deleted the neutral `ClaimIdleWorker` block + its replenish triggers, and the
  runtime-store-less `findReservableWarmWorkerLocked` branch (now just spawns).
- Deleted old warm-slot `spawnReservedWorker(ctx, slot)@~689`; **renamed**
  `foregroundSpawnReservedWorker`→`spawnReservedWorker` (k8s_pool.go +
  org_reserved_pool.go:190 call site).

Why it's behaviorally safe even before deleting the rest: `K8sWorkerPool.SpawnMinWorkers`
is never called in production (remote uses OrgReservedPool on-demand; process uses
FlightWorkerPool), and the only `SetWarmCapacityTarget` caller passes 0 → `minWorkers`
stays 0 → every `shouldReplenishWarmCapacityLocked()` block is inert. So no warm
worker is ever spawned; the remaining warm-spawn funcs are pure dead code.

### To finish Stage 2 (next session)
1. `git stash pop` (stash@{0}).
2. Rewrite/delete the obsolete ReserveSharedWorker tests in `k8s_pool_test.go`
   (these HANG because the fake clientset never readies the spawned pod):
   - DELETE: `...RuntimeMissDoesNotUseInMemoryFallback` (1731),
     `...BackpressuresWhenRuntimeClaimReturnsNil` (2010),
     `...ColdRuntimeBackpressuresWithoutSpawning` (2428),
     `...ColdPoolBackpressuresWithoutSpawning` (3564),
     `...ClaimsRuntimeWorkerAndAdoptsPod` (neutral claim, ~1839).
   - KEEP/verify cap tests: `...ColdBackpressuresWhenWarmupBlockedByGlobalCap`
     (2453) and the 3 `DoesNotRecord*Cap*` still validate cap → error (spawn slot
     nil); confirm they don't hang (they hit the cap before pod-ready).
   - The 6 "ReserveSharedWorker contract" tests in CLAUDE.md need updating to the
     new spawn contract.
   - Make the fake runtime store's `CreateSpawningWorkerSlot` + fake clientset
     ready the pod so a spawn test can assert success (mirror an existing
     successful-spawn test).
3. THEN delete the now-dead warm-spawn funcs + their inert call sites:
   `SpawnMinWorkers` (k8s → make a no-op returning nil, interface-required),
   `SpawnMinWorkersForImage`, `triggerPerImageReplenish`,
   `findReservableWarmWorkerLocked`, `idleWarmWorkerCountLocked`,
   `idleWarmWorkerCountByImageLocked`, `shouldReplenishWarmCapacityLocked`,
   `spawnWarmWorker`/`spawnWarmWorkerBackground`(+`spawnWarmWorkerFunc`/`BackgroundFunc`
   fields), `SetWarmCapacityTarget`/`WarmCapacityTarget`/`SetPerImageWarmTargets`/
   `PerImageWarmTargets`, `minWorkers`/`perImageWarmTarget` fields, and the
   replenish blocks in the health-check loop (~2586/2637/2730/3070/3254/3308) and
   crash-replacement (~3398/3603/3619). Also drop the `SetWarmCapacityTarget(0)`
   shutdown call (control.go:1553) + adapter (multitenant.go:53) + interface
   method (control.go:254) + the org_router adapter test.

## Remaining stages (each its own green commit)

- **Stage 3 — colocate / tiers.** Remove `WorkerProfile.Colocate` (+`Parts()` 3rd
  return), `colocate` GUC, `AllowClientExclusiveNode`, `WorkerTiers`/`WorkerProfileSpec`,
  `Colocated*` config + k8s_pool spawn branches + `nodeSelectorForProfile`,
  `OrgMaxColocated*` + org_reserved_pool colocated-cap logic
  (`assignedColocatedResourcesLocked`, `maxColocated*`), configstore
  `ProfileColocate` column + `sumOrgColocatedResources`/`parseColocated*`/
  `countNeutralWarmWorkers`, the colocated-quota metric. (Full file:line map was
  produced by the investigator agents — re-run if needed.)
- **Stage 4 — configstore warm methods + config plumbing + renames.** Delete
  configstore `ClaimIdleWorker`, `CreateNeutralWarmWorkerSlot*`,
  `RecordWarmCapacityMiss`, `ListWarmCapacityMissesSince`,
  `PruneWarmCapacityMissBuckets`, `WarmCapacityMissBucket`/`Aggregate` models (and
  drop from `RuntimeWorkerStore` interface + fakes). Remove config fields
  (`SharedWarmTarget`, `SharedWarmWorkers`, `DynamicWarmCapacityEnabled`,
  `WarmCapacityMiss*`, `WarmCapacityDemandTTL`, `WarmCapacityDynamic*`) +
  CLI flags/env (`configresolve/cliflags.go`, `resolve.go`, `main.go`,
  `cmd/duckgres-controlplane/main.go`) + the colocate/tier flags. Rename surviving
  capacity symbols: `WarmCapacityExhaustedError`→`WorkerCapacityExhaustedError`,
  `New*`/`DefaultWarmCapacityRetryAfter`→`DefaultWorkerSpawnRetryAfter`,
  `warm_capacity_policy.go`→`capacity_policy.go`, `warmCapacityMissPolicy*`→
  `capacityMissPolicy*`, `WarmAcquireTimeout`/`warmAcquireTimeout`→`AcquireTimeout`,
  `isRetryableWarmMiss`→`isRetryableAcquireMiss`, `WarmAcquireRetryInterval`/
  `WarmMissRecordInterval`/`SuppressWarmMissRecord` cleanup.
- **Stage 5 — remaining test cleanup** (`tests/configstore/*`, `main_test.go`,
  `configresolve/*_test.go`, `control_cancel_test.go`, `flight_ingress_test.go`,
  `org_router_test_helpers_test.go`, the SpawnMin*/SetWarmCapacityTarget tests).
- **Stage 6 — docs.** CLAUDE.md (worker-session-model section, the warm-pool /
  flag references, "Worker Drain"/warm mentions), `tests/e2e-mw-dev/README.md` +
  `harness.sh` warm-backpressure assertion (now spawn-on-demand),
  `docs/design/worker-ttl-pool.md`, manifests/chart values comments
  (`SHARED_WARM_TARGET`, `DYNAMIC_WARM_CAPACITY_ENABLED`, colocate knobs).

## Guardrails (do NOT remove)

`ClaimHotIdleWorker` (org reuse — e2e asserts it), `orgAcquireGate` FIFO
anti-snatch, hot-idle TTL reaping, headroom controller, the per-org/global worker
caps + their cap-error path, the process backend (FlightWorkerPool + its
`leastLoadedWorkerLocked`/`AcquireWorker`). `K8sWorkerPool.AcquireWorker` (flat
least-loaded) is dead in remote but interface-required — de-warm, don't delete.
