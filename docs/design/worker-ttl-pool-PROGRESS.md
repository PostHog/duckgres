# Worker TTL-pool — PROGRESS / HANDOFF (resume after compaction)

Working doc for the multi-PR rebuild of the duckgres k8s worker pool. Design:
`docs/design/worker-ttl-pool.md`. Scope: **remote/k8s backend only** (`-tags
kubernetes`, `controlplane/`). Standalone + process backends untouched.

## The model we're building
- A worker pod serves **one** client query session at a time. Workers are only
  **hot-active** (running a session) or **hot-idle** (alive until TTL). **No warm
  pool, no neutral/idle pre-spawned workers.**
- A connection picks a worker shape + idle TTL via libpq GUCs:
  `duckgres.worker_cpu`, `duckgres.worker_memory`, `duckgres.worker_ttl`
  (gated by `AllowClientWorkerProfile`, clamped to `[min,max]` / `[0,maxTTL]`).
  No GUC ⇒ default profile (nil ⇒ pool-global default size), TTL default 20m.
- Acquire: reuse a same-shape **hot-idle** worker (`ClaimHotIdleWorker`) else
  **foreground-spawn** one (`foregroundSpawnReservedWorker`). TTL resets per query.
- **Headroom controller** (a janitor reconcile hook, NOT a separate loop): keeps
  `HeadroomPercent`% of worker-nodepool allocatable free via low-priority
  placeholder pods; workers preempt them.

## PRs (all MERGED unless noted) — posthog/duckgres
- #692 sizing GUCs `worker_cpu/memory/ttl` + clamp/gate (slice 1 + design doc)
- #693 fix: no-sizing request returns nil default profile (unbroke warm-pool deploys — concrete-default key never matched neutral `||false`)
- #694 headroom placeholder pods (janitor reconcile hook; gated off by default)
- #695 persist per-worker TTL (`WorkerRecord.TTLMinutes`) + per-row hot-idle reaping (`ListExpiredHotIdleWorkers(now, defaultTTL)`)
- #696 foreground-spawn a **sized** worker when none fits (`spawnReservedSizedWorker`, now renamed)
- #698 fix: allocate sized-spawn worker id from the DB slot (`CreateSpawningWorkerSlot`), not placeholder `0` (was colliding on id 0 → churn)
- #699 fix: `adoptClaimedWorker` restores profile from the record on re-adopt (kept); a redundant slot-stamp (reverted in #700)
- #700 fix: **`UpsertWorkerRecord` DoUpdates now includes profile_cpu/memory/colocate + ttl_minutes** (the real reuse blocker — upsert silently dropped profile)
- #701 fix: headroom placeholder pods use `GenerateName` (was colliding on one name → only 1 placeholder)
- #702 fix: reuse path (`reserveClaimedWorker`) resets TTL from the request (was dropping ttl → ttl_minutes=0 on reuse)
- #703 warm-pool teardown behavior: `foregroundSpawnReservedWorker` (generalized, handles default size too); `OrgReservedPool.AcquireWorker` foreground-spawns default requests on no-idle instead of waiting; `ReserveSharedWorker` contract for default unchanged (6 contract tests stay green)
- #704 fix: per-image warm floor driven by `SharedWarmTarget` (`computePerImageWarmTargets`); `SharedWarmTarget=0` ⇒ no floors ⇒ no neutral workers
- **charts PR PostHog/charts#11845** (OPEN): persists mw-dev values (gate, clamps, `sharedWarmTarget=0`, headroom 5% / placeholder 1cpu-2Gi, the two PriorityClasses + node ClusterRole). So Argo re-sync keeps the hand-applied config.

## Current mw-dev state (as of this writing)
- CP image `9612a35…` (#704) deployed by hand (Argo auto-sync OFF).
- Env set by hand on `deploy/duckgres` (namespace `duckgres`):
  `DUCKGRES_K8S_ALLOW_CLIENT_WORKER_PROFILE=true`,
  `_WORKER_PROFILE_MIN_CPU=1 _MAX_CPU=16 _MIN_MEMORY=2Gi _MAX_MEMORY=64Gi`,
  `_WORKER_MAX_TTL=24h`, `_SHARED_WARM_TARGET=0`,
  `_HEADROOM_PERCENT=5 _PLACEHOLDER_CPU=1 _PLACEHOLDER_MEMORY=2Gi`,
  `_PLACEHOLDER_PRIORITY_CLASS=duckgres-headroom _WORKER_PRIORITY_CLASS=duckgres-worker`.
- Applied to cluster by hand: PriorityClasses `duckgres-worker`(1000000) /
  `duckgres-headroom`(-10); ClusterRole+Binding `duckgres-headroom-nodes` (SA `duckgres`).

## VERIFIED working live on mw-dev
- Sized spawn: `worker_cpu=2 worker_memory=4Gi` → pod with `cpu=2 mem=4Gi`, org-assigned.
- Clamp: `worker_memory=1Gi` → clamped to `2Gi` (CP logs `clamped to "2Gi"`).
- Same-size reuse: 2nd same-size request reuses the hot-idle worker (one pod, no churn).
- Profile + TTL persist: DB `hot_idle` row carries `profile_cpu/profile_memory` + `ttl_minutes=20`.
- Headroom: multiple placeholder pods (GenerateName), `priorityClass=duckgres-headroom`, on worker nodepool; log `Headroom: scaled placeholder pods up`.
- Warm pool off: clean slate held 0 worker pods; default + sized both foreground-spawn on demand.

## OUTSTANDING TASKS
1. **Residual stray idle worker (open thread).** After a default + sized request,
   one extra `state=idle, org=` worker appeared (e.g. `11212`) and persisted ~75s,
   but did NOT replenish (only 1, no 2nd). `control.go:583 SpawnMinWorkers(warmTarget=processMinWorkers=0)`
   is NOT the source (process_min_workers=0). Likely either: a `triggerPerImageReplenish`
   call on a claim path that #704 didn't fully zero, OR it just hasn't hit
   `idleTimeout` (default ~minutes) yet and will drain. **TODO:** confirm it drains;
   if it persistently respawns, trace the spawn (look for `publishIdle=true` /
   `spawnWarmWorker` callers reachable in the runtime-store path). The dead-code
   removal (task 3) eliminates this regardless.
2. **≥-fit reuse** (optimization, deferred from #696/#703): let a smaller request
   reuse a LARGER hot-idle worker. Needs numeric quantity comparison in
   `ClaimHotIdleWorker` SQL (compare cpu cores / mem bytes) or an in-memory
   smallest-fitting match. Same-size reuse already works; this just avoids
   spawning when a bigger idle worker exists.
3. **Dead-code deletion** (the mechanical teardown follow-up): remove the now-inert
   warm machinery — `reconcileWarmCapacity` + per-image targets + dynamic warm
   capacity (`computeEffectiveWarmCapacityTargets`, miss-demand recording),
   `ClaimIdleWorker` neutral path + `CreateNeutralWarmWorkerSlot*` +
   `findReservableWarmWorkerLocked`, `SharedWarmTarget`/`minWorkers`/`SpawnMinWorkers`,
   `reapIdleWorkers` warm-floor logic, the `#677` `warmAcquireTimeout` wait
   (now unused — `OrgReservedPool` foreground-spawns instead of waiting),
   `WarmCapacityExhaustedError` NoIdle path where it's dead. Keep `ClaimHotIdleWorker`
   (reuse) + `foregroundSpawnReservedWorker` + the janitor hot-idle TTL reaper.
   Update/remove the 6 `ReserveSharedWorker` "backpressure not spawning" contract
   tests since the contract changes when the warm path is gone.
4. **Remove colocate + profiles/tiers entirely** (user asked; partially done —
   `Colocate` field still exists set false, tiers config still present). Strip
   `colocate` GUC remnants, `WorkerTiers`, `Colocated*` config + k8s_pool branches,
   `OrgMaxColocated*`, `AllowClientExclusiveNode`. Rename `WorkerProfile`→ maybe
   `WorkerSize`. Big mechanical change.
5. **e2e harness assertion** (`tests/e2e-mw-dev/harness.sh`, per CLAUDE.md): the
   per-PR CP runs `SHARED_WARM_TARGET=0`, so default `basic_query` already exercises
   foreground-spawn. Add an explicit assertion for sized request → sized pod (needs
   gate on in the harness CP — currently off) and TTL reaping. Document any in-Job
   limitation.
6. **Default worker SIZE for nil-profile requests** is currently the pool-global
   `WorkerCPURequest/MemoryRequest` (unset on mw-dev → BestEffort/empty). Decide if
   default should be 8/16 (set `workerCPURequest/workerMemoryRequest` in chart) so
   default connections get a real-sized pod, not BestEffort.

## HOW TO VERIFY LIVE (detailed)

### Access
- kube context: ALWAYS `kubectl --context posthog-mw-dev` (NEVER prod). Namespace `duckgres`.
- Connect creds: `~/.duckgres-mw-dev-ducklings.txt` (tab-separated:
  org<TAB>user<TAB>pass<TAB>host<TAB>db<TAB>provisioned). Working orgs are
  `ben-cnpg-*` and `ben-ext-*` (the `ben-aur-*` ones are BROKEN — dead aurora backend).
  Catalog dbname is `ducklake` (for -dl/-both) or `iceberg` (for -ice), NOT the org name.
- Config-store DB (read worker_records):
  `dsn=$(kubectl --context posthog-mw-dev -n duckgres get secret duckgres-config-store -o jsonpath='{.data.dsn}' | base64 -d)`
  then run psql via a throwaway pod:
  `kubectl --context posthog-mw-dev -n duckgres run pgq-$RANDOM --rm -i --restart=Never --image=public.ecr.aws/docker/library/postgres:16-alpine --command -- psql "$dsn" -tAc "<SQL>"`
  Table is `cp_runtime.worker_records` (schema-qualified!). Columns: worker_id,
  state, org_id, profile_cpu, profile_memory, ttl_minutes, updated_at.

### Deploy a new image (Argo off → patch by hand)
```
REF="795637471508.dkr.ecr.us-east-1.amazonaws.com/duckgres:<sha>@sha256:<digest>"
kubectl --context posthog-mw-dev -n duckgres set env  deploy/duckgres DUCKGRES_K8S_WORKER_IMAGE="$REF"
kubectl --context posthog-mw-dev -n duckgres set image deploy/duckgres duckgres="$REF"
kubectl --context posthog-mw-dev -n duckgres rollout status deploy/duckgres --timeout=150s
```
(The user builds+pushes the image from a merged PR and gives the `<sha>@sha256:<digest>`.)

### Worker pod snapshot (org + size)
```
kubectl --context posthog-mw-dev -n duckgres get pod -l app=duckgres-worker \
 -o jsonpath='{range .items[*]}{.metadata.name}{" org="}{.metadata.labels.duckgres/active-org}{" cpu="}{.spec.containers[0].resources.requests.cpu}{" mem="}{.spec.containers[0].resources.requests.memory}{"\n"}{end}'
```

### Test: sized spawn + clamp
```
PGOPTIONS='-c duckgres.worker_cpu=2 -c duckgres.worker_memory=4Gi' \
 PGPASSWORD='<pw>' psql "sslmode=require host=<org>.dw.dev.postwh.com port=5432 user=root dbname=ducklake connect_timeout=120" -tAc "SELECT 1;"
```
Expect RC 0 + a worker pod with `cpu=2 mem=4Gi` for that org. Clamp: request
`worker_memory=1Gi` (< min 2Gi) → pod shows `2Gi` + CP log `clamped to "2Gi"`.

### Test: reuse + TTL (the bit that took 4 fix PRs)
Two SAME-size requests on a fresh org/size; expect ONE worker pod (no 2nd), and:
```
SELECT worker_id,state,org_id,profile_cpu,profile_memory,ttl_minutes
FROM cp_runtime.worker_records WHERE org_id='<org>' AND state='hot_idle';
```
must show `profile_cpu/profile_memory` set AND `ttl_minutes = requested ttl` (default 20).
If `profile_cpu` empty or `ttl_minutes=0` → a regression in the persist/reuse chain
(#699/#700/#702 territory).

### Test: warm-pool teardown
Delete all org-less workers, wait ~40s, then:
- `kubectl ... get pod -l app=duckgres-worker` → expect NONE neutral (org= empty)
  pods respawn; CP logs no `Warm capacity target … base_target=<nonzero>`.
- A default (no-GUC) connection + a sized connection → both RC 0, workers appear
  on demand, org-assigned. (Watch for any persistent `state=idle, org=` row — task 1.)

### Test: headroom
Need gate not required; set `HEADROOM_PERCENT>0` + small `PLACEHOLDER_CPU/MEMORY`
(dev nodes are ~4cpu). After a janitor tick:
```
kubectl --context posthog-mw-dev -n duckgres get pod -l app=duckgres-headroom \
 -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.phase}{" pc="}{.spec.priorityClassName}{"\n"}{end}'
```
Expect the target count of Running placeholders, `pc=duckgres-headroom`. CP log
`Headroom: scaled placeholder pods up. from=X to=Y`. Names must be GenerateName
(random suffix, not `…-0`) — else the #701 regression is back.

## KEY FILES / SYMBOLS
- `controlplane/worker_profile.go` — `resolveWorkerProfile` (GUC→profile, clamp/gate), `defaultWorkerTTL=20m`.
- `controlplane/org_reserved_pool.go` — `AcquireWorker` (reuse → foreground-spawn default), FIFO `gate` anti-snatch, `atOrgWorkerCap`.
- `controlplane/k8s_pool.go` — `ReserveSharedWorker` (sized foreground-spawn), `foregroundSpawnReservedWorker`, `reserveClaimedWorker` (reuse, sets profile+ttl), `adoptClaimedWorker` (restores profile), `spawnReservedWorker` (the OLD warm-slot spawn — different fn, don't confuse), `reapIdleWorkers`, `headroom.go` reconcile.
- `controlplane/headroom.go` — `reconcileHeadroom` (janitor hook), `headroomPlaceholdersNeeded` (pure, tested), `createPlaceholderPod` (GenerateName).
- `controlplane/configstore/store.go` — `UpsertWorkerRecord` (DoUpdates must include profile+ttl_minutes), `ListExpiredHotIdleWorkers`, `CreateSpawningWorkerSlot`, `ClaimHotIdleWorker`/`ClaimIdleWorker`.
- `controlplane/configstore/models.go` — `WorkerRecord.TTLMinutes`.
- `controlplane/org_router.go` — `computePerImageWarmTargets` (floor = SharedWarmTarget).
- `controlplane/janitor.go` — `reconcileHeadroom` hook + hot-idle TTL reaper (`ListExpiredHotIdleSnapshots(now, hotIdleTTL)`).
- Config plumbing: `configresolve/resolve.go` (K8s* fields, env parse), `cmd/duckgres-controlplane/main.go` + `main.go` (assign into `K8sConfig`), `controlplane/control.go` `K8sConfig` struct.
- Charts: `~/code/posthog/charts/charts/duckgres/templates/{deployment,priorityclass,rbac}.yaml` + `values.yaml`; `argocd/duckgres/values/values.managed-warehouse-dev.yaml`.

## CONSTRAINTS / GOTCHAS
- mw-dev ONLY, never prod. Explicit `--context posthog-mw-dev` every kubectl.
  (memory: project_worker_ttl_pool_mwdev_only, feedback_kubectl_explicit_context)
- I have no local docker → can't build images. User builds from merged PRs + gives the ref.
- configstore changes can't run locally (need docker/Postgres) — rely on CI + mw-dev DB queries.
- Build/test: `go build -tags kubernetes ./...`; `go test -tags kubernetes ./controlplane/ -timeout 90s`.
  The 3 env-dependent suites (`tests/configstore`, `tests/controlplane`, `tests/integration`)
  FAIL locally (need docker/pg) — that's expected, not my breakage.
- Branch prefix `ben/`, no Co-Authored-By line (global CLAUDE.md). Commit footer uses
  `Co-Authored-By: Claude Opus 4.8` ONLY where the repo convention asks — duckgres PRs here used 🤖 footer, no co-author line.
