# Worker TTL pool: one pool, size-on-request, TTL lifecycle, node headroom

Status: in progress (branch `ben/duckgres-worker-ttl-pool`). Scope: **remote/k8s
backend only** (`-tags kubernetes`, `OrgReservedPool` / `K8sWorkerPool`).
Standalone and process backends are unchanged.

## Goal

Replace the warm-pool + worker-profile/colocate machinery with a simple,
predictable model:

- A worker pod runs **one query session at a time** (already true) and has only
  two live states: **hot-active** (a session is running) and **hot-idle** (no
  session, alive until its TTL expires). No warm/neutral pool, no
  reserved/activating warm slots kept around speculatively.
- A connection asks for a worker **size** (`cpu`, `memory`) and a **ttl**. On a
  query:
  - if the org already has a **hot-idle** worker with cpu **and** memory ≥ the
    request → reuse it (smallest-fitting one);
  - else → spawn a fresh worker of the requested size.
- **TTL** = how long a worker stays alive after its last query finishes. Every
  query resets the worker's TTL to its initial value. The user picks it (and pays
  for the idle time).
- A **headroom controller** in the control plane keeps a configurable % of
  cluster-allocatable CPU+memory free by running low-priority placeholder
  ("pause") pods, so a real worker spawn schedules immediately (preempting the
  placeholders) instead of waiting on a fresh Karpenter node.

Removed concepts: warm pool / neutral pool / shared-warm-target, worker
**profiles** and **tiers**, the **colocate** flag and the colocated nodepool /
quota / warm-shapes, per-image warm reconcilers.

Implemented follow-up: orgs may set `default_worker_min_hot_idle` through the
admin API to retain a minimum number of default-profile hot-idle workers for
small/default traffic. The default is `0` (disabled). This is a retention floor:
the janitor skips TTL retirement when retiring an expired compatible worker would
drop the org below the floor. It does not proactively spawn workers. Arbitrary
per-profile reserved capacity remains future work.

## Request grammar (libpq `options`, parsed like the existing GUCs)

```
options=-c duckgres.worker_cpu=8 -c duckgres.worker_memory=16Gi -c duckgres.worker_ttl=20m
```

- `duckgres.worker_cpu` — integer cores. Default **8**.
- `duckgres.worker_memory` — k8s quantity. Default **16Gi**.
- `duckgres.worker_ttl` — Go duration. Default **20m**.
- Absent GUCs → defaults. Gated behind `AllowClientWorkerSizing`; sizes clamped
  to `[min,max]` and ttl to `[0,maxTTL]` per deployment (out-of-range → clamp +
  warn). Gate off → every request uses the defaults.

TTL resolution, per request (the same chain whether the request is sized or
not — there is exactly ONE default TTL however a worker comes to have no
explicit one):

1. client GUC `duckgres.worker_ttl` (gated, clamped to `WORKER_MAX_TTL`)
2. org default `default_worker_ttl` (admin API `PUT /orgs/:id`, #742)
3. deployment default `DUCKGRES_K8S_WORKER_DEFAULT_TTL`
4. built-in **20m**

`duckgres.colocate` / `worker_tier` are removed (unknown GUCs are ignored, so old
clients that still send them degrade to defaults rather than erroring).

## Worker identity / match

A worker carries its size `{CPU cores, Memory bytes}` and `TTL`. Match for reuse:
same org, lifecycle hot-idle, `worker.CPU >= req.CPU && worker.Mem >= req.Mem`.
Pick the smallest-fitting (least CPU then least memory) to avoid pinning a big
worker on a small query. On reuse, the worker's TTL is reset to the request's
ttl and it transitions hot-idle → hot-active.

DuckDB limits for the single session derive from the worker's **actual** size
(75% of pod memory, all cores) — unchanged from `workerDuckDBLimits`, now keyed
off size not profile.

## Lifecycle

```
(none) --spawn(size)--> Activating --activated--> Hot(active, 1 session)
Hot --session ends--> HotIdle (deadline = now + ttl)
HotIdle --new query (size fits)--> Hot (deadline cleared, ttl reset on session end)
HotIdle --deadline passed--> Retired (pod deleted, graceful drain)
Hot/HotIdle --crash/evict--> Lost --> removed
```

The reaper (existing `idleReaper`, 1-min tick or faster) retires hot-idle workers
whose `idleDeadline` (last-session-end + ttl) has passed. TTL is stored per
worker (set at spawn, reset on each session end). No global `idleTimeout` knob
governs it anymore — ttl is per worker.

## Acquire path (consolidated `OrgReservedPool`)

1. Reuse: smallest-fitting hot-idle org worker with size ≥ request → bump to
   hot-active, return. (Ungated fast path; only reuses org-owned workers.)
2. Else, under the FIFO gate (anti-snatch, kept): spawn a new worker of the
   requested size for the org; activate; return. Bounded by ctx; clear org-cap
   error if a per-org worker cap is hit (cap retained, now size-agnostic count).

`ReserveSharedWorker` / `ClaimIdleWorker` / `ClaimHotIdleWorker` warm-claim
machinery and the neutral-warm DB rows collapse to: "is there a reusable hot-idle
worker for this org of sufficient size?" (in-memory + runtime-store query), else
spawn. The runtime store keeps tracking workers (for cross-CP visibility and
crash recovery) but the warm/neutral slot concept is gone.

## Default hot-idle retention floor

`default_worker_min_hot_idle` is enforced inside the janitor's hot-idle TTL
cleanup path. It protects naturally-created hot-idle workers from expiry; it does
not create new capacity.

When the janitor considers an expired hot-idle worker, it:

- resolves the org's current default worker profile and image;
- counts compatible `hot_idle` workers for that org/image/profile bucket;
- skips retiring the candidate when the compatible count is at or below the
  configured floor.

Because there is no background floor spawn, cold orgs still start cold and a
burst can consume the retained hot-idle workers. The floor only preserves idle
capacity that prior sessions have already created.

## Headroom reconcile (new janitor hook, not a separate controller)

Implemented as a **janitor reconcile hook** (`reconcileHeadroom`), invoked from
`janitor.runOnce()` — it reuses the janitor's existing leader-gated tick, so
there is no new loop, goroutine, or leader election. On each tick (leader CP
only) it:

- reads node allocatable CPU+memory (k8s API, the worker nodepool) and current
  worker+placeholder usage;
- maintains low-priority **placeholder pods** so that ≥ `HeadroomPercent` of
  cluster-allocatable CPU and memory is held by placeholders (preemptible);
- placeholder pods use a PriorityClass **below** the worker PriorityClass, so a
  real worker spawn preempts them and schedules immediately; the evicted
  placeholder triggers Karpenter to add a node in the background.
- the placeholder target is sized against **current worker demand** (the
  summed requests of live worker pods), floor one placeholder while enabled.
  Node allocatable is the wrong baseline in any form: even with the
  placeholders' own share subtracted it counts FREE node capacity as
  "capacity needing headroom" (free capacity IS headroom), so one stray
  oversized node inflates the target and the placeholders pin it alive.
  Keyed to worker demand the target tracks load by construction and node
  geometry drops out entirely.
- placeholders are sized to the pool's **default worker shape**
  (`DUCKGRES_K8S_WORKER_CPU_REQUEST`/`_MEMORY_REQUEST`, else the built-in
  default) — there is no separate placeholder sizing knob
  (`DUCKGRES_K8S_PLACEHOLDER_CPU`/`_MEMORY` are removed). One preempted
  placeholder always frees exactly one default-worker slot, identically in
  every environment.
- workers carry `karpenter.sh/do-not-disrupt` **only while busy** (set at pod
  create — covering spawn/activate and the first session — re-added per
  session, removed when parked hot-idle). With the worker nodepool on
  `WhenEmptyOrUnderutilized`, Karpenter can consolidate nodes holding only
  idle workers/placeholders, while a node running a query is never voluntarily
  disrupted; pinning is bounded by query lifetime, not the worker TTL (which
  would stall drift rollouts).

Config: `DUCKGRES_K8S_HEADROOM_PERCENT` (0 = disabled), placeholder pod size,
the two PriorityClass names. Manifests add the PriorityClasses.

## Config knobs (env-only K8s, per existing convention)

Kept (client sizing + headroom): `DUCKGRES_K8S_ALLOW_CLIENT_WORKER_PROFILE`
(master gate for the `duckgres.worker_cpu`/`worker_memory`/`worker_ttl` startup
options), `DUCKGRES_K8S_WORKER_PROFILE_MIN_CPU`/`_MAX_CPU`/`_MIN_MEMORY`/
`_MAX_MEMORY` (clamps), `DUCKGRES_K8S_WORKER_MAX_TTL` (clamp ceiling) and
`DUCKGRES_K8S_WORKER_DEFAULT_TTL` (the default for requests that specify no
ttl — see the TTL resolution chain above),
`DUCKGRES_K8S_HEADROOM_PERCENT`, `DUCKGRES_K8S_PLACEHOLDER_IMAGE`/`_PRIORITY_CLASS`
(`_PLACEHOLDER_CPU`/`_MEMORY` are removed — placeholders take the default
worker shape).

Removed: all `DUCKGRES_K8S_*COLOCATED*`, `*WORKER_TIERS*`,
`*ALLOW_CLIENT_EXCLUSIVE_NODE*`, `*SHARED_WARM_TARGET*`,
`*DYNAMIC_WARM_CAPACITY*`, `*WARM_CAPACITY_*`, `*WARM_ACQUIRE_TIMEOUT*`,
`*WORKER_EXCLUSIVE_NODE*` (the one-worker-per-node pod anti-affinity is gone;
isolation comes from resource requests — workers are never BestEffort, falling
back to the built-in default shape (8/16Gi) when neither the profile nor
`DUCKGRES_K8S_WORKER_CPU_REQUEST`/`_MEMORY_REQUEST` set one, so co-scheduled
workers cannot overcommit a node).

## Testing

- Unit: GUC parse/clamp/default; smallest-fitting reuse vs spawn; TTL reset on
  query + reap after deadline; headroom controller target math (placeholder
  count from allocatable + percent); gate/clamp.
- e2e `harness.sh`: a query with `worker_cpu/memory/ttl` lands on a worker of
  that size; a second query of a smaller size reuses it; after ttl the worker is
  reaped; one-session-per-worker still holds; placeholder pods exist at the
  configured headroom. Verified on cnpg + ext backends.

## Rollout

Build arm64 control-plane + worker images, push to ECR, deploy to **mw-dev only**
(`tests/e2e-mw-dev/run.sh`), run harness, iterate. Never prod.
