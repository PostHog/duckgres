# Connection-string worker profiles (`colocate` + inline cpu/mem)

Status: in progress. Lets a duckgres client choose its worker pod shape per session
via libpq startup options, so backfills / quick-and-dirty jobs get a small pod
bin-packed onto a shared node (schedules in seconds) while heavy jobs get a full
exclusive node (slow boot is fine). Fixes the duckling-backfill `ConnectionTimeout`:
today every worker is one pod on an exclusive 46-CPU/360Gi Karpenter node, so
backfills starve waiting on cold *node* provisioning (minutes).

## Grammar (libpq `options`, parsed like the existing `search_path` GUC)

```
options=-c duckgres.colocate=true -c duckgres.worker_cpu=4 -c duckgres.worker_memory=16Gi
options=-c duckgres.worker_tier=backfill        # ergonomic alias = {cpu, mem, colocate}
```

- `colocate=true`  → `exclusiveNode=false`, bin-pack nodeSelector, small request → fast schedule, multi-tenant node.
- `colocate=false` → `exclusiveNode=true`, today's big-node pool, one pod/node, slow boot OK.
- **Defaults:** `colocate=true` + no size → 4 CPU / 16Gi; `colocate=false` or absent → 46 CPU / 360Gi (today).
- **Absent `colocate` ⇒ false ⇒ byte-for-byte backward compatible.** Product/query connections never send the GUCs.

## Resolved design decisions

- **`WorkerProfile{CPU, Memory, Colocate, NodeSelector}` is a new match dimension on `WorkerAssignment`**, orthogonal to the per-org `Image` axis — NOT folded into the image string. Pod resources/nodeSelector/anti-affinity are pool-global mutable state today, so the profile must travel per-acquisition. A reserved/warm worker may only be handed to a request whose profile `Equal()`s it; absent profile = the **default** profile, never "any".
- **Default profile normalizes to empty CPU/Memory + `Colocate=false`** (NOT literal 46/360). The pool-global request applies for empty fields. This keeps pre-existing `worker_records` rows (`"","",false`) claimable with no migration `UPDATE`.
- **Runtime-store delta: additive GORM columns, no `.sql` migration** (`worker_records` is AutoMigrated on boot). Adds `ProfileCPU`, `ProfileMemory`, `ProfileColocate` + composite index.
- **Guaranteed QoS already holds** (`workerResources()` copies requests→limits) and MUST be preserved; colocated profiles are always non-empty (clamped 4/16) so they never schedule BestEffort.
- **Wire-supplied cpu/mem are clamped to `[min,max]`, behind a per-deployment gate, and accounted against a per-org resource quota** (count-based `maxWorkers` is insufficient when pods vary ~50×). Explicit `colocate=false` is separately gated (a full node is the expensive button).
- **Warmth = node headroom** on the bin-pack nodepool (Karpenter overprovision pause pods), not warm worker pools. The client's `connect_timeout=60s` + retry absorbs residual schedule/bootstrap latency.

## Build sequence (default behavior unchanged until P6)

- **P1** — config + `WorkerProfile` struct + resolver (gate/clamp/default/tier) + tests. Dark.
- **P2** — profile as assignment/match dimension + runtime-store columns + profile-aware pod spawn (`colocate`→exclusiveNode+nodeSelector+resources, limits==requests) + profile-aware `workerDuckDBLimits`. Dark.
- **P3** — `control.go` GUC parse + thread `*WorkerProfile` through `CreateSession`→`AcquireWorker`→`ReserveSharedWorker`. Dark.
- **P4** — resource-aware per-org quota.
- **P5** — charts values+template + bin-pack Karpenter nodepool + overprovision headroom. (`/render-appset` required.)
- **P6** — posthog `make_duckgres_conninfo` for events+persons backfills; enable via `DUCKGRES_WORKER_PROFILE_ENABLED` last, narrow→wide (PostHog's own org first).

## Profile taxonomy

| Profile | colocate | CPU / Mem | Node | Reached by |
|---|---|---|---|---|
| default / exclusive | false | 46 / 360Gi (pool-global; normalized `""/""`) | `duckgres-workers` (today) | absent GUCs / gate off / `worker_tier=heavy` |
| backfill | true | 4 / 16Gi | `duckgres-workers-colocated` | `colocate=true` no size / `worker_tier=backfill` |
| iceberg backfill | true | 8 / 48Gi | `duckgres-workers-colocated` | `colocate=true worker_cpu=8 worker_memory=48Gi` (Iceberg-allowlisted org) |

Iceberg dual-write does `INSERT … SELECT * FROM read_parquet(full-day)` (memory-heavy; can OOM 16Gi) — hence
8/48 for allowlisted orgs; the common DuckLake register path is metadata-only and fine on 4/16.

## Open decisions (locked for this implementation)

1. Explicit `colocate=false` when `AllowClientExclusiveNode=false` → **reject** (clear error), not silent downgrade.
2. Default profile → **empty strings**, not 46/360 (legacy claimability).
3. `WorkerAssignment.Profile` is `*WorkerProfile` (nil = default); `MatchKey`/`Equal` are nil-safe.
