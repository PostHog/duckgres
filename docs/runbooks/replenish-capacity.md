# Runbook: Replenish Warm Pool Capacity

## When to use

- `sum(duckgres_worker_lifecycle_count{state="idle",binding="neutral"})` is 0 and sessions are queuing
- After a mass retirement event (planned control-plane rollout, crash storm)
- Scaling up for anticipated traffic

## Background

The shared warm pool maintains `minWorkers` idle workers at all times. When a worker is reserved, retired, or crashes, the pool automatically spawns a replacement if the idle count drops below `minWorkers`. If the Kubernetes cluster cannot schedule new pods (resource pressure, node failures), the pool can become depleted.

## Metrics to watch

| Metric | Alert threshold | What it means |
|--------|----------------|---------------|
| `sum(duckgres_worker_lifecycle_count{state="idle",binding="neutral"})` | < 1 for > 30s | No idle capacity, new sessions must wait for a spawn |
| `sum(duckgres_worker_lifecycle_count{state="hot"})` | Near `maxWorkers` | Pool is at capacity, may need to scale `maxWorkers` |
| Crash-rate query (see procedure step 1) | Spike | Workers are crashing, replacements may also crash |

## Metric migration notes

The lifecycle observability refactor intentionally replaced the old global warm-pool gauges with per-image lifecycle series:

| Old metric/query | New query |
|------------------|-----------|
| `duckgres_warm_workers` | `sum(duckgres_worker_lifecycle_count{state="idle",binding="neutral"})` |
| `duckgres_reserved_workers` | `sum(duckgres_worker_lifecycle_count{state="reserved"})` |
| `duckgres_activating_workers` | `sum(duckgres_worker_lifecycle_count{state="activating"})` |
| `duckgres_hot_workers` | `sum(duckgres_worker_lifecycle_count{state="hot"})` |
| `duckgres_hot_idle_workers` | `sum(duckgres_worker_lifecycle_count{state="hot_idle"})` |
| `duckgres_draining_workers` | `sum(duckgres_worker_lifecycle_count{state="draining"})` |
| `duckgres_worker_retirements_total{reason="crash"}` | `sum by (origin) (rate(duckgres_worker_lifecycle_transitions_total{operation=~"retire_.*|mark_lost_from_lease",outcome="transitioned",origin=~"health_check_crash|reserve_failure|spawn_failure|crash_generic|informer_crash"}[5m]))` |

Warm-capacity misses now use a bare `image` label in Prometheus (`image="duckgres:tag"`), while the runtime-store demand buckets continue to use `scope="image:duckgres:tag"` because that database column can hold other scope families. `duckgres_warm_capacity_headroom` is now a scalar gauge; remove any old `scope="global"` selector. Activation latency and failure metrics are now per-image, so aggregate with `sum(...)` when matching old dashboard totals.

## Procedure

1. **Check why capacity is low.** Use this PromQL (kept outside the table so the `|` alternation isn't escaped by markdown rendering):

   ```promql
   sum by (origin)(
     rate(duckgres_worker_lifecycle_transitions_total{
       operation=~"retire_.*|mark_lost_from_lease",
       outcome="transitioned",
       origin=~"health_check_crash|reserve_failure|spawn_failure|crash_generic|informer_crash"
     }[5m])
   )
   ```

   The crash-like origins to filter on:

   ```promql
   origin=~"health_check_crash|reserve_failure|spawn_failure|crash_generic|informer_crash"
   ```

   What each origin means:
   - `health_check_crash` — periodic health-check loop tripped consecutive-failure threshold
   - `crash_generic` — reserved-worker liveness recheck failure or no-runtime-store fallback
   - `informer_crash` — Kubernetes informer observed a worker pod termination
   - `reserve_failure` — claim succeeded but reservation activation failed
   - `spawn_failure` — pod spawn returned an error; use `sum by (reason) (rate(duckgres_worker_spawn_failures_total[5m]))` for stage breakdown

   Other dominant origins:
   - `idle_timeout` — workers are being reaped, increase `minWorkers` or decrease `idleTimeout`
   - `janitor_stuck_activating` — activation is broken, see [stuck-activating-workers](stuck-activating-workers.md)

2. **Check Kubernetes scheduling.** If pods are Pending:
   ```bash
   kubectl get pods -l app=duckgres-worker --field-selector status.phase=Pending
   kubectl describe pod <pending-pod>
   ```
   Common causes: insufficient memory/CPU, node pool at max size, resource quotas.

3. **Scale up if needed.**
   - Increase `minWorkers` in the control plane config to pre-warm more workers
   - Increase `maxWorkers` if the pool is legitimately at capacity
   - Scale the Kubernetes node pool if scheduling is the bottleneck

4. **Verify recovery.** After scaling, confirm:
   - `sum(duckgres_worker_lifecycle_count{state="idle",binding="neutral"})` returns to `minWorkers`
   - New sessions are no longer queuing (check `duckgres_control_plane_worker_acquire_seconds` if available)

## Emergency: Force-spawn workers

If the auto-replenishment loop is stuck, restart the control plane pod. On startup it calls `SpawnMinWorkers(minWorkers)` which synchronously creates the minimum pool.
