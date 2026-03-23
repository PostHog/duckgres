# Runbook: Replenish Warm Pool Capacity

## When to use

- `duckgres_warm_workers` is 0 and sessions are queuing
- After a mass retirement event (rolling update, crash storm)
- Scaling up for anticipated traffic

## Background

The shared warm pool maintains `minWorkers` idle workers at all times. When a worker is reserved, retired, or crashes, the pool automatically spawns a replacement if the idle count drops below `minWorkers`. If the Kubernetes cluster cannot schedule new pods (resource pressure, node failures), the pool can become depleted.

## Metrics to watch

| Metric | Alert threshold | What it means |
|--------|----------------|---------------|
| `duckgres_warm_workers` | < 1 for > 30s | No idle capacity, new sessions must wait for a spawn |
| `duckgres_hot_workers` | Near `maxWorkers` | Pool is at capacity, may need to scale `maxWorkers` |
| `duckgres_worker_retirements_total{reason="crash"}` | Spike | Workers are crashing, replacements may also crash |

## Procedure

1. **Check why capacity is low.** Look at retirement reasons:
   - `crash` — workers are dying, check pod logs and OOM events
   - `idle_timeout` — workers are being reaped, increase `minWorkers` or decrease `idleTimeout`
   - `stuck_activating` — activation is broken, see [stuck-activating-workers](stuck-activating-workers.md)

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
   - `duckgres_warm_workers` returns to `minWorkers`
   - New sessions are no longer queuing (check `duckgres_worker_acquire_duration_seconds` if available)

## Emergency: Force-spawn workers

If the auto-replenishment loop is stuck, restart the control plane pod. On startup it calls `SpawnMinWorkers(minWorkers)` which synchronously creates the minimum pool.
