# Runbook: Worker Capacity / Slow Worker Acquisition

## When to use

- New sessions are slow to get a worker, or fail with `worker capacity exhausted` /
  `no Duckgres worker is currently available; ... retry in about Ns` /
  `timed out waiting for an available worker`.
- Worker pods are stuck `Pending` (cluster can't schedule them).
- The node-headroom placeholder pool is depleted, so every spawn waits on a cold
  Karpenter node instead of preempting a placeholder.
- After a mass retirement event (planned control-plane rollout, crash storm).

## Background

There is **no warm pool**. In the remote/k8s backend a worker pod is spawned
**on demand** for a specific org, sized from the connection's
`duckgres.worker_cpu`/`worker_memory` request, and kept **hot-idle** after the
session ends so the same org can reuse it (by exact shape) until its
`duckgres.worker_ttl` expires. Startup latency is hidden by the **node-headroom
controller** (`reconcileHeadroom`, a janitor hook): it keeps worker-sized
low-priority placeholder ("pause") pods ready, with the count following the
peak spawn burst of the last hour (floor 1, capped at `max(4, 25% of live
workers)`) and the size following the largest worker shape spawned in the last
7 days — both derived from the worker spawn log, no config knobs; a real worker
spawn preempts a placeholder and schedules immediately. So "low capacity" now means one of: spawns
are failing, the cluster can't schedule pods, headroom is exhausted, or an org
has hit its per-org worker cap (`Org.MaxWorkers`; 0 = unbounded). There is no
global/cluster worker cap — the node pool / autoscaler is the only shared ceiling.

## Metrics to watch

| Metric | What it means |
|--------|---------------|
| `sum(duckgres_worker_lifecycle_count{state="spawning"})` sustained > 0 | Spawns are slow/stuck (image pull, Pending pods, cold nodes) |
| `sum by (reason)(rate(duckgres_worker_spawn_failures_total[5m]))` | Pod spawn errors by stage |
| `duckgres_control_plane_worker_acquire_seconds` (if present) | Session acquire latency |
| Crash-rate query (procedure step 1) | Workers crashing → repeated respawns |

Note: `binding="neutral"` lifecycle series are legacy (no worker is spawned
unassigned anymore); production capacity lives in `binding="org_bound"`.

## Procedure

1. **Check why capacity churns.** Crash-like retirements (each forces the next
   session to pay a fresh spawn):

   ```promql
   sum by (origin)(
     rate(duckgres_worker_lifecycle_transitions_total{
       operation=~"retire_.*|mark_lost_from_lease",
       outcome="transitioned",
       origin=~"health_check_crash|reserve_failure|spawn_failure|crash_generic|informer_crash"
     }[5m])
   )
   ```

   What each origin means:
   - `health_check_crash` — periodic health-check loop tripped consecutive-failure threshold
   - `crash_generic` — reserved-worker liveness recheck failure or no-runtime-store fallback
   - `informer_crash` — Kubernetes informer observed a worker pod termination
   - `reserve_failure` — claim/spawn succeeded but reservation activation failed
   - `spawn_failure` — pod spawn returned an error; use `sum by (reason) (rate(duckgres_worker_spawn_failures_total[5m]))` for stage breakdown
   - `idle_timeout` — hot-idle workers reaped at their TTL (expected; only a concern if TTLs are too short for the traffic)
   - `janitor_stuck_activating` — activation is broken, see [stuck-activating-workers](stuck-activating-workers.md)

2. **Check Kubernetes scheduling.** If worker pods are Pending:
   ```bash
   kubectl get pods -l app=duckgres-worker --field-selector status.phase=Pending
   kubectl describe pod <pending-pod>
   ```
   Common causes: insufficient memory/CPU, node pool at max size, resource quotas,
   a sized request larger than any node.

3. **Check node headroom.** If every spawn waits on a cold node, the placeholder
   pool is too small or being starved:
   ```bash
   kubectl get pods -l app.kubernetes.io/component=duckgres-headroom -o wide
   ```
   - Confirm placeholder pods exist and are `Running` (they should be preempted,
     then rescheduled, as real workers spawn). Placeholders use a PriorityClass
     ranked **below** the worker PriorityClass so workers always win; headroom
     is enabled iff `DUCKGRES_K8S_PLACEHOLDER_PRIORITY_CLASS` is set.
   - There is no count/size knob: the controller follows demand
     (`duckgres_headroom_slots_desired` / `_slots_cap` /
     `_peak_spawn_burst` gauges). `desired` pinned at `cap` means demand wants
     more headroom than the fleet-relative ceiling (`max(4, 25% of live
     workers)`) allows — that ceiling only binds during a spawn storm; if it
     binds in steady state, revisit the constants in `controlplane/headroom.go`.

4. **Check the cap.** If you hit `worker capacity exhausted for organization`,
   the request is at a real cap, not a scheduling problem:
   - Per-org: the org reached its configured max concurrent workers
     (`Org.MaxWorkers`) and all are busy — expected backpressure; the client
     should retry as queries finish. Raise the org's `max_workers` (or set it to
     0 = unbounded) if it is legitimately undersized; otherwise scale the worker
     node pool. There is no global worker cap to raise.

5. **Verify recovery.** New sessions stop getting capacity errors;
   `duckgres_worker_lifecycle_count{state="spawning"}` settles back toward 0.

## Emergency

If acquisition is wedged (e.g. a leaked accounting state at the cap), restart the
control-plane pod. Workers are durable in the config store and are re-adopted on
startup; there is no warm pool to repopulate.
