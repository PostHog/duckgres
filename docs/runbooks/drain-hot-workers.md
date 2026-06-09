# Runbook: Drain Hot Workers

## When to use

- Rolling out a new worker image or DuckDB version
- Reducing capacity for a specific org
- Investigating a misbehaving worker that is still serving sessions

## Background

There is no warm pool: workers are spawned on demand per org and kept hot-idle
until their TTL. Deleting a worker pod does **not** trigger a replacement spawn —
the next session for that org spawns a fresh worker on demand. On SIGTERM (pod
delete) a worker **drains** (finishes in-flight work, rejects new) before exiting;
the control plane marks it `Draining` then retires it cleanly (worker drain
protocol, #690).

## Metrics to watch

| Metric | What it means |
|--------|---------------|
| `sum(duckgres_worker_lifecycle_count{state="hot"})` | Workers actively serving a session |
| `sum(duckgres_worker_lifecycle_count{state="draining"})` > 0 sustained | Workers stuck draining (in-flight work not ending) |
| `sum(duckgres_worker_lifecycle_count{state="spawning"})` | On-demand spawns in flight |

## Procedure

1. **Identify the worker(s) to drain.** Use pod labels (`duckgres/active-org`,
   `duckgres/worker-id`) and the control-plane logs to find the worker pod
   serving the sessions you want to replace.

2. **Delete the worker pod.** `kubectl delete pod <worker>` sends SIGTERM; the
   worker drains in-flight work, then exits, and the CP retires it. In-flight
   queries on that pod complete (they are not dropped).

3. **Verify.** The next session for that org spawns a fresh worker on demand;
   confirm `duckgres_worker_lifecycle_count{state="draining"}` returns to 0 and
   no sessions errored.

## Rollback / stuck drain

- A drain that exceeds `terminationGracePeriodSeconds` is killed by Kubernetes; a
  query still running at that point would error. Don't mass-delete more pods than
  the org can re-spawn under its worker cap.
- If a worker is stuck `Draining` for > 5 minutes (sessions not ending),
  force-retire it by deleting the pod with `--grace-period=0`; the CP detects the
  termination and cleans up the row.
