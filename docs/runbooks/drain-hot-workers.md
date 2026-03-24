# Runbook: Drain Hot Workers

## When to use

- Rolling out a new worker image or DuckDB version
- Reducing capacity for a specific org
- Investigating a misbehaving worker that is still serving sessions

## Metrics to watch

| Metric | Alert threshold | What it means |
|--------|----------------|---------------|
| `duckgres_hot_workers` | Dropping faster than expected | Workers are retiring before replacements come up |
| `duckgres_draining_workers` | > 0 sustained | Workers are stuck draining (sessions not ending) |
| `duckgres_warm_workers` | < `minWorkers` | Replacement pool is depleted |

## Procedure

1. **Identify the worker(s) to drain.** Use pod labels and the control-plane logs to identify the worker pod serving the sessions you want to replace.

2. **Delete the worker pod.** The control plane treats the worker as retired/crashed and replaces it if the pool is below `minWorkers`.

3. **Watch replacement capacity.** Monitor `duckgres_warm_workers` and `duckgres_hot_workers` while the replacement comes up.

4. **Verify replacement capacity.** After the worker retires, confirm that `duckgres_warm_workers` returns to the expected level. The pool auto-replenishes when idle count drops below `minWorkers`.

## Rollback

If draining causes capacity issues:

1. Check `duckgres_warm_workers` — if it's 0, the pool is depleted and new sessions will block.
2. Increase `minWorkers` temporarily to force more pre-warmed capacity.
3. If a drain is stuck (worker stays in draining > 5 minutes), force-retire it by deleting the pod. The control plane will detect the crash and replenish.
