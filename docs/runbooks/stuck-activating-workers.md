# Runbook: Recover Stuck Activating Workers

## When to use

- `duckgres_activating_workers` gauge is non-zero for more than 2 minutes
- `duckgres_activation_failures_total` is increasing
- Sessions are timing out because no hot workers are available

## Background

When an org requests a session and no hot worker is available, the control plane reserves an idle warm worker and activates it (loads the org's DuckLake catalog, configures tenant settings, etc.). If activation fails or hangs, the worker stays in `reserved` or `activating` state indefinitely.

The automatic stuck-worker reaper runs every minute and retires workers that have been in `reserved` or `activating` state for longer than 2 minutes. Reaped workers are replaced automatically if the pool is below `minWorkers`.

## Metrics to watch

| Metric | What it means |
|--------|---------------|
| `duckgres_activating_workers` | Workers currently activating (should be 0 or briefly 1-2) |
| `duckgres_reserved_workers` | Workers reserved but not yet activating |
| `duckgres_activation_failures_total` | Total failed activations; use control-plane logs for failure details |
| `duckgres_worker_retirements_total{reason="stuck_activating"}` | How many workers have been auto-reaped |

## Procedure

1. **Check if the reaper is working.** Look for `duckgres_worker_retirements_total{reason="stuck_activating"}` increasing. If it is, the system is self-healing — focus on why activations are failing.

2. **Diagnose activation failures.** Check control-plane logs for activation errors. Common causes:
   - Org config resolver failing (missing config in configstore)
   - DuckLake catalog unreachable (S3/Postgres connectivity)
   - Worker pod OOMKilled during activation

3. **If the reaper is not running** (e.g., idleReaper goroutine crashed), manually delete the stuck pods:
   ```bash
   kubectl get pods -l app=duckgres-worker --field-selector status.phase=Running | grep activating
   kubectl delete pod <pod-name> --grace-period=10
   ```

4. **Verify recovery.** After stuck workers are cleaned up, check that `duckgres_warm_workers` replenishes to `minWorkers`.

## Prevention

- Ensure org configs are pre-validated before allowing session creation
- Monitor `duckgres_activation_duration_seconds` p99 to detect slow activations before they time out
