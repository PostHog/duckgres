# Runbook: Control-Plane Rolling Rollout

## Goal

Replace Duckgres control-plane replicas without breaking most existing sessions during a planned deployment.

## Requirements

- The Deployment uses rolling replacement with overlap:
  - `maxUnavailable: 0`
  - `maxSurge: 1`
- `terminationGracePeriodSeconds` is at least the configured drain timeout
- The control plane sets `--handover-drain-timeout 15m` (or another explicit value appropriate for the cluster)

## Expected behavior

1. The old replica receives `SIGTERM`.
2. It marks itself `draining` in runtime state and fails `/health`.
3. New pgwire sessions are rejected on the draining replica.
4. New Flight bootstrap sessions are rejected on the draining replica.
5. Existing pgwire connections and existing Flight sessions continue until they finish or the drain timeout expires.
6. When the timeout expires, the replica force-shuts down remaining sessions and workers.

Unplanned control-plane failure is different:

- live pgwire connections are lost immediately
- durable Flight reconnect may recover only when the worker survives and the session token is still valid

## Rollout procedure

1. Start the rollout.
   ```bash
   kubectl -n duckgres rollout restart deploy/duckgres-control-plane
   kubectl -n duckgres rollout status deploy/duckgres-control-plane
   ```

2. Watch old and new pods during overlap.
   ```bash
   kubectl -n duckgres get pods -l app=duckgres-control-plane -w
   ```

3. Verify the old pod becomes unready before it exits.
   ```bash
   kubectl -n duckgres get pods -l app=duckgres-control-plane
   kubectl -n duckgres logs <old-pod-name>
   ```

4. Confirm the new pod is serving traffic and warm capacity recovers.
   - `duckgres_warm_workers` returns to target
   - `duckgres_hot_workers` does not drop unexpectedly
   - client reconnect errors do not spike

## If a rollout stalls

- Check whether the old pod is still draining active sessions:
  ```bash
  kubectl -n duckgres logs <old-pod-name> | rg "drain|draining|shutdown"
  ```
- Check whether the pod termination grace period is shorter than the configured drain timeout.
- Check whether long-lived idle clients are holding pgwire connections open.
- Check whether Flight sessions are still active and not timing out.

## If the timeout is too short

- Increase both:
  - `--handover-drain-timeout`
  - `terminationGracePeriodSeconds`

Keep those values aligned. If the pod is killed before the drain timeout elapses, Kubernetes will cut the drain short.
