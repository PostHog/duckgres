# Runbook: Control-Plane Rolling Rollout

## Goal

Replace Duckgres control-plane replicas without breaking most existing sessions during a planned deployment.

## Requirements

- The Deployment uses rolling replacement with overlap:
  - `maxUnavailable: 0`
  - `maxSurge: 1`
- A `preStop` retirement hook keeps the listeners available while the
  terminating pod is withdrawn from Service routing. The isolated E2E fixture
  uses `exec: { command: ["sleep", "5"] }`; verify the corresponding lifecycle
  setting in the deployment being rolled out.
- `terminationGracePeriodSeconds` includes both the hook and the configured
  drain timeout. The isolated fixture uses 35 seconds to preserve its previous
  30-second drain budget after the five-second hook.
- The control plane sets `--handover-drain-timeout 15m` (or another explicit value appropriate for the cluster)

## Expected behavior

1. Kubernetes marks the old pod terminating and asynchronously withdraws it
   from Service routing. The preStop hook keeps its listeners available during
   this propagation window.
2. After the hook, the old replica receives `SIGTERM`, closes local pgwire
   admission, and fails `/health`.
3. It publishes `draining` in runtime state. New pgwire sessions are rejected.
4. Existing pgwire connections continue until they finish or the drain timeout
   expires. With no connections, the process can exit immediately.
5. When the timeout expires, the replica force-shuts down remaining sessions
   and workers.

`kubectl rollout status` observes Deployment replica counters; it does not
confirm that the previous pods have finished terminating or that every client
node has updated Service routing. Without a retirement hook, requests can
still reach an old pod after its listener has closed. A hook alone also does
not prove that a configuration read reaches the new revision: the old pod can
still answer during that hook.

See [Org connection admission](org-connection-admission.md) for the
mixed-version admission boundary during a rolling deployment.

Unplanned control-plane failure is different:

- live pgwire connections are lost immediately
- clients must establish a new pgwire connection and worker session

## Rollout procedure

1. Capture the previous pods, then start the rollout.
   ```bash
   old_pods="$(kubectl -n duckgres get pods -l app=duckgres-control-plane -o name)"
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

4. Before asserting that a configuration change is visible, wait for the
   captured previous pods to finish retiring. Select a timeout that includes
   the configured session-drain budget; long-lived sessions may keep them
   alive. Do not wait on the label selector, which also selects the new pods.
   ```bash
   # Splitting is intentional: kubectl emitted one resource name per line.
   kubectl -n duckgres wait --for=delete $old_pods --timeout=20m
   ```

5. Confirm the new pod is serving traffic and new sessions acquire workers.
   - `sum(duckgres_worker_lifecycle_count{state="spawning"})` settles back toward 0 (on-demand spawns succeed)
   - `sum(duckgres_worker_lifecycle_count{state="hot"})` does not drop unexpectedly
   - client reconnect errors do not spike

## If a rollout stalls

- Check whether the old pod is still draining active sessions:
  ```bash
  kubectl -n duckgres logs <old-pod-name> | rg "drain|draining|shutdown"
  ```
- Inspect pod events for `FailedPreStopHook` and verify the hook command exists
  in the image.
- Check whether the pod termination grace period includes both the hook and
  the configured drain timeout.
- Check whether long-lived idle clients are holding pgwire connections open.

## If the timeout is too short

- Increase both:
  - `--handover-drain-timeout`
  - `terminationGracePeriodSeconds`

Keep those values aligned. If the pod is killed before the drain timeout elapses, Kubernetes will cut the drain short.

If new connections are refused immediately after rollout completion, collect
old/new pod logs and EndpointSlice watches alongside client-node routing or
packet evidence during the transition. Distinguish a reset from an exited old
pod from a Service with no usable backend. Preserve the failure instead of
retrying a configuration assertion until it happens to select the new pod.
