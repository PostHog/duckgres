# Resharding Operations

## Local development

Run the normal local checks with `just build`, `just test-unit`, and
`just lint`. Kubernetes-tagged reshard tests use the fake client and run under
`just test-controlplane-k8s`; they do not require a live cluster, though other
packages in that recipe may require the repository's Postgres test setup.

Runner pod resources default to `2` CPU and `8Gi` memory. Override them with
`DUCKGRES_RESHARD_POD_CPU` and `DUCKGRES_RESHARD_POD_MEMORY`. Invalid
quantities fail the pod spawn with an operator-visible error; they never panic
the control plane.

## Failed or stale runner

1. Inspect the operation log and `duckgres-reshard-op-<id>` pod events.
2. Do not manually set the warehouse to `ready`. The runner only unblocks it
   after a verified forward completion or verified rollback.
3. Fix the scheduling, image, RBAC, config-store, or target connectivity error.
4. If automatic respawn paused after three attempts, reset only the operation's
   durable counter:

   ```sql
   UPDATE duckgres_reshard_operations
   SET respawn_attempts = 0
   WHERE id = <operation-id> AND state IN ('pending', 'running');
   ```

5. The leader reconciler will recreate the runner. A takeover never rediscovers
   the source from post-cutover Duckling status; it resumes only safe durable
   phases and otherwise rolls back.

## Recovery remains blocked

If the operation is terminal-failed while the warehouse remains `resharding`,
read the final recovery observations before changing state. Confirm the
Duckling points at the intended source or target, its tenant role answers, and
the catalog fingerprint/table set is complete. Only then restore warehouse
readiness. For a damaged catalog, use the `pg_restore` command recorded beside
`backup_s3_uri` in the operation log.

Never delete or recreate a retained cnpg database until the operation log shows
that the external target passed content fingerprint verification.
