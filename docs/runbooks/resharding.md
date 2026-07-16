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

One blocked shape is deliberate: when the op's recorded source identity was
unusable (empty/invalid `from_shard`, or an external source block without
endpoint/password secret), rollback refuses to emit the flip-back patch — the
XRD would reject it — and leaves the warehouse blocked with an ERROR carrying
the manual steps. The duckling then still points at the WRONG (target) store.
Determine where the catalog actually lives (duckling status history, the
pre-flip `backup_s3_uri`), patch `spec.metadataStore` to it, verify a client
can activate against the real catalog, and only then set the warehouse state
back to `ready`. Never unblock first — clients would activate against the
wrong (likely empty) catalog.

The same blocked state is deliberate after an accepted rollback patch when
Duckling status still names the failed target, is not Ready, does not identify
the recorded source, or the source tenant-role probe does not answer. The
runner logs periodic observations and a terminal one with the last status or
probe error. Repair the Duckling composition/source catalog, then confirm the
status and a tenant activation both use the intended source before setting the
warehouse back to `ready`. Compaction intentionally remains paused in this
state; do not re-enable it until that recovery check succeeds.

Never delete or recreate a retained cnpg database until the operation log shows
that the external target passed content fingerprint verification.
