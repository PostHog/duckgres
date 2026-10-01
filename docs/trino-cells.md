# Trino shared compute pools

All configured Trino cells use `mode: "shared-pool"`. Duckgres creates and
retires immutable compute instances. Gateway routes new work to serving
instances and preserves query and transaction ownership while an instance drains.
There is no standalone coordinator, fixed blue/green topology, canary warehouse,
or implicit legacy assignment path.

## Configuration

`DUCKGRES_TRINO_CELLS_FILE` defaults to unset. Set it to a mounted JSON registry
with at most 16 cells. Every cell has a distinct ID, namespace, and routing group.
All control-plane replicas must receive the same registry. The operator reads
updated desired settings from the configured ConfigMap; a missing or invalid
pool freezes changes rather than deleting instances.

```json
{
  "cells": [{
    "id": "pool-a",
    "namespace": "trino-cells",
    "client_url": "https://warehouse.example.test",
    "routing_group": "pool-a",
    "mode": "shared-pool",
    "pool": {
      "desired_instances": 3,
      "min_serving": 3,
      "max_surge": 1,
      "max_repair": 1,
      "blueprint_file": "/etc/trino-pools/blueprint.json",
      "coordinator_service_port": 8080,
      "node_environment": "warehouse",
      "tenant_admission": true
    }
  }]
}
```

Client URLs use HTTPS. Internal coordinator Services use HTTP with forwarded
HTTPS metadata. Catalogs are published directly to the shared catalog store
under the pool writer fence. Per-pool OPA sidecars fetch
`/bundles/trino/<cell-id>` using the namespace's bundle token.

`DUCKGRES_TRINO_POOL_ENABLED`, `DUCKGRES_TRINO_POOL_OPERATOR_ENABLED`, and
`DUCKGRES_TRINO_POOL_CATALOG_WRITER_ENABLED` default to false. Enable them with
valid Gateway, catalog-store, and pinned blueprint configuration. The existing
`DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE` and `DUCKGRES_TRINO_MANAGED_GATEWAY_USERNAME`
settings authenticate pool operations to Gateway; these are active credentials,
not switches for the removed fixed-slot rollout API.

## Automatic placement runbook

`DUCKGRES_TRINO_DEFAULT_CELL` defaults to unset. Set it to a configured pool's
public ID to select that pool atomically on first enablement. The pool must
have tenant admission and all three pool gates enabled. Existing ownership and
backend choices never change implicitly.

Without a default, select a cell through the admin UI or
`PUT /api/v1/orgs/<org>/trino/cell` before enabling Trino. Selection does not
enable Trino. The stored ID remains `registered:<cell-id>`; this is a durable
ownership key, not a compute instance name.

Operational admin requests require `?cell=<cell-id>`. Org detail follows the
persisted owner. An absent or unconfigured owner never falls back to another
pool. An explicit move between configured pools remains a maintenance operation,
not transparent resharding. Dynamic compute replacements within a pool do not
move its warehouse assignments.

## Removing an old standalone deployment

Roll out the shutdown chart first, then delete its cloud resources, then deploy
the shared-pool-only binary. Do not reverse this order. Before shutdown, inspect:

```sql
SELECT enabled, COALESCE(NULLIF(trino_cell_id, ''), '<unassigned>') AS cell_id,
       count(*)
FROM duckgres_managed_warehouse_trino
GROUP BY 1, 2 ORDER BY 1, 2;
```

No enabled row may retain an unassigned or removed owner at final code rollout.
Set `DUCKGRES_TRINO_DEFAULT_CELL` before the upgrade if warehouse-provisioning
requests enable Trino without an explicit saved pool selection.
Use the existing admin API to disable Trino for affected warehouses, or explicitly
move them while both source and destination are still configured. Confirm the
result from the store before proceeding. Never delete the warehouse, metadata,
objects, backend choice, or password as part of compute removal.

Disabled historical assignments stay in the database. Re-enabling one after its
owner is removed is rejected. Decide whether to move or explicitly clear a
disabled assignment during maintenance before removing the old runtime; the new
binary includes no permanent legacy rescue route. This change does not modify
existing tenant rows or drop historical migration tables.

An approved assignment reset must also return provisioning state to `pending`
and clear old readiness/failure timestamps and status. Initial selection rejects
previously provisioned rows. Preserve the backend choice and every warehouse
storage field, and verify those invariants in the same maintenance transaction.

## Pool alert metrics

`duckgres_trino_pool_serving_instance_info` maps durable `SERVING` rows to their
stored coordinator and worker Deployment names. Its labels are `pool`,
`pool_instance`, `workload_namespace`, `coordinator_deployment`, and
`worker_deployment`. A value of `1` identifies an instance; it does not prove
that its pods are healthy. Join Kubernetes availability metrics to assess health.
The workload namespace comes from each instance's immutable blueprint, so old
instances remain observable when desired placement changes.

`duckgres_trino_pool_min_serving{pool,workload_namespace}` exposes the durable
configured serving minimum, including snapshots with no serving instances.
Its namespace label is current desired placement, not each instance's pinned namespace.
Both metrics belong to the current operator term and disappear on term loss.
Replacing the snapshot removes departed serving instances rather than retaining
their labels indefinitely.

`duckgres_trino_pool_configured{pool,workload_namespace}=1` identifies enabled
pools from this process's startup registry, independently of operator ownership.
It remains when one pool loses authority and is removed on API shutdown. Pool
registry additions and removals require a process restart. Aggregate expectations
by pool; individual serving instances can retain an older pinned namespace.

Require a fresh `duckgres_trino_pool_snapshot_timestamp_seconds` from the same
scraped control-plane instance before using the inventory. Failed reads retain
the previous samples without refreshing that timestamp. Alert separately when
an expected configured pool has no fresh telemetry; absent data is not zero
healthy instances or evidence that the pool was intentionally disabled.
Frozen configuration or Gateway failures can stop reconciliation before the
snapshot is read. Freshness alerts also cover those pauses.

## Validation

Run `just test-trino`, `just test-trino-opa`, `just test-configstore-integration`,
and `just test-trino-admin`. Use `DUCKGRES_TEST_PG_DSN` for the configstore suite
when running against an isolated native PostgreSQL instance.

The [node replacement runbook](runbooks/trino-pool-node-replacement.md) describes
controlled disruption and query-preserving drain. The pool's publication,
authorization, admission, and retirement fences remain required.
