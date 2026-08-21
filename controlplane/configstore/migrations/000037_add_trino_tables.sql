-- +goose Up
-- Re-introduce the Trino subsystem's two config tables. They were created by
-- an earlier iteration of the feature and dropped again by migration 000002
-- when the customer-facing Trino work was unwound for product reasons; this
-- migration brings them back, with the differences the intervening year of
-- schema work implies:
--
--   * duckgres_managed_warehouse_trino gains trino_cell_id — the Trino cell
--     that owns the org. Exactly one cell exists today (see
--     configstore.DefaultTrinoCellID); the column exists so a second one is a
--     data change rather than a migration under pressure. Empty means
--     UNASSIGNED and the first reconciling provisioner claims the row.
--   * Nothing Iceberg-shaped survives: catalogs are DuckLake now (migration
--     000014 dropped every iceberg_* column), so the per-org catalog is built
--     from the warehouse row's metadata_store_* / s3_* / worker_identity_*
--     blocks and carries no Lakekeeper reference.
--
-- duckgres_trino_cluster_bootstrap is a one-bit-per-namespace sentinel, not a
-- credential store: it only records that the cluster's K8s Secrets have been
-- generated at least once, so a Secret that goes missing AFTER bootstrap can
-- fail loud instead of being silently regenerated (a regenerated
-- internal-communication shared secret would split-brain a running Trino
-- cluster). The credential VALUES live only in the K8s Secrets.

CREATE TABLE IF NOT EXISTS duckgres_managed_warehouse_trino (
    org_id         TEXT        NOT NULL,
    enabled        BOOLEAN     NOT NULL DEFAULT FALSE,
    tier           TEXT        NOT NULL DEFAULT '',
    trino_cell_id  TEXT        NOT NULL DEFAULT '',
    state          TEXT        NOT NULL DEFAULT 'pending',
    status_message TEXT        NOT NULL DEFAULT '',
    ready_at       TIMESTAMPTZ NULL,
    failed_at      TIMESTAMPTZ NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id),
    CONSTRAINT fk_duckgres_managed_warehouse_trino_org
        FOREIGN KEY (org_id) REFERENCES duckgres_orgs (name) ON DELETE CASCADE
);

-- The reconcile loop's only listing query is "every enabled org", and it runs
-- on every controller tick. Partial index so it stays proportional to the
-- opted-in orgs, not to the fleet.
CREATE INDEX IF NOT EXISTS idx_duckgres_managed_warehouse_trino_enabled
    ON duckgres_managed_warehouse_trino (org_id)
    WHERE enabled;

CREATE TABLE IF NOT EXISTS duckgres_trino_cluster_bootstrap (
    namespace       TEXT        NOT NULL,
    bootstrapped_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace)
);

-- +goose Down
DROP TABLE IF EXISTS duckgres_trino_cluster_bootstrap;

DROP TABLE IF EXISTS duckgres_managed_warehouse_trino;
