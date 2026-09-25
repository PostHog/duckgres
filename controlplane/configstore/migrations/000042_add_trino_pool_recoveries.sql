-- +goose Up
CREATE TABLE duckgres_trino_pool_recoveries (
    operation_id              TEXT PRIMARY KEY CHECK (length(operation_id) BETWEEN 1 AND 128),
    pool_id                   TEXT NOT NULL REFERENCES duckgres_trino_pools (pool_id),
    instance_id               TEXT NOT NULL UNIQUE REFERENCES duckgres_trino_pool_instances (instance_id),
    expected_generation       BIGINT NOT NULL CHECK (expected_generation > 0),
    incarnation               TEXT NOT NULL,
    pod_uid                   TEXT NOT NULL,
    boot_id                   TEXT NOT NULL,
    node_id                   TEXT NOT NULL,
    coordinator_id            TEXT NOT NULL,
    requested_by              TEXT NOT NULL CHECK (length(requested_by) BETWEEN 1 AND 320),
    reason                    TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 256),
    destructive_authorization BOOLEAN NOT NULL CHECK (destructive_authorization),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_duckgres_trino_pool_recoveries_pool
    ON duckgres_trino_pool_recoveries (pool_id, created_at, operation_id);

CREATE INDEX idx_duckgres_trino_pool_instances_live
    ON duckgres_trino_pool_instances (pool_id, instance_id)
    WHERE phase NOT IN ('RETIRED', 'FAILURE_RETIRED');

-- +goose Down
DROP INDEX idx_duckgres_trino_pool_instances_live;
DROP TABLE duckgres_trino_pool_recoveries;
