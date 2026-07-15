-- +goose Up

-- Durable reconciler intervention count. Leader-local counters reset during
-- failover and defeat the advertised retry bound.
ALTER TABLE duckgres_reshard_operations
    ADD COLUMN IF NOT EXISTS respawn_attempts BIGINT NOT NULL DEFAULT 0;

-- +goose Down

ALTER TABLE duckgres_reshard_operations
    DROP COLUMN IF EXISTS respawn_attempts;
