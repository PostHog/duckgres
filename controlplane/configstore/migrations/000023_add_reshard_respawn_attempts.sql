-- +goose Up

-- Durable reconciler intervention count. Leader-local counters reset during
-- failover and defeat the advertised retry bound.
ALTER TABLE duckgres_reshard_operations
    ADD COLUMN IF NOT EXISTS respawn_attempts BIGINT NOT NULL DEFAULT 0;
ALTER TABLE duckgres_reshard_operations
    ADD COLUMN IF NOT EXISTS runner_image TEXT NOT NULL DEFAULT '';

-- +goose Down

ALTER TABLE duckgres_reshard_operations
    DROP COLUMN IF EXISTS runner_image;
ALTER TABLE duckgres_reshard_operations
    DROP COLUMN IF EXISTS respawn_attempts;
