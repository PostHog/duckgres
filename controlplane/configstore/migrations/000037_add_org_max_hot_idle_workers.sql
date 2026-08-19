-- +goose Up
ALTER TABLE duckgres_orgs
    ADD COLUMN IF NOT EXISTS max_hot_idle_workers BIGINT DEFAULT 0;

ALTER TABLE duckgres_orgs
    ADD COLUMN IF NOT EXISTS max_hot_idle_cpu VARCHAR(32) NOT NULL DEFAULT '';

ALTER TABLE duckgres_orgs
    ADD COLUMN IF NOT EXISTS max_hot_idle_memory VARCHAR(32) NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE duckgres_orgs
    DROP COLUMN IF EXISTS max_hot_idle_workers;

ALTER TABLE duckgres_orgs
    DROP COLUMN IF EXISTS max_hot_idle_cpu;

ALTER TABLE duckgres_orgs
    DROP COLUMN IF EXISTS max_hot_idle_memory;
