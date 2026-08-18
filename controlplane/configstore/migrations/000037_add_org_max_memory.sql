-- +goose Up

ALTER TABLE duckgres_orgs
    ADD COLUMN IF NOT EXISTS max_memory VARCHAR(32) NOT NULL DEFAULT '';

-- +goose Down

ALTER TABLE duckgres_orgs
    DROP COLUMN IF EXISTS max_memory;
