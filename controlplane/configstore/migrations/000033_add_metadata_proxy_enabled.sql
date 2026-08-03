-- +goose Up
ALTER TABLE duckgres_managed_warehouses
    ADD COLUMN IF NOT EXISTS metadata_proxy_enabled BOOLEAN NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE duckgres_managed_warehouses
    DROP COLUMN IF EXISTS metadata_proxy_enabled;
