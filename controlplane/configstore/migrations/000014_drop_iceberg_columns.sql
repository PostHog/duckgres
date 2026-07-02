-- +goose Up
-- Iceberg/Lakekeeper support was removed from duckgres: the per-org Lakekeeper
-- Iceberg catalog, its provisioner, and the per-user default_catalog selector
-- (whose only non-empty value was "iceberg") are gone. DuckLake is the only
-- catalog.
ALTER TABLE duckgres_managed_warehouses
    DROP COLUMN IF EXISTS iceberg_enabled,
    DROP COLUMN IF EXISTS iceberg_backend,
    DROP COLUMN IF EXISTS iceberg_namespace,
    DROP COLUMN IF EXISTS iceberg_region,
    DROP COLUMN IF EXISTS iceberg_lakekeeper_endpoint,
    DROP COLUMN IF EXISTS iceberg_lakekeeper_warehouse,
    DROP COLUMN IF EXISTS iceberg_lakekeeper_client_id,
    DROP COLUMN IF EXISTS iceberg_lakekeeper_o_auth2_server_uri,
    DROP COLUMN IF EXISTS iceberg_lakekeeper_client_credentials_namespace,
    DROP COLUMN IF EXISTS iceberg_lakekeeper_client_credentials_name,
    DROP COLUMN IF EXISTS iceberg_lakekeeper_client_credentials_key,
    DROP COLUMN IF EXISTS iceberg_state;

ALTER TABLE duckgres_org_users
    DROP COLUMN IF EXISTS default_catalog;

-- +goose Down
ALTER TABLE duckgres_managed_warehouses
    ADD COLUMN IF NOT EXISTS iceberg_enabled BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS iceberg_backend VARCHAR(32) DEFAULT 'lakekeeper',
    ADD COLUMN IF NOT EXISTS iceberg_namespace VARCHAR(255),
    ADD COLUMN IF NOT EXISTS iceberg_region VARCHAR(64),
    ADD COLUMN IF NOT EXISTS iceberg_lakekeeper_endpoint VARCHAR(512),
    ADD COLUMN IF NOT EXISTS iceberg_lakekeeper_warehouse VARCHAR(128),
    ADD COLUMN IF NOT EXISTS iceberg_lakekeeper_client_id VARCHAR(128),
    ADD COLUMN IF NOT EXISTS iceberg_lakekeeper_o_auth2_server_uri VARCHAR(512),
    ADD COLUMN IF NOT EXISTS iceberg_lakekeeper_client_credentials_namespace VARCHAR(255),
    ADD COLUMN IF NOT EXISTS iceberg_lakekeeper_client_credentials_name VARCHAR(255),
    ADD COLUMN IF NOT EXISTS iceberg_lakekeeper_client_credentials_key VARCHAR(255),
    ADD COLUMN IF NOT EXISTS iceberg_state VARCHAR(32);

ALTER TABLE duckgres_org_users
    ADD COLUMN IF NOT EXISTS default_catalog VARCHAR(255);
