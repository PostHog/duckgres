-- +goose Up

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
    DROP COLUMN IF EXISTS iceberg_state,
    DROP COLUMN IF EXISTS iceberg_status_message;
