-- +goose Up
-- Drop config-store surface that was seeded/served but never read to drive
-- runtime behavior, plus managed-warehouse columns with no provisioning,
-- worker-activation, Crossplane-composition, or curated-response consumer.
--
-- The four cluster-wide singleton config tables are superseded entirely: the
-- control plane takes its effective config from CLI flags/env (server.Config)
-- and the per-org managed-warehouse contract. They were only ever seeded,
-- loaded into the snapshot, and edited via the admin /config API — no behavioral
-- reader exists.

DROP TABLE IF EXISTS duckgres_global_config;
DROP TABLE IF EXISTS duckgres_ducklake_config;
DROP TABLE IF EXISTS duckgres_rate_limit_config;
DROP TABLE IF EXISTS duckgres_query_log_config;

-- Managed-warehouse dead columns. The warehouse_database_* (name/user/region)
-- and metadata_store_{engine,region} fields are never read; the worker
-- ServiceAccount name is computed, not read from config; warehouse_database has
-- no provisioning sub-state; and the provisioner only ever writes the top-level
-- status_message, so the per-component *_status_message columns are dead.
ALTER TABLE duckgres_managed_warehouses
    DROP COLUMN IF EXISTS warehouse_database_region,
    DROP COLUMN IF EXISTS warehouse_database_database_name,
    DROP COLUMN IF EXISTS warehouse_database_username,
    DROP COLUMN IF EXISTS metadata_store_engine,
    DROP COLUMN IF EXISTS metadata_store_region,
    DROP COLUMN IF EXISTS worker_identity_service_account_name,
    DROP COLUMN IF EXISTS warehouse_database_state,
    DROP COLUMN IF EXISTS warehouse_database_status_message,
    DROP COLUMN IF EXISTS metadata_store_status_message,
    DROP COLUMN IF EXISTS s3_status_message,
    DROP COLUMN IF EXISTS iceberg_status_message,
    DROP COLUMN IF EXISTS identity_status_message,
    DROP COLUMN IF EXISTS secrets_status_message;

-- Runtime coordination tables (runtime schema, normally AutoMigrate'd). These
-- drops are best-effort and schema-qualified at apply time would require the
-- runtime schema name; the columns are harmless if left (always empty / never
-- read), so we only drop them from the public-schema copy if present. The Go
-- structs no longer carry these fields, so nothing reads or writes them.
-- cp_instances.pod_uid/boot_id are already encoded into the primary-key id;
-- worker_records.pod_uid was never written; org_connection_queue.canceled_at
-- was never set (cancel is a DELETE).
-- NOTE: runtime tables live in the per-deployment runtime schema, not public —
-- AutoMigrate created them there and will simply stop touching the orphaned
-- columns. No DROP is emitted here to avoid guessing the schema name.

-- +goose Down
CREATE TABLE IF NOT EXISTS duckgres_global_config (
    id BIGINT PRIMARY KEY,
    memory_budget VARCHAR(32),
    memory_rebalance BOOLEAN DEFAULT false,
    max_connections BIGINT DEFAULT 0,
    idle_timeout_s BIGINT DEFAULT 0,
    worker_queue_timeout_s BIGINT DEFAULT 0,
    worker_idle_timeout_s BIGINT DEFAULT 0,
    extensions VARCHAR(1024),
    updated_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS duckgres_ducklake_config (
    id BIGINT PRIMARY KEY,
    metadata_store VARCHAR(1024),
    object_store VARCHAR(1024),
    data_path VARCHAR(1024),
    s3_provider VARCHAR(64),
    s3_endpoint VARCHAR(512),
    s3_access_key VARCHAR(255),
    s3_secret_key VARCHAR(255),
    s3_region VARCHAR(64),
    s3_use_ssl BOOLEAN DEFAULT false,
    s3_url_style VARCHAR(16),
    s3_chain VARCHAR(255),
    s3_profile VARCHAR(255),
    delta_catalog_enabled BOOLEAN DEFAULT true,
    delta_catalog_path VARCHAR(1024),
    updated_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS duckgres_rate_limit_config (
    id BIGINT PRIMARY KEY,
    max_failed_attempts BIGINT DEFAULT 0,
    failed_attempt_window_s BIGINT DEFAULT 0,
    ban_duration_s BIGINT DEFAULT 0,
    max_connections_per_ip BIGINT DEFAULT 0,
    updated_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS duckgres_query_log_config (
    id BIGINT PRIMARY KEY,
    enabled BOOLEAN DEFAULT false,
    flush_interval_s BIGINT DEFAULT 0,
    batch_size BIGINT DEFAULT 0,
    compact_interval_s BIGINT DEFAULT 0,
    data_inlining_row_limit BIGINT DEFAULT 0,
    updated_at TIMESTAMPTZ
);

ALTER TABLE duckgres_managed_warehouses
    ADD COLUMN IF NOT EXISTS warehouse_database_region VARCHAR(64),
    ADD COLUMN IF NOT EXISTS warehouse_database_database_name VARCHAR(255),
    ADD COLUMN IF NOT EXISTS warehouse_database_username VARCHAR(255),
    ADD COLUMN IF NOT EXISTS metadata_store_engine VARCHAR(64),
    ADD COLUMN IF NOT EXISTS metadata_store_region VARCHAR(64),
    ADD COLUMN IF NOT EXISTS worker_identity_service_account_name VARCHAR(255),
    ADD COLUMN IF NOT EXISTS warehouse_database_state VARCHAR(32),
    ADD COLUMN IF NOT EXISTS warehouse_database_status_message VARCHAR(1024),
    ADD COLUMN IF NOT EXISTS metadata_store_status_message VARCHAR(1024),
    ADD COLUMN IF NOT EXISTS s3_status_message VARCHAR(1024),
    ADD COLUMN IF NOT EXISTS iceberg_status_message VARCHAR(1024),
    ADD COLUMN IF NOT EXISTS identity_status_message VARCHAR(1024),
    ADD COLUMN IF NOT EXISTS secrets_status_message VARCHAR(1024);
