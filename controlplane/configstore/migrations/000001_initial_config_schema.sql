-- +goose Up

CREATE TABLE IF NOT EXISTS duckgres_orgs (
    name VARCHAR(255) PRIMARY KEY,
    database_name VARCHAR(255),
    hostname_alias VARCHAR(255),
    max_workers BIGINT DEFAULT 0,
    max_connections BIGINT DEFAULT 0,
    memory_budget VARCHAR(32),
    idle_timeout_s BIGINT DEFAULT 0,
    worker_cpu_request VARCHAR(32),
    worker_memory_request VARCHAR(32),
    default_worker_cpu VARCHAR(32),
    default_worker_memory VARCHAR(32),
    default_worker_ttl VARCHAR(32),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS database_name VARCHAR(255);
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS hostname_alias VARCHAR(255);
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS max_workers BIGINT DEFAULT 0;
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS max_connections BIGINT DEFAULT 0;
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS memory_budget VARCHAR(32);
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS idle_timeout_s BIGINT DEFAULT 0;
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS worker_cpu_request VARCHAR(32);
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS worker_memory_request VARCHAR(32);
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS default_worker_cpu VARCHAR(32);
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS default_worker_memory VARCHAR(32);
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS default_worker_ttl VARCHAR(32);
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS idx_duckgres_orgs_database_name ON duckgres_orgs (database_name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_duckgres_orgs_hostname_alias ON duckgres_orgs (hostname_alias);

CREATE TABLE IF NOT EXISTS duckgres_org_users (
    org_id VARCHAR(255),
    username VARCHAR(255),
    password VARCHAR(255) NOT NULL,
    passthrough BOOLEAN NOT NULL DEFAULT false,
    default_catalog VARCHAR(255),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

ALTER TABLE duckgres_org_users ADD COLUMN IF NOT EXISTS org_id VARCHAR(255);
ALTER TABLE duckgres_org_users ADD COLUMN IF NOT EXISTS username VARCHAR(255);
ALTER TABLE duckgres_org_users ADD COLUMN IF NOT EXISTS password VARCHAR(255);
ALTER TABLE duckgres_org_users ADD COLUMN IF NOT EXISTS passthrough BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE duckgres_org_users ADD COLUMN IF NOT EXISTS default_catalog VARCHAR(255);
ALTER TABLE duckgres_org_users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE duckgres_org_users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

UPDATE duckgres_org_users
SET org_id = ''
WHERE org_id IS NULL;

ALTER TABLE duckgres_org_users ALTER COLUMN org_id DROP DEFAULT;
ALTER TABLE duckgres_org_users ALTER COLUMN password SET NOT NULL;

-- +goose StatementBegin
DO $$
DECLARE
    existing_pk TEXT;
    composite_pk BOOLEAN;
BEGIN
    SELECT c.conname INTO existing_pk
    FROM pg_constraint c
    WHERE c.conrelid = 'duckgres_org_users'::regclass
      AND c.contype = 'p'
    LIMIT 1;

    SELECT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ord) ON true
        JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = cols.attnum
        WHERE c.conrelid = 'duckgres_org_users'::regclass
          AND c.contype = 'p'
        GROUP BY c.conname
        HAVING array_agg(a.attname::text ORDER BY cols.ord) = ARRAY['org_id', 'username']
    ) INTO composite_pk;

    IF existing_pk IS NOT NULL AND NOT composite_pk THEN
        EXECUTE format('ALTER TABLE duckgres_org_users DROP CONSTRAINT %I', existing_pk);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'duckgres_org_users'::regclass
          AND contype = 'p'
    ) THEN
        ALTER TABLE duckgres_org_users ADD PRIMARY KEY (org_id, username);
    END IF;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS src_cols(attnum, ord) ON true
        JOIN LATERAL unnest(c.confkey) WITH ORDINALITY AS dst_cols(attnum, ord) ON dst_cols.ord = src_cols.ord
        JOIN pg_attribute src_attr ON src_attr.attrelid = c.conrelid AND src_attr.attnum = src_cols.attnum
        JOIN pg_attribute dst_attr ON dst_attr.attrelid = c.confrelid AND dst_attr.attnum = dst_cols.attnum
        WHERE c.conrelid = 'duckgres_org_users'::regclass
          AND c.confrelid = 'duckgres_orgs'::regclass
          AND c.contype = 'f'
          AND c.confdeltype = 'a'
        GROUP BY c.oid
        HAVING array_agg(src_attr.attname::text ORDER BY src_cols.ord) = ARRAY['org_id']
           AND array_agg(dst_attr.attname::text ORDER BY src_cols.ord) = ARRAY['name']
    ) THEN
        ALTER TABLE duckgres_org_users
            ADD CONSTRAINT fk_duckgres_orgs_users
            FOREIGN KEY (org_id) REFERENCES duckgres_orgs(name);
    END IF;
END $$;
-- +goose StatementEnd

CREATE TABLE IF NOT EXISTS duckgres_org_user_secrets (
    org_id VARCHAR(255),
    username VARCHAR(255),
    secret_name VARCHAR(255),
    ciphertext BYTEA NOT NULL,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (org_id, username, secret_name)
);

ALTER TABLE duckgres_org_user_secrets ADD COLUMN IF NOT EXISTS org_id VARCHAR(255);
ALTER TABLE duckgres_org_user_secrets ADD COLUMN IF NOT EXISTS username VARCHAR(255);
ALTER TABLE duckgres_org_user_secrets ADD COLUMN IF NOT EXISTS secret_name VARCHAR(255);
ALTER TABLE duckgres_org_user_secrets ADD COLUMN IF NOT EXISTS ciphertext BYTEA;
ALTER TABLE duckgres_org_user_secrets ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE duckgres_org_user_secrets ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

ALTER TABLE duckgres_org_user_secrets ALTER COLUMN ciphertext SET NOT NULL;

-- +goose StatementBegin
DO $$
DECLARE
    existing_pk TEXT;
    composite_pk BOOLEAN;
BEGIN
    SELECT c.conname INTO existing_pk
    FROM pg_constraint c
    WHERE c.conrelid = 'duckgres_org_user_secrets'::regclass
      AND c.contype = 'p'
    LIMIT 1;

    SELECT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ord) ON true
        JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = cols.attnum
        WHERE c.conrelid = 'duckgres_org_user_secrets'::regclass
          AND c.contype = 'p'
        GROUP BY c.conname
        HAVING array_agg(a.attname::text ORDER BY cols.ord) = ARRAY['org_id', 'username', 'secret_name']
    ) INTO composite_pk;

    IF existing_pk IS NOT NULL AND NOT composite_pk THEN
        EXECUTE format('ALTER TABLE duckgres_org_user_secrets DROP CONSTRAINT %I', existing_pk);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'duckgres_org_user_secrets'::regclass
          AND contype = 'p'
    ) THEN
        ALTER TABLE duckgres_org_user_secrets ADD PRIMARY KEY (org_id, username, secret_name);
    END IF;
END $$;
-- +goose StatementEnd

CREATE TABLE IF NOT EXISTS duckgres_managed_warehouses (
    org_id VARCHAR(255) PRIMARY KEY,
    image VARCHAR(512),
    duck_lake_version VARCHAR(32),
    warehouse_database_region VARCHAR(64),
    warehouse_database_endpoint VARCHAR(512),
    warehouse_database_port BIGINT,
    warehouse_database_database_name VARCHAR(255),
    warehouse_database_username VARCHAR(255),
    metadata_store_kind VARCHAR(64),
    metadata_store_engine VARCHAR(64),
    metadata_store_region VARCHAR(64),
    metadata_store_endpoint VARCHAR(512),
    metadata_store_port BIGINT,
    metadata_store_database_name VARCHAR(255),
    metadata_store_username VARCHAR(255),
    metadata_store_password_aws_secret VARCHAR(255),
    data_store_kind VARCHAR(32),
    data_store_bucket_name VARCHAR(255),
    data_store_region VARCHAR(64),
    pgbouncer_enabled BOOLEAN DEFAULT false,
    s3_provider VARCHAR(64),
    s3_region VARCHAR(64),
    s3_bucket VARCHAR(255),
    s3_path_prefix VARCHAR(1024),
    s3_endpoint VARCHAR(512),
    s3_use_ssl BOOLEAN DEFAULT false,
    s3_url_style VARCHAR(16),
    s3_delta_catalog_enabled BOOLEAN DEFAULT true,
    s3_delta_catalog_path VARCHAR(1024),
    ducklake_enabled BOOLEAN DEFAULT false,
    worker_identity_namespace VARCHAR(255),
    worker_identity_service_account_name VARCHAR(255),
    worker_identity_iam_role_arn VARCHAR(512),
    warehouse_database_credentials_namespace VARCHAR(255),
    warehouse_database_credentials_name VARCHAR(255),
    warehouse_database_credentials_key VARCHAR(255),
    metadata_store_credentials_namespace VARCHAR(255),
    metadata_store_credentials_name VARCHAR(255),
    metadata_store_credentials_key VARCHAR(255),
    s3_credentials_namespace VARCHAR(255),
    s3_credentials_name VARCHAR(255),
    s3_credentials_key VARCHAR(255),
    runtime_config_namespace VARCHAR(255),
    runtime_config_name VARCHAR(255),
    runtime_config_key VARCHAR(255),
    state VARCHAR(32),
    status_message VARCHAR(1024),
    warehouse_database_state VARCHAR(32),
    warehouse_database_status_message VARCHAR(1024),
    metadata_store_state VARCHAR(32),
    metadata_store_status_message VARCHAR(1024),
    s3_state VARCHAR(32),
    s3_status_message VARCHAR(1024),
    identity_state VARCHAR(32),
    identity_status_message VARCHAR(1024),
    secrets_state VARCHAR(32),
    secrets_status_message VARCHAR(1024),
    provisioning_started_at TIMESTAMPTZ,
    ready_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS image VARCHAR(512);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS duck_lake_version VARCHAR(32);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS warehouse_database_region VARCHAR(64);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS warehouse_database_endpoint VARCHAR(512);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS warehouse_database_port BIGINT;
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS warehouse_database_database_name VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS warehouse_database_username VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_kind VARCHAR(64);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_engine VARCHAR(64);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_region VARCHAR(64);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_endpoint VARCHAR(512);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_port BIGINT;
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_database_name VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_username VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_password_aws_secret VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS data_store_kind VARCHAR(32);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS data_store_bucket_name VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS data_store_region VARCHAR(64);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS pgbouncer_enabled BOOLEAN DEFAULT false;
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_provider VARCHAR(64);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_region VARCHAR(64);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_bucket VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_path_prefix VARCHAR(1024);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_endpoint VARCHAR(512);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_use_ssl BOOLEAN DEFAULT false;
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_url_style VARCHAR(16);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_delta_catalog_enabled BOOLEAN DEFAULT true;
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_delta_catalog_path VARCHAR(1024);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS ducklake_enabled BOOLEAN DEFAULT false;
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS worker_identity_namespace VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS worker_identity_service_account_name VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS worker_identity_iam_role_arn VARCHAR(512);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS warehouse_database_credentials_namespace VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS warehouse_database_credentials_name VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS warehouse_database_credentials_key VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_credentials_namespace VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_credentials_name VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_credentials_key VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_credentials_namespace VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_credentials_name VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_credentials_key VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS runtime_config_namespace VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS runtime_config_name VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS runtime_config_key VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS state VARCHAR(32);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS status_message VARCHAR(1024);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS warehouse_database_state VARCHAR(32);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS warehouse_database_status_message VARCHAR(1024);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_state VARCHAR(32);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_status_message VARCHAR(1024);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_state VARCHAR(32);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS s3_status_message VARCHAR(1024);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS identity_state VARCHAR(32);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS identity_status_message VARCHAR(1024);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS secrets_state VARCHAR(32);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS secrets_status_message VARCHAR(1024);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS provisioning_started_at TIMESTAMPTZ;
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS ready_at TIMESTAMPTZ;
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ;
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS src_cols(attnum, ord) ON true
        JOIN LATERAL unnest(c.confkey) WITH ORDINALITY AS dst_cols(attnum, ord) ON dst_cols.ord = src_cols.ord
        JOIN pg_attribute src_attr ON src_attr.attrelid = c.conrelid AND src_attr.attnum = src_cols.attnum
        JOIN pg_attribute dst_attr ON dst_attr.attrelid = c.confrelid AND dst_attr.attnum = dst_cols.attnum
        WHERE c.conrelid = 'duckgres_managed_warehouses'::regclass
          AND c.confrelid = 'duckgres_orgs'::regclass
          AND c.contype = 'f'
          AND c.confdeltype = 'c'
        GROUP BY c.oid
        HAVING array_agg(src_attr.attname::text ORDER BY src_cols.ord) = ARRAY['org_id']
           AND array_agg(dst_attr.attname::text ORDER BY src_cols.ord) = ARRAY['name']
    ) THEN
        ALTER TABLE duckgres_managed_warehouses
            ADD CONSTRAINT fk_duckgres_orgs_warehouse
            FOREIGN KEY (org_id) REFERENCES duckgres_orgs(name)
            ON DELETE CASCADE;
    END IF;
END $$;
-- +goose StatementEnd

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

ALTER TABLE duckgres_global_config ADD COLUMN IF NOT EXISTS memory_budget VARCHAR(32);
ALTER TABLE duckgres_global_config ADD COLUMN IF NOT EXISTS memory_rebalance BOOLEAN DEFAULT false;
ALTER TABLE duckgres_global_config ADD COLUMN IF NOT EXISTS max_connections BIGINT DEFAULT 0;
ALTER TABLE duckgres_global_config ADD COLUMN IF NOT EXISTS idle_timeout_s BIGINT DEFAULT 0;
ALTER TABLE duckgres_global_config ADD COLUMN IF NOT EXISTS worker_queue_timeout_s BIGINT DEFAULT 0;
ALTER TABLE duckgres_global_config ADD COLUMN IF NOT EXISTS worker_idle_timeout_s BIGINT DEFAULT 0;
ALTER TABLE duckgres_global_config ADD COLUMN IF NOT EXISTS extensions VARCHAR(1024);
ALTER TABLE duckgres_global_config ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

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

ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS metadata_store VARCHAR(1024);
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS object_store VARCHAR(1024);
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS data_path VARCHAR(1024);
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS s3_provider VARCHAR(64);
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS s3_endpoint VARCHAR(512);
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS s3_access_key VARCHAR(255);
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS s3_secret_key VARCHAR(255);
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS s3_region VARCHAR(64);
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS s3_use_ssl BOOLEAN DEFAULT false;
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS s3_url_style VARCHAR(16);
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS s3_chain VARCHAR(255);
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS s3_profile VARCHAR(255);
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS delta_catalog_enabled BOOLEAN DEFAULT true;
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS delta_catalog_path VARCHAR(1024);
ALTER TABLE duckgres_ducklake_config ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS duckgres_rate_limit_config (
    id BIGINT PRIMARY KEY,
    max_failed_attempts BIGINT DEFAULT 0,
    failed_attempt_window_s BIGINT DEFAULT 0,
    ban_duration_s BIGINT DEFAULT 0,
    max_connections_per_ip BIGINT DEFAULT 0,
    updated_at TIMESTAMPTZ
);

ALTER TABLE duckgres_rate_limit_config ADD COLUMN IF NOT EXISTS max_failed_attempts BIGINT DEFAULT 0;
ALTER TABLE duckgres_rate_limit_config ADD COLUMN IF NOT EXISTS failed_attempt_window_s BIGINT DEFAULT 0;
ALTER TABLE duckgres_rate_limit_config ADD COLUMN IF NOT EXISTS ban_duration_s BIGINT DEFAULT 0;
ALTER TABLE duckgres_rate_limit_config ADD COLUMN IF NOT EXISTS max_connections_per_ip BIGINT DEFAULT 0;
ALTER TABLE duckgres_rate_limit_config ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS duckgres_query_log_config (
    id BIGINT PRIMARY KEY,
    enabled BOOLEAN DEFAULT false,
    flush_interval_s BIGINT DEFAULT 0,
    batch_size BIGINT DEFAULT 0,
    compact_interval_s BIGINT DEFAULT 0,
    data_inlining_row_limit BIGINT DEFAULT 0,
    updated_at TIMESTAMPTZ
);

ALTER TABLE duckgres_query_log_config ADD COLUMN IF NOT EXISTS enabled BOOLEAN DEFAULT false;
ALTER TABLE duckgres_query_log_config ADD COLUMN IF NOT EXISTS flush_interval_s BIGINT DEFAULT 0;
ALTER TABLE duckgres_query_log_config ADD COLUMN IF NOT EXISTS batch_size BIGINT DEFAULT 0;
ALTER TABLE duckgres_query_log_config ADD COLUMN IF NOT EXISTS compact_interval_s BIGINT DEFAULT 0;
ALTER TABLE duckgres_query_log_config ADD COLUMN IF NOT EXISTS data_inlining_row_limit BIGINT DEFAULT 0;
ALTER TABLE duckgres_query_log_config ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
