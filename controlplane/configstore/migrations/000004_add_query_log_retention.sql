-- +goose Up

ALTER TABLE duckgres_query_log_config ADD COLUMN IF NOT EXISTS retention_period_s BIGINT DEFAULT 0;
ALTER TABLE duckgres_query_log_config ADD COLUMN IF NOT EXISTS retention_interval_s BIGINT DEFAULT 0;
