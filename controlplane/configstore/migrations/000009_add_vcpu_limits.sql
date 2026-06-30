-- +goose Up

ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS max_vcpus BIGINT DEFAULT 0;
ALTER TABLE duckgres_org_users ADD COLUMN IF NOT EXISTS max_vcpus BIGINT DEFAULT 0;

