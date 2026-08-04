-- +goose Up
-- org max_connections was the old K8s multi-tenant connection-count admission
-- knob. Admission now uses runtime-store vCPU leases via max_vcpus; standalone
-- rate_limit.max_connections remains a separate config path outside this table.
ALTER TABLE duckgres_orgs
    DROP COLUMN IF EXISTS max_connections;

-- +goose Down
ALTER TABLE duckgres_orgs
    ADD COLUMN IF NOT EXISTS max_connections BIGINT DEFAULT 0;
