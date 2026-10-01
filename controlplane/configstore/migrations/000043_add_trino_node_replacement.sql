-- +goose Up
ALTER TABLE duckgres_trino_pool_instances
    ADD COLUMN node_replacement_evidence JSONB;

-- +goose Down
ALTER TABLE duckgres_trino_pool_instances
    DROP COLUMN node_replacement_evidence;
