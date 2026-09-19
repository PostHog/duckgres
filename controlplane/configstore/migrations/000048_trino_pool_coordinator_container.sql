-- +goose Up
-- The container instance that hosted the admitted coordinator process.
--
-- A loss claim may only rest on evidence about the EXACT process that was
-- admitted. A pod's container status carries a termination record, but that
-- record names one container instance: an unrelated restart from before
-- admission looks identical to the one that ended the admitted process, and a
-- second coordinator pod makes an endpoint probe ambiguous about which process
-- answered. Recording the container instance at registration is what lets the
-- termination record be correlated with the process the Gateway admitted.
ALTER TABLE duckgres_trino_pool_instances
    ADD COLUMN IF NOT EXISTS coordinator_container_id TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE duckgres_trino_pool_instances DROP COLUMN IF EXISTS coordinator_container_id;
