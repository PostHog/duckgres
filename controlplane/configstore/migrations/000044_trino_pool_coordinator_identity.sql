-- +goose Up
-- The coordinator identity the GATEWAY observed at member registration.
--
-- A loss claim must carry podUid, bootId, nodeId AND coordinatorId exactly as
-- the Gateway recorded them, or the evidence is refused. Only the node id was
-- kept, and it was sent for both fields, so a failed member could never be
-- released and kept occupying the pool's live budget.
ALTER TABLE duckgres_trino_pool_instances
    ADD COLUMN IF NOT EXISTS coordinator_id TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE duckgres_trino_pool_instances DROP COLUMN IF EXISTS coordinator_id;
