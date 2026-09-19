-- +goose Up
-- A monotonic generation for the desired specification.
--
-- The authority epoch alone does not order desired state. Two control planes
-- can hold the same epoch at different times, and a replica that was carrying
-- an older configuration file could win the authority and then publish that
-- older spec over a newer one - a legal fenced write of stale content. The
-- generation makes the CONTENT ordered independently of who wrote it: a
-- publication that does not advance it is refused.
ALTER TABLE duckgres_trino_pools
    ADD COLUMN IF NOT EXISTS desired_generation BIGINT NOT NULL DEFAULT 0
        CHECK (desired_generation >= 0);

-- +goose Down
ALTER TABLE duckgres_trino_pools DROP COLUMN IF EXISTS desired_generation;
