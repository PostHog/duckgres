-- +goose Up
-- The publication barrier's own revisions, as the strings the Gateway records.
--
-- The existing BIGINT columns count the catalog store's revision. The barrier
-- is a different fact: which BINDING (a tenant's principals) and which
-- CONFIGURATION (catalog plus the authorization and authentication projections)
-- every serving member acknowledged before the tenant was admitted. Those are
-- controller-defined revision strings, and the Gateway compares them verbatim.
--
-- They are durable because an in-memory record of "already published" cannot
-- survive a restart or a leadership move: the next leader would either
-- republish blindly or, worse, assume an admission that never committed.
ALTER TABLE duckgres_trino_pool_publications
    ADD COLUMN IF NOT EXISTS principal_revision       TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS target_revision          TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS admitted_target_revision TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE duckgres_trino_pool_publications
    DROP COLUMN IF EXISTS admitted_target_revision,
    DROP COLUMN IF EXISTS target_revision,
    DROP COLUMN IF EXISTS principal_revision;
