-- +goose Up
-- Per-tenant publication scheduling: which ATTEMPT is in flight, and when this
-- tenant may next be worked on.
--
-- attempt is a monotone occurrence counter. Every barrier and every revocation
-- carries it in its durable step identity, so a reopened barrier after an
-- abandon - or a second revocation after a tenant was re-enabled - is a NEW
-- operation rather than a replay of the first one, which would return the old
-- outcome and leave the current intent unapplied.
--
-- attempts/next_attempt_at are the same durable backoff the pool's operations
-- carry, per tenant: without them one permanently failing warehouse is retried
-- on every five-second tick and, because the driver takes one tenant at a time,
-- starves every other tenant behind it.
ALTER TABLE duckgres_trino_pool_publications
    ADD COLUMN IF NOT EXISTS attempt         BIGINT      NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    ADD COLUMN IF NOT EXISTS attempts        BIGINT      NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NULL;

-- +goose Down
ALTER TABLE duckgres_trino_pool_publications
    DROP COLUMN IF EXISTS next_attempt_at,
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS attempt;
