-- +goose Up

-- Durable acknowledgements for the CNPG source-login fence. The reshard step
-- is recorded at step entry, so takeover must use these success-only markers
-- to distinguish "requested" from "observed in PostgreSQL".
ALTER TABLE duckgres_reshard_operations
    ADD COLUMN IF NOT EXISTS maintenance_prepared_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS source_fence_requested_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS source_fenced_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS target_rendered_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS target_login_ready_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS external_verified_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS source_drop_committed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS source_dropped_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS maintenance_disabled_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS maintenance_cleaned_at TIMESTAMPTZ;

-- +goose Down
ALTER TABLE duckgres_reshard_operations
    DROP COLUMN IF EXISTS maintenance_cleaned_at,
    DROP COLUMN IF EXISTS maintenance_disabled_at,
    DROP COLUMN IF EXISTS source_dropped_at,
    DROP COLUMN IF EXISTS source_drop_committed_at,
    DROP COLUMN IF EXISTS external_verified_at,
    DROP COLUMN IF EXISTS target_login_ready_at,
    DROP COLUMN IF EXISTS target_rendered_at,
    DROP COLUMN IF EXISTS source_fenced_at,
    DROP COLUMN IF EXISTS source_fence_requested_at,
    DROP COLUMN IF EXISTS maintenance_prepared_at;
