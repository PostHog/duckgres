-- +goose Up

-- Pre-flip catalog backup (safety net "layer C"): before a reshard mutates a
-- tenant's DuckLake metadata catalog, the runner pg_dumps the SOURCE catalog to
-- the org's own S3 data bucket under the reserved _reshard_catalog_backups/
-- prefix. This column records the resulting artifact's s3:// URI so an operator
-- (and the op log) can point at the exact restore source. Empty when no backup
-- was taken yet (or a best-effort backup was skipped on a non-destructive
-- direction). See docs/design/resharding.md.
ALTER TABLE duckgres_reshard_operations
    ADD COLUMN IF NOT EXISTS backup_s3_uri TEXT NOT NULL DEFAULT '';

-- +goose Down

ALTER TABLE duckgres_reshard_operations
    DROP COLUMN IF EXISTS backup_s3_uri;
