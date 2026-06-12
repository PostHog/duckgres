-- +goose Up

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM duckgres_schema_migrations
        WHERE name = '2026_05_delta_catalog_default_enabled'
    ) THEN
        UPDATE duckgres_ducklake_config
        SET delta_catalog_enabled = true
        WHERE delta_catalog_enabled IS DISTINCT FROM true;

        UPDATE duckgres_managed_warehouses
        SET s3_delta_catalog_enabled = true
        WHERE s3_delta_catalog_enabled IS DISTINCT FROM true;
    END IF;
END $$;
-- +goose StatementEnd

INSERT INTO duckgres_schema_migrations (name, checksum, applied_at)
VALUES ('2026_05_delta_catalog_default_enabled', '', now())
ON CONFLICT (name) DO NOTHING;
