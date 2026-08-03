-- +goose Up
ALTER TABLE duckgres_orgs
    ADD COLUMN IF NOT EXISTS data_imports_table_naming_version VARCHAR(32) NOT NULL DEFAULT 'legacy_batch_v1';

ALTER TABLE duckgres_orgs
    ALTER COLUMN data_imports_table_naming_version SET DEFAULT 'copy_v1';

ALTER TABLE duckgres_orgs
    ADD CONSTRAINT duckgres_orgs_data_imports_table_naming_version_check
    CHECK (data_imports_table_naming_version IN ('legacy_batch_v1', 'copy_v1'));

-- +goose Down
ALTER TABLE duckgres_orgs
    DROP CONSTRAINT IF EXISTS duckgres_orgs_data_imports_table_naming_version_check;

ALTER TABLE duckgres_orgs
    DROP COLUMN IF EXISTS data_imports_table_naming_version;
