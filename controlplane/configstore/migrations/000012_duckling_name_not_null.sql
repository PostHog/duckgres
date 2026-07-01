-- +goose Up

-- Make the Duckling CR name mandatory. Re-run the lower(org_id) backfill first
-- as a safety net (mirrors migration 000008) so any row left NULL/empty is
-- filled before the NOT NULL constraint is applied, then enforce NOT NULL.
UPDATE duckgres_managed_warehouses
SET duckling_name = lower(org_id)
WHERE duckling_name IS NULL OR duckling_name = '';

ALTER TABLE duckgres_managed_warehouses ALTER COLUMN duckling_name SET NOT NULL;

-- +goose Down

ALTER TABLE duckgres_managed_warehouses ALTER COLUMN duckling_name DROP NOT NULL;
