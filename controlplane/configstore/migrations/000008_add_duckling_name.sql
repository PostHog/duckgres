-- +goose Up

-- Store the k8s Duckling CR name explicitly instead of deriving it from the
-- org ID at lookup time. Canonical name = lowercased org ID (mirrors
-- provisioner.ducklingName). Backfill existing rows from lower(org_id) so the
-- column is authoritative for both old and new warehouses.
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS duckling_name VARCHAR(255);

UPDATE duckgres_managed_warehouses
SET duckling_name = lower(org_id)
WHERE duckling_name IS NULL OR duckling_name = '';

-- +goose Down

ALTER TABLE duckgres_managed_warehouses DROP COLUMN IF EXISTS duckling_name;
