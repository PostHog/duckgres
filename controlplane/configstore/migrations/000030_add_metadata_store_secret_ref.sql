-- +goose Up

-- Discovery-only mirror of the Duckling CR's metadata credential Secret
-- reference (status.metadataStore.credentialSecretRef), written by the
-- provisioner's ready-reconcile (reconcileMetadataStoreRow) and served on
-- GET /api/v1/warehouses as password_secret_ref.
--
-- DELIBERATELY SEPARATE from metadata_store_credentials_*: that column set
-- is a worker-activation input validated tenant-owned by
-- ValidateManagedWarehouseSecretRefs (ref namespace must equal
-- worker_identity.namespace, name must be org-prefixed) — the
-- composition-owned refs the mirror carries (ducklings namespace,
-- cnpg-tenant-<org>-password) fail that validation by design, and writing
-- them there would break every cold worker activation.
-- Plain nullable, matching the sibling SecretRef column sets (and the gorm
-- model, pinned by TestConfigStoreSQLMigrationsMatchGORMModelMetadata).
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_secret_ref_namespace VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_secret_ref_name VARCHAR(255);
ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS metadata_store_secret_ref_key VARCHAR(255);

-- +goose Down
ALTER TABLE duckgres_managed_warehouses DROP COLUMN IF EXISTS metadata_store_secret_ref_namespace;
ALTER TABLE duckgres_managed_warehouses DROP COLUMN IF EXISTS metadata_store_secret_ref_name;
ALTER TABLE duckgres_managed_warehouses DROP COLUMN IF EXISTS metadata_store_secret_ref_key;
