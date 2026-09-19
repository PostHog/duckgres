-- +goose Up
-- Three corrections to the shared-pool instance model, all additive and all
-- inert while the feature is disabled.

-- 1. The worker ConfigMap was created but never recorded, so retiring an
--    instance left `<instance>-worker-config` behind forever. Deletion is UID-
--    preconditioned, so the name alone was not enough to clean it up either.
ALTER TABLE duckgres_trino_pool_instances
    ADD COLUMN IF NOT EXISTS worker_config_map_name TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS worker_config_map_uid  TEXT NOT NULL DEFAULT '';

-- 2. A repair instance has to name the instance it replaces: the Gateway
--    charges the activation to the repair budget only when `repairFor` names a
--    failed member. Without it every repair spent the single planned surge, so
--    a failure during a release rollout could not be repaired at all.
ALTER TABLE duckgres_trino_pool_instances
    ADD COLUMN IF NOT EXISTS repair_for TEXT NOT NULL DEFAULT '';

-- 3. Why a candidate failed, kept on the row. A terminal FAILED_PREPARING with
--    no reason tells an operator nothing about whether to retry or investigate.
ALTER TABLE duckgres_trino_pool_instances
    ADD COLUMN IF NOT EXISTS failure_reason TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE duckgres_trino_pool_instances
    DROP COLUMN IF EXISTS failure_reason,
    DROP COLUMN IF EXISTS repair_for,
    DROP COLUMN IF EXISTS worker_config_map_uid,
    DROP COLUMN IF EXISTS worker_config_map_name;
