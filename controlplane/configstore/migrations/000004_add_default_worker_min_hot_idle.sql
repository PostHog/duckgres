ALTER TABLE duckgres_orgs
    ADD COLUMN IF NOT EXISTS default_worker_min_hot_idle BIGINT DEFAULT 0;

UPDATE duckgres_orgs
SET default_worker_min_hot_idle = 0
WHERE default_worker_min_hot_idle IS NULL;
