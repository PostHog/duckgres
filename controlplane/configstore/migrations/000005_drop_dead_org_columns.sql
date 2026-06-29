-- +goose Up
-- Drop dead/footgun per-org columns from duckgres_orgs.
--   memory_budget, idle_timeout_s — dead: only copied into the snapshot
--     OrgConfig + shown in the admin OrgStatus; no code reads them to drive
--     behavior.
--   worker_cpu_request, worker_memory_request — footgun: org_router applied the
--     per-org value to the SHARED worker pool (last-org-wins pod sizing across
--     all orgs). Per-org sizing remains via default_worker_cpu/memory + client
--     GUCs; the deployment-wide K8sConfig.WorkerCPURequest/MemoryRequest is a
--     different (kept) field.
ALTER TABLE duckgres_orgs
    DROP COLUMN IF EXISTS memory_budget,
    DROP COLUMN IF EXISTS idle_timeout_s,
    DROP COLUMN IF EXISTS worker_cpu_request,
    DROP COLUMN IF EXISTS worker_memory_request;

-- +goose Down
ALTER TABLE duckgres_orgs
    ADD COLUMN IF NOT EXISTS memory_budget VARCHAR(32),
    ADD COLUMN IF NOT EXISTS idle_timeout_s BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS worker_cpu_request VARCHAR(32),
    ADD COLUMN IF NOT EXISTS worker_memory_request VARCHAR(32);
