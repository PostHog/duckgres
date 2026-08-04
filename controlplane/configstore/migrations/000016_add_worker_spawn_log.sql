-- +goose Up

-- Append-only log of real worker pod spawns (remote/k8s backend), written
-- best-effort by every CP replica at pod create. The leader-only headroom
-- controller reads it to size the placeholder pool dynamically:
--   * slot COUNT follows the peak spawn burst in a recent window
--   * slot SIZE follows the largest worker shape actually spawned recently
-- Rows are pruned by the headroom reconcile beyond the sizing window; the
-- table never grows past (spawn rate x window).
CREATE TABLE IF NOT EXISTS duckgres_worker_spawn_log (
    id         BIGSERIAL   PRIMARY KEY,
    org_id     TEXT        NOT NULL DEFAULT '',
    cpu_millis BIGINT      NOT NULL,
    mem_bytes  BIGINT      NOT NULL,
    spawned_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_duckgres_worker_spawn_log_spawned_at
    ON duckgres_worker_spawn_log (spawned_at);

-- +goose Down

DROP TABLE IF EXISTS duckgres_worker_spawn_log;
