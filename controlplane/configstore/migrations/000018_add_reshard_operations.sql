-- +goose Up

-- Reshard operations: operator-driven migration of an org's DuckLake metadata
-- catalog between metadata stores (cnpg shard -> cnpg shard, external -> cnpg,
-- cnpg -> external). One row per operation; a partial unique index forbids two
-- active operations for the same org. The verbose per-step trail lives in
-- duckgres_reshard_operation_log so an operator can follow exactly what
-- happened and when (the admin console polls it incrementally by id).
CREATE TABLE IF NOT EXISTS duckgres_reshard_operations (
    id                     BIGSERIAL   PRIMARY KEY,
    org_id                 TEXT        NOT NULL,
    duckling_name          TEXT        NOT NULL,
    -- "cnpg-shard" or "external" (the SOURCE metadata store kind).
    source_kind            TEXT        NOT NULL,
    -- Source description: shard name for cnpg sources, endpoint for external.
    from_shard             TEXT        NOT NULL DEFAULT '',
    source_endpoint        TEXT        NOT NULL DEFAULT '',
    -- Target: shard name for cnpg targets; endpoint + secret NAME for external
    -- targets (the external target password is ephemeral and never persisted).
    target_kind            TEXT        NOT NULL,
    to_shard               TEXT        NOT NULL DEFAULT '',
    target_endpoint        TEXT        NOT NULL DEFAULT '',
    target_password_secret TEXT        NOT NULL DEFAULT '',
    target_user            TEXT        NOT NULL DEFAULT '',
    target_database        TEXT        NOT NULL DEFAULT '',
    source_user            TEXT        NOT NULL DEFAULT '',
    source_password_secret TEXT        NOT NULL DEFAULT '',
    source_database        TEXT        NOT NULL DEFAULT '',
    -- Pre-reshard compaction spec state, persisted so a takeover runner can
    -- restore it exactly (key-absent vs explicit false differ: the chart
    -- name-list can enable compaction when the key is absent).
    compaction_was_present BOOLEAN     NOT NULL DEFAULT false,
    compaction_was_enabled BOOLEAN     NOT NULL DEFAULT false,
    -- pending | running | succeeded | failed | cancelled
    state                  TEXT        NOT NULL DEFAULT 'pending',
    step                   TEXT        NOT NULL DEFAULT '',
    error                  TEXT        NOT NULL DEFAULT '',
    cancel_requested       BOOLEAN     NOT NULL DEFAULT false,
    drain_timeout_seconds  BIGINT      NOT NULL DEFAULT 1800,
    -- Runner fencing: the claiming CP + a monotonically bumped epoch; every
    -- runner-side write is CAS-fenced on (runner_cp, runner_epoch) so a
    -- zombie ex-runner's writes fail after a takeover.
    runner_cp              TEXT        NOT NULL DEFAULT '',
    runner_epoch           BIGINT      NOT NULL DEFAULT 0,
    heartbeat_at           TIMESTAMPTZ,
    -- Maintenance-mode window (warehouse state resharding): block -> unblock.
    blocked_at             TIMESTAMPTZ,
    unblocked_at           TIMESTAMPTZ,
    -- Copy report counters.
    tables_copied          BIGINT      NOT NULL DEFAULT 0,
    rows_copied            BIGINT      NOT NULL DEFAULT 0,
    bytes_copied           BIGINT      NOT NULL DEFAULT 0,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at             TIMESTAMPTZ,
    finished_at            TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_duckgres_reshard_operations_active_org
    ON duckgres_reshard_operations (org_id)
    WHERE state IN ('pending', 'running');

CREATE INDEX IF NOT EXISTS idx_duckgres_reshard_operations_org
    ON duckgres_reshard_operations (org_id, id DESC);

CREATE TABLE IF NOT EXISTS duckgres_reshard_operation_log (
    id           BIGSERIAL   PRIMARY KEY,
    operation_id BIGINT      NOT NULL,
    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- info | warn | error
    level        TEXT        NOT NULL DEFAULT 'info',
    message      TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_duckgres_reshard_operation_log_op
    ON duckgres_reshard_operation_log (operation_id, id);

-- +goose Down

DROP TABLE IF EXISTS duckgres_reshard_operation_log;
DROP TABLE IF EXISTS duckgres_reshard_operations;
