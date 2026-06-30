-- +goose Up

-- Durable buffer for managed-warehouse compute-usage billing (remote/k8s
-- backend). The in-process per-org counter flushes here every ~15s via
-- UPSERT-increment (summing contributions across all CP pods); the leader
-- drain loop ships each closed bucket to PostHog ingestion and deletes the
-- row (ship-then-delete, at-least-once). Aggregated rows only (org ×
-- time-bucket) — never per-query rows. See
-- docs/design/billing-compute-seconds-plan.md.
CREATE TABLE IF NOT EXISTS duckgres_org_compute_usage (
    org_id          TEXT        NOT NULL,
    bucket_start    TIMESTAMPTZ NOT NULL,
    cpu_seconds     BIGINT      NOT NULL,
    memory_seconds  BIGINT      NOT NULL,
    PRIMARY KEY (org_id, bucket_start)
);

-- Per-org high-water mark of the last bucket successfully shipped to billing.
-- The drain loop only ships buckets strictly newer than this and advances it
-- in the same transaction as the row delete, so a late-arriving re-INSERT of
-- an already-drained bucket is skipped (and swept).
CREATE TABLE IF NOT EXISTS duckgres_org_compute_drain_state (
    org_id              TEXT        PRIMARY KEY,
    last_drained_bucket TIMESTAMPTZ NOT NULL
);

-- +goose Down

DROP TABLE IF EXISTS duckgres_org_compute_drain_state;
DROP TABLE IF EXISTS duckgres_org_compute_usage;
