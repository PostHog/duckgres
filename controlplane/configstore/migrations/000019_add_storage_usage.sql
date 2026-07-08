-- +goose Up

-- Storage-usage buffer for managed-warehouse billing (pull model, storage
-- metric — see docs/design/billing-pull-api.md "Storage metric"). A
-- leader-only sampler reads each org's DuckLake metadata Postgres every ~30min
-- and credits tracked_bytes × interval_seconds byte-seconds into the
-- sample-minute's bucket. NUMERIC because byte-seconds overflow BIGINT for
-- large warehouses (100 TB × a day ≈ 8.6e18). Served aggregated per key per
-- UTC day by GET /billing/usage (as exact-decimal GiB-seconds), deleted by the
-- same ack watermark as compute usage, swept by the same 30-day safety GC.
CREATE TABLE IF NOT EXISTS duckgres_org_storage_usage (
    org_id       TEXT        NOT NULL,
    team_id      BIGINT      NOT NULL DEFAULT 0,
    bucket_start TIMESTAMPTZ NOT NULL,
    byte_seconds NUMERIC     NOT NULL,
    PRIMARY KEY (org_id, team_id, bucket_start)
);

-- +goose Down

DROP TABLE IF EXISTS duckgres_org_storage_usage;
