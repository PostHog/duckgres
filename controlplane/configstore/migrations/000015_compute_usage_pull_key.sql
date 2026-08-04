-- +goose Up

-- Pull-based compute billing (docs/design/billing-pull-api.md): billing now
-- PULLS usage buckets from duckgres over HTTP instead of duckgres pushing
-- capture events to ingestion. The buffer key gains the billing dimensions —
-- team_id (the org's default team), query_source (standard|endpoints, from the
-- duckgres.query_source session GUC) and the provisioned worker size (cpu vCPU
-- / mem_gib GiB as exact NUMERIC decimals, so fractional sizes group exactly) —
-- and the per-org push-drain high-water mark is replaced by a single global
-- ack cursor.
ALTER TABLE duckgres_org_compute_usage
    ADD COLUMN IF NOT EXISTS team_id      TEXT    NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS query_source TEXT    NOT NULL DEFAULT 'standard',
    ADD COLUMN IF NOT EXISTS cpu          NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS mem_gib      NUMERIC NOT NULL DEFAULT 0;

-- Backfill team_id for buckets accumulated before this deploy from the org's
-- default team (all orgs are backfilled with default_team_id). Their cpu /
-- mem_gib stay 0 ("size unknown" legacy rows) — the values were already summed
-- without the size dimension.
UPDATE duckgres_org_compute_usage u
SET team_id = COALESCE(o.default_team_id, '')
FROM duckgres_orgs o
WHERE o.name = u.org_id AND u.team_id = '';

ALTER TABLE duckgres_org_compute_usage
    DROP CONSTRAINT duckgres_org_compute_usage_pkey;
ALTER TABLE duckgres_org_compute_usage
    ADD PRIMARY KEY (org_id, team_id, query_source, cpu, mem_gib, bucket_start);

-- Single global ack cursor (one billing consumer): everything at or below
-- last_acked has been pulled AND acked by billing, and is deleted. Replaces
-- the per-org push-drain high-water mark.
CREATE TABLE IF NOT EXISTS duckgres_compute_billing_cursor (
    id         SMALLINT    PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_acked TIMESTAMPTZ NOT NULL
);

DROP TABLE IF EXISTS duckgres_org_compute_drain_state;

-- +goose Down

-- The buffer is transient (unshipped counts only); collapsing the widened key
-- back into (org_id, bucket_start) would have to merge rows, so the down
-- migration clears it instead — acceptable for a buffer, matching the
-- best-effort metering contract.
DELETE FROM duckgres_org_compute_usage;
ALTER TABLE duckgres_org_compute_usage
    DROP CONSTRAINT duckgres_org_compute_usage_pkey;
ALTER TABLE duckgres_org_compute_usage
    DROP COLUMN team_id,
    DROP COLUMN query_source,
    DROP COLUMN cpu,
    DROP COLUMN mem_gib;
ALTER TABLE duckgres_org_compute_usage
    ADD PRIMARY KEY (org_id, bucket_start);

DROP TABLE IF EXISTS duckgres_compute_billing_cursor;

CREATE TABLE IF NOT EXISTS duckgres_org_compute_drain_state (
    org_id              TEXT        PRIMARY KEY,
    last_drained_bucket TIMESTAMPTZ NOT NULL
);
