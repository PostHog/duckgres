-- +goose Up

-- earliest_event_date is PostHog's cached "earliest event date" for the team:
-- the historical-backfill floor its Dagster sensor computes from ClickHouse.
-- NULL means "not yet resolved" (the sensor computes and sets it via the
-- provisioning team upsert); the sentinel 9999-12-31 (PostHog's
-- NO_HISTORY_SENTINEL) means "team has no event history", stored so the
-- sensor never re-queries ClickHouse for it. The value
-- is a cache OWNED by PostHog — duckgres stores and serves it but never
-- computes or interprets it.
ALTER TABLE duckgres_org_teams ADD COLUMN IF NOT EXISTS earliest_event_date DATE;

-- +goose Down

ALTER TABLE duckgres_org_teams DROP COLUMN IF EXISTS earliest_event_date;
