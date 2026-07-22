-- +goose Up

-- backfill_enabled mirrors a Django BooleanField(default=True) on the PostHog
-- side, so the column becomes NOT NULL DEFAULT TRUE: an unset value can no
-- longer be misread as "off" by a reader flipping on NULL. Existing NULL rows
-- (the tri-state's old "unset") become TRUE — the default they always meant.
UPDATE duckgres_org_teams SET backfill_enabled = TRUE WHERE backfill_enabled IS NULL;
ALTER TABLE duckgres_org_teams ALTER COLUMN backfill_enabled SET DEFAULT TRUE;
ALTER TABLE duckgres_org_teams ALTER COLUMN backfill_enabled SET NOT NULL;

-- +goose Down

-- Values are kept; only the NOT NULL stance and the default are dropped.
ALTER TABLE duckgres_org_teams ALTER COLUMN backfill_enabled DROP NOT NULL;
ALTER TABLE duckgres_org_teams ALTER COLUMN backfill_enabled DROP DEFAULT;
