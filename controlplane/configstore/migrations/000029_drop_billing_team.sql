-- +goose Up

-- duckgres no longer owns team-level billing attribution: the external
-- billing service maps org → team(s) itself. The is_billing_team marker (and
-- its at-most-one-per-org partial unique index, migration 000024) goes away.
-- Usage buckets keep their team_id key column, but the stamped value is now
-- informational only — the connecting user's team (compute) or the org's
-- oldest team — and team changes never re-attribute existing buckets.
DROP INDEX IF EXISTS idx_duckgres_org_teams_billing_org;
ALTER TABLE duckgres_org_teams DROP COLUMN IF EXISTS is_billing_team;

-- +goose Down

-- Best-effort reversal: the column returns, but billing marks are not
-- recoverable (the forward migration deliberately discarded the ownership
-- concept). Rows come back with NULL = "not the billing team".
ALTER TABLE duckgres_org_teams ADD COLUMN IF NOT EXISTS is_billing_team BOOLEAN;
CREATE UNIQUE INDEX IF NOT EXISTS idx_duckgres_org_teams_billing_org ON duckgres_org_teams (org_id) WHERE is_billing_team IS TRUE;
