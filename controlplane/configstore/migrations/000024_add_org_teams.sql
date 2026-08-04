-- +goose Up

-- duckgres_org_teams replaces the single per-org default_team_id column: an
-- org can now carry MANY PostHog teams, each mapped to a warehouse schema.
-- Exactly one row per org may be the billing team (is_billing_team = TRUE):
-- the team pull-based billing keys the org's usage buckets by, taking over
-- the role default_team_id played. enabled is the per-team serving switch;
-- backfill_enabled is tri-state (NULL = unset).
CREATE TABLE IF NOT EXISTS duckgres_org_teams (
    org_id VARCHAR(255) NOT NULL REFERENCES duckgres_orgs (name) ON DELETE CASCADE,
    team_id BIGINT NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    is_billing_team BOOLEAN,
    backfill_enabled BOOLEAN,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (org_id, team_id)
);

-- At most ONE billing team per org (partial: non-billing rows are unlimited).
CREATE UNIQUE INDEX IF NOT EXISTS idx_duckgres_org_teams_billing_org ON duckgres_org_teams (org_id) WHERE is_billing_team IS TRUE;

-- Backfill: every org's default team becomes its (only) team row, marked as
-- the billing team, with the conventional per-team schema name. The column is
-- NOT NULL (migration 000020) so the NULL guard is belt-and-braces only.
INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, is_billing_team, backfill_enabled, created_at, updated_at)
SELECT name, default_team_id, 'team_' || default_team_id, TRUE, TRUE, NULL, now(), now()
FROM duckgres_orgs
WHERE default_team_id IS NOT NULL
ON CONFLICT (org_id, team_id) DO NOTHING;

ALTER TABLE duckgres_orgs DROP COLUMN IF EXISTS default_team_id;

-- +goose Down

-- Best-effort reversal: restore the column (nullable — the NOT NULL stance of
-- 000020 belongs to the forward world) from each org's billing-team row, then
-- drop the table. Non-billing team rows are lost by design.
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS default_team_id BIGINT;

UPDATE duckgres_orgs o
SET default_team_id = t.team_id
FROM duckgres_org_teams t
WHERE t.org_id = o.name
  AND t.is_billing_team IS TRUE;

DROP TABLE IF EXISTS duckgres_org_teams;
