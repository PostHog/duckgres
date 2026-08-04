-- +goose Up

-- Per-team legacy table-name overrides. NULL (the default, and the value for
-- every row created going forward) means "derive from schema_name": events
-- live at <schema_name>.events, persons at <schema_name>.persons, and data
-- imports under the <schema_name>_data_imports schema. Non-NULL values are
-- grandfathered explicit names for pre-existing teams whose warehouse tables
-- predate the schema-per-team convention — the PostHog-side backfill sets
-- them via the provisioning team upsert.
ALTER TABLE duckgres_org_teams ADD COLUMN IF NOT EXISTS events_table_name VARCHAR(255);
ALTER TABLE duckgres_org_teams ADD COLUMN IF NOT EXISTS persons_table_name VARCHAR(255);
ALTER TABLE duckgres_org_teams ADD COLUMN IF NOT EXISTS schema_data_imports_name VARCHAR(255);

-- Two teams in one org must never share a schema name: the schema is where a
-- team's warehouse data lives, so a collision would silently interleave two
-- teams' tables. Safe on existing data — migration 000024 backfilled at most
-- one row per org ('team_<id>', keyed by the unique default_team_id).
CREATE UNIQUE INDEX IF NOT EXISTS idx_duckgres_org_teams_org_schema ON duckgres_org_teams (org_id, schema_name);

-- +goose Down

DROP INDEX IF EXISTS idx_duckgres_org_teams_org_schema;
ALTER TABLE duckgres_org_teams DROP COLUMN IF EXISTS schema_data_imports_name;
ALTER TABLE duckgres_org_teams DROP COLUMN IF EXISTS persons_table_name;
ALTER TABLE duckgres_org_teams DROP COLUMN IF EXISTS events_table_name;
