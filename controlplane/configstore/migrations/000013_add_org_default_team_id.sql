-- +goose Up

-- default_team_id links an org to its default PostHog team (a team id, stored
-- as a string). It is a prerequisite for pull-based compute billing: usage
-- buckets are keyed by team_id = the org's default team. NULLABLE and optional
-- everywhere for now — existing orgs are backfilled separately and a follow-up
-- makes it required. NULL is valid; no default; callers must tolerate empty.
ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS default_team_id VARCHAR(255);

-- +goose Down

ALTER TABLE duckgres_orgs DROP COLUMN IF EXISTS default_team_id;
