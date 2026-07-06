-- +goose Up

-- default_team_id becomes a BIGINT end-to-end. PostHog team ids are integers
-- (Team.id), so callers naturally send JSON numbers — the string-typed column
-- forced a string JSON field, which 400ed the obvious `"default_team_id":
-- 12345` provision payload. All stored values are numeric strings (set via the
-- provisioning API / admin UI), so the cast is safe; a non-numeric value would
-- fail this migration loudly, which is the correct outcome (fix the row, not
-- the type). NULL stays NULL (empty string normalizes to NULL). The ::text in
-- the USING clauses makes the rewrite idempotent — a replay onto an
-- already-BIGINT column (goose re-run in tests) still type-checks.
ALTER TABLE duckgres_orgs
    ALTER COLUMN default_team_id TYPE BIGINT USING NULLIF(default_team_id::text, '')::bigint;

-- The compute-usage bucket key mirrors the org column. team_id sits in the
-- PRIMARY KEY so it must stay NOT NULL: 0 = "org had no default team" (PostHog
-- team ids start at 1), replacing the old '' sentinel.
ALTER TABLE duckgres_org_compute_usage
    ALTER COLUMN team_id TYPE BIGINT USING COALESCE(NULLIF(team_id::text, ''), '0')::bigint,
    ALTER COLUMN team_id SET DEFAULT 0;

-- +goose Down

-- Best-effort reversal: the 0 sentinel downgrades to '0' (not the old ''),
-- which is fine for a transient buffer.
ALTER TABLE duckgres_org_compute_usage
    ALTER COLUMN team_id TYPE TEXT USING team_id::text,
    ALTER COLUMN team_id SET DEFAULT '';

ALTER TABLE duckgres_orgs
    ALTER COLUMN default_team_id TYPE VARCHAR(255) USING default_team_id::text;
