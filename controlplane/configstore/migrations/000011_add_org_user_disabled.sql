-- +goose Up

-- Per-user kill switch: a disabled (org_id, username) is refused at connect time
-- on both the PG wire and Flight SQL front-ends. Mirrors the passthrough column
-- (NOT NULL DEFAULT false). Toggled via the admin API (Users → disable/enable),
-- which also tears down the user's live sessions when flipping to disabled.
ALTER TABLE duckgres_org_users ADD COLUMN IF NOT EXISTS disabled BOOLEAN NOT NULL DEFAULT false;
