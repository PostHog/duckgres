-- +goose Up
ALTER TABLE duckgres_org_users
    ADD COLUMN access_mode VARCHAR(32) NOT NULL DEFAULT 'unrestricted',
    ADD COLUMN team_id BIGINT;

ALTER TABLE duckgres_org_users
    ADD CONSTRAINT duckgres_org_users_access_mode_check
        CHECK (access_mode IN ('unrestricted', 'project_reader')),
    ADD CONSTRAINT duckgres_org_users_project_reader_check
        CHECK (
            (access_mode = 'unrestricted' AND team_id IS NULL)
            OR (access_mode = 'project_reader' AND team_id IS NOT NULL AND passthrough IS FALSE)
        );

CREATE UNIQUE INDEX idx_duckgres_org_users_project_reader_team
    ON duckgres_org_users (org_id, team_id)
    WHERE access_mode = 'project_reader';

-- +goose Down
DROP INDEX IF EXISTS idx_duckgres_org_users_project_reader_team;
ALTER TABLE duckgres_org_users
    DROP CONSTRAINT IF EXISTS duckgres_org_users_project_reader_check,
    DROP CONSTRAINT IF EXISTS duckgres_org_users_access_mode_check,
    DROP COLUMN IF EXISTS team_id,
    DROP COLUMN IF EXISTS access_mode;
