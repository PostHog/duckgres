-- +goose Up
-- project_user joins project_reader as a team-scoped login. Both modes bind a
-- team row (which is what derives the allowed namespaces) and both forbid
-- passthrough, because passthrough bypasses the compat layer that enforces the
-- scope. They differ only in whether writes are authorized, so the shape
-- constraint is now shared between them.
ALTER TABLE duckgres_org_users
    DROP CONSTRAINT IF EXISTS duckgres_org_users_access_mode_check,
    DROP CONSTRAINT IF EXISTS duckgres_org_users_project_reader_check;

ALTER TABLE duckgres_org_users
    ADD CONSTRAINT duckgres_org_users_access_mode_check
        CHECK (access_mode IN ('unrestricted', 'project_reader', 'project_user')),
    ADD CONSTRAINT duckgres_org_users_project_scoped_check
        CHECK (
            (access_mode = 'unrestricted' AND team_id IS NULL)
            OR (access_mode IN ('project_reader', 'project_user')
                AND team_id IS NOT NULL AND passthrough IS FALSE)
        );

-- One project_user per team, mirroring the project_reader index. The two
-- indexes are deliberately separate (not one index over both modes) so a team
-- can hold a reader AND a writer at the same time.
CREATE UNIQUE INDEX IF NOT EXISTS idx_duckgres_org_users_project_user_team
    ON duckgres_org_users (org_id, team_id)
    WHERE access_mode = 'project_user';

-- +goose Down
DROP INDEX IF EXISTS idx_duckgres_org_users_project_user_team;
-- The restored constraint has no room for project_user rows, so they cannot
-- survive the downgrade. Deleting them only drops the login: the project's
-- schemas and data are untouched, and re-applying Up plus a PUT on the
-- project-user endpoint mints an equivalent credential.
DELETE FROM duckgres_org_users WHERE access_mode = 'project_user';
ALTER TABLE duckgres_org_users
    DROP CONSTRAINT IF EXISTS duckgres_org_users_project_scoped_check,
    DROP CONSTRAINT IF EXISTS duckgres_org_users_access_mode_check;
ALTER TABLE duckgres_org_users
    ADD CONSTRAINT duckgres_org_users_access_mode_check
        CHECK (access_mode IN ('unrestricted', 'project_reader')),
    ADD CONSTRAINT duckgres_org_users_project_reader_check
        CHECK (
            (access_mode = 'unrestricted' AND team_id IS NULL)
            OR (access_mode = 'project_reader' AND team_id IS NOT NULL AND passthrough IS FALSE)
        );
