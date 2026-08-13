-- +goose Up
-- Service credentials stop being a project_user concern: a service mint now
-- binds the org-level `service` login (an ordinary `unrestricted` org user —
-- see controlplane/configstore/service_credential.go) rather than the team's
-- project_user read/write login. The only leftover project-account machinery
-- the mint needed is the TTL clock (service_grant_expires_at, 000035): the
-- new mint reuses the live credential or force-rotates it, never consults the
-- grant clock, and its reuse-vs-rotate decision is driven by the row's
-- password hash + updated_at. This migration drops now-dead mint state —
-- no scoped-login schema or logchange: duckgres_org_users.project_user rows
-- and every table/constraint/index the query gateway enforces stay.
ALTER TABLE duckgres_org_users
    DROP COLUMN IF EXISTS service_grant_expires_at;

-- +goose Down
ALTER TABLE duckgres_org_users
    ADD COLUMN service_grant_expires_at TIMESTAMPTZ NULL;
