-- +goose Up
-- Service account credentials reborn as AWS-style access keys: each minted
-- credential is its own row, has its own TTL, and is a first-class lifecycle
-- owned by the caller that minted it. Replaces the project_user-backed shape
-- of #1058 (which stored the mint clock on duckgres_org_users and forced the
-- credential to be team-scoped through the access_mode ACL).
--
--   * duckgres_service_grants: one row PER credential. A credential is a
--     (credential_id, secret) pair — the caller presents credential_id as the
--     pgwire username and the plaintext secret as the password. The bcrypt
--     hash lives on the grant row, NOT in duckgres_org_users, so operator
--     writes to the org's users table can never touch a minted credential.
--     Every service login is implicit in the grant rows themselves — there
--     are no separate duckgres_org_users rows to manage or tear down.
--   * service_grant_expires_at (000035) drops: it was the per-USER mint
--     clock the now-deleted project_user mint relied on, and grants are
--     per-CREDENTIAL, not per-user.
CREATE TABLE IF NOT EXISTS duckgres_service_grants (
    org_id          TEXT        NOT NULL,
    credential_id   TEXT        NOT NULL,
    principal       TEXT        NOT NULL,
    password_hash   TEXT        NOT NULL,
    minted_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_rotated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,
    revoked_at      TIMESTAMPTZ NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, credential_id)
);

-- Auth-time lookup: snapshot refreshes load every grant; the pgwire handshake
-- for an svc_-prefixed username checks OrgUserGrantByCredentialID straight.
-- Used by no other index shape, deliberately.
CREATE INDEX IF NOT EXISTS idx_duckgres_service_grants_org_principal
    ON duckgres_service_grants (org_id, principal);

-- Anything the admin UI lists by state ("which credentials are live right
-- now?") needs expires_at/revoked_at lookups — one composite index over the
-- two serves both filters without scanning.
CREATE INDEX IF NOT EXISTS idx_duckgres_service_grants_org_state
    ON duckgres_service_grants (org_id, expires_at, revoked_at);

ALTER TABLE duckgres_org_users
    DROP COLUMN IF EXISTS service_grant_expires_at;

-- +goose Down
ALTER TABLE duckgres_org_users
    ADD COLUMN service_grant_expires_at TIMESTAMPTZ NULL;

DROP TABLE IF EXISTS duckgres_service_grants;
