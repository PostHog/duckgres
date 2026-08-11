-- +goose Up
-- Service credentials (controlplane/configstore/service_credential.go) rotate
-- the team's project_user password hash on a TTL. The TTL clock must NOT be
-- inferred from duckgres_org_users.updated_at: the admin project-login
-- endpoint (UpsertProjectLogin) also bumps updated_at whenever an operator
-- rotates the credential, which would silently reset a service-credential
-- grant's expiry AND hand a fresh fetcher an empty plaintext (the reuse path
-- would compute age≈0 and return no password for a credential the job never
-- saw). service_grant_expires_at is mint-time state only the service
-- credential issuer writes; NULL means no outstanding service-issued grant
-- (either never minted, or the row's credential was last set by the admin
-- path, whose rotation MUST clear this column so a subsequent service mint
-- rotates instead of trusting the shared row).
ALTER TABLE duckgres_org_users
    ADD COLUMN service_grant_expires_at TIMESTAMPTZ NULL;

-- +goose Down
ALTER TABLE duckgres_org_users
    DROP COLUMN IF EXISTS service_grant_expires_at;
