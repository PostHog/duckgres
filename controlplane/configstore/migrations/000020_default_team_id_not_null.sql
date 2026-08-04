-- +goose Up

-- default_team_id becomes NOT NULL: every org must carry its default PostHog
-- team id (the compute/storage billing bucket key). All orgs in all envs have
-- been backfilled, and every create path (provisioning API, admin API) already
-- requires a positive team id, so no NULL should exist. If one does, this
-- migration fails loudly at deploy — which is the correct outcome: fix the
-- row, not the constraint. The admin API's clear-to-NULL semantics were
-- removed alongside this migration, so nothing can reintroduce a NULL.
ALTER TABLE duckgres_orgs
    ALTER COLUMN default_team_id SET NOT NULL;

-- +goose Down

ALTER TABLE duckgres_orgs
    ALTER COLUMN default_team_id DROP NOT NULL;
