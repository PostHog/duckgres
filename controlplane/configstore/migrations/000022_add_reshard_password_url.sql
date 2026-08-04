-- +goose Up

-- Reshard operations now execute in a dedicated per-operation pod
-- (duckgres-reshard-op-<id>) instead of inside a control-plane process. For a
-- cnpg→external operation the runner pod pulls the EPHEMERAL target password
-- from the in-memory stash of the control-plane replica that took the start
-- request, over the internal-secret-authed admin API. This column records that
-- pull URL (http://<creating-cp-pod-ip>:8080/api/v1/reshards/<id>/password —
-- the URL only, NEVER the password) so the leader reconciler can respawn a
-- crashed runner pod with the same handoff wiring while the stashing replica
-- is still alive. Empty for cnpg targets (no ephemeral password involved).
-- See docs/design/resharding.md.
ALTER TABLE duckgres_reshard_operations
    ADD COLUMN IF NOT EXISTS password_url TEXT NOT NULL DEFAULT '';

-- +goose Down

ALTER TABLE duckgres_reshard_operations
    DROP COLUMN IF EXISTS password_url;
