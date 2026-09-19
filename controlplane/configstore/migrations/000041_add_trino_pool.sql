-- +goose Up
-- Durable state for the shared Trino compute pool (the operator side of the
-- shared-pool work). Every table here is inert while the feature flags are off:
-- nothing reads or writes them unless a registered cell declares
-- `mode: "shared-pool"` AND DUCKGRES_TRINO_POOL_ENABLED is set.
--
-- Why these live in duckgres rather than in the Gateway database: duckgres owns
-- the DESIRED pool specification, the immutable instance identities and the
-- durable reconcile operations. The Gateway owns admission and the irreversible
-- retirement claim. There is no distributed transaction between the two; the
-- ordering is durable-intent-first, external-effect-second, read-back on any
-- unknown outcome. The receipt columns below are where a Gateway outcome is
-- checkpointed after it has been read back.

CREATE TABLE duckgres_trino_pools (
    pool_id                  TEXT        PRIMARY KEY,
    public_id                TEXT        NOT NULL,
    api_mode                 TEXT        NOT NULL DEFAULT 'legacy',
    desired_release_id       TEXT        NOT NULL DEFAULT '',
    desired_blueprint_digest TEXT        NOT NULL DEFAULT '',
    -- Sizing. desired_instances is deliberately NOT NULL with no default of
    -- zero-meaning-empty: a missing or unreadable desired configuration sets
    -- `frozen` instead. "Desired count zero" must never be the way a config
    -- problem expresses itself, or a bad mount deletes the fleet.
    desired_instances        INTEGER     NOT NULL DEFAULT 3 CHECK (desired_instances >= 1),
    min_serving              INTEGER     NOT NULL DEFAULT 3 CHECK (min_serving >= 1),
    max_surge                INTEGER     NOT NULL DEFAULT 1 CHECK (max_surge >= 0),
    max_repair               INTEGER     NOT NULL DEFAULT 1 CHECK (max_repair >= 0),
    -- Monotonic authority epoch. Bumped on leader takeover, sent to the Gateway
    -- as controllerEpoch and to the catalog store as writer_epoch, so one
    -- number fences every external effect of this pool.
    authority_epoch          BIGINT      NOT NULL DEFAULT 0 CHECK (authority_epoch >= 0),
    authority_owner          TEXT        NOT NULL DEFAULT '',
    -- Desired and admitted configuration revisions are separate values on
    -- purpose: advancing desired state must not instantly make every existing
    -- coordinator ineligible.
    publication_revision     BIGINT      NOT NULL DEFAULT 0 CHECK (publication_revision >= 0),
    admitted_revision        BIGINT      NOT NULL DEFAULT 0 CHECK (admitted_revision >= 0),
    frozen                   BOOLEAN     NOT NULL DEFAULT FALSE,
    frozen_reason            TEXT        NOT NULL DEFAULT '',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE duckgres_trino_pool_instances (
    -- Never reused, for the lifetime of the pool: a retired identity coming
    -- back would let a stale Gateway or Kubernetes reference resolve to a live
    -- instance. The primary key covers retired rows too, which is what enforces
    -- it.
    instance_id                 TEXT        PRIMARY KEY,
    pool_id                     TEXT        NOT NULL REFERENCES duckgres_trino_pools (pool_id) ON DELETE CASCADE,
    release_id                  TEXT        NOT NULL,
    spec_digest                 TEXT        NOT NULL,
    -- The instance's own immutable copy of the blueprint. Argo may replace or
    -- prune the source ConfigMap during a new release; a PREPARING, SERVING or
    -- DRAINING instance keeps the configuration it was created with.
    blueprint_snapshot          JSONB       NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(blueprint_snapshot) = 'object'),
    phase                       TEXT        NOT NULL DEFAULT 'PENDING',
    phase_changed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    owner_epoch                 BIGINT      NOT NULL DEFAULT 0,
    -- Charged to the failure-repair budget rather than the planned surge.
    repair                      BOOLEAN     NOT NULL DEFAULT FALSE,
    -- Kubernetes inventory. UIDs are recorded so a delete can carry a
    -- precondition: a name alone could match an object somebody else recreated.
    coordinator_deployment_name TEXT        NOT NULL DEFAULT '',
    coordinator_deployment_uid  TEXT        NOT NULL DEFAULT '',
    worker_deployment_name      TEXT        NOT NULL DEFAULT '',
    worker_deployment_uid       TEXT        NOT NULL DEFAULT '',
    service_name                TEXT        NOT NULL DEFAULT '',
    service_uid                 TEXT        NOT NULL DEFAULT '',
    config_map_name             TEXT        NOT NULL DEFAULT '',
    config_map_uid              TEXT        NOT NULL DEFAULT '',
    coordinator_pod_uid         TEXT        NOT NULL DEFAULT '',
    -- Process identity observed from the candidate itself, never asserted.
    coordinator_node_id         TEXT        NOT NULL DEFAULT '',
    coordinator_boot_id         TEXT        NOT NULL DEFAULT '',
    endpoint_url                TEXT        NOT NULL DEFAULT '',
    tls_server_name             TEXT        NOT NULL DEFAULT '',
    -- Gateway's view, checkpointed after read-back. Gateway stays authoritative.
    gateway_incarnation         TEXT        NOT NULL DEFAULT '',
    gateway_backend_name        TEXT        NOT NULL DEFAULT '',
    gateway_state               TEXT        NOT NULL DEFAULT '',
    gateway_generation          BIGINT      NOT NULL DEFAULT 0,
    applied_catalog_revision    BIGINT      NOT NULL DEFAULT 0,
    validation_receipt          JSONB       NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(validation_receipt) = 'object'),
    validated_at                TIMESTAMPTZ NULL,
    retirement_receipt          JSONB       NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(retirement_receipt) = 'object'),
    last_error                  TEXT        NOT NULL DEFAULT '',
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The reconcile loop's only listing query is "every instance of this pool".
CREATE INDEX idx_duckgres_trino_pool_instances_pool
    ON duckgres_trino_pool_instances (pool_id, phase);

-- A pooled endpoint is never reused by a second instance, mirroring the same
-- rule on the Gateway side. Partial so retired rows keep their history without
-- reserving the address forever.
CREATE UNIQUE INDEX idx_duckgres_trino_pool_instances_endpoint
    ON duckgres_trino_pool_instances (pool_id, endpoint_url)
    WHERE endpoint_url <> '' AND phase NOT IN ('RETIRED', 'FAILURE_RETIRED', 'FAILED_PREPARING');

CREATE TABLE duckgres_trino_pool_operations (
    operation_id    TEXT        PRIMARY KEY,
    pool_id         TEXT        NOT NULL REFERENCES duckgres_trino_pools (pool_id) ON DELETE CASCADE,
    instance_id     TEXT        NOT NULL DEFAULT '',
    kind            TEXT        NOT NULL,
    -- The immutable intent. The same operation id with a different hash is a
    -- conflict, never an overwrite: that is what makes a lost response safe to
    -- resolve by read-back instead of by inventing a new operation.
    intent_hash     TEXT        NOT NULL,
    owner_epoch     BIGINT      NOT NULL DEFAULT 0,
    step            TEXT        NOT NULL DEFAULT '',
    phase           TEXT        NOT NULL DEFAULT 'pending',
    receipts        JSONB       NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(receipts) = 'object'),
    last_error      TEXT        NOT NULL DEFAULT '',
    attempts        BIGINT      NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    terminal_at     TIMESTAMPTZ NULL
);

CREATE INDEX idx_duckgres_trino_pool_operations_open
    ON duckgres_trino_pool_operations (pool_id, phase)
    WHERE terminal_at IS NULL;

-- Per-step idempotency. Scoping replay identity to the step as well as the
-- parent operation is what lets a resumed operation re-run only the step that
-- was interrupted.
CREATE TABLE duckgres_trino_pool_operation_steps (
    operation_id TEXT        NOT NULL REFERENCES duckgres_trino_pool_operations (operation_id) ON DELETE CASCADE,
    step_id      TEXT        NOT NULL,
    payload_hash TEXT        NOT NULL,
    outcome      TEXT        NOT NULL DEFAULT 'pending',
    result       JSONB       NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(result) = 'object'),
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (operation_id, step_id)
);

-- Per-warehouse publication. Desired, published and admitted are three distinct
-- facts and are stored as three distinct values; overloading one boolean is how
-- a half-published tenant ends up looking ready.
CREATE TABLE duckgres_trino_pool_publications (
    pool_id                  TEXT        NOT NULL REFERENCES duckgres_trino_pools (pool_id) ON DELETE CASCADE,
    org_id                   TEXT        NOT NULL,
    desired_revision         BIGINT      NOT NULL DEFAULT 0,
    published_revision       BIGINT      NOT NULL DEFAULT 0,
    admitted_revision        BIGINT      NOT NULL DEFAULT 0,
    publication_id           TEXT        NOT NULL DEFAULT '',
    publication_operation_id TEXT        NOT NULL DEFAULT '',
    state                    TEXT        NOT NULL DEFAULT 'pending',
    gateway_receipt          JSONB       NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(gateway_receipt) = 'object'),
    last_error               TEXT        NOT NULL DEFAULT '',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pool_id, org_id)
);

-- The authorization-projection watermark. A duckgres replica consults this
-- before serving an OPA bundle and refuses to serve anything older, so a stale
-- replica never emits a regressing bundle in the first place. An ETag on the
-- producer alone cannot do that: it cannot reject a response that is already in
-- flight, and stock OPA does not compare custom bundle revisions.
CREATE TABLE duckgres_trino_pool_projection (
    pool_id            TEXT        PRIMARY KEY REFERENCES duckgres_trino_pools (pool_id) ON DELETE CASCADE,
    authority_epoch    BIGINT      NOT NULL DEFAULT 0 CHECK (authority_epoch >= 0),
    accepted_revision  BIGINT      NOT NULL DEFAULT 0 CHECK (accepted_revision >= 0),
    accepted_digest    TEXT        NOT NULL DEFAULT '',
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS duckgres_trino_pool_projection;
DROP TABLE IF EXISTS duckgres_trino_pool_publications;
DROP TABLE IF EXISTS duckgres_trino_pool_operation_steps;
DROP TABLE IF EXISTS duckgres_trino_pool_operations;
DROP TABLE IF EXISTS duckgres_trino_pool_instances;
DROP TABLE IF EXISTS duckgres_trino_pools;
