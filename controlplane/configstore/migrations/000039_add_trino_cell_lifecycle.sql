-- +goose Up
-- Ownership has no timeout. Unknown remote mutation outcomes require recovery.
CREATE TABLE duckgres_trino_cell_lifecycle (
    cell_id TEXT PRIMARY KEY,
    reconcile_owner TEXT NOT NULL DEFAULT '',
    reconcile_epoch BIGINT NOT NULL DEFAULT 0 CHECK (reconcile_epoch >= 0),
    intent_sequence BIGINT NOT NULL DEFAULT 0 CHECK (intent_sequence >= 0),
    intent JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(intent) = 'object'),
    admission_epoch BIGINT NOT NULL DEFAULT 0 CHECK (admission_epoch >= 0),
    freeze_operation_id TEXT NOT NULL DEFAULT '',
    freeze_plan_hash TEXT NOT NULL DEFAULT '',
    freeze_target TEXT NOT NULL DEFAULT '',
    freeze_stable BOOLEAN NOT NULL DEFAULT FALSE,
    certificate JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(certificate) = 'object'),
    released_operation_id TEXT NOT NULL DEFAULT '',
    released_admission_epoch BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE duckgres_trino_cell_lifecycle;
