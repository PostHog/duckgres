-- +goose Up

-- Retain ownership independently of tenant lifecycle. Never reuse a principal
-- for a different organization: late completion events carry only that identity.
CREATE TABLE duckgres_trino_usage_principals (
    principal TEXT PRIMARY KEY,
    org_id TEXT NOT NULL,
    team_id BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO duckgres_trino_usage_principals (principal, org_id, team_id)
SELECT o.database_name, o.name,
       COALESCE((SELECT team_id FROM duckgres_org_teams t WHERE t.org_id = o.name ORDER BY created_at, team_id LIMIT 1), 0)
FROM duckgres_orgs o WHERE o.database_name IS NOT NULL AND o.database_name <> '';

CREATE TABLE duckgres_billing_batches (
    batch_id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    acknowledged_at TIMESTAMPTZ
);
-- A single durable consumer lock serializes both batch creation and ack.
CREATE TABLE duckgres_billing_consumer (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    outstanding_batch_id TEXT REFERENCES duckgres_billing_batches(batch_id)
);
INSERT INTO duckgres_billing_consumer (id) VALUES (1);

CREATE TABLE duckgres_trino_query_usage (
    id BIGSERIAL PRIMARY KEY,
    cluster_id TEXT NOT NULL,
    query_id TEXT NOT NULL,
    principal TEXT NOT NULL,
    org_id TEXT,
    team_id BIGINT NOT NULL DEFAULT 0,
    source TEXT NOT NULL,
    state TEXT NOT NULL,
    error_code TEXT NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    physical_input_bytes BIGINT NOT NULL CHECK (physical_input_bytes >= 0),
    processed_input_bytes BIGINT NOT NULL CHECK (processed_input_bytes >= 0),
    statistics_complete BOOLEAN NOT NULL,
    trino_version TEXT NOT NULL,
    metric_version INTEGER NOT NULL DEFAULT 1,
    batch_id TEXT REFERENCES duckgres_billing_batches(batch_id),
    UNIQUE (cluster_id, query_id)
);
CREATE INDEX duckgres_trino_query_usage_pending ON duckgres_trino_query_usage (id) WHERE batch_id IS NULL;
CREATE INDEX duckgres_trino_query_usage_batch ON duckgres_trino_query_usage (batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX duckgres_trino_query_usage_completed ON duckgres_trino_query_usage (completed_at);

-- Storage samples continue accumulating exactly as before. Claim only the
-- unexported delta under a row lock, so a later write to the same minute is
-- delivered in the next batch without changing any already-created batch.
ALTER TABLE duckgres_org_storage_usage ADD COLUMN exported_byte_seconds NUMERIC NOT NULL DEFAULT 0;
CREATE INDEX duckgres_storage_usage_pending ON duckgres_org_storage_usage (bucket_start, org_id, team_id)
    WHERE byte_seconds > exported_byte_seconds;
CREATE TABLE duckgres_billing_batch_storage (
    batch_id TEXT NOT NULL REFERENCES duckgres_billing_batches(batch_id),
    org_id TEXT NOT NULL,
    team_id BIGINT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    byte_seconds NUMERIC NOT NULL CHECK (byte_seconds > 0),
    PRIMARY KEY (batch_id, org_id, team_id, bucket_start)
);

-- +goose Down
DROP TABLE duckgres_billing_batch_storage;
DROP INDEX duckgres_storage_usage_pending;
ALTER TABLE duckgres_org_storage_usage DROP COLUMN exported_byte_seconds;
DROP TABLE duckgres_trino_query_usage;
DROP TABLE duckgres_billing_consumer;
DROP TABLE duckgres_billing_batches;
DROP TABLE duckgres_trino_usage_principals;
