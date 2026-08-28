-- +goose Up
CREATE TABLE IF NOT EXISTS duckgres_hogql_exchange_rate_snapshots (
    generation       BIGINT      NOT NULL CHECK (generation > 0),
    protocol_version INTEGER     NOT NULL CHECK (protocol_version = 1),
    schema_version   INTEGER     NOT NULL,
    base_currency    TEXT        NOT NULL,
    decimal_scale    INTEGER     NOT NULL,
    snapshot         JSONB       NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (generation)
);

-- +goose Down
DROP TABLE IF EXISTS duckgres_hogql_exchange_rate_snapshots;
