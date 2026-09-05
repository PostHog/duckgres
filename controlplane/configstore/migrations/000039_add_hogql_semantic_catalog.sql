-- +goose Up
CREATE TABLE IF NOT EXISTS duckgres_hogql_semantic_catalog_snapshots (
    catalog_value     TEXT        NOT NULL,
    catalog_delimited BOOLEAN     NOT NULL,
    generation        BIGINT      NOT NULL CHECK (generation > 0),
    protocol_version  INTEGER     NOT NULL CHECK (protocol_version = 1),
    schema_version    INTEGER     NOT NULL,
    language_version  TEXT        NOT NULL,
    manifest          JSONB       NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (catalog_value, catalog_delimited, generation)
);

-- +goose Down
DROP TABLE IF EXISTS duckgres_hogql_semantic_catalog_snapshots;
