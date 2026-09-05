-- +goose Up
CREATE TABLE IF NOT EXISTS duckgres_hogql_physical_catalog_refresh_leases (
    catalog_value     TEXT        NOT NULL,
    catalog_delimited BOOLEAN     NOT NULL,
    epoch             BIGINT      NOT NULL CHECK (epoch > 0),
    lease_token       TEXT        NULL,
    lease_expires_at  TIMESTAMPTZ NULL,
    next_refresh_at   TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
    last_success_at   TIMESTAMPTZ NULL,
    PRIMARY KEY (catalog_value, catalog_delimited),
    CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL))
);

-- +goose Down
DROP TABLE IF EXISTS duckgres_hogql_physical_catalog_refresh_leases;
