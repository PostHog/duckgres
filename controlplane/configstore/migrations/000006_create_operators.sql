-- +goose Up
-- Operators are the admin-console RBAC principals (authoritative access
-- control, not rebuildable runtime state) — hence a goose migration in the
-- config schema rather than runtime AutoMigrate.
CREATE TABLE IF NOT EXISTS duckgres_operators (
    email VARCHAR(255) PRIMARY KEY,
    role VARCHAR(16) NOT NULL DEFAULT 'viewer',
    added_by VARCHAR(255),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

-- +goose Down
DROP TABLE IF EXISTS duckgres_operators;
