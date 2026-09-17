-- +goose Up
ALTER TABLE duckgres_managed_warehouse_trino
    ADD COLUMN backend TEXT NOT NULL DEFAULT 'ducklake'
        CHECK (backend IN ('ducklake', 'hoglake')),
    ADD COLUMN backend_selected BOOLEAN NOT NULL DEFAULT TRUE;

-- Prior disabled rows do not record whether enablement was attempted. Pin all
-- existing rows to DuckLake rather than risk switching a previously used catalog.
-- This includes old cell-only selections; use a new tenant for a Hoglake pilot.
UPDATE duckgres_managed_warehouse_trino SET backend_selected = TRUE;

-- Older control-plane replicas update enabled without knowing backend_selected.
-- Record their first enablement too, even for a cell-only row inserted by a
-- newer replica. Disabled rows retain the marker through future re-enablement.
-- +goose StatementBegin
CREATE FUNCTION duckgres_select_trino_backend_on_enable() RETURNS trigger AS $$
BEGIN
    IF NEW.enabled THEN
        -- Old replicas omit backend_selected when enabling a new cell-only row.
        -- Their provisioner creates DuckLake, so preserve that actual backend.
        -- New replicas set backend_selected together with their Hoglake choice.
        IF NOT NEW.backend_selected THEN
            NEW.backend := 'ducklake';
        END IF;
        NEW.backend_selected := TRUE;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER duckgres_select_trino_backend_on_enable
    BEFORE INSERT OR UPDATE OF enabled ON duckgres_managed_warehouse_trino
    FOR EACH ROW EXECUTE FUNCTION duckgres_select_trino_backend_on_enable();

-- +goose Down
DROP TRIGGER duckgres_select_trino_backend_on_enable ON duckgres_managed_warehouse_trino;
DROP FUNCTION duckgres_select_trino_backend_on_enable();
ALTER TABLE duckgres_managed_warehouse_trino
    DROP COLUMN backend_selected,
    DROP COLUMN backend;
