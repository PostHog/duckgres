CREATE SCHEMA IF NOT EXISTS frozen_v1;

CREATE OR REPLACE VIEW frozen_v1.persons_file_view AS
SELECT *
FROM read_parquet('${env:DUCKGRES_SCENARIO_FROZEN_S3_URI}persons/*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW frozen_v1.events_file_view AS
SELECT *
FROM read_parquet('${env:DUCKGRES_SCENARIO_FROZEN_S3_URI}events/*.parquet', union_by_name = true);

CREATE TABLE IF NOT EXISTS main.dataset_manifest (
    dataset_version VARCHAR,
    dataset_uri VARCHAR,
    created_at TIMESTAMPTZ
);

DELETE FROM main.dataset_manifest
WHERE dataset_version = 'posthog-file-views-v1';

INSERT INTO main.dataset_manifest (dataset_version, dataset_uri, created_at)
VALUES ('posthog-file-views-v1', '${env:DUCKGRES_SCENARIO_FROZEN_S3_URI}', now());
