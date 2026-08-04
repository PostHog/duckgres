-- Production schema reference: PostHog/posthog@056583335dc739b9e025efede811c9b4f5e153f5
-- posthog/dags/events_backfill_to_duckling.py (EVENTS_TABLE_DDL and PERSONS_TABLE_DDL).
--
-- The frozen objects are immutable, read-only fixtures. Register their Parquet
-- footers in DuckLake rather than copying their rows into new Parquet files:
-- this preserves the raw/table comparison while avoiding a large rewrite and
-- its temporary-disk use. The source object names are not Hive directories, so
-- registration has to precede the table partition specification.

-- Verify that the unioned source schema can populate the production table
-- schema. `allow_missing` below remains necessary for individual files from a
-- schema-evolving export: an absent field is read as NULL, as it is by the raw
-- `union_by_name` view.
WITH expected_events(column_name) AS (
    VALUES
        ('uuid'), ('event'), ('properties'), ('timestamp'), ('team_id'), ('project_id'),
        ('distinct_id'), ('elements_chain'), ('created_at'), ('person_id'),
        ('person_created_at'), ('person_properties'), ('group0_properties'),
        ('group1_properties'), ('group2_properties'), ('group3_properties'),
        ('group4_properties'), ('group0_created_at'), ('group1_created_at'),
        ('group2_created_at'), ('group3_created_at'), ('group4_created_at'),
        ('person_mode'), ('historical_migration'), ('_inserted_at')
),
missing_events AS (
    SELECT expected_events.column_name
    FROM expected_events
    LEFT JOIN information_schema.columns AS source_columns
      ON source_columns.table_schema = 'frozen_v1'
     AND source_columns.table_name = 'events_file_view'
     AND source_columns.column_name = expected_events.column_name
    WHERE source_columns.column_name IS NULL
)
SELECT CASE
    WHEN COUNT(*) = 0 THEN 1
    ELSE error('posthog events missing required source columns: ' || string_agg(column_name, ', '))
END
FROM missing_events;

WITH expected_persons(column_name) AS (
    VALUES
        ('team_id'), ('distinct_id'), ('id'), ('properties'), ('created_at'),
        ('is_identified'), ('person_distinct_id_version'), ('person_version'),
        ('_timestamp'), ('_inserted_at')
),
missing_persons AS (
    SELECT expected_persons.column_name
    FROM expected_persons
    LEFT JOIN information_schema.columns AS source_columns
      ON source_columns.table_schema = 'frozen_v1'
     AND source_columns.table_name = 'persons_file_view'
     AND source_columns.column_name = expected_persons.column_name
    WHERE source_columns.column_name IS NULL
)
SELECT CASE
    WHEN COUNT(*) = 0 THEN 1
    ELSE error('posthog persons missing required source columns: ' || string_agg(column_name, ', '))
END
FROM missing_persons;

CREATE SCHEMA IF NOT EXISTS posthog;

CREATE TABLE IF NOT EXISTS main.posthog_table_setup_manifest (
    fixture_schema_revision VARCHAR,
    load_mode VARCHAR,
    events_source_files BIGINT,
    events_registered_files BIGINT,
    persons_source_files BIGINT,
    persons_registered_files BIGINT,
    created_at TIMESTAMPTZ
);

-- `ducklake_add_data_files` appends metadata registrations. Drop and recreate
-- these scenario-only tables so a retry cannot retain a partial registration.
BEGIN TRANSACTION;

DROP TABLE IF EXISTS posthog.events;
DROP TABLE IF EXISTS posthog.persons;

CREATE TABLE posthog.events (
    uuid VARCHAR,
    event VARCHAR,
    properties VARCHAR,
    timestamp TIMESTAMPTZ,
    team_id BIGINT,
    project_id BIGINT,
    distinct_id VARCHAR,
    elements_chain VARCHAR,
    created_at TIMESTAMPTZ,
    person_id VARCHAR,
    person_created_at TIMESTAMPTZ,
    person_properties VARCHAR,
    group0_properties VARCHAR,
    group1_properties VARCHAR,
    group2_properties VARCHAR,
    group3_properties VARCHAR,
    group4_properties VARCHAR,
    group0_created_at TIMESTAMPTZ,
    group1_created_at TIMESTAMPTZ,
    group2_created_at TIMESTAMPTZ,
    group3_created_at TIMESTAMPTZ,
    group4_created_at TIMESTAMPTZ,
    person_mode VARCHAR,
    historical_migration BOOLEAN,
    _inserted_at TIMESTAMPTZ
);

CREATE TABLE posthog.persons (
    team_id BIGINT,
    distinct_id VARCHAR,
    id VARCHAR,
    properties VARCHAR,
    created_at TIMESTAMPTZ,
    is_identified BOOLEAN,
    person_distinct_id_version BIGINT,
    person_version UBIGINT,
    _timestamp TIMESTAMPTZ,
    _inserted_at TIMESTAMPTZ
);

CALL ducklake_add_data_files(
    'ducklake',
    'events',
    '${env:DUCKGRES_SCENARIO_FROZEN_S3_URI}events/*.parquet',
    schema => 'posthog',
    allow_missing => true
);

CALL ducklake_add_data_files(
    'ducklake',
    'persons',
    '${env:DUCKGRES_SCENARIO_FROZEN_S3_URI}persons/*.parquet',
    schema => 'posthog',
    allow_missing => true
);

-- The registered fixture paths are flat names, not `key=value/` Hive paths.
-- Defining this afterwards retains the production table specification without
-- making the registration infer nonexistent partition values from those paths.
ALTER TABLE posthog.events
SET PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp));

ALTER TABLE posthog.persons
SET PARTITIONED BY (year(_timestamp), month(_timestamp));

DELETE FROM main.posthog_table_setup_manifest
WHERE fixture_schema_revision = '056583335dc739b9e025efede811c9b4f5e153f5';

INSERT INTO main.posthog_table_setup_manifest (
    fixture_schema_revision,
    load_mode,
    events_source_files,
    events_registered_files,
    persons_source_files,
    persons_registered_files,
    created_at
)
SELECT
    '056583335dc739b9e025efede811c9b4f5e153f5',
    'registered_frozen_parquet',
    (SELECT COUNT(*) FROM glob('${env:DUCKGRES_SCENARIO_FROZEN_S3_URI}events/*.parquet')),
    (SELECT COUNT(*) FROM ducklake_list_files('ducklake', 'events', schema => 'posthog')),
    (SELECT COUNT(*) FROM glob('${env:DUCKGRES_SCENARIO_FROZEN_S3_URI}persons/*.parquet')),
    (SELECT COUNT(*) FROM ducklake_list_files('ducklake', 'persons', schema => 'posthog')),
    now();

COMMIT;
