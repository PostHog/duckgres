-- Production schema reference: PostHog/posthog@056583335dc739b9e025efede811c9b4f5e153f5
-- posthog/dags/events_backfill_to_duckling.py (EVENTS_TABLE_DDL and PERSONS_TABLE_DDL).
-- The frozen Parquet views remain the raw performance-control representation.
-- Mapping exception: production derives project_id from team_id, so this rewrite
-- does the same and intentionally does not require a project_id fixture column.

-- Verify the frozen export carries every production backfill column before DDL or
-- DML. The column names are non-sensitive and make a stale fixture failure actionable.
WITH expected_events(column_name) AS (
    VALUES
        ('uuid'), ('event'), ('properties'), ('timestamp'), ('team_id'),
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

CREATE TABLE IF NOT EXISTS posthog.events (
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

CREATE TABLE IF NOT EXISTS posthog.persons (
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

ALTER TABLE posthog.events
SET PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp));

ALTER TABLE posthog.persons
SET PARTITIONED BY (year(_timestamp), month(_timestamp));

CREATE TABLE IF NOT EXISTS main.posthog_table_setup_manifest (
    production_schema_revision VARCHAR,
    load_mode VARCHAR,
    events_partition_spec VARCHAR,
    persons_partition_spec VARCHAR,
    events_source_rows BIGINT,
    events_destination_rows BIGINT,
    persons_source_rows BIGINT,
    persons_destination_rows BIGINT,
    created_at TIMESTAMPTZ
);

-- Rewritten inserts make a partial setup retry deterministic: each run replaces
-- the table contents rather than appending a second copy of the frozen fixture.
DELETE FROM posthog.events;
DELETE FROM posthog.persons;

-- The frozen fixture is larger than the worker's temporary disk. Do not retain
-- insertion order while writing its partitioned DuckLake tables: this lets
-- DuckDB flush completed data blocks instead of accumulating spill state for
-- the full backfill. Each large insert commits independently so its temporary
-- state cannot be retained by the other relation's load.
SET preserve_insertion_order = false;

INSERT INTO posthog.events (
    uuid, event, properties, timestamp, team_id, project_id, distinct_id,
    elements_chain, created_at, person_id, person_created_at, person_properties,
    group0_properties, group1_properties, group2_properties, group3_properties,
    group4_properties, group0_created_at, group1_created_at, group2_created_at,
    group3_created_at, group4_created_at, person_mode, historical_migration,
    _inserted_at
)
SELECT
    CAST(uuid AS VARCHAR),
    CAST(event AS VARCHAR),
    CAST(properties AS VARCHAR),
    CAST("timestamp" AS TIMESTAMPTZ),
    CAST(team_id AS BIGINT),
    CAST(team_id AS BIGINT) AS project_id,
    CAST(distinct_id AS VARCHAR),
    CAST(elements_chain AS VARCHAR),
    CAST(created_at AS TIMESTAMPTZ),
    CAST(person_id AS VARCHAR),
    CAST(person_created_at AS TIMESTAMPTZ),
    CAST(person_properties AS VARCHAR),
    CAST(group0_properties AS VARCHAR),
    CAST(group1_properties AS VARCHAR),
    CAST(group2_properties AS VARCHAR),
    CAST(group3_properties AS VARCHAR),
    CAST(group4_properties AS VARCHAR),
    CAST(group0_created_at AS TIMESTAMPTZ),
    CAST(group1_created_at AS TIMESTAMPTZ),
    CAST(group2_created_at AS TIMESTAMPTZ),
    CAST(group3_created_at AS TIMESTAMPTZ),
    CAST(group4_created_at AS TIMESTAMPTZ),
    CAST(person_mode AS VARCHAR),
    CAST(historical_migration AS BOOLEAN),
    CAST(_inserted_at AS TIMESTAMPTZ)
FROM frozen_v1.events_file_view;

INSERT INTO posthog.persons (
    team_id, distinct_id, id, properties, created_at, is_identified,
    person_distinct_id_version, person_version, _timestamp, _inserted_at
)
SELECT
    CAST(team_id AS BIGINT),
    CAST(distinct_id AS VARCHAR),
    CAST(id AS VARCHAR),
    CAST(properties AS VARCHAR),
    CAST(created_at AS TIMESTAMPTZ),
    CAST(is_identified AS BOOLEAN),
    CAST(person_distinct_id_version AS BIGINT),
    CAST(person_version AS UBIGINT),
    CAST(_timestamp AS TIMESTAMPTZ),
    CAST(_inserted_at AS TIMESTAMPTZ)
FROM frozen_v1.persons_file_view;

BEGIN TRANSACTION;

DELETE FROM main.posthog_table_setup_manifest
WHERE production_schema_revision = '056583335dc739b9e025efede811c9b4f5e153f5';

INSERT INTO main.posthog_table_setup_manifest (
    production_schema_revision,
    load_mode,
    events_partition_spec,
    persons_partition_spec,
    events_source_rows,
    events_destination_rows,
    persons_source_rows,
    persons_destination_rows,
    created_at
)
SELECT
    '056583335dc739b9e025efede811c9b4f5e153f5',
    'streaming_rewritten_insert',
    'year(timestamp), month(timestamp), day(timestamp)',
    'year(_timestamp), month(_timestamp)',
    (SELECT COUNT(*) FROM frozen_v1.events_file_view),
    (SELECT COUNT(*) FROM posthog.events),
    (SELECT COUNT(*) FROM frozen_v1.persons_file_view),
    (SELECT COUNT(*) FROM posthog.persons),
    now();

COMMIT;
