-- Validation for the production-shaped PostHog DuckLake tables. Every assertion
-- raises a concise, non-sensitive SQL error so the scenario fails before perf runs.

DESCRIBE posthog.events;
DESCRIBE posthog.persons;

WITH expected(table_name, ordinal_position, column_name, data_type) AS (
    VALUES
        ('events', 1, 'uuid', 'VARCHAR'), ('events', 2, 'event', 'VARCHAR'),
        ('events', 3, 'properties', 'VARCHAR'), ('events', 4, 'timestamp', 'TIMESTAMP WITH TIME ZONE'),
        ('events', 5, 'team_id', 'BIGINT'), ('events', 6, 'project_id', 'BIGINT'),
        ('events', 7, 'distinct_id', 'VARCHAR'), ('events', 8, 'elements_chain', 'VARCHAR'),
        ('events', 9, 'created_at', 'TIMESTAMP WITH TIME ZONE'), ('events', 10, 'person_id', 'VARCHAR'),
        ('events', 11, 'person_created_at', 'TIMESTAMP WITH TIME ZONE'), ('events', 12, 'person_properties', 'VARCHAR'),
        ('events', 13, 'group0_properties', 'VARCHAR'), ('events', 14, 'group1_properties', 'VARCHAR'),
        ('events', 15, 'group2_properties', 'VARCHAR'), ('events', 16, 'group3_properties', 'VARCHAR'),
        ('events', 17, 'group4_properties', 'VARCHAR'), ('events', 18, 'group0_created_at', 'TIMESTAMP WITH TIME ZONE'),
        ('events', 19, 'group1_created_at', 'TIMESTAMP WITH TIME ZONE'), ('events', 20, 'group2_created_at', 'TIMESTAMP WITH TIME ZONE'),
        ('events', 21, 'group3_created_at', 'TIMESTAMP WITH TIME ZONE'), ('events', 22, 'group4_created_at', 'TIMESTAMP WITH TIME ZONE'),
        ('events', 23, 'person_mode', 'VARCHAR'), ('events', 24, 'historical_migration', 'BOOLEAN'),
        ('events', 25, '_inserted_at', 'TIMESTAMP WITH TIME ZONE'),
        ('persons', 1, 'team_id', 'BIGINT'), ('persons', 2, 'distinct_id', 'VARCHAR'),
        ('persons', 3, 'id', 'VARCHAR'), ('persons', 4, 'properties', 'VARCHAR'),
        ('persons', 5, 'created_at', 'TIMESTAMP WITH TIME ZONE'), ('persons', 6, 'is_identified', 'BOOLEAN'),
        ('persons', 7, 'person_distinct_id_version', 'BIGINT'), ('persons', 8, 'person_version', 'UBIGINT'),
        ('persons', 9, '_timestamp', 'TIMESTAMP WITH TIME ZONE'), ('persons', 10, '_inserted_at', 'TIMESTAMP WITH TIME ZONE')
),
schema_mismatches AS (
    SELECT expected.table_name || '.' || expected.column_name AS mismatch
    FROM expected
    LEFT JOIN information_schema.columns AS actual
      ON actual.table_schema = 'posthog'
     AND actual.table_name = expected.table_name
     AND actual.ordinal_position = expected.ordinal_position
     AND actual.column_name = expected.column_name
     AND actual.data_type = expected.data_type
    WHERE actual.column_name IS NULL
    UNION ALL
    SELECT actual.table_name || '.' || actual.column_name AS mismatch
    FROM information_schema.columns AS actual
    LEFT JOIN expected
     ON expected.table_name = actual.table_name
     AND expected.ordinal_position = actual.ordinal_position
     AND expected.column_name = actual.column_name
     AND expected.data_type = actual.data_type
    WHERE actual.table_schema = 'posthog'
      AND expected.column_name IS NULL
)
SELECT CASE
    WHEN COUNT(*) = 0 THEN 1
    ELSE error('posthog schema mismatch: ' || string_agg(mismatch, ', '))
END
FROM schema_mismatches;

WITH expected(table_name, partition_key_index, column_name, transform) AS (
    VALUES
        ('events', 0, 'timestamp', 'year'), ('events', 1, 'timestamp', 'month'),
        ('events', 2, 'timestamp', 'day'), ('persons', 0, '_timestamp', 'year'),
        ('persons', 1, '_timestamp', 'month')
),
actual AS (
    SELECT
        table_metadata.table_name,
        partition_column.partition_key_index,
        column_metadata.column_name,
        partition_column.transform
    FROM "__ducklake_metadata_ducklake".ducklake_partition_column AS partition_column
    JOIN "__ducklake_metadata_ducklake".ducklake_partition_info AS partition_info
      ON partition_info.partition_id = partition_column.partition_id
     AND partition_info.table_id = partition_column.table_id
     AND partition_info.end_snapshot IS NULL
    JOIN "__ducklake_metadata_ducklake".ducklake_table AS table_metadata
      ON table_metadata.table_id = partition_info.table_id
    JOIN "__ducklake_metadata_ducklake".ducklake_schema AS schema_metadata
      ON schema_metadata.schema_id = table_metadata.schema_id
    JOIN "__ducklake_metadata_ducklake".ducklake_column AS column_metadata
      ON column_metadata.column_id = partition_column.column_id
     AND column_metadata.table_id = partition_column.table_id
     AND column_metadata.end_snapshot IS NULL
    WHERE schema_metadata.schema_name = 'posthog'
),
partition_mismatches AS (
    SELECT expected.table_name || '.' || expected.transform || '(' || expected.column_name || ')' AS mismatch
    FROM expected
    LEFT JOIN actual
      ON actual.table_name = expected.table_name
     AND actual.partition_key_index = expected.partition_key_index
     AND actual.column_name = expected.column_name
     AND actual.transform = expected.transform
    WHERE actual.column_name IS NULL
    UNION ALL
    SELECT actual.table_name || '.' || actual.transform || '(' || actual.column_name || ')' AS mismatch
    FROM actual
    LEFT JOIN expected
      ON expected.table_name = actual.table_name
     AND expected.partition_key_index = actual.partition_key_index
     AND expected.column_name = actual.column_name
     AND expected.transform = actual.transform
    WHERE expected.column_name IS NULL
)
SELECT CASE
    WHEN COUNT(*) = 0 THEN 1
    ELSE error('posthog partition metadata mismatch: ' || string_agg(mismatch, ', '))
END
FROM partition_mismatches;

WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM frozen_v1.events_file_view) AS events_source_rows,
        (SELECT COUNT(*) FROM posthog.events) AS events_destination_rows,
        (SELECT COUNT(*) FROM frozen_v1.persons_file_view) AS persons_source_rows,
        (SELECT COUNT(*) FROM posthog.persons) AS persons_destination_rows
)
SELECT CASE
    WHEN events_source_rows = events_destination_rows THEN 1
    ELSE error('posthog.events row-count mismatch')
END,
CASE
    WHEN persons_source_rows = persons_destination_rows THEN 1
    ELSE error('posthog.persons row-count mismatch')
END
FROM counts;

WITH ranges AS (
    SELECT
        (SELECT MIN(CAST("timestamp" AS TIMESTAMPTZ)) FROM frozen_v1.events_file_view) AS events_source_min,
        (SELECT MAX(CAST("timestamp" AS TIMESTAMPTZ)) FROM frozen_v1.events_file_view) AS events_source_max,
        (SELECT MIN(timestamp) FROM posthog.events) AS events_destination_min,
        (SELECT MAX(timestamp) FROM posthog.events) AS events_destination_max,
        (SELECT MIN(CAST(_timestamp AS TIMESTAMPTZ)) FROM frozen_v1.persons_file_view) AS persons_source_min,
        (SELECT MAX(CAST(_timestamp AS TIMESTAMPTZ)) FROM frozen_v1.persons_file_view) AS persons_source_max,
        (SELECT MIN(_timestamp) FROM posthog.persons) AS persons_destination_min,
        (SELECT MAX(_timestamp) FROM posthog.persons) AS persons_destination_max
)
SELECT CASE
    WHEN events_source_min IS NOT DISTINCT FROM events_destination_min
     AND events_source_max IS NOT DISTINCT FROM events_destination_max THEN 1
    ELSE error('posthog.events timestamp range mismatch')
END,
CASE
    WHEN persons_source_min IS NOT DISTINCT FROM persons_destination_min
     AND persons_source_max IS NOT DISTINCT FROM persons_destination_max THEN 1
    ELSE error('posthog.persons timestamp range mismatch')
END
FROM ranges;

WITH null_counts AS (
    SELECT
        (SELECT COUNT(*) FROM frozen_v1.events_file_view WHERE uuid IS NULL OR event IS NULL OR "timestamp" IS NULL OR team_id IS NULL) AS events_source_nulls,
        (SELECT COUNT(*) FROM posthog.events WHERE uuid IS NULL OR event IS NULL OR timestamp IS NULL OR team_id IS NULL) AS events_destination_nulls,
        (SELECT COUNT(*) FROM frozen_v1.persons_file_view WHERE team_id IS NULL OR distinct_id IS NULL OR id IS NULL OR _timestamp IS NULL) AS persons_source_nulls,
        (SELECT COUNT(*) FROM posthog.persons WHERE team_id IS NULL OR distinct_id IS NULL OR id IS NULL OR _timestamp IS NULL) AS persons_destination_nulls
)
SELECT CASE
    WHEN events_source_nulls = events_destination_nulls THEN 1
    ELSE error('posthog.events key null-count mismatch')
END,
CASE
    WHEN persons_source_nulls = persons_destination_nulls THEN 1
    ELSE error('posthog.persons key null-count mismatch')
END
FROM null_counts;

WITH source_events AS (
    SELECT
        CAST(uuid AS VARCHAR) AS uuid, CAST(event AS VARCHAR) AS event,
        CAST(properties AS VARCHAR) AS properties, CAST("timestamp" AS TIMESTAMPTZ) AS timestamp,
        CAST(team_id AS BIGINT) AS team_id, CAST(team_id AS BIGINT) AS project_id,
        CAST(distinct_id AS VARCHAR) AS distinct_id, CAST(elements_chain AS VARCHAR) AS elements_chain,
        CAST(created_at AS TIMESTAMPTZ) AS created_at, CAST(person_id AS VARCHAR) AS person_id,
        CAST(person_created_at AS TIMESTAMPTZ) AS person_created_at, CAST(person_properties AS VARCHAR) AS person_properties,
        CAST(group0_properties AS VARCHAR) AS group0_properties, CAST(group1_properties AS VARCHAR) AS group1_properties,
        CAST(group2_properties AS VARCHAR) AS group2_properties, CAST(group3_properties AS VARCHAR) AS group3_properties,
        CAST(group4_properties AS VARCHAR) AS group4_properties, CAST(group0_created_at AS TIMESTAMPTZ) AS group0_created_at,
        CAST(group1_created_at AS TIMESTAMPTZ) AS group1_created_at, CAST(group2_created_at AS TIMESTAMPTZ) AS group2_created_at,
        CAST(group3_created_at AS TIMESTAMPTZ) AS group3_created_at, CAST(group4_created_at AS TIMESTAMPTZ) AS group4_created_at,
        CAST(person_mode AS VARCHAR) AS person_mode, CAST(historical_migration AS BOOLEAN) AS historical_migration,
        CAST(_inserted_at AS TIMESTAMPTZ) AS _inserted_at
    FROM frozen_v1.events_file_view
),
left_difference AS (
    SELECT uuid, event, properties, timestamp, team_id, project_id, distinct_id, elements_chain, created_at,
        person_id, person_created_at, person_properties, group0_properties, group1_properties, group2_properties,
        group3_properties, group4_properties, group0_created_at, group1_created_at, group2_created_at,
        group3_created_at, group4_created_at, person_mode, historical_migration, _inserted_at
    FROM source_events
    EXCEPT ALL
    SELECT uuid, event, properties, timestamp, team_id, project_id, distinct_id, elements_chain, created_at,
        person_id, person_created_at, person_properties, group0_properties, group1_properties, group2_properties,
        group3_properties, group4_properties, group0_created_at, group1_created_at, group2_created_at,
        group3_created_at, group4_created_at, person_mode, historical_migration, _inserted_at
    FROM posthog.events
),
right_difference AS (
    SELECT uuid, event, properties, timestamp, team_id, project_id, distinct_id, elements_chain, created_at,
        person_id, person_created_at, person_properties, group0_properties, group1_properties, group2_properties,
        group3_properties, group4_properties, group0_created_at, group1_created_at, group2_created_at,
        group3_created_at, group4_created_at, person_mode, historical_migration, _inserted_at
    FROM posthog.events
    EXCEPT ALL
    SELECT uuid, event, properties, timestamp, team_id, project_id, distinct_id, elements_chain, created_at,
        person_id, person_created_at, person_properties, group0_properties, group1_properties, group2_properties,
        group3_properties, group4_properties, group0_created_at, group1_created_at, group2_created_at,
        group3_created_at, group4_created_at, person_mode, historical_migration, _inserted_at
    FROM source_events
)
SELECT CASE
    WHEN (SELECT COUNT(*) FROM left_difference) = 0 AND (SELECT COUNT(*) FROM right_difference) = 0 THEN 1
    ELSE error('posthog events parity mismatch')
END;

WITH source_persons AS (
    SELECT
        CAST(team_id AS BIGINT) AS team_id, CAST(distinct_id AS VARCHAR) AS distinct_id,
        CAST(id AS VARCHAR) AS id, CAST(properties AS VARCHAR) AS properties,
        CAST(created_at AS TIMESTAMPTZ) AS created_at, CAST(is_identified AS BOOLEAN) AS is_identified,
        CAST(person_distinct_id_version AS BIGINT) AS person_distinct_id_version,
        CAST(person_version AS UBIGINT) AS person_version, CAST(_timestamp AS TIMESTAMPTZ) AS _timestamp,
        CAST(_inserted_at AS TIMESTAMPTZ) AS _inserted_at
    FROM frozen_v1.persons_file_view
),
left_difference AS (
    SELECT team_id, distinct_id, id, properties, created_at, is_identified,
        person_distinct_id_version, person_version, _timestamp, _inserted_at
    FROM source_persons
    EXCEPT ALL
    SELECT team_id, distinct_id, id, properties, created_at, is_identified,
        person_distinct_id_version, person_version, _timestamp, _inserted_at
    FROM posthog.persons
),
right_difference AS (
    SELECT team_id, distinct_id, id, properties, created_at, is_identified,
        person_distinct_id_version, person_version, _timestamp, _inserted_at
    FROM posthog.persons
    EXCEPT ALL
    SELECT team_id, distinct_id, id, properties, created_at, is_identified,
        person_distinct_id_version, person_version, _timestamp, _inserted_at
    FROM source_persons
)
SELECT CASE
    WHEN (SELECT COUNT(*) FROM left_difference) = 0 AND (SELECT COUNT(*) FROM right_difference) = 0 THEN 1
    ELSE error('posthog persons parity mismatch')
END;
