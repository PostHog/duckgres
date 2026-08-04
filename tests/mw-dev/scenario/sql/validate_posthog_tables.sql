-- Validation for the registered PostHog DuckLake tables. These assertions use
-- schemas and file metadata only: the perf step is the first one that reads
-- the fixture rows.

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

-- Exact file-list equality proves the tables refer to precisely the frozen
-- objects. No data-file scan is required for this check.
WITH expected_files AS (
    SELECT 'events' AS table_name, file AS data_file
    FROM glob('${env:DUCKGRES_SCENARIO_FROZEN_S3_URI}events/*.parquet')
    UNION ALL
    SELECT 'persons' AS table_name, file AS data_file
    FROM glob('${env:DUCKGRES_SCENARIO_FROZEN_S3_URI}persons/*.parquet')
),
registered_files AS (
    SELECT 'events' AS table_name, data_file, delete_file
    FROM ducklake_list_files('ducklake', 'events', schema => 'posthog')
    UNION ALL
    SELECT 'persons' AS table_name, data_file, delete_file
    FROM ducklake_list_files('ducklake', 'persons', schema => 'posthog')
),
missing_registrations AS (
    SELECT table_name, data_file FROM expected_files
    EXCEPT ALL
    SELECT table_name, data_file FROM registered_files
),
unexpected_registrations AS (
    SELECT table_name, data_file FROM registered_files
    EXCEPT ALL
    SELECT table_name, data_file FROM expected_files
),
delete_files AS (
    SELECT table_name FROM registered_files WHERE delete_file IS NOT NULL
)
SELECT CASE
    WHEN (SELECT COUNT(*) FROM missing_registrations) = 0
     AND (SELECT COUNT(*) FROM unexpected_registrations) = 0
     AND (SELECT COUNT(*) FROM delete_files) = 0 THEN 1
    ELSE error('posthog frozen-file registration mismatch')
END;
