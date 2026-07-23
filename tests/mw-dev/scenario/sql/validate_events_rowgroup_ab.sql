-- SET VARIABLE is intentionally first. Duckgres treats this as a native
-- DuckDB utility batch, preserving both variables for the footer assertions.
SET VARIABLE events_rg_128mib_files = (
    SELECT list(data_file ORDER BY data_file)
    FROM ducklake_list_files('ducklake', 'events_rg_128mib', schema => 'rowgroup_ab')
    WHERE data_file IS NOT NULL
);

SET VARIABLE events_rg_1gib_files = (
    SELECT list(data_file ORDER BY data_file)
    FROM ducklake_list_files('ducklake', 'events_rg_1gib', schema => 'rowgroup_ab')
    WHERE data_file IS NOT NULL
);

WITH
events_128mib AS (
    SELECT
        count(*) AS row_count,
        bit_xor(hash(
            uuid,
            event,
            properties,
            "timestamp",
            team_id,
            project_id,
            distinct_id,
            elements_chain,
            created_at,
            person_id,
            person_created_at,
            person_properties,
            group0_properties,
            group1_properties,
            group2_properties,
            group3_properties,
            group4_properties,
            group0_created_at,
            group1_created_at,
            group2_created_at,
            group3_created_at,
            group4_created_at,
            person_mode,
            historical_migration,
            _inserted_at
        )) AS fingerprint
    FROM rowgroup_ab.events_rg_128mib
),
events_1gib AS (
    SELECT
        count(*) AS row_count,
        bit_xor(hash(
            uuid,
            event,
            properties,
            "timestamp",
            team_id,
            project_id,
            distinct_id,
            elements_chain,
            created_at,
            person_id,
            person_created_at,
            person_properties,
            group0_properties,
            group1_properties,
            group2_properties,
            group3_properties,
            group4_properties,
            group0_created_at,
            group1_created_at,
            group2_created_at,
            group3_created_at,
            group4_created_at,
            person_mode,
            historical_migration,
            _inserted_at
        )) AS fingerprint
    FROM rowgroup_ab.events_rg_1gib
)
SELECT CASE
    WHEN events_128mib.row_count = 524288
     AND events_1gib.row_count = events_128mib.row_count
     AND events_1gib.fingerprint = events_128mib.fingerprint
    THEN true
    ELSE error(printf(
        'row-group A/B content mismatch: 128MiB rows=%d; 1GiB rows=%d',
        events_128mib.row_count,
        events_1gib.row_count
    ))
END AS contents_equal
FROM events_128mib, events_1gib;

WITH geometry AS (
    SELECT
        '128mib' AS layout,
        count(*) AS file_count,
        coalesce(sum(num_rows), 0) AS row_count,
        coalesce(sum(num_row_groups), 0) AS row_group_count
    FROM parquet_file_metadata(getvariable('events_rg_128mib_files'))

    UNION ALL

    SELECT
        '1gib' AS layout,
        count(*) AS file_count,
        coalesce(sum(num_rows), 0) AS row_count,
        coalesce(sum(num_row_groups), 0) AS row_group_count
    FROM parquet_file_metadata(getvariable('events_rg_1gib_files'))
),
actual AS (
    SELECT
        max(file_count) FILTER (WHERE layout = '128mib') AS files_128mib,
        max(row_count) FILTER (WHERE layout = '128mib') AS rows_128mib,
        max(row_group_count) FILTER (WHERE layout = '128mib') AS row_groups_128mib,
        max(file_count) FILTER (WHERE layout = '1gib') AS files_1gib,
        max(row_count) FILTER (WHERE layout = '1gib') AS rows_1gib,
        max(row_group_count) FILTER (WHERE layout = '1gib') AS row_groups_1gib
    FROM geometry
)
SELECT CASE
    WHEN files_128mib = 1
     AND rows_128mib = 524288
     AND files_1gib = 1
     AND rows_1gib = 524288
     AND row_groups_1gib < row_groups_128mib
    THEN true
    ELSE error(printf(
        'unexpected physical layout: 128MiB files=%d rows=%d row_groups=%d; 1GiB files=%d rows=%d row_groups=%d',
        files_128mib,
        rows_128mib,
        row_groups_128mib,
        files_1gib,
        rows_1gib,
        row_groups_1gib
    ))
END AS physical_layout_ok
FROM actual;

WITH expected_options(scope_entry, option_name, value) AS (
    VALUES
        ('rowgroup_ab.events_rg_128mib', 'parquet_compression', 'zstd'),
        ('rowgroup_ab.events_rg_128mib', 'parquet_version', 'V2'),
        ('rowgroup_ab.events_rg_128mib', 'parquet_row_group_size', '250000'),
        ('rowgroup_ab.events_rg_128mib', 'parquet_row_group_size_bytes', '134217728'),
        ('rowgroup_ab.events_rg_128mib', 'per_thread_output', 'false'),
        ('rowgroup_ab.events_rg_1gib', 'parquet_compression', 'zstd'),
        ('rowgroup_ab.events_rg_1gib', 'parquet_version', 'V2'),
        ('rowgroup_ab.events_rg_1gib', 'parquet_row_group_size', '250000'),
        ('rowgroup_ab.events_rg_1gib', 'parquet_row_group_size_bytes', '1073741824'),
        ('rowgroup_ab.events_rg_1gib', 'per_thread_output', 'false')
),
missing_options AS (
    SELECT * FROM expected_options
    EXCEPT
    SELECT scope_entry, option_name, value
    FROM ducklake_options('ducklake')
    WHERE scope = 'TABLE'
)
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM missing_options)
    THEN error('row-group A/B tables are missing expected table-scoped writer options')
    ELSE true
END AS options_ok;
