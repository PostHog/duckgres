SET threads = 1;
SET preserve_insertion_order = false;

CREATE SCHEMA IF NOT EXISTS rowgroup_ab;

CREATE OR REPLACE VIEW rowgroup_ab.source_events AS
SELECT *
FROM read_parquet(
    '${env:DUCKGRES_SCENARIO_FROZEN_S3_URI}events/*year=2024__month=10__day=01__*.parquet',
    union_by_name = true
);

DROP TABLE IF EXISTS rowgroup_ab.events_rg_128mib;
DROP TABLE IF EXISTS rowgroup_ab.events_rg_1gib;

CREATE TABLE rowgroup_ab.events_rg_128mib AS
SELECT * FROM rowgroup_ab.source_events LIMIT 0;

CALL ducklake_set_option('ducklake', 'data_inlining_row_limit', 0,
    schema => 'rowgroup_ab', table_name => 'events_rg_128mib');
CALL ducklake_set_option('ducklake', 'parquet_compression', 'zstd',
    schema => 'rowgroup_ab', table_name => 'events_rg_128mib');
CALL ducklake_set_option('ducklake', 'parquet_version', 2,
    schema => 'rowgroup_ab', table_name => 'events_rg_128mib');
CALL ducklake_set_option('ducklake', 'parquet_row_group_size', 250000,
    schema => 'rowgroup_ab', table_name => 'events_rg_128mib');
CALL ducklake_set_option('ducklake', 'parquet_row_group_size_bytes', '128MiB',
    schema => 'rowgroup_ab', table_name => 'events_rg_128mib');
CALL ducklake_set_option('ducklake', 'target_file_size', '10GB',
    schema => 'rowgroup_ab', table_name => 'events_rg_128mib');
CALL ducklake_set_option('ducklake', 'per_thread_output', false,
    schema => 'rowgroup_ab', table_name => 'events_rg_128mib');

INSERT INTO rowgroup_ab.events_rg_128mib
SELECT *
FROM rowgroup_ab.source_events
LIMIT 524288;

CREATE TABLE rowgroup_ab.events_rg_1gib AS
SELECT * FROM rowgroup_ab.events_rg_128mib LIMIT 0;

CALL ducklake_set_option('ducklake', 'data_inlining_row_limit', 0,
    schema => 'rowgroup_ab', table_name => 'events_rg_1gib');
CALL ducklake_set_option('ducklake', 'parquet_compression', 'zstd',
    schema => 'rowgroup_ab', table_name => 'events_rg_1gib');
CALL ducklake_set_option('ducklake', 'parquet_version', 2,
    schema => 'rowgroup_ab', table_name => 'events_rg_1gib');
CALL ducklake_set_option('ducklake', 'parquet_row_group_size', 250000,
    schema => 'rowgroup_ab', table_name => 'events_rg_1gib');
CALL ducklake_set_option('ducklake', 'parquet_row_group_size_bytes', '1GiB',
    schema => 'rowgroup_ab', table_name => 'events_rg_1gib');
CALL ducklake_set_option('ducklake', 'target_file_size', '10GB',
    schema => 'rowgroup_ab', table_name => 'events_rg_1gib');
CALL ducklake_set_option('ducklake', 'per_thread_output', false,
    schema => 'rowgroup_ab', table_name => 'events_rg_1gib');

-- Rewrite from the control table rather than rescanning S3. This locks the
-- candidate to the exact same rows. One writer thread keeps group boundaries
-- deterministic while allowing the row-group byte guard.
INSERT INTO rowgroup_ab.events_rg_1gib
SELECT * FROM rowgroup_ab.events_rg_128mib;

-- These are worker-global DuckDB settings. Restore their defaults before the
-- benchmark reconnects to this warm worker so scans use the requested profile.
RESET threads;
RESET preserve_insertion_order;
