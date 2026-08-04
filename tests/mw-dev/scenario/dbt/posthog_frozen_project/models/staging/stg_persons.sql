SELECT
    row_number() OVER () AS person_snapshot_id,
    _timestamp AS person_created_at,
    CAST(date_trunc('day', _timestamp) AS DATE) AS person_day
FROM {{ source('frozen_v1', 'persons_file_view') }}
WHERE _timestamp IS NOT NULL
