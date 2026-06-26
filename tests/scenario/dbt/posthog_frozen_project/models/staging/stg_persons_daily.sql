SELECT
    date_trunc('day', _timestamp) AS person_day,
    count(*) AS persons
FROM {{ source('frozen_v1', 'persons_file_view') }}
WHERE _timestamp IS NOT NULL
GROUP BY 1
