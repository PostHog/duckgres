SELECT
    person_id,
    count(*) AS events,
    min(event_timestamp) AS first_event_timestamp,
    max(event_timestamp) AS last_event_timestamp
FROM {{ ref('stg_events') }}
WHERE person_id IS NOT NULL
GROUP BY 1
