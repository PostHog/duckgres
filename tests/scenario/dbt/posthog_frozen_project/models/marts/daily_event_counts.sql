SELECT
    date_trunc('day', event_timestamp) AS event_day,
    event,
    count(*) AS events
FROM {{ ref('stg_events') }}
GROUP BY 1, 2
