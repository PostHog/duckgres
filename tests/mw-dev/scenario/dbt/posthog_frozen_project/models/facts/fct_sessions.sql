SELECT
    session_id,
    person_id,
    min(event_timestamp) AS session_start,
    max(event_timestamp) AS session_end,
    CAST(date_trunc('day', min(event_timestamp)) AS DATE) AS session_day,
    date_diff('second', min(event_timestamp), max(event_timestamp)) AS session_duration_seconds,
    count(*) AS events,
    sum(CASE WHEN event_category = 'pageview' THEN 1 ELSE 0 END) AS pageview_events,
    sum(CASE WHEN event_category = 'feature' THEN 1 ELSE 0 END) AS feature_events,
    count(DISTINCT feature_area) AS feature_areas_touched
FROM {{ ref('int_sessionized_events') }}
GROUP BY 1, 2
