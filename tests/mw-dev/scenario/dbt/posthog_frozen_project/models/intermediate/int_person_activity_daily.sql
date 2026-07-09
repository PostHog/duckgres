SELECT
    person_id,
    event_day,
    count(*) AS events,
    min(event_timestamp) AS first_event_timestamp,
    max(event_timestamp) AS last_event_timestamp,
    sum(CASE WHEN event_category = 'pageview' THEN 1 ELSE 0 END) AS pageview_events,
    sum(CASE WHEN event_category = 'feature' THEN 1 ELSE 0 END) AS feature_events,
    sum(CASE WHEN event_category = 'autocapture' THEN 1 ELSE 0 END) AS autocapture_events,
    count(DISTINCT event) AS unique_event_names,
    count(DISTINCT feature_area) AS feature_areas_touched
FROM {{ ref('stg_events') }}
WHERE person_id IS NOT NULL
GROUP BY 1, 2
