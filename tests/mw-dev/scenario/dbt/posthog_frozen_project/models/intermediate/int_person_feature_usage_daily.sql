SELECT
    person_id,
    event_day,
    feature_area,
    count(*) AS events,
    sum(CASE WHEN event_category = 'pageview' THEN 1 ELSE 0 END) AS pageview_events,
    sum(CASE WHEN event_category = 'feature' THEN 1 ELSE 0 END) AS feature_events
FROM {{ ref('stg_events') }}
WHERE person_id IS NOT NULL
GROUP BY 1, 2, 3
