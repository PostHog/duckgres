SELECT
    event_day,
    feature_area,
    count(*) AS users,
    sum(events) AS events,
    sum(pageview_events) AS pageview_events,
    sum(feature_events) AS feature_events
FROM {{ ref('int_person_feature_usage_daily') }}
GROUP BY 1, 2
