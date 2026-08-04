WITH person_events AS (
    SELECT
        person_id,
        min(CASE WHEN event_category = 'pageview' THEN event_timestamp END) AS first_pageview_timestamp,
        min(CASE WHEN event_category = 'feature' THEN event_timestamp END) AS first_feature_timestamp,
        min(CASE WHEN event_category NOT IN ('pageview', 'autocapture') THEN event_timestamp END) AS first_product_event_timestamp,
        count(*) AS total_events,
        count(DISTINCT event_day) AS active_days
    FROM {{ ref('stg_events') }}
    WHERE person_id IS NOT NULL
    GROUP BY 1
)

SELECT
    first_seen.person_id,
    first_seen.first_seen_timestamp,
    person_events.first_pageview_timestamp,
    person_events.first_feature_timestamp,
    person_events.first_product_event_timestamp,
    person_events.first_pageview_timestamp IS NOT NULL AS saw_pageview,
    person_events.first_feature_timestamp IS NOT NULL AS used_feature,
    person_events.first_product_event_timestamp IS NOT NULL AS performed_product_action,
    person_events.first_feature_timestamp IS NOT NULL
        OR person_events.first_product_event_timestamp IS NOT NULL AS activated,
    person_events.total_events,
    person_events.active_days
FROM {{ ref('int_person_first_seen') }} AS first_seen
LEFT JOIN person_events
    ON first_seen.person_id = person_events.person_id
