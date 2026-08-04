SELECT
    row_number() OVER () AS event_row_id,
    event,
    person_id,
    "timestamp" AS event_timestamp,
    CAST(date_trunc('day', "timestamp") AS DATE) AS event_day,
    CASE
        WHEN event = '$pageview' THEN 'pageview'
        WHEN event = '$autocapture' THEN 'autocapture'
        WHEN event IN ('$identify', '$set') THEN 'identity'
        WHEN lower(event) LIKE '%feature%'
            OR lower(event) LIKE '%insight%'
            OR lower(event) LIKE '%dashboard%'
            OR lower(event) LIKE '%recording%'
            OR lower(event) LIKE '%experiment%'
            THEN 'feature'
        ELSE 'other'
    END AS event_category,
    CASE
        WHEN event IN ('$pageview', '$pageleave') THEN 'web'
        WHEN lower(event) LIKE '%dashboard%' THEN 'dashboards'
        WHEN lower(event) LIKE '%insight%' THEN 'insights'
        WHEN lower(event) LIKE '%feature%' THEN 'feature_flags'
        WHEN lower(event) LIKE '%recording%' THEN 'session_replay'
        WHEN lower(event) LIKE '%experiment%' THEN 'experiments'
        ELSE 'product'
    END AS feature_area
FROM {{ source('frozen_v1', 'events_file_view') }}
WHERE "timestamp" IS NOT NULL
    AND event IS NOT NULL
