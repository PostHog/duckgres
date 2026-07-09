WITH person_activity AS (
    SELECT
        person_id,
        event_day AS activity_day,
        events
    FROM {{ ref('int_person_activity_daily') }}
),

retention_rows AS (
    SELECT
        first_seen.person_id,
        CAST(date_trunc('day', first_seen.first_seen_timestamp) AS DATE) AS cohort_day,
        person_activity.activity_day,
        date_diff(
            'day',
            CAST(date_trunc('day', first_seen.first_seen_timestamp) AS DATE),
            person_activity.activity_day
        ) AS days_since_first_seen,
        person_activity.events
    FROM {{ ref('int_person_first_seen') }} AS first_seen
    INNER JOIN person_activity
        ON first_seen.person_id = person_activity.person_id
)

SELECT
    cohort_day,
    activity_day,
    days_since_first_seen,
    count(DISTINCT person_id) AS retained_persons,
    sum(events) AS events
FROM retention_rows
WHERE days_since_first_seen >= 0
GROUP BY 1, 2, 3
