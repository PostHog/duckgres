WITH event_first_seen AS (
    SELECT
        person_id,
        min(event_timestamp) AS first_event_timestamp,
        max(event_timestamp) AS last_event_timestamp,
        count(*) AS lifetime_events,
        count(DISTINCT event_day) AS active_days
    FROM {{ ref('stg_events') }}
    WHERE person_id IS NOT NULL
    GROUP BY 1
),

person_dataset_bounds AS (
    SELECT
        count(*) AS person_snapshots
    FROM {{ ref('stg_persons') }}
)

SELECT
    event_first_seen.person_id,
    event_first_seen.first_event_timestamp,
    CAST(NULL AS TIMESTAMP) AS first_person_timestamp,
    event_first_seen.first_event_timestamp AS first_seen_timestamp,
    event_first_seen.last_event_timestamp,
    event_first_seen.lifetime_events,
    event_first_seen.active_days,
    person_dataset_bounds.person_snapshots
FROM event_first_seen
CROSS JOIN person_dataset_bounds
