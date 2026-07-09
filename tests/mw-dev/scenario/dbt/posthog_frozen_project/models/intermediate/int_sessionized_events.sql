WITH ordered_events AS (
    SELECT
        event_row_id,
        person_id,
        event,
        event_timestamp,
        event_day,
        event_category,
        feature_area,
        lag(event_timestamp) OVER (
            PARTITION BY person_id
            ORDER BY event_timestamp, event_row_id
        ) AS previous_event_timestamp
    FROM {{ ref('stg_events') }}
    WHERE person_id IS NOT NULL
),

session_boundaries AS (
    SELECT
        *,
        CASE
            WHEN previous_event_timestamp IS NULL THEN 1
            WHEN date_diff('minute', previous_event_timestamp, event_timestamp) > 30 THEN 1
            ELSE 0
        END AS is_new_session
    FROM ordered_events
),

numbered_sessions AS (
    SELECT
        *,
        sum(is_new_session) OVER (
            PARTITION BY person_id
            ORDER BY event_timestamp, event_row_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_number
    FROM session_boundaries
)

SELECT
    CAST(person_id AS VARCHAR) || '-' || CAST(session_number AS VARCHAR) AS session_id,
    session_number,
    event_row_id,
    person_id,
    event,
    event_timestamp,
    event_day,
    event_category,
    feature_area,
    previous_event_timestamp,
    is_new_session
FROM numbered_sessions
