SELECT
    event_days.event_day,
    coalesce(person_days.active_persons, 0) AS active_persons,
    event_days.events,
    event_days.pageview_events,
    event_days.feature_events,
    event_days.autocapture_events,
    event_days.unique_event_names,
    event_days.feature_areas_touched
FROM {{ ref('int_event_days') }} AS event_days
LEFT JOIN (
    SELECT
        event_day,
        count(*) AS active_persons
    FROM {{ ref('int_person_activity_daily') }}
    GROUP BY 1
) AS person_days
    ON event_days.event_day = person_days.event_day
