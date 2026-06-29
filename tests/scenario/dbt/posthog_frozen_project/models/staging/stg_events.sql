SELECT
    event,
    person_id,
    "timestamp" AS event_timestamp
FROM {{ source('frozen_v1', 'events_file_view') }}
WHERE "timestamp" IS NOT NULL
