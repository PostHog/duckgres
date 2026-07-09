WITH days AS (
    SELECT event_day AS metric_day FROM {{ ref('fct_user_activity_daily') }}
    UNION
    SELECT session_day AS metric_day FROM {{ ref('fct_sessions') }}
    UNION
    SELECT CAST(date_trunc('day', first_seen_timestamp) AS DATE) AS metric_day FROM {{ ref('fct_activation_funnel') }}
    UNION
    SELECT cohort_day AS metric_day FROM {{ ref('fct_retention_daily') }}
    UNION
    SELECT person_day AS metric_day FROM {{ ref('stg_persons') }}
),

session_metrics AS (
    SELECT
        session_day,
        count(*) AS sessions,
        count(DISTINCT person_id) AS session_users,
        avg(session_duration_seconds) AS avg_session_duration_seconds
    FROM {{ ref('fct_sessions') }}
    GROUP BY 1
),

activation_metrics AS (
    SELECT
        CAST(date_trunc('day', first_seen_timestamp) AS DATE) AS first_seen_day,
        count(*) AS first_seen_users,
        sum(CASE WHEN saw_pageview THEN 1 ELSE 0 END) AS users_with_pageview,
        sum(CASE WHEN used_feature THEN 1 ELSE 0 END) AS users_with_feature,
        sum(CASE WHEN activated THEN 1 ELSE 0 END) AS activated_users
    FROM {{ ref('fct_activation_funnel') }}
    GROUP BY 1
),

retention_metrics AS (
    SELECT
        cohort_day,
        sum(CASE WHEN days_since_first_seen = 1 THEN retained_persons ELSE 0 END) AS retained_users_d1,
        sum(CASE WHEN days_since_first_seen = 7 THEN retained_persons ELSE 0 END) AS retained_users_d7,
        sum(CASE WHEN days_since_first_seen = 30 THEN retained_persons ELSE 0 END) AS retained_users_d30,
        sum(CASE WHEN days_since_first_seen > 0 THEN retained_persons ELSE 0 END) AS retained_users
    FROM {{ ref('fct_retention_daily') }}
    GROUP BY 1
),

person_metrics AS (
    SELECT
        person_day,
        count(*) AS person_snapshots
    FROM {{ ref('stg_persons') }}
    GROUP BY 1
)

SELECT
    days.metric_day,
    coalesce(activity.active_persons, 0) AS daily_active_users,
    coalesce(activity.events, 0) AS total_events,
    coalesce(activity.pageview_events, 0) AS pageview_events,
    coalesce(activity.feature_events, 0) AS feature_events,
    coalesce(activity.autocapture_events, 0) AS autocapture_events,
    coalesce(sessions.sessions, 0) AS sessions,
    coalesce(sessions.session_users, 0) AS session_users,
    coalesce(sessions.avg_session_duration_seconds, 0) AS avg_session_duration_seconds,
    coalesce(activation.first_seen_users, 0) AS first_seen_users,
    coalesce(activation.users_with_pageview, 0) AS users_with_pageview,
    coalesce(activation.users_with_feature, 0) AS users_with_feature,
    coalesce(activation.activated_users, 0) AS activated_users,
    coalesce(retention.retained_users, 0) AS retained_users,
    coalesce(retention.retained_users_d1, 0) AS retained_users_d1,
    coalesce(retention.retained_users_d7, 0) AS retained_users_d7,
    coalesce(retention.retained_users_d30, 0) AS retained_users_d30,
    coalesce(persons.person_snapshots, 0) AS person_snapshots
FROM days
LEFT JOIN {{ ref('fct_user_activity_daily') }} AS activity
    ON days.metric_day = activity.event_day
LEFT JOIN session_metrics AS sessions
    ON days.metric_day = sessions.session_day
LEFT JOIN activation_metrics AS activation
    ON days.metric_day = activation.first_seen_day
LEFT JOIN retention_metrics AS retention
    ON days.metric_day = retention.cohort_day
LEFT JOIN person_metrics AS persons
    ON days.metric_day = persons.person_day
