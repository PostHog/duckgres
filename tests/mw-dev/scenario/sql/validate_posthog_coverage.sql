-- Fail rather than record misleading fast results for an empty workload.
-- Checks expose only fixed assertion labels, never fixture identifiers.
SELECT CASE WHEN EXISTS (
    SELECT 1 FROM posthog.events
    WHERE timestamp >= TIMESTAMPTZ '2026-03-17 00:00:00+00'
      AND timestamp < TIMESTAMPTZ '2026-03-24 00:00:00+00'
      AND event = '$pageview' AND person_id IS NOT NULL
) THEN 1 ELSE error('coverage fixture has no first-week pageview actors') END;

SELECT CASE WHEN EXISTS (
    SELECT 1 FROM posthog.events
    WHERE timestamp >= TIMESTAMPTZ '2026-03-17 00:00:00+00'
      AND timestamp < TIMESTAMPTZ '2026-03-24 00:00:00+00'
      AND event = '$autocapture' AND person_id IS NOT NULL
) THEN 1 ELSE error('coverage fixture has no first-week autocapture actors') END;

SELECT CASE WHEN EXISTS (
    SELECT 1 FROM posthog.events
    WHERE timestamp >= TIMESTAMPTZ '2026-03-24 00:00:00+00'
      AND timestamp < TIMESTAMPTZ '2026-03-31 00:00:00+00'
      AND person_id IS NOT NULL
) THEN 1 ELSE error('coverage fixture has no return-week actors') END;

SELECT CASE WHEN EXISTS (
    SELECT 1 FROM posthog.events e
    INNER JOIN posthog.persons p ON e.team_id = p.team_id AND e.person_id = p.id
    WHERE e.timestamp >= TIMESTAMPTZ '2026-03-17 00:00:00+00'
      AND e.timestamp < TIMESTAMPTZ '2026-03-24 00:00:00+00'
) THEN 1 ELSE error('coverage fixture has no first-week event/person overlap') END;

WITH starts AS (
    SELECT team_id, person_id, MIN(timestamp) AS started_at
    FROM posthog.events
    WHERE timestamp >= TIMESTAMPTZ '2026-03-17 00:00:00+00'
      AND timestamp < TIMESTAMPTZ '2026-03-24 00:00:00+00'
      AND event = '$pageview' AND person_id IS NOT NULL
    GROUP BY team_id, person_id
)
SELECT CASE WHEN EXISTS (
    SELECT 1 FROM starts s
    INNER JOIN posthog.events e ON s.team_id = e.team_id AND s.person_id = e.person_id
    WHERE e.event = '$autocapture' AND e.timestamp > s.started_at
      AND e.timestamp <= s.started_at + INTERVAL '7' DAY
      AND e.timestamp >= TIMESTAMPTZ '2026-03-17 00:00:00+00'
      AND e.timestamp < TIMESTAMPTZ '2026-03-31 00:00:00+00'
) THEN 1 ELSE error('coverage fixture has no ordered funnel completions') END;

SELECT CASE WHEN EXISTS (
    SELECT 1 FROM posthog.events first_week
    INNER JOIN posthog.events second_week
      ON first_week.team_id = second_week.team_id AND first_week.person_id = second_week.person_id
    WHERE first_week.timestamp >= TIMESTAMPTZ '2026-03-17 00:00:00+00'
      AND first_week.timestamp < TIMESTAMPTZ '2026-03-24 00:00:00+00'
      AND second_week.timestamp >= TIMESTAMPTZ '2026-03-24 00:00:00+00'
      AND second_week.timestamp < TIMESTAMPTZ '2026-03-31 00:00:00+00'
) THEN 1 ELSE error('coverage fixture has no retained actors across weeks') END;
