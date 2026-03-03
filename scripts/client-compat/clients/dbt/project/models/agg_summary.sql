-- A table materialization that aggregates from the view
{{ config(materialized='table') }}

SELECT
    count(*) AS total_rows,
    min(value) AS min_value,
    max(value) AS max_value,
    avg(value) AS avg_value
FROM {{ ref('staging_numbers') }}
