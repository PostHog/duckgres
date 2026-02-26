-- A simple view model that generates test data
{{ config(materialized='view') }}

SELECT
    i AS id,
    'item_' || i::VARCHAR AS name,
    (i * 10.5)::DOUBLE AS value
FROM generate_series(1, 100) AS t(i)
