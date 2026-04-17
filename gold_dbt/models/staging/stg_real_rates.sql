{{ config(materialized='view') }}

WITH raw AS (
    SELECT * FROM {{ source('bronze', 'real_interest_rates_api') }}
)

SELECT
    CAST(period AS DATE) AS observation_date,
    CAST(value AS DOUBLE) AS real_rate_10y,
    'DBnomics: FED/H15/RIFLGFCY10_XII_N.M' AS source
FROM raw
WHERE value IS NOT NULL
