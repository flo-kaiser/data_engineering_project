{{ config(materialized='view') }}

WITH raw AS (
    SELECT * FROM {{ source('bronze', 'gold_reserves_api') }}
)

SELECT
    CAST(period AS DATE) AS observation_date,
    CAST(value AS DOUBLE) AS gold_reserves_ozt,
    -- Requirement: Umrechnung von Feinunzen in metrische Tonnen
    -- 1t = 32150.7 oz
    CAST(value / 32150.7 AS DOUBLE) AS gold_reserves_tonnes,
    'DBnomics: IMF/IFS/M.W00.RAFAGOLDV_OZT' AS source
FROM raw
WHERE value IS NOT NULL
