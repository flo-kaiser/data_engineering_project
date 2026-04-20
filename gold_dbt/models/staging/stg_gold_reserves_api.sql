{{ config(materialized='view') }}

WITH raw AS (
    SELECT * FROM {{ source('bronze', 'gold_reserves_api') }}
)

SELECT
    CAST(period AS DATE) AS observation_date,
    CAST(value * 1000000 AS {{ type_float() }}) AS gold_reserves_ozt,
    -- Requirement: Umrechnung von Millionen Feinunzen in metrische Tonnen
    -- (value * 1.000.000) / 32150.7
    CAST((value * 1000000) / 32150.7 AS {{ type_float() }}) AS gold_reserves_tonnes,
    'DBnomics: IMF/IFS/M.W00.RAFAGOLDV_OZT' AS source
FROM raw
WHERE value IS NOT NULL
