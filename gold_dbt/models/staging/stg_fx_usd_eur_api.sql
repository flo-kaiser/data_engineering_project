{{ config(materialized='view') }}

WITH raw AS (
    SELECT * FROM {{ source('bronze', 'fx_usd_eur_api') }}
)

SELECT
    CAST(period AS DATE) AS observation_date,
    CAST(value AS {{ type_float() }}) AS usd_per_eur,
    'DBnomics: ECB/EXR/M.USD.EUR.SP00.A' AS source
FROM raw
WHERE value IS NOT NULL
