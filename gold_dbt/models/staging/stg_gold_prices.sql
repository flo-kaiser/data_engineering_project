/*
    MODEL: stg_gold_prices
    DESCRIPTION: 
    Silver Layer - Staging model for Gold Spot Prices.
    Converts Fine Ounces to Metric Tons for global standardization.
*/

{{ config(materialized='view') }}

WITH raw AS (
    SELECT * FROM {{ source('bronze', 'gold_prices_api') }}
)

SELECT
    CAST(period AS DATE) AS price_date,
    CAST(value AS DOUBLE) AS price_usd_per_oz,
    -- Requirement: Umrechnung von Feinunzen in metrische Tonnen (1t = 32150.7 oz)
    CAST(value * 32150.7 AS DOUBLE) AS price_usd_per_ton,
    'USD' AS currency,
    'DBnomics: WB/commodity_prices/FGOLD-1W' AS source
FROM raw
WHERE value IS NOT NULL
