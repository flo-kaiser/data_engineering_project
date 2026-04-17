/*
    MODEL: fct_gold_valuation_index
    DESCRIPTION: 
    Gold Layer - Valuation Index based on three macro-economic drivers.
    Ensures all input components (Correlation, FX, Reserves) are available.
*/

{{ config(materialized='table') }}

WITH base AS (
    SELECT 
        month,
        avg_gold_price_usd,
        avg_fx_usd_eur,
        total_gold_reserves_tonnes,
        rolling_corr_12m
    FROM {{ ref('fct_market_summary') }}
    -- Ensure all components for the valuation index are present
    WHERE rolling_corr_12m IS NOT NULL
      AND avg_fx_usd_eur IS NOT NULL
      AND total_gold_reserves_tonnes IS NOT NULL
),

calc AS (
    SELECT
        month,
        avg_gold_price_usd,
        (1 / NULLIF(avg_fx_usd_eur, 0)) AS eur_strength,
        total_gold_reserves_tonnes,
        rolling_corr_12m
    FROM base
)

SELECT
    month,
    avg_gold_price_usd,
    (total_gold_reserves_tonnes * 0.4 + (1/NULLIF(eur_strength, 0)) * 0.3 + ABS(rolling_corr_12m) * 0.3) AS valuation_score
FROM calc
ORDER BY month DESC
