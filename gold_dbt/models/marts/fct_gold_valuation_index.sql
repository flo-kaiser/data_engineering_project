/*
    MODEL: fct_gold_valuation_index
    DESCRIPTION: 
    Gold Layer - Valuation Index based on three macro-economic drivers.
    Mathematical Logic: Weighted score using normalized inputs:
    - 40% Gold Reserves (Global Central Bank accumulation)
    - 30% EUR Strength (Inverse of USD impact)
    - 30% Negative Correlation with Real Rates (Safe Haven Status)
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
    WHERE rolling_corr_12m IS NOT NULL
      AND avg_fx_usd_eur IS NOT NULL
      AND total_gold_reserves_tonnes IS NOT NULL
),

stats AS (
    -- Normalization helpers: Min/Max values for the components
    SELECT
        MIN(total_gold_reserves_tonnes) as min_res,
        MAX(total_gold_reserves_tonnes) as max_res,
        MIN(1/avg_fx_usd_eur) as min_eur,
        MAX(1/avg_fx_usd_eur) as max_eur
    FROM base
),

calc AS (
    SELECT
        b.month,
        b.avg_gold_price_usd,
        -- Normalize components to 0-1 scale. Use COALESCE to handle cases where max=min (division by zero)
        COALESCE((b.total_gold_reserves_tonnes - s.min_res) / NULLIF(s.max_res - s.min_res, 0), 0.5) AS norm_reserves,
        COALESCE(((1/b.avg_fx_usd_eur) - s.min_eur) / NULLIF(s.max_eur - s.min_eur, 0), 0.5) AS norm_eur_strength,
        -- Safe haven status is higher when correlation is more negative (close to -1)
        (1 - (b.rolling_corr_12m + 1) / 2) AS norm_safe_haven_status
    FROM base b, stats s
),

final AS (
    SELECT
        month,
        avg_gold_price_usd,
        (norm_reserves * 0.4 + norm_eur_strength * 0.3 + norm_safe_haven_status * 0.3) * 100 AS valuation_score
    FROM calc
)

SELECT * 
FROM final 
WHERE valuation_score IS NOT NULL 
  AND NOT CAST(valuation_score AS DOUBLE) = 'NaN'::DOUBLE
ORDER BY month DESC
