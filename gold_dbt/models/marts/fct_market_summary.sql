/*
    MODEL: fct_market_summary
    DESCRIPTION: 
    Gold Layer - Monthly Market Summary.
    Aggregates Price, Reserves, FX-Rates and Correlation into a flat analytical table.
*/

{{ config(materialized='table') }}

WITH base_months AS (
    SELECT DISTINCT date_trunc('month', price_date) AS month
    FROM {{ ref('stg_gold_prices') }}
),

gold_prices AS (
    SELECT 
        date_trunc('month', price_date) AS month,
        AVG(price_usd_per_oz) AS avg_gold_price_usd,
        AVG(price_usd_per_ton) AS avg_gold_price_usd_ton
    FROM {{ ref('stg_gold_prices') }}
    GROUP BY 1
),

reserves AS (
    SELECT 
        date_trunc('month', observation_date) AS month,
        SUM(gold_reserves_tonnes) AS total_gold_reserves_tonnes
    FROM {{ ref('stg_gold_reserves_api') }}
    GROUP BY 1
),

fx AS (
    SELECT 
        date_trunc('month', observation_date) AS month,
        AVG(usd_per_eur) AS avg_fx_usd_eur
    FROM {{ ref('stg_fx_usd_eur_api') }}
    GROUP BY 1
),

correlation AS (
    SELECT
        month,
        rolling_corr_12m
    FROM {{ ref('fct_gold_correlation') }}
)
SELECT 
    b.month,
    p.avg_gold_price_usd,
    p.avg_gold_price_usd_ton,
    r.total_gold_reserves_tonnes,
    f.avg_fx_usd_eur,
    c.rolling_corr_12m,
    -- Basic calculation for EUR-denominated Gold Price
    (p.avg_gold_price_usd / NULLIF(f.avg_fx_usd_eur, 0)) AS gold_price_eur
FROM base_months b
LEFT JOIN gold_prices p ON b.month = p.month
LEFT JOIN reserves r ON b.month = r.month
LEFT JOIN fx f ON b.month = f.month
LEFT JOIN correlation c ON b.month = c.month
WHERE p.avg_gold_price_usd IS NOT NULL
ORDER BY b.month DESC
