/*
    MODEL: fct_gold_correlation
    LAYER: Gold (Marts)
    DESCRIPTION: 
    Calculates the rolling 12-month Pearson correlation between Gold Prices and 10Y Real Interest Rates.
    This is a key macro-economic indicator for gold valuation.
    Mathematical Logic: Rolling window correlation using native CORR() function.
*/

{{ config(materialized='table') }}

WITH prices AS (
    SELECT 
        date_trunc('month', price_date) as month,
        AVG(price_usd_per_oz) as avg_price
    FROM {{ ref('stg_gold_prices') }}
    GROUP BY 1
),

rates AS (
    SELECT 
        date_trunc('month', observation_date) as month,
        AVG(real_rate_10y) as avg_rate
    FROM {{ ref('stg_real_rates') }}
    GROUP BY 1
),

joined AS (
    SELECT 
        p.month,
        p.avg_price,
        r.avg_rate
    FROM prices p
    JOIN rates r ON p.month = r.month
)

SELECT 
    month,
    avg_price,
    avg_rate,
    -- Pearson Correlation via SQL Window Functions
    CORR(avg_price, avg_rate) OVER (
        ORDER BY month 
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_corr_12m
FROM joined
QUALIFY rolling_corr_12m IS NOT NULL
ORDER BY month DESC
