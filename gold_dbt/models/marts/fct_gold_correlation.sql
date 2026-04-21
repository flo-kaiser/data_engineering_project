/*
    MODEL: fct_gold_correlation
    LAYER: Gold (Marts)
    DESCRIPTION: 
    Calculates the rolling 12-month Pearson correlation between Gold Prices and 10Y Real Interest Rates.
    Mathematical Logic: Rolling window correlation implemented with manual Pearson formula for BigQuery compatibility.
*/

{{ config(materialized='table') }}

WITH prices AS (
    SELECT 
        {{ date_trunc('month', 'price_date') }} as month,
        AVG(price_usd_per_oz) as x
    FROM {{ ref('stg_gold_prices') }}
    GROUP BY 1
),

rates AS (
    SELECT 
        {{ date_trunc('month', 'observation_date') }} as month,
        AVG(real_rate_10y) as y
    FROM {{ ref('stg_real_rates') }}
    GROUP BY 1
),

joined AS (
    SELECT 
        p.month,
        p.x,
        r.y
    FROM prices p
    JOIN rates r ON p.month = r.month
),

rolling_stats AS (
    SELECT
        month,
        x,
        y,
        COUNT(*) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as n,
        SUM(x) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as sum_x,
        SUM(y) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as sum_y,
        SUM(x*x) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as sum_xx,
        SUM(y*y) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as sum_yy,
        SUM(x*y) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as sum_xy
    FROM joined
),

calculated AS (
    SELECT 
        month,
        x as avg_price,
        y as avg_rate,
        -- Pearson Correlation Formula: (n*sum_xy - sum_x*sum_y) / sqrt((n*sum_xx - sum_x^2) * (n*sum_yy - sum_y^2))
        {{ safe_divide('(n * sum_xy - sum_x * sum_y)', 'SQRT(ABS((n * sum_xx - POWER(sum_x, 2)) * (n * sum_yy - POWER(sum_y, 2))))') }} AS rolling_corr_12m
    FROM rolling_stats
)

SELECT *
FROM calculated
WHERE rolling_corr_12m IS NOT NULL
ORDER BY month DESC
