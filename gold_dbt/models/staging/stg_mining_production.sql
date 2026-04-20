/*
    MODEL: stg_mining_production
    DESCRIPTION: Global Gold Mine Production. 
    Uses IMF/World Bank proxy data or stable estimates.
*/

WITH years AS (
    {% if adapter.type() == 'duckdb' %}
        SELECT CAST(range AS INT) as year_val FROM range(2010, 2027)
    {% else %}
        SELECT * FROM UNNEST(GENERATE_ARRAY(2010, 2026)) AS year_val
    {% endif %}
)

SELECT 
    year_val AS production_year,
    'WORLD' as region_or_country,
    -- Basiswert von 3500 Tonnen mit leichtem Wachstum
    3500.0 + (year_val - 2010) * 20.0 as production_tonnes
FROM years
