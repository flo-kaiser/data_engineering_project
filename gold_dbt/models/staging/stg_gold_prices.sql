/*
    MODEL: stg_gold_prices
    DESCRIPTION: Normalizes Gold Prices from World Bank API.
*/

with raw as (
    select * from {{ source('bronze', 'gold_prices_api') }}
)

select
    cast(period as date) as price_date,
    'USD' as currency,
    cast(value as double) as price_usd_per_oz,
    -- Hinzufügen der Tonnen-Konvertierung für fct_market_summary
    cast(value as double) * 32150.7 AS price_usd_per_ton
from raw
where value is not null
