/*
    MODEL: stg_gold_prices
    DESCRIPTION: Normalizes Gold Prices from Yahoo Finance (GC=F).
*/

with raw as (
    -- Die Daten kommen nun über die YFinance Ingestion
    select * from {{ source('bronze', 'gold_prices_api') }}
)

select
    cast("Date" as date) as price_date,
    'USD' as currency,
    cast("Close" as double) as price_usd_per_oz,
    -- Umrechnung in Tonnen
    cast("Close" as double) * 32150.7 AS price_usd_per_ton
from raw
where "Close" is not null
